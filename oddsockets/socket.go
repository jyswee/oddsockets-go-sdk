package oddsockets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// credentials are the auth values presented in the Socket.IO CONNECT
// handshake. Exactly one of token (minted realtime token, FEAT-2026-0824-0040)
// or apiKey is used; token takes precedence when set.
type credentials struct {
	apiKey string
	token  string
}

// socketHandler is a single registered listener for a Socket.IO event.
type socketHandler struct {
	id    uint64
	once  bool
	match func(interface{}) bool
	fn    func(interface{})
}

// socketIO is a minimal Socket.IO (Engine.IO v4) client over a single
// WebSocket connection. It speaks the worker's wire protocol directly:
// Engine.IO framing (OPEN/PING/PONG/MESSAGE) wrapping Socket.IO packets
// (CONNECT/EVENT). Auth is delivered in the Socket.IO CONNECT handshake so
// it lands in the worker's handshake.auth, exactly like socket.io-client.
type socketIO struct {
	wsURL  string
	userID string

	authMu sync.Mutex
	auth   credentials

	conn    *websocket.Conn
	writeMu sync.Mutex

	hmu      sync.Mutex
	handlers map[string][]*socketHandler
	nextID   uint64

	anyMu  sync.Mutex
	anyFns []func(event string, arg interface{})

	connectedCh chan struct{}
	connectOnce sync.Once
	closeCh     chan struct{}
	closeOnce   sync.Once
	connected   int32
}

// newSocketIO builds a socket for the given worker URL. The worker URL is an
// http(s) origin; it is rewritten to the ws(s) Socket.IO endpoint.
func newSocketIO(workerURL string, auth credentials, userID string) (*socketIO, error) {
	u, err := url.Parse(workerURL)
	if err != nil {
		return nil, fmt.Errorf("invalid worker URL: %w", err)
	}

	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	}

	u.Path = strings.TrimRight(u.Path, "/") + "/socket.io/"
	q := u.Query()
	q.Set("EIO", "4")
	q.Set("transport", "websocket")
	u.RawQuery = q.Encode()

	return &socketIO{
		wsURL:       u.String(),
		auth:        auth,
		userID:      userID,
		handlers:    make(map[string][]*socketHandler),
		connectedCh: make(chan struct{}),
		closeCh:     make(chan struct{}),
	}, nil
}

// updateAuth swaps the handshake credentials in place. A refreshed minted token
// installed here is carried by the next transport (re)connect; the live
// connection is not forcibly torn down. (FEAT-2026-0824-0040)
func (s *socketIO) updateAuth(c credentials) {
	s.authMu.Lock()
	s.auth = c
	s.authMu.Unlock()
}

// connect dials the worker and blocks until the Socket.IO CONNECT handshake
// completes (or the timeout / context fires).
func (s *socketIO) connect(ctx context.Context, timeout time.Duration) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, s.wsURL, nil)
	if err != nil {
		return fmt.Errorf("websocket dial failed: %w", err)
	}
	s.conn = conn

	go s.readLoop()

	select {
	case <-s.connectedCh:
		atomic.StoreInt32(&s.connected, 1)
		return nil
	case <-time.After(timeout):
		s.close()
		return fmt.Errorf("timed out waiting for socket.io connect")
	case <-ctx.Done():
		s.close()
		return ctx.Err()
	}
}

// readLoop consumes Engine.IO frames until the socket closes.
func (s *socketIO) readLoop() {
	defer s.close()

	for {
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			return
		}
		if len(data) == 0 {
			continue
		}

		switch data[0] {
		case '0': // Engine.IO OPEN: reply with Socket.IO CONNECT + auth.
			s.authMu.Lock()
			cred := s.auth
			s.authMu.Unlock()
			auth := map[string]string{}
			if cred.token != "" {
				auth["token"] = cred.token
			} else {
				auth["apiKey"] = cred.apiKey
			}
			if s.userID != "" {
				auth["userId"] = s.userID
			}
			b, _ := json.Marshal(auth)
			_ = s.writeRaw("40" + string(b))
		case '1': // Engine.IO CLOSE
			return
		case '2': // Engine.IO PING
			_ = s.writeRaw("3")
		case '4': // Engine.IO MESSAGE -> Socket.IO packet
			s.handleSocketIO(data[1:])
		}
	}
}

// handleSocketIO dispatches a Socket.IO packet (the byte after Engine.IO '4').
func (s *socketIO) handleSocketIO(payload []byte) {
	if len(payload) == 0 {
		return
	}

	switch payload[0] {
	case '0': // CONNECT ack (e.g. 0{"sid":"..."})
		s.connectOnce.Do(func() { close(s.connectedCh) })
	case '2': // EVENT
		s.handleEvent(payload[1:])
	case '4': // CONNECT_ERROR
		var arg interface{}
		if idx := bytes.IndexByte(payload, '{'); idx >= 0 {
			_ = json.Unmarshal(payload[idx:], &arg)
		}
		s.dispatch("error", arg)
	}
}

// handleEvent decodes an EVENT array ["name", arg] and dispatches it. A
// numeric ack id may precede the JSON array; the payload is located by the
// first '['.
func (s *socketIO) handleEvent(b []byte) {
	idx := bytes.IndexByte(b, '[')
	if idx < 0 {
		return
	}

	var arr []json.RawMessage
	if err := json.Unmarshal(b[idx:], &arr); err != nil || len(arr) == 0 {
		return
	}

	var event string
	if err := json.Unmarshal(arr[0], &event); err != nil {
		return
	}

	var arg interface{}
	if len(arr) > 1 {
		_ = json.Unmarshal(arr[1], &arg)
	}

	s.dispatch(event, arg)
}

// dispatch fires matching handlers (removing satisfied once-handlers) and all
// catch-all (OnAny) forwarders for an event.
func (s *socketIO) dispatch(event string, arg interface{}) {
	s.hmu.Lock()
	hs := s.handlers[event]
	var toCall []*socketHandler
	remaining := make([]*socketHandler, 0, len(hs))
	for _, h := range hs {
		if h.match == nil || h.match(arg) {
			toCall = append(toCall, h)
			if !h.once {
				remaining = append(remaining, h)
			}
		} else {
			remaining = append(remaining, h)
		}
	}
	if len(remaining) == 0 {
		delete(s.handlers, event)
	} else {
		s.handlers[event] = remaining
	}
	s.hmu.Unlock()

	for _, h := range toCall {
		go safeCall(h.fn, arg)
	}

	s.anyMu.Lock()
	anyFns := append([]func(string, interface{}){}, s.anyFns...)
	s.anyMu.Unlock()
	for _, fn := range anyFns {
		go func(f func(string, interface{})) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("socket OnAny handler panic: %v", r)
				}
			}()
			f(event, arg)
		}(fn)
	}
}

// On registers a persistent listener for an event.
func (s *socketIO) On(event string, fn func(interface{})) uint64 {
	return s.add(event, false, nil, fn)
}

// Once registers a one-shot listener for an event.
func (s *socketIO) Once(event string, fn func(interface{})) uint64 {
	return s.add(event, true, nil, fn)
}

func (s *socketIO) add(event string, once bool, match func(interface{}) bool, fn func(interface{})) uint64 {
	s.hmu.Lock()
	defer s.hmu.Unlock()
	s.nextID++
	id := s.nextID
	s.handlers[event] = append(s.handlers[event], &socketHandler{
		id:    id,
		once:  once,
		match: match,
		fn:    fn,
	})
	return id
}

// Off removes a previously registered listener by id.
func (s *socketIO) Off(event string, id uint64) {
	s.hmu.Lock()
	defer s.hmu.Unlock()
	hs := s.handlers[event]
	out := hs[:0]
	for _, h := range hs {
		if h.id != id {
			out = append(out, h)
		}
	}
	if len(out) == 0 {
		delete(s.handlers, event)
	} else {
		s.handlers[event] = out
	}
}

// OnAny registers a catch-all forwarder invoked for every event.
func (s *socketIO) OnAny(fn func(event string, arg interface{})) {
	s.anyMu.Lock()
	s.anyFns = append(s.anyFns, fn)
	s.anyMu.Unlock()
}

// Emit sends an EVENT to the worker. Null-valued object fields are pruned so
// the worker's option destructuring (which only handles `undefined`) is not
// tripped by explicit JSON nulls.
func (s *socketIO) Emit(event string, data interface{}) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	raw = pruneNullFields(raw)

	eventJSON, _ := json.Marshal(event)

	var buf bytes.Buffer
	buf.WriteString("42[")
	buf.Write(eventJSON)
	if len(raw) > 0 && string(raw) != "null" {
		buf.WriteByte(',')
		buf.Write(raw)
	}
	buf.WriteByte(']')

	return s.writeRaw(buf.String())
}

// request emits an event and waits for a correlated response, matching the
// response payload with match (typically on the channel name). An "error"
// event from the worker fails the request.
func (s *socketIO) request(emitEvent string, data interface{}, respEvent string, match func(map[string]interface{}) bool, timeout time.Duration) (map[string]interface{}, error) {
	resCh := make(chan map[string]interface{}, 1)
	errCh := make(chan error, 1)

	rid := s.add(respEvent, true, func(arg interface{}) bool {
		m, ok := arg.(map[string]interface{})
		return ok && (match == nil || match(m))
	}, func(arg interface{}) {
		if m, ok := arg.(map[string]interface{}); ok {
			select {
			case resCh <- m:
			default:
			}
		}
	})

	eid := s.add("error", true, nil, func(arg interface{}) {
		select {
		case errCh <- fmt.Errorf("%v", extractErrorMessage(arg)):
		default:
		}
	})

	cleanup := func() {
		s.Off(respEvent, rid)
		s.Off("error", eid)
	}

	if err := s.Emit(emitEvent, data); err != nil {
		cleanup()
		return nil, err
	}
	defer cleanup()

	select {
	case m := <-resCh:
		return m, nil
	case err := <-errCh:
		return nil, err
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for %q response", respEvent)
	case <-s.closeCh:
		return nil, fmt.Errorf("socket closed while waiting for %q", respEvent)
	}
}

func (s *socketIO) writeRaw(msg string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.conn == nil {
		return fmt.Errorf("socket not connected")
	}
	return s.conn.WriteMessage(websocket.TextMessage, []byte(msg))
}

func (s *socketIO) isConnected() bool {
	return atomic.LoadInt32(&s.connected) == 1
}

// close tears down the socket, best-effort sending a Socket.IO DISCONNECT.
func (s *socketIO) close() {
	s.closeOnce.Do(func() {
		atomic.StoreInt32(&s.connected, 0)
		close(s.closeCh)
		if s.conn != nil {
			s.writeMu.Lock()
			_ = s.conn.WriteMessage(websocket.TextMessage, []byte("41"))
			_ = s.conn.Close()
			s.writeMu.Unlock()
		}
	})
}

func safeCall(fn func(interface{}), arg interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("socket handler panic: %v", r)
		}
	}()
	fn(arg)
}

// pruneNullFields removes null-valued keys from a top-level JSON object.
func pruneNullFields(raw json.RawMessage) json.RawMessage {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return raw
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &m); err != nil {
		return raw
	}

	changed := false
	for k, v := range m {
		if string(bytes.TrimSpace(v)) == "null" {
			delete(m, k)
			changed = true
		}
	}
	if !changed {
		return raw
	}

	out, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return out
}

// extractErrorMessage pulls a human-readable message out of a worker error
// payload, falling back to the raw value.
func extractErrorMessage(arg interface{}) interface{} {
	if m, ok := arg.(map[string]interface{}); ok {
		if msg, ok := m["message"]; ok {
			return msg
		}
		if t, ok := m["type"]; ok {
			return t
		}
	}
	return arg
}
