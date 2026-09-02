package oddsockets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Client represents the main OddSockets client
type Client struct {
	config *Config
	state  ConnectionState
	userID string

	// Enhanced features (67 new Slack-like events)
	Enhanced *EnhancedFeatures

	// Live Socket.IO transport to the assigned worker
	socket *socketIO

	// Channels
	channels map[string]*Channel
	mu       sync.RWMutex

	// Event handling
	eventHandlers map[EventType][]EventHandler
	eventMu       sync.RWMutex

	// Connection management
	reconnectCount    int
	maxReconnectAttempts int
	reconnectDelay    time.Duration
	lastError         error

	// Worker assignment
	workerURL        string
	workerID         string
	clientIdentifier string
	sessionInfo      *SessionInfo

	// Minted-token auth (FEAT-2026-0824-0040). Populated only when the config
	// supplies a TokenProvider instead of an APIKey.
	token          string
	tokenExpiresAt int64 // epoch millis, 0 = unknown
	tokenRefreshTimer *time.Timer
	tokenMu        sync.Mutex

	// Manager discovery
	managerDiscovery *ManagerDiscovery

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Heartbeat
	heartbeatTicker *time.Ticker
	heartbeatDone   chan bool
}

// NewClient creates a new OddSockets client
func NewClient(config *Config) (*Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Validate required fields. Either an API key or a TokenProvider is
	// acceptable; a game client using minted tokens has neither an ak_ key nor
	// the ak_ prefix, so the format check only applies to key-mode.
	tokenMode := config.TokenProvider != nil
	if !tokenMode {
		if config.APIKey == "" {
			return nil, fmt.Errorf("either an API key or a TokenProvider is required")
		}
		if !strings.HasPrefix(config.APIKey, "ak_") {
			return nil, fmt.Errorf("invalid API key format")
		}
	}

	if config.TokenRefreshLeadMs == 0 {
		config.TokenRefreshLeadMs = 120000
	}

	// Resolve the manager endpoint up front so an invalid value fails here
	// rather than quietly sending traffic somewhere the caller did not ask for.
	managerDiscovery, err := NewManagerDiscovery(config.ManagerURL)
	if err != nil {
		return nil, err
	}
	config.ManagerURL = managerDiscovery.ManagerURL()

	if config.UserID == "" {
		config.UserID = fmt.Sprintf("user_%s", uuid.New().String()[:8])
	}

	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 30 * time.Second
	}

	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &Client{
		config:               config,
		state:                Disconnected,
		userID:               config.UserID,
		channels:             make(map[string]*Channel),
		eventHandlers:        make(map[EventType][]EventHandler),
		maxReconnectAttempts: 5,
		reconnectDelay:       1000 * time.Millisecond,
		managerDiscovery:     managerDiscovery,
		ctx:                  ctx,
		cancel:               cancel,
		heartbeatDone:        make(chan bool),
	}

	// Generate client identifier for session stickiness. In token mode there is
	// no API key to seed from, so fall back to a stable placeholder.
	identifierSeed := config.APIKey
	if identifierSeed == "" {
		identifierSeed = "token-client"
	}
	client.clientIdentifier = generateClientIdentifier(identifierSeed, config.UserID)

	// Initialize enhanced features (67 new Slack-like events)
	client.Enhanced = NewEnhancedFeatures(client)

	log.Printf("OddSockets client initialized for user: %s (client ID: %s)", client.userID, client.clientIdentifier)

	// Auto-connect if requested
	if config.AutoConnect {
		go func() {
			if err := client.Connect(context.Background()); err != nil {
				log.Printf("Auto-connect failed: %v", err)
				client.emitEvent(EventError, err)
			}
		}()
	}

	return client, nil
}

// Connect establishes connection to OddSockets platform
// Handles the Manager → Worker assignment internally
func (c *Client) Connect(ctx context.Context) error {
	if c.state == Connected {
		log.Println("Already connected")
		return nil
	}

	if c.state == Connecting {
		log.Println("Connection already in progress")
		return nil
	}

	c.setState(Connecting)
	c.emitEvent("connecting", nil)
	log.Println("Connecting to OddSockets...")

	// Step 0: In token mode, resolve a fresh minted token before every
	// (re)connect so both the manager select-worker call and the worker
	// handshake carry a valid token. (FEAT-2026-0824-0040)
	if c.isTokenMode() {
		if err := c.resolveToken(ctx); err != nil {
			c.setState(Disconnected)
			c.lastError = err
			c.emitEvent(EventError, err)
			if c.reconnectCount < c.maxReconnectAttempts {
				c.scheduleReconnect()
			} else {
				c.emitEvent("max_reconnect_attempts_reached", nil)
			}
			return err
		}
	}

	// Step 1: Get worker assignment from manager
	if err := c.getWorkerAssignment(ctx); err != nil {
		c.setState(Disconnected)
		c.lastError = err
		c.emitEvent(EventError, err)
		
		// Auto-reconnect with exponential backoff
		if c.reconnectCount < c.maxReconnectAttempts {
			c.scheduleReconnect()
		} else {
			c.emitEvent("max_reconnect_attempts_reached", nil)
		}
		return err
	}

	// Step 2: Connect to assigned worker
	if err := c.connectToWorker(ctx); err != nil {
		c.setState(Disconnected)
		c.lastError = err
		c.emitEvent(EventError, err)
		
		// Auto-reconnect with exponential backoff
		if c.reconnectCount < c.maxReconnectAttempts {
			c.scheduleReconnect()
		} else {
			c.emitEvent("max_reconnect_attempts_reached", nil)
		}
		return err
	}

	c.setState(Connected)
	c.reconnectCount = 0
	c.reconnectDelay = 1000 * time.Millisecond
	c.lastError = nil

	log.Println("Successfully connected to OddSockets")
	c.emitEvent(EventConnected, map[string]interface{}{
		"user_id":   c.userID,
		"timestamp": time.Now(),
	})

	return nil
}

// Disconnect closes the connection to OddSockets platform
func (c *Client) Disconnect() error {
	if c.state == Disconnected {
		log.Println("Already disconnected")
		return nil
	}

	log.Println("Disconnecting from OddSockets...")

	// Stop heartbeat
	c.stopHeartbeat()

	// Cancel any pending token refresh.
	c.tokenMu.Lock()
	if c.tokenRefreshTimer != nil {
		c.tokenRefreshTimer.Stop()
		c.tokenRefreshTimer = nil
	}
	c.tokenMu.Unlock()

	// Unsubscribe from all channels
	c.mu.RLock()
	channels := make([]*Channel, 0, len(c.channels))
	for _, ch := range c.channels {
		channels = append(channels, ch)
	}
	c.mu.RUnlock()

	for _, ch := range channels {
		ch.Unsubscribe()
	}

	// Close the live transport.
	if c.socket != nil {
		c.socket.close()
		c.socket = nil
	}

	c.setState(Disconnected)
	log.Println("Disconnected from OddSockets")
	c.emitEvent(EventDisconnected, map[string]interface{}{
		"user_id":   c.userID,
		"timestamp": time.Now(),
	})

	return nil
}

// Close closes the client and releases all resources
func (c *Client) Close() error {
	c.cancel()
	return c.Disconnect()
}

// Channel returns a channel instance for the given name
func (c *Client) Channel(name string) *Channel {
	if name == "" {
		panic("channel name is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Return existing channel if already created
	if ch, exists := c.channels[name]; exists {
		return ch
	}

	// Create new channel
	ch := newChannel(name, c)
	c.channels[name] = ch

	log.Printf("Created channel: %s", name)
	return ch
}

// IsConnected returns true if the client is connected
func (c *Client) IsConnected() bool {
	return c.state == Connected && c.socket != nil && c.socket.isConnected()
}

// GetConnectionState returns the current connection state
func (c *Client) GetConnectionState() ConnectionState {
	return c.state
}

// GetUserID returns the user ID
func (c *Client) GetUserID() string {
	return c.userID
}

// On adds an event listener
func (c *Client) On(eventType EventType, handler EventHandler) {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()

	c.eventHandlers[eventType] = append(c.eventHandlers[eventType], handler)
	log.Printf("Added listener for event: %s", eventType)
}

// Off removes event listeners
func (c *Client) Off(eventType EventType, handler EventHandler) {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()

	if handler == nil {
		// Remove all handlers for this event type
		delete(c.eventHandlers, eventType)
		log.Printf("Removed all listeners for event: %s", eventType)
		return
	}

	// Remove specific handler (this is tricky in Go, so we'll just clear all for now)
	// In a real implementation, you might use a different approach
	delete(c.eventHandlers, eventType)
	log.Printf("Removed listeners for event: %s", eventType)
}

// PublishBulk publishes multiple messages at once
func (c *Client) PublishBulk(ctx context.Context, messages []BulkMessage) ([]BulkResult, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	results := make([]BulkResult, len(messages))

	for i, msg := range messages {
		if msg.Channel == "" || msg.Message == nil {
			results[i] = BulkResult{
				Success: false,
				Error:   "missing channel or message",
			}
			continue
		}

		channel := c.Channel(msg.Channel)
		result, err := channel.Publish(ctx, msg.Message, nil)
		if err != nil {
			results[i] = BulkResult{
				Success: false,
				Error:   err.Error(),
			}
		} else {
			results[i] = BulkResult{
				Success: true,
				Result:  result,
			}
		}
	}

	return results, nil
}

// validateAPIKey validates the API key format
func (c *Client) validateAPIKey() bool {
	return len(c.config.APIKey) >= 20 && strings.HasPrefix(c.config.APIKey, "ak_")
}

// setState sets the connection state
func (c *Client) setState(state ConnectionState) {
	c.state = state
}

// startHeartbeat starts the heartbeat goroutine
func (c *Client) startHeartbeat() {
	if c.heartbeatTicker != nil {
		return
	}

	c.heartbeatTicker = time.NewTicker(c.config.HeartbeatInterval)
	log.Println("Started heartbeat")

	go func() {
		for {
			select {
			case <-c.heartbeatTicker.C:
				if c.state == Connected {
					log.Println("Sending heartbeat")
					// In real implementation, this would send a ping to the server
				}

			case <-c.heartbeatDone:
				return

			case <-c.ctx.Done():
				return
			}
		}
	}()
}

// stopHeartbeat stops the heartbeat goroutine
func (c *Client) stopHeartbeat() {
	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
		c.heartbeatTicker = nil
		close(c.heartbeatDone)
		c.heartbeatDone = make(chan bool)
		log.Println("Stopped heartbeat")
	}
}

// handleConnectionError handles connection errors and attempts reconnection
func (c *Client) handleConnectionError(err error) {
	log.Printf("Connection error: %v", err)

	if c.reconnectCount < c.config.ReconnectAttempts {
		c.setState(Reconnecting)
		c.reconnectCount++

		log.Printf("Attempting reconnection %d/%d", c.reconnectCount, c.config.ReconnectAttempts)

		// Exponential backoff
		backoff := time.Duration(1<<c.reconnectCount) * time.Second
		time.Sleep(backoff)

		if err := c.Connect(context.Background()); err == nil {
			log.Println("Reconnection successful")
			c.emitEvent(EventReconnected, map[string]interface{}{
				"attempt":   c.reconnectCount,
				"timestamp": time.Now(),
			})
		} else {
			log.Printf("Reconnection failed: %v", err)
			if c.reconnectCount >= c.config.ReconnectAttempts {
				c.setState(Failed)
				c.emitEvent(EventError, err)
			}
		}
	} else {
		c.setState(Failed)
		log.Println("Max reconnection attempts reached")
		c.emitEvent(EventError, err)
	}
}

// emitEvent emits an event to all registered handlers
func (c *Client) emitEvent(eventType EventType, data interface{}) {
	c.eventMu.RLock()
	handlers := c.eventHandlers[eventType]
	c.eventMu.RUnlock()

	for _, handler := range handlers {
		go func(h EventHandler) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Error in event handler for %s: %v", eventType, r)
				}
			}()
			h(eventType, data)
		}(handler)
	}
}

// getWorkerAssignment gets worker assignment from manager
func (c *Client) getWorkerAssignment(ctx context.Context) error {
	// Use the configured manager verbatim; there is no alternative endpoint to
	// fall back to if it is unreachable.
	managerURL, err := c.managerDiscovery.DiscoverManagerURL(c.config.APIKey)
	if err != nil {
		return fmt.Errorf("failed to resolve manager URL: %w", err)
	}

	// Build request URL
	reqURL, err := url.Parse(managerURL + "/api/cluster/select-worker")
	if err != nil {
		return fmt.Errorf("invalid manager URL: %w", err)
	}

	// Add query parameters. In token mode present the minted token instead of
	// an API key. (FEAT-2026-0824-0041)
	params := url.Values{}
	if c.isTokenMode() {
		params.Add("token", c.currentToken())
	} else {
		params.Add("apiKey", c.config.APIKey)
	}
	params.Add("userId", c.userID)
	params.Add("clientIdentifier", c.clientIdentifier)
	reqURL.RawQuery = params.Encode()

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", "OddSockets-Go-SDK/1.0.0")

	// Make HTTP request
	client := &http.Client{Timeout: c.config.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		// Classified on the error type rather than its wording, which changes
		// between Go releases and platforms. Either way the call fails: there is
		// no second manager to try.
		var opErr *net.OpError
		if errors.As(err, &opErr) && opErr.Op == "dial" {
			return fmt.Errorf("manager is offline. Cannot assign worker without session stickiness")
		}
		return fmt.Errorf("failed to get worker assignment: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("worker assignment failed: %s (status: %d)", string(body), resp.StatusCode)
	}

	// Parse response
	var assignment WorkerAssignment
	if err := json.NewDecoder(resp.Body).Decode(&assignment); err != nil {
		return fmt.Errorf("failed to parse worker assignment: %w", err)
	}

	if assignment.URL == "" {
		return fmt.Errorf("invalid worker assignment response")
	}

	c.workerURL = assignment.URL
	c.workerID = assignment.WorkerID
	c.sessionInfo = assignment.Session

	c.emitEvent("worker_assigned", map[string]interface{}{
		"workerId":         c.workerID,
		"workerUrl":        c.workerURL,
		"session":          c.sessionInfo,
		"clientIdentifier": c.clientIdentifier,
		"managerUrl":       managerURL,
	})

	log.Printf("Assigned to worker: %s (%s)", c.workerID, c.workerURL)
	return nil
}

// connectToWorker establishes the live Socket.IO connection to the assigned
// worker and wires the receive path (channel message delivery + enhanced
// broadcast forwarding onto the public event surface).
func (c *Client) connectToWorker(ctx context.Context) error {
	if c.workerURL == "" {
		return fmt.Errorf("no worker URL available")
	}

	// In token mode present the minted token in the handshake auth; otherwise
	// the API key. (FEAT-2026-0824-0039)
	auth := credentials{apiKey: c.config.APIKey}
	if c.isTokenMode() {
		auth = credentials{token: c.currentToken()}
	}

	socket, err := newSocketIO(c.workerURL, auth, c.userID)
	if err != nil {
		return err
	}

	if err := socket.connect(ctx, 15*time.Second); err != nil {
		return err
	}

	c.socket = socket
	c.setupReceivePath(socket)

	// Arm the ahead-of-expiry refresh once the live socket exists so the timer
	// can swap the token into the handshake in place.
	if c.isTokenMode() {
		c.scheduleTokenRefresh()
	}

	log.Printf("Connected to worker: %s", c.workerURL)
	c.startHeartbeat()
	return nil
}

// setupReceivePath routes inbound worker events. Channel message envelopes are
// delivered to the owning channel; enhanced-feature broadcasts are forwarded
// onto the client's public event surface so apps can listen with
// client.On("reaction_added", handler), etc. Correlated request/response acks
// (subscribed, published, presence, history) are consumed by the Channel
// methods directly and are intentionally not handled here.
func (c *Client) setupReceivePath(socket *socketIO) {
	socket.On("message", func(arg interface{}) {
		m, ok := arg.(map[string]interface{})
		if !ok {
			return
		}
		name, _ := m["channel"].(string)
		c.mu.RLock()
		ch := c.channels[name]
		c.mu.RUnlock()
		if ch != nil {
			ch.handleIncoming(m)
		}
	})

	for _, event := range enhancedBroadcastEvents {
		event := event
		socket.On(event, func(arg interface{}) {
			c.emitEvent(EventType(event), arg)
		})
	}
}

// enhancedBroadcastEvents are the enhanced (Slack-like) events the worker
// delivers to other members of a room. They are forwarded onto the client
// event surface so apps can subscribe with client.On(name, handler).
var enhancedBroadcastEvents = []string{
	"reaction_added", "reaction_removed",
	"user_typing", "user_stopped_typing",
	"user_read", "unread_count_updated", "all_marked_read",
	"thread_reply", "thread_subscribed", "thread_followed", "thread_unfollowed", "thread_read_updated",
	"message_edited", "message_deleted", "message_pinned", "message_unpinned",
	"user_status_changed", "custom_status_updated", "custom_status_cleared", "dnd_status_changed", "status_updated",
	"file_upload_completed", "file_upload_progress", "file_upload_failed",
	"dm_created", "dm_received",
	"notification", "notification_read", "all_notifications_read", "notifications_cleared",
	"channel_created", "channel_updated", "user_invited", "user_joined_channel", "user_left_channel", "user_removed",
	"challenge_progress", "leaderboard_rank_change", "challenge_complete", "achievement_unlock", "achievement_progress",
	"challenge_invited", "challenge_reply_received", "challenge_invite_cancelled",
}

// scheduleReconnect schedules reconnection with exponential backoff
func (c *Client) scheduleReconnect() {
	if c.state == Connected {
		return
	}

	c.setState(Reconnecting)
	c.reconnectCount++

	delay := time.Duration(c.reconnectDelay.Nanoseconds() * int64(1<<(c.reconnectCount-1)))
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}

	c.emitEvent("reconnecting", map[string]interface{}{
		"attempt":     c.reconnectCount,
		"maxAttempts": c.maxReconnectAttempts,
		"delay":       delay.Milliseconds(),
	})

	log.Printf("Scheduling reconnection attempt %d/%d in %v", c.reconnectCount, c.maxReconnectAttempts, delay)

	go func() {
		time.Sleep(delay)
		if c.state == Reconnecting {
			c.Connect(context.Background())
		}
	}()
}

// GetWorkerInfo returns assigned worker information
func (c *Client) GetWorkerInfo() map[string]interface{} {
	if c.workerID == "" || c.workerURL == "" {
		return nil
	}

	return map[string]interface{}{
		"workerId":  c.workerID,
		"workerUrl": c.workerURL,
	}
}

// GetClientIdentifier returns the client identifier used for session stickiness
func (c *Client) GetClientIdentifier() string {
	return c.clientIdentifier
}

// GetSessionInfo returns session information
func (c *Client) GetSessionInfo() *SessionInfo {
	return c.sessionInfo
}

// isTokenMode reports whether the client authenticates with minted tokens from
// a TokenProvider rather than a static API key. (FEAT-2026-0824-0040)
func (c *Client) isTokenMode() bool {
	return c.config.TokenProvider != nil
}

// currentToken returns the most recently resolved minted token.
func (c *Client) currentToken() string {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	return c.token
}

// resolveToken invokes the configured TokenProvider and caches the fresh token
// along with its computed expiry (epoch millis).
func (c *Client) resolveToken(ctx context.Context) error {
	tok, err := c.config.TokenProvider(ctx)
	if err != nil {
		return fmt.Errorf("token provider failed: %w", err)
	}
	if tok.Token == "" {
		return fmt.Errorf("token provider returned an empty token")
	}

	expMs := expiryFromToken(tok)

	c.tokenMu.Lock()
	c.token = tok.Token
	c.tokenExpiresAt = expMs
	c.tokenMu.Unlock()

	log.Printf("Resolved minted token (expires in %dms)", expMs-time.Now().UnixMilli())
	return nil
}

// scheduleTokenRefresh arms a one-shot timer to re-resolve the token ahead of
// its expiry, swap it into the live socket handshake auth, emit
// token_refreshed, then re-arm for the next cycle.
func (c *Client) scheduleTokenRefresh() {
	c.tokenMu.Lock()
	if c.tokenRefreshTimer != nil {
		c.tokenRefreshTimer.Stop()
		c.tokenRefreshTimer = nil
	}
	expMs := c.tokenExpiresAt
	c.tokenMu.Unlock()

	if expMs <= 0 {
		// Unknown expiry: cannot schedule an ahead-of-time refresh.
		return
	}

	delayMs := expMs - time.Now().UnixMilli() - int64(c.config.TokenRefreshLeadMs)
	if delayMs < 0 {
		delayMs = 0
	}

	timer := time.AfterFunc(time.Duration(delayMs)*time.Millisecond, func() {
		if c.state != Connected {
			return
		}
		if err := c.resolveToken(c.ctx); err != nil {
			log.Printf("Token refresh failed: %v", err)
			c.emitEvent(EventError, err)
			return
		}
		if c.socket != nil {
			c.socket.updateAuth(credentials{token: c.currentToken()})
		}
		c.emitEvent(EventType("token_refreshed"), map[string]interface{}{
			"expiresAt": c.tokenExpiresAt,
		})
		// Re-arm for the following cycle.
		c.scheduleTokenRefresh()
	})

	c.tokenMu.Lock()
	c.tokenRefreshTimer = timer
	c.tokenMu.Unlock()
}

// expiryFromToken derives the token expiry in epoch millis, preferring the
// explicit ExpiresAt / Exp fields and falling back to decoding the JWT.
func expiryFromToken(tok Token) int64 {
	if tok.ExpiresAt != "" {
		if ms := parseExpiresAt(tok.ExpiresAt); ms > 0 {
			return ms
		}
	}
	if tok.Exp > 0 {
		return tok.Exp * 1000
	}
	return expiryFromJwt(tok.Token)
}

// parseExpiresAt interprets an ExpiresAt value that may be a numeric epoch (in
// seconds or millis) or an ISO-8601 timestamp, returning epoch millis.
func parseExpiresAt(s string) int64 {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n < 1e12 {
			return n * 1000 // epoch seconds
		}
		return n // epoch millis
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UnixMilli()
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixMilli()
	}
	return 0
}

// expiryFromJwt base64url-decodes a JWT payload and returns its exp claim as
// epoch millis, or 0 when the token is not a decodable JWT.
func expiryFromJwt(token string) int64 {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return 0
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		if payload, err = base64.URLEncoding.DecodeString(parts[1]); err != nil {
			return 0
		}
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == 0 {
		return 0
	}
	return claims.Exp * 1000
}
