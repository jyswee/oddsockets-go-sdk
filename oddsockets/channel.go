package oddsockets

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// validateMessageSize validates message size against limits
func validateMessageSize(message interface{}) error {
	var messageStr string
	if str, ok := message.(string); ok {
		messageStr = str
	} else {
		messageBytes, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to serialize message: %w", err)
		}
		messageStr = string(messageBytes)
	}

	messageSize := len([]byte(messageStr))

	if messageSize > MaxMessageSize {
		return fmt.Errorf(
			"message size (%dKB) exceeds maximum allowed size of %dKB. "+
				"This limit matches industry standards for reliable real-time messaging",
			messageSize/1024, MaxMessageSizeKB,
		)
	}

	return nil
}

// Channel represents a messaging channel
type Channel struct {
	name   string
	client *Client

	// Subscription state
	subscribed     bool
	messageChan    chan *Message
	subscribeOpts  *SubscribeOptions
	messageHistory []*Message
	presenceUsers  []string
	mu             sync.RWMutex

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc
}

// newChannel creates a new channel instance
func newChannel(name string, client *Client) *Channel {
	ctx, cancel := context.WithCancel(context.Background())

	return &Channel{
		name:           name,
		client:         client,
		subscribed:     false,
		messageHistory: make([]*Message, 0),
		presenceUsers:  make([]string, 0),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// channelMatch reports whether a worker response payload targets this channel.
func (ch *Channel) channelMatch(m map[string]interface{}) bool {
	name, _ := m["channel"].(string)
	return name == ch.name
}

// Subscribe subscribes to messages on this channel via the live worker.
func (ch *Channel) Subscribe(ctx context.Context, messageChan chan *Message, options *SubscribeOptions) error {
	if !ch.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	if messageChan == nil {
		return fmt.Errorf("message channel is required")
	}

	ch.mu.Lock()
	if ch.subscribed {
		ch.mu.Unlock()
		log.Printf("Channel '%s' already subscribed", ch.name)
		return nil
	}
	ch.messageChan = messageChan
	ch.subscribeOpts = options
	if ch.subscribeOpts == nil {
		ch.subscribeOpts = &SubscribeOptions{}
	}
	opts := ch.subscribeOpts
	ch.mu.Unlock()

	payload := map[string]interface{}{
		"channel": ch.name,
		"options": subscribeOptionsPayload(opts),
	}

	if _, err := ch.client.socket.request("subscribe", payload, "subscribed", ch.channelMatch, 10*time.Second); err != nil {
		return err
	}

	ch.mu.Lock()
	ch.subscribed = true
	if opts.EnablePresence {
		ch.presenceUsers = appendUnique(ch.presenceUsers, ch.client.GetUserID())
	}
	ch.mu.Unlock()

	log.Printf("Subscribed to channel: %s", ch.name)
	return nil
}

// Unsubscribe unsubscribes from messages on this channel.
func (ch *Channel) Unsubscribe() error {
	ch.mu.Lock()
	subscribed := ch.subscribed
	ch.mu.Unlock()

	if !subscribed {
		log.Printf("Channel '%s' not subscribed", ch.name)
		return nil
	}

	if ch.client.socket != nil {
		if _, err := ch.client.socket.request("unsubscribe", map[string]interface{}{
			"channel": ch.name,
		}, "unsubscribed", ch.channelMatch, 5*time.Second); err != nil {
			log.Printf("Unsubscribe for '%s' did not confirm: %v", ch.name, err)
		}
	}

	ch.mu.Lock()
	ch.subscribed = false
	ch.messageChan = nil
	userID := ch.client.GetUserID()
	for i, user := range ch.presenceUsers {
		if user == userID {
			ch.presenceUsers = append(ch.presenceUsers[:i], ch.presenceUsers[i+1:]...)
			break
		}
	}
	ch.mu.Unlock()

	ch.cancel()
	log.Printf("Unsubscribed from channel: %s", ch.name)
	return nil
}

// Publish publishes a message to this channel and returns the worker ack.
func (ch *Channel) Publish(ctx context.Context, message interface{}, options *PublishOptions) (*PublishResult, error) {
	if !ch.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	if err := validateMessageSize(message); err != nil {
		return nil, err
	}

	pubOpts := map[string]interface{}{}
	if options != nil {
		if options.TTL > 0 {
			pubOpts["ttl"] = options.TTL
		}
		if options.Metadata != nil {
			pubOpts["metadata"] = options.Metadata
		}
		if options.StoreInHistory {
			pubOpts["storeInHistory"] = true
		}
	}

	payload := map[string]interface{}{
		"channel": ch.name,
		"message": message,
		"options": pubOpts,
	}

	resp, err := ch.client.socket.request("publish", payload, "published", ch.channelMatch, 10*time.Second)
	if err != nil {
		return nil, err
	}

	messageID := stringField(resp, "messageId")
	if messageID == "" {
		messageID = stringField(resp, "message_id")
	}

	log.Printf("Published message to channel '%s'", ch.name)
	return &PublishResult{
		MessageID: messageID,
		Timestamp: time.Now(),
		Channel:   ch.name,
		Success:   true,
	}, nil
}

// GetHistory retrieves message history for this channel from the worker.
func (ch *Channel) GetHistory(ctx context.Context, options *HistoryOptions) ([]*Message, error) {
	if !ch.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	payload := map[string]interface{}{"channel": ch.name}
	if options != nil {
		if options.Limit > 0 {
			payload["count"] = options.Limit
		}
		if options.Start != nil {
			payload["start"] = options.Start.Format(time.RFC3339)
		}
		if options.End != nil {
			payload["end"] = options.End.Format(time.RFC3339)
		}
	}

	resp, err := ch.client.socket.request("get_history", payload, "history", ch.channelMatch, 10*time.Second)
	if err != nil {
		return nil, err
	}

	messages := parseMessages(resp, ch.name)
	log.Printf("Retrieved %d messages from channel '%s' history", len(messages), ch.name)
	return messages, nil
}

// GetPresence retrieves presence information for this channel from the worker.
func (ch *Channel) GetPresence(ctx context.Context) (*PresenceInfo, error) {
	if !ch.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	resp, err := ch.client.socket.request("get_presence", map[string]interface{}{
		"channel": ch.name,
	}, "presence", ch.channelMatch, 5*time.Second)
	if err != nil {
		return nil, err
	}

	users, count := parsePresence(resp)
	presence := &PresenceInfo{
		Channel:   ch.name,
		Users:     users,
		Count:     count,
		Timestamp: time.Now(),
	}

	log.Printf("Retrieved presence for channel '%s': %d users", ch.name, presence.Count)
	return presence, nil
}

// IsSubscribed returns true if the channel is subscribed
func (ch *Channel) IsSubscribed() bool {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return ch.subscribed
}

// GetName returns the channel name
func (ch *Channel) GetName() string {
	return ch.name
}

// handleIncoming routes a worker message envelope to the subscriber and,
// when enabled, retains it in local history.
func (ch *Channel) handleIncoming(envelope map[string]interface{}) {
	msg := envelopeToMessage(envelope, ch.name)

	ch.mu.RLock()
	subscribed := ch.subscribed
	hasChan := ch.messageChan != nil
	retain := ch.subscribeOpts != nil && ch.subscribeOpts.RetainHistory
	ch.mu.RUnlock()

	if retain {
		ch.mu.Lock()
		ch.messageHistory = append(ch.messageHistory, msg)
		if len(ch.messageHistory) > 100 {
			ch.messageHistory = ch.messageHistory[len(ch.messageHistory)-100:]
		}
		ch.mu.Unlock()
	}

	if subscribed && hasChan {
		ch.deliverMessage(msg)
	}
}

// deliverMessage delivers a message to the subscriber channel (non-blocking).
func (ch *Channel) deliverMessage(message *Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Error delivering message: %v", r)
		}
	}()

	ch.mu.RLock()
	filter := ""
	if ch.subscribeOpts != nil {
		filter = ch.subscribeOpts.FilterExpression
	}
	target := ch.messageChan
	ch.mu.RUnlock()

	if filter != "" && !ch.evaluateFilter(message, filter) {
		return
	}

	if target == nil {
		return
	}

	select {
	case target <- message:
	default:
		log.Printf("Warning: message channel full for channel '%s'", ch.name)
	}
}

// evaluateFilter evaluates a simple substring filter against a message.
func (ch *Channel) evaluateFilter(message *Message, filterExpr string) bool {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Error evaluating filter: %v", r)
		}
	}()

	messageBytes, err := json.Marshal(message.Data)
	if err != nil {
		return true
	}

	messageStr := strings.ToLower(string(messageBytes))
	filterStr := strings.ToLower(filterExpr)

	return strings.Contains(messageStr, filterStr)
}

// subscribeOptionsPayload converts SubscribeOptions to the worker's camelCase
// wire shape.
func subscribeOptionsPayload(opts *SubscribeOptions) map[string]interface{} {
	p := map[string]interface{}{
		"enablePresence": opts.EnablePresence,
		"retainHistory":  opts.RetainHistory,
	}
	if opts.FilterExpression != "" {
		p["filterExpression"] = opts.FilterExpression
	}
	return p
}

// envelopeToMessage adapts a worker "message" envelope to a Message.
func envelopeToMessage(env map[string]interface{}, channelName string) *Message {
	msg := &Message{Channel: channelName, Timestamp: time.Now()}

	if id := stringField(env, "id"); id != "" {
		msg.ID = id
	} else if id := stringField(env, "messageId"); id != "" {
		msg.ID = id
	}

	if inner, ok := env["message"]; ok {
		msg.Data = inner
	} else if data, ok := env["data"]; ok {
		msg.Data = data
	}

	if pub, ok := env["publisher"].(map[string]interface{}); ok {
		msg.UserID = stringField(pub, "userId")
	} else if uid := stringField(env, "userId"); uid != "" {
		msg.UserID = uid
	}

	if md, ok := env["metadata"].(map[string]interface{}); ok {
		msg.Metadata = md
	}

	if name := stringField(env, "channel"); name != "" {
		msg.Channel = name
	}

	return msg
}

// parseMessages extracts a slice of Messages from a "history" response.
func parseMessages(resp map[string]interface{}, channelName string) []*Message {
	raw, ok := resp["messages"].([]interface{})
	if !ok {
		return []*Message{}
	}

	messages := make([]*Message, 0, len(raw))
	for _, item := range raw {
		if env, ok := item.(map[string]interface{}); ok {
			messages = append(messages, envelopeToMessage(env, channelName))
		}
	}
	return messages
}

// parsePresence extracts user ids and occupancy from a "presence" response.
func parsePresence(resp map[string]interface{}) ([]string, int) {
	users := make([]string, 0)

	if occupants, ok := resp["occupants"].([]interface{}); ok {
		for _, o := range occupants {
			if m, ok := o.(map[string]interface{}); ok {
				if uid := stringField(m, "userId"); uid != "" {
					users = append(users, uid)
				}
			} else if s, ok := o.(string); ok {
				users = append(users, s)
			}
		}
	}

	count := len(users)
	if occ, ok := resp["occupancy"].(float64); ok {
		count = int(occ)
	}

	return users, count
}

// stringField returns m[key] as a string, or "" if absent/not a string.
func stringField(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// appendUnique appends value to slice only if not already present.
func appendUnique(slice []string, value string) []string {
	for _, s := range slice {
		if s == value {
			return slice
		}
	}
	return append(slice, value)
}
