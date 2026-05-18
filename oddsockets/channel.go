package oddsockets

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
				"This limit matches industry standards (PubNub, Socket.IO) for reliable real-time messaging",
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
	subscribed      bool
	messageChan     chan *Message
	subscribeOpts   *SubscribeOptions
	messageHistory  []*Message
	presenceUsers   []string
	mu              sync.RWMutex

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

// Subscribe subscribes to messages on this channel
func (ch *Channel) Subscribe(ctx context.Context, messageChan chan *Message, options *SubscribeOptions) error {
	if !ch.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	if messageChan == nil {
		return fmt.Errorf("message channel is required")
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.subscribed {
		log.Printf("Channel '%s' already subscribed", ch.name)
		return nil
	}

	// Store subscription details
	ch.messageChan = messageChan
	ch.subscribeOpts = options
	if ch.subscribeOpts == nil {
		ch.subscribeOpts = &SubscribeOptions{}
	}

	// Simulate subscription process
	select {
	case <-time.After(50 * time.Millisecond): // Simulate network delay
		ch.subscribed = true
		log.Printf("Subscribed to channel: %s", ch.name)

		// If presence is enabled, add current user
		if ch.subscribeOpts.EnablePresence {
			ch.presenceUsers = append(ch.presenceUsers, ch.client.GetUserID())
		}

		// Start message handling goroutine
		go ch.handleMessages(ctx)

		// Simulate receiving initial messages
		go ch.simulateInitialMessages()

		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unsubscribe unsubscribes from messages on this channel
func (ch *Channel) Unsubscribe() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.subscribed {
		log.Printf("Channel '%s' not subscribed", ch.name)
		return nil
	}

	// Simulate unsubscription process
	time.Sleep(50 * time.Millisecond) // Simulate network delay

	ch.subscribed = false
	ch.messageChan = nil
	ch.subscribeOpts = nil

	// Remove from presence
	userID := ch.client.GetUserID()
	for i, user := range ch.presenceUsers {
		if user == userID {
			ch.presenceUsers = append(ch.presenceUsers[:i], ch.presenceUsers[i+1:]...)
			break
		}
	}

	ch.cancel()
	log.Printf("Unsubscribed from channel: %s", ch.name)

	return nil
}

// Publish publishes a message to this channel
func (ch *Channel) Publish(ctx context.Context, message interface{}, options *PublishOptions) (*PublishResult, error) {
	if !ch.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	// Validate message size before publishing
	if err := validateMessageSize(message); err != nil {
		return nil, err
	}

	// Create message object
	msg := &Message{
		ID:        fmt.Sprintf("msg_%s", uuid.New().String()[:12]),
		Channel:   ch.name,
		Data:      message,
		Timestamp: time.Now(),
		UserID:    ch.client.GetUserID(),
	}

	if options != nil && options.Metadata != nil {
		msg.Metadata = options.Metadata
	}

	// Simulate publishing process
	select {
	case <-time.After(20 * time.Millisecond): // Simulate network delay
		// Store in history if requested
		ch.mu.Lock()
		if (options != nil && options.StoreInHistory) ||
			(ch.subscribeOpts != nil && ch.subscribeOpts.RetainHistory) {
			ch.messageHistory = append(ch.messageHistory, msg)
			// Keep only last 100 messages
			if len(ch.messageHistory) > 100 {
				ch.messageHistory = ch.messageHistory[len(ch.messageHistory)-100:]
			}
		}

		// Deliver to local subscriber if subscribed
		if ch.subscribed && ch.messageChan != nil {
			go ch.deliverMessage(msg)
		}
		ch.mu.Unlock()

		log.Printf("Published message to channel '%s': %v", ch.name, message)

		return &PublishResult{
			MessageID: msg.ID,
			Timestamp: msg.Timestamp,
			Channel:   ch.name,
			Success:   true,
		}, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetHistory retrieves message history for this channel
func (ch *Channel) GetHistory(ctx context.Context, options *HistoryOptions) ([]*Message, error) {
	if !ch.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	// Simulate API call delay
	select {
	case <-time.After(100 * time.Millisecond):
		ch.mu.RLock()
		messages := make([]*Message, len(ch.messageHistory))
		copy(messages, ch.messageHistory)
		ch.mu.RUnlock()

		// Apply filters if specified
		if options != nil {
			if options.Start != nil {
				filtered := make([]*Message, 0)
				for _, msg := range messages {
					if msg.Timestamp.After(*options.Start) || msg.Timestamp.Equal(*options.Start) {
						filtered = append(filtered, msg)
					}
				}
				messages = filtered
			}

			if options.End != nil {
				filtered := make([]*Message, 0)
				for _, msg := range messages {
					if msg.Timestamp.Before(*options.End) || msg.Timestamp.Equal(*options.End) {
						filtered = append(filtered, msg)
					}
				}
				messages = filtered
			}

			// Apply limit
			if options.Limit > 0 && len(messages) > options.Limit {
				if options.Reverse {
					// Take last N messages
					messages = messages[len(messages)-options.Limit:]
				} else {
					// Take first N messages
					messages = messages[:options.Limit]
				}
			}

			// Reverse if requested
			if options.Reverse {
				for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
					messages[i], messages[j] = messages[j], messages[i]
				}
			}
		}

		log.Printf("Retrieved %d messages from channel '%s' history", len(messages), ch.name)
		return messages, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetPresence retrieves presence information for this channel
func (ch *Channel) GetPresence(ctx context.Context) (*PresenceInfo, error) {
	if !ch.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	// Simulate API call delay
	select {
	case <-time.After(50 * time.Millisecond):
		ch.mu.RLock()
		users := make([]string, len(ch.presenceUsers))
		copy(users, ch.presenceUsers)
		ch.mu.RUnlock()

		presence := &PresenceInfo{
			Channel:   ch.name,
			Users:     users,
			Count:     len(users),
			Timestamp: time.Now(),
		}

		log.Printf("Retrieved presence for channel '%s': %d users", ch.name, presence.Count)
		return presence, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
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

// handleMessages handles incoming messages in a goroutine
func (ch *Channel) handleMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch.ctx.Done():
			return
		default:
			// In a real implementation, this would listen for incoming messages
			// from the WebSocket connection
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// deliverMessage delivers a message to the subscriber
func (ch *Channel) deliverMessage(message *Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Error delivering message: %v", r)
		}
	}()

	// Apply filter if specified
	if ch.subscribeOpts != nil && ch.subscribeOpts.FilterExpression != "" {
		if !ch.evaluateFilter(message, ch.subscribeOpts.FilterExpression) {
			return
		}
	}

	// Deliver message to channel (non-blocking)
	select {
	case ch.messageChan <- message:
		// Message delivered successfully
	default:
		// Channel is full, log warning
		log.Printf("Warning: message channel full for channel '%s'", ch.name)
	}
}

// evaluateFilter evaluates a filter expression against a message
func (ch *Channel) evaluateFilter(message *Message, filterExpr string) bool {
	// Simple filter evaluation (in real SDK, this would be more sophisticated)
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Error evaluating filter: %v", r)
		}
	}()

	// For demo purposes, just check if filter string is in message data
	messageBytes, err := json.Marshal(message.Data)
	if err != nil {
		return true // If we can't marshal, pass the message
	}

	messageStr := strings.ToLower(string(messageBytes))
	filterStr := strings.ToLower(filterExpr)

	return strings.Contains(messageStr, filterStr)
}

// simulateInitialMessages simulates receiving some initial messages for demo purposes
func (ch *Channel) simulateInitialMessages() {
	time.Sleep(100 * time.Millisecond) // Wait a bit

	ch.mu.RLock()
	if !ch.subscribed || ch.messageChan == nil {
		ch.mu.RUnlock()
		return
	}
	ch.mu.RUnlock()

	// Create a welcome message
	welcomeMessage := &Message{
		ID:      fmt.Sprintf("msg_%s", uuid.New().String()[:12]),
		Channel: ch.name,
		Data: map[string]interface{}{
			"type":      "system",
			"text":      fmt.Sprintf("Welcome to channel '%s'!", ch.name),
			"timestamp": time.Now().Format(time.RFC3339),
		},
		Timestamp: time.Now(),
		UserID:    "system",
	}

	// Deliver the welcome message
	ch.deliverMessage(welcomeMessage)

	// Store in history if enabled
	ch.mu.Lock()
	if ch.subscribeOpts != nil && ch.subscribeOpts.RetainHistory {
		ch.messageHistory = append(ch.messageHistory, welcomeMessage)
	}
	ch.mu.Unlock()
}
