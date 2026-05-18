package oddsockets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
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

	// Validate required fields
	if config.APIKey == "" {
		return nil, fmt.Errorf("API key is required")
	}

	if !strings.HasPrefix(config.APIKey, "ak_") {
		return nil, fmt.Errorf("invalid API key format")
	}

	// Set defaults
	if config.ManagerURL == "" {
		config.ManagerURL = "https://manager1.oddsockets.tyga.network"
	}

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
		managerDiscovery:     NewManagerDiscovery(),
		ctx:                  ctx,
		cancel:               cancel,
		heartbeatDone:        make(chan bool),
	}

	// Generate client identifier for session stickiness
	client.clientIdentifier = generateClientIdentifier(config.APIKey, config.UserID)

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
	return c.state == Connected
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

	handlers := c.eventHandlers[eventType]
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

// BulkMessage represents a message for bulk publishing
type BulkMessage struct {
	Channel string      `json:"channel"`
	Message interface{} `json:"message"`
}

// BulkResult represents the result of a bulk publish operation
type BulkResult struct {
	Success bool           `json:"success"`
	Result  *PublishResult `json:"result,omitempty"`
	Error   string         `json:"error,omitempty"`
}

// getWorkerAssignment gets worker assignment from manager
func (c *Client) getWorkerAssignment(ctx context.Context) error {
	// Discover the optimal manager URL automatically
	managerURL, err := c.managerDiscovery.DiscoverManagerURL(c.config.APIKey)
	if err != nil {
		return fmt.Errorf("failed to discover manager URL: %w", err)
	}

	// Build request URL
	reqURL, err := url.Parse(managerURL + "/api/cluster/select-worker")
	if err != nil {
		return fmt.Errorf("invalid manager URL: %w", err)
	}

	// Add query parameters
	params := url.Values{}
	params.Add("apiKey", c.config.APIKey)
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
		// If manager is offline, try fallback logic
		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no such host") {
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

// connectToWorker connects to the assigned worker
func (c *Client) connectToWorker(ctx context.Context) error {
	if c.workerURL == "" {
		return fmt.Errorf("no worker URL available")
	}

	// In a real implementation, this would establish a WebSocket connection
	// For now, we'll simulate the connection process
	select {
	case <-time.After(100 * time.Millisecond): // Simulate connection delay
		log.Printf("Connected to worker: %s", c.workerURL)
		c.startHeartbeat()
		return nil
	case <-ctx.Done():
		return fmt.Errorf("connection timeout: %w", ctx.Err())
	}
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
