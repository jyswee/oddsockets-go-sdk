package oddsockets

import (
	"time"
)

// Config represents the configuration for OddSockets client
type Config struct {
	// APIKey is the OddSockets API key (required)
	APIKey string

	// ManagerURL is the manager URL (optional, defaults to https://manager1.oddsockets.tyga.network)
	ManagerURL string

	// UserID is the user identifier (optional, auto-generated if not provided)
	UserID string

	// AutoConnect determines if the client should auto-connect on creation
	AutoConnect bool

	// ReconnectAttempts is the maximum number of reconnection attempts
	ReconnectAttempts int

	// HeartbeatInterval is the interval between heartbeat messages
	HeartbeatInterval time.Duration

	// Timeout is the request timeout duration
	Timeout time.Duration
}

// DefaultConfig returns a Config with default values
func DefaultConfig() *Config {
	return &Config{
		ManagerURL:        "https://manager1.oddsockets.tyga.network",
		AutoConnect:       true,
		ReconnectAttempts: 5,
		HeartbeatInterval: 30 * time.Second,
		Timeout:           10 * time.Second,
	}
}

// Message represents a message received from OddSockets
type Message struct {
	// ID is the unique message identifier
	ID string `json:"id"`

	// Channel is the channel name
	Channel string `json:"channel"`

	// Data is the message payload
	Data interface{} `json:"data"`

	// Timestamp is when the message was sent
	Timestamp time.Time `json:"timestamp"`

	// UserID is the sender's user ID
	UserID string `json:"user_id,omitempty"`

	// Metadata contains additional message metadata
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// PresenceInfo represents presence information for a channel
type PresenceInfo struct {
	// Channel is the channel name
	Channel string `json:"channel"`

	// Users is the list of user IDs present in the channel
	Users []string `json:"users"`

	// Count is the total number of users present
	Count int `json:"count"`

	// Timestamp is when the presence snapshot was taken
	Timestamp time.Time `json:"timestamp"`
}

// SubscribeOptions contains options for channel subscription
type SubscribeOptions struct {
	// EnablePresence enables presence tracking for the channel
	EnablePresence bool

	// RetainHistory enables message history retention
	RetainHistory bool

	// FilterExpression is a filter expression for messages
	FilterExpression string
}

// PublishOptions contains options for message publishing
type PublishOptions struct {
	// TTL is the time to live for the message in seconds
	TTL int

	// Metadata contains additional metadata for the message
	Metadata map[string]interface{}

	// StoreInHistory determines if the message should be stored in history
	StoreInHistory bool
}

// HistoryOptions contains options for retrieving message history
type HistoryOptions struct {
	// Limit is the maximum number of messages to retrieve
	Limit int

	// Start is the start time for the history query
	Start *time.Time

	// End is the end time for the history query
	End *time.Time

	// Reverse determines if messages should be returned in reverse chronological order
	Reverse bool
}

// ConnectionState represents the connection state
type ConnectionState int

const (
	// Disconnected indicates the client is disconnected
	Disconnected ConnectionState = iota

	// Connecting indicates the client is connecting
	Connecting

	// Connected indicates the client is connected
	Connected

	// Reconnecting indicates the client is reconnecting
	Reconnecting

	// Failed indicates the connection has failed
	Failed
)

// String returns the string representation of ConnectionState
func (cs ConnectionState) String() string {
	switch cs {
	case Disconnected:
		return "disconnected"
	case Connecting:
		return "connecting"
	case Connected:
		return "connected"
	case Reconnecting:
		return "reconnecting"
	case Failed:
		return "failed"
	default:
		return "unknown"
	}
}

// EventType represents different event types
type EventType string

const (
	// EventConnected is emitted when the client connects
	EventConnected EventType = "connected"

	// EventDisconnected is emitted when the client disconnects
	EventDisconnected EventType = "disconnected"

	// EventReconnected is emitted when the client reconnects
	EventReconnected EventType = "reconnected"

	// EventError is emitted when an error occurs
	EventError EventType = "error"

	// EventMessage is emitted when a message is received
	EventMessage EventType = "message"

	// EventPresence is emitted when presence information changes
	EventPresence EventType = "presence"
)

// EventHandler is a function that handles events
type EventHandler func(eventType EventType, data interface{})

// PublishResult represents the result of a publish operation
type PublishResult struct {
	// MessageID is the unique identifier of the published message
	MessageID string `json:"message_id"`

	// Timestamp is when the message was published
	Timestamp time.Time `json:"timestamp"`

	// Channel is the channel the message was published to
	Channel string `json:"channel"`

	// Success indicates if the publish was successful
	Success bool `json:"success"`
}

// BulkMessage represents a message for bulk publishing
type BulkMessage struct {
	// Channel is the channel name
	Channel string `json:"channel"`

	// Message is the message payload
	Message interface{} `json:"message"`
}

// BulkResult represents the result of a bulk publish operation
type BulkResult struct {
	// Success indicates if the publish was successful
	Success bool `json:"success"`

	// Result contains the publish result if successful
	Result *PublishResult `json:"result,omitempty"`

	// Error contains the error message if unsuccessful
	Error string `json:"error,omitempty"`
}

// WorkerAssignment represents a worker assignment from the manager
type WorkerAssignment struct {
	// WorkerID is the unique identifier of the assigned worker
	WorkerID string `json:"workerId"`

	// URL is the WebSocket URL of the assigned worker
	URL string `json:"url"`

	// Session contains session information
	Session *SessionInfo `json:"session,omitempty"`
}

// SessionInfo represents session information for stickiness
type SessionInfo struct {
	// IsExisting indicates if this is an existing session
	IsExisting bool `json:"isExisting"`

	// AgeMs is the age of the session in milliseconds
	AgeMs int64 `json:"ageMs"`

	// ClientIdentifier is the unique client identifier
	ClientIdentifier string `json:"clientIdentifier"`
}

// MessageSizeLimits defines message size constraints
const (
	// MaxMessageSize is the maximum message size in bytes (32KB)
	MaxMessageSize = 32768

	// MaxMessageSizeKB is the maximum message size in KB
	MaxMessageSizeKB = 32
)
