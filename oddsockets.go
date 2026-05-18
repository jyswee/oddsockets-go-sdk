// Package oddsockets provides a Go SDK for OddSockets real-time messaging platform.
//
// This package offers a complete Go implementation of the OddSockets client with
// native Go patterns including goroutines, channels, and context cancellation.
//
// Basic usage:
//
//	client, err := oddsockets.NewClient(&oddsockets.Config{
//		APIKey: "ak_live_1234567890abcdef",
//		UserID: "my-user-id",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Connect to OddSockets
//	ctx := context.Background()
//	if err := client.Connect(ctx); err != nil {
//		log.Fatal(err)
//	}
//
//	// Create a channel and subscribe
//	channel := client.Channel("my-channel")
//	messages := make(chan *oddsockets.Message, 100)
//
//	err = channel.Subscribe(ctx, messages, &oddsockets.SubscribeOptions{
//		EnablePresence: true,
//		RetainHistory:  true,
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Handle messages in a goroutine
//	go func() {
//		for msg := range messages {
//			fmt.Printf("Received: %+v\n", msg.Data)
//		}
//	}()
//
//	// Publish a message
//	result, err := channel.Publish(ctx, "Hello from Go!", &oddsockets.PublishOptions{
//		StoreInHistory: true,
//		Metadata: map[string]interface{}{
//			"source": "go-sdk",
//		},
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("Published: %s\n", result.MessageID)
//
// For more examples and advanced usage, see the examples directory.
package oddsockets

// This package provides direct access to OddSockets functionality.
// All types and functions are available directly from the oddsockets subpackage.

import (
	"github.com/tygacloud/oddsocketsai-go-sdk/oddsockets"
)

// Re-export key types for easier access
type (
	// Client is the main OddSockets client
	Client = oddsockets.Client

	// Channel represents a messaging channel  
	Channel = oddsockets.Channel

	// Config represents the configuration for OddSockets client
	Config = oddsockets.Config

	// Message represents a message received from OddSockets
	Message = oddsockets.Message

	// PresenceInfo represents presence information for a channel
	PresenceInfo = oddsockets.PresenceInfo

	// SubscribeOptions contains options for channel subscription
	SubscribeOptions = oddsockets.SubscribeOptions

	// PublishOptions contains options for message publishing
	PublishOptions = oddsockets.PublishOptions

	// HistoryOptions contains options for retrieving message history
	HistoryOptions = oddsockets.HistoryOptions

	// PublishResult represents the result of a publish operation
	PublishResult = oddsockets.PublishResult

	// BulkMessage represents a message for bulk publishing
	BulkMessage = oddsockets.BulkMessage

	// BulkResult represents the result of a bulk publish operation
	BulkResult = oddsockets.BulkResult

	// ConnectionState represents the connection state
	ConnectionState = oddsockets.ConnectionState

	// EventType represents different event types
	EventType = oddsockets.EventType

	// EventHandler is a function that handles events
	EventHandler = oddsockets.EventHandler
)

// Re-export constants
const (
	// Connection states
	Disconnected  = oddsockets.Disconnected
	Connecting    = oddsockets.Connecting
	Connected     = oddsockets.Connected
	Reconnecting  = oddsockets.Reconnecting
	Failed        = oddsockets.Failed

	// Event types
	EventConnected    = oddsockets.EventConnected
	EventDisconnected = oddsockets.EventDisconnected
	EventReconnected  = oddsockets.EventReconnected
	EventError        = oddsockets.EventError
	EventMessage      = oddsockets.EventMessage
	EventPresence     = oddsockets.EventPresence
)

// Re-export functions
var (
	// NewClient creates a new OddSockets client
	NewClient = oddsockets.NewClient

	// DefaultConfig returns a Config with default values
	DefaultConfig = oddsockets.DefaultConfig
)
