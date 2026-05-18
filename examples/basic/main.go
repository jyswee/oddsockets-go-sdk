package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tygacloud/oddsocketsai-go-sdk/oddsockets"
)

func main() {
	fmt.Println("🐹 OddSockets Go SDK - Basic Example")
	fmt.Println("====================================")

	// Create client with configuration
	client, err := oddsockets.NewClient(&oddsockets.Config{
		APIKey:     "ak_live_1234567890abcdef",
		ManagerURL: "https://manager1.oddsockets.tyga.network",
		UserID:     "go-demo-user",
		AutoConnect: false, // Don't auto-connect for this example
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	fmt.Printf("✅ Client created for user: %s\n", client.GetUserID())

	// Connect to OddSockets
	ctx := context.Background()
	fmt.Println("🔌 Connecting to OddSockets...")
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	fmt.Println("✅ Connected successfully")

	// Create channel
	channel := client.Channel("go-demo-channel")
	fmt.Printf("📡 Created channel: %s\n", channel.GetName())

	// Create message channel for receiving messages
	messages := make(chan *oddsockets.Message, 100)

	// Subscribe to messages
	fmt.Println("📥 Subscribing to channel...")
	if err := channel.Subscribe(ctx, messages, &oddsockets.SubscribeOptions{
		EnablePresence: true,
		RetainHistory:  true,
	}); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	fmt.Println("✅ Subscribed successfully")

	// Handle messages in a goroutine
	go func() {
		fmt.Println("👂 Listening for messages...")
		for msg := range messages {
			fmt.Printf("📨 Received message: %+v\n", msg.Data)
			fmt.Printf("   Channel: %s\n", msg.Channel)
			fmt.Printf("   From: %s\n", msg.UserID)
			fmt.Printf("   Time: %s\n", msg.Timestamp.Format(time.RFC3339))
			if msg.Metadata != nil {
				fmt.Printf("   Metadata: %+v\n", msg.Metadata)
			}
			fmt.Println()
		}
	}()

	// Wait for welcome message
	time.Sleep(200 * time.Millisecond)

	// Publish some test messages
	testMessages := []interface{}{
		"Hello from Go! 🐹",
		map[string]interface{}{
			"type": "notification",
			"text": "This is a structured message",
		},
		map[string]interface{}{
			"user":   "alice",
			"action": "joined",
			"timestamp": time.Now().Format(time.RFC3339),
		},
		[]int{1, 2, 3, 4, 5}, // Arrays work too
		42, // Numbers work
	}

	fmt.Printf("📤 Publishing %d test messages...\n", len(testMessages))
	for i, message := range testMessages {
		fmt.Printf("📤 Publishing message %d/%d\n", i+1, len(testMessages))
		
		result, err := channel.Publish(ctx, message, &oddsockets.PublishOptions{
			Metadata: map[string]interface{}{
				"example":        true,
				"message_number": i + 1,
			},
			StoreInHistory: true,
		})
		
		if err != nil {
			log.Printf("Failed to publish message %d: %v", i+1, err)
			continue
		}
		
		fmt.Printf("✅ Published: %s\n", result.MessageID)
		time.Sleep(500 * time.Millisecond) // Small delay between messages
	}

	// Get channel presence
	fmt.Println("👥 Getting channel presence...")
	presence, err := channel.GetPresence(ctx)
	if err != nil {
		log.Printf("Failed to get presence: %v", err)
	} else {
		fmt.Printf("✅ Channel presence: %d users online\n", presence.Count)
		fmt.Printf("   Users: %v\n", presence.Users)
	}

	// Get message history
	fmt.Println("📚 Getting message history...")
	history, err := channel.GetHistory(ctx, &oddsockets.HistoryOptions{
		Limit:   10,
		Reverse: true,
	})
	if err != nil {
		log.Printf("Failed to get history: %v", err)
	} else {
		fmt.Printf("✅ Retrieved %d messages from history\n", len(history))
		if len(history) > 0 {
			fmt.Println("   Recent messages:")
			for i, msg := range history {
				if i >= 3 { // Show only last 3 messages
					break
				}
				fmt.Printf("   %s: %v\n", msg.Timestamp.Format("15:04:05"), msg.Data)
			}
		}
	}

	// Test bulk publishing
	fmt.Println("📦 Testing bulk publishing...")
	bulkMessages := []oddsockets.BulkMessage{
		{Channel: "go-demo-channel", Message: "Bulk message 1"},
		{Channel: "go-demo-channel", Message: "Bulk message 2"},
		{Channel: "go-demo-channel", Message: "Bulk message 3"},
	}

	bulkResults, err := client.PublishBulk(ctx, bulkMessages)
	if err != nil {
		log.Printf("Failed to publish bulk messages: %v", err)
	} else {
		successful := 0
		for _, result := range bulkResults {
			if result.Success {
				successful++
			}
		}
		fmt.Printf("✅ Bulk published %d/%d messages successfully\n", successful, len(bulkMessages))
	}

	// Test event handling
	fmt.Println("🎯 Testing event handling...")
	client.On(oddsockets.EventConnected, func(eventType oddsockets.EventType, data interface{}) {
		fmt.Printf("🎉 Event received: %s\n", eventType)
	})

	// Keep connection alive for a bit to receive messages
	fmt.Println("⏳ Keeping connection alive for 3 seconds...")
	time.Sleep(3 * time.Second)

	// Clean up
	fmt.Println("🧹 Cleaning up...")
	if err := channel.Unsubscribe(); err != nil {
		log.Printf("Failed to unsubscribe: %v", err)
	} else {
		fmt.Println("✅ Unsubscribed from channel")
	}

	if err := client.Disconnect(); err != nil {
		log.Printf("Failed to disconnect: %v", err)
	} else {
		fmt.Println("✅ Disconnected successfully")
	}

	fmt.Println()
	fmt.Println("🎉 Example completed successfully!")
	fmt.Println()
	fmt.Println("Key Features Demonstrated:")
	fmt.Println("✅ Client creation and connection")
	fmt.Println("✅ Channel creation and management")
	fmt.Println("✅ Message subscription with goroutines")
	fmt.Println("✅ Message publishing with options")
	fmt.Println("✅ Presence tracking")
	fmt.Println("✅ Message history retrieval")
	fmt.Println("✅ Bulk message publishing")
	fmt.Println("✅ Event handling")
	fmt.Println("✅ Proper cleanup and resource management")
	fmt.Println()
	fmt.Println("🚀 Go SDK is working correctly!")
}
