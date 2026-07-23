// Command demo performs a full OddSockets pub/sub round-trip:
// connect -> subscribe to a unique channel -> publish a message ->
// receive that same message back and verify it by its nonce.
//
// It targets the OddSockets manager at https://connect.oddsockets.tyga.network,
// which transparently assigns a worker for the connection.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/jyswee/oddsockets-go-sdk/oddsockets"
)

const (
	managerURL     = "https://connect.oddsockets.tyga.network"
	roundTripLimit = 15 * time.Second
)

// randomHex returns n random bytes encoded as a hex string.
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Fall back to a time-based value; uniqueness is best-effort for the demo.
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func main() {
	apiKey := os.Getenv("ODDSOCKETS_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "ODDSOCKETS_API_KEY is not set.")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Get a free API key (no card, two-step email verification):")
		fmt.Fprintln(os.Stderr, "  1. curl -X POST https://oddsockets.com/api/agent-signup \\")
		fmt.Fprintln(os.Stderr, "       -H 'Content-Type: application/json' \\")
		fmt.Fprintln(os.Stderr, "       -d '{\"email\":\"you@example.com\"}'")
		fmt.Fprintln(os.Stderr, "  2. curl -X POST https://oddsockets.com/api/agent-signup/verify \\")
		fmt.Fprintln(os.Stderr, "       -H 'Content-Type: application/json' \\")
		fmt.Fprintln(os.Stderr, "       -d '{\"email\":\"you@example.com\",\"code\":\"CODE_FROM_EMAIL\"}'")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Then: export ODDSOCKETS_API_KEY=ak_live_your_key_here")
		os.Exit(1)
	}

	// Create the client pointed at the OddSockets manager.
	client, err := oddsockets.NewClient(&oddsockets.Config{
		APIKey:      apiKey,
		ManagerURL:  managerURL,
		UserID:      "go-demo-" + randomHex(4),
		AutoConnect: false,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Bound the whole round-trip with a 15-second timeout.
	ctx, cancel := context.WithTimeout(context.Background(), roundTripLimit)
	defer cancel()

	fmt.Println("Connecting to OddSockets...")
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Connected.")

	// Unique channel and message nonce for this run.
	nonce := randomHex(8)
	channelName := "demo-" + randomHex(6)
	channel := client.Channel(channelName)
	fmt.Printf("Using channel: %s\n", channelName)

	// Subscribe to the channel; received messages arrive on this Go channel.
	messages := make(chan *oddsockets.Message, 100)
	if err := channel.Subscribe(ctx, messages, &oddsockets.SubscribeOptions{
		EnablePresence: true,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Subscribed.")

	// Signalled when our own message (matching nonce) is echoed back.
	verified := make(chan struct{}, 1)

	go func() {
		for msg := range messages {
			data, ok := msg.Data.(map[string]interface{})
			if !ok {
				continue
			}
			if got, _ := data["nonce"].(string); got == nonce {
				select {
				case verified <- struct{}{}:
				default:
				}
				return
			}
		}
	}()

	// Publish our message. The nonce lets us recognise our own echo.
	payload := map[string]interface{}{
		"text":  "hello from the go demo",
		"nonce": nonce,
	}
	fmt.Println("Publishing message...")
	result, err := channel.Publish(ctx, payload, &oddsockets.PublishOptions{
		StoreInHistory: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to publish: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Published message id: %s\n", result.MessageID)

	// Wait for our own message to come back, or time out.
	select {
	case <-verified:
		fmt.Println("OK - round-trip verified")
		os.Exit(0)
	case <-ctx.Done():
		fmt.Fprintf(os.Stderr, "Timed out after %s waiting for round-trip\n", roundTripLimit)
		os.Exit(1)
	}
}
