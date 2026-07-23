// Command enhanced runs a two-client enhanced-events regression for the
// OddSockets Go SDK. It proves the RECEIVE path for enhanced (Slack-like)
// events: an action fired by one client (bob) is broadcast by the worker and
// surfaces on the OTHER client's public event stream (alice).
//
// Because publisher and subscriber are separate connections, an event reaching
// alice can only have travelled through the OddSockets worker - an honest
// end-to-end test, no local echo:
//
//	bob -> Enhanced.StartTyping  -> alice client.On("user_typing")
//	bob -> Enhanced.AddReaction  -> alice client.On("reaction_added")
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
	managerURL = "https://connect.oddsockets.tyga.network"
	timeout    = 20 * time.Second
)

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func mustConnect(apiKey, userID string) *oddsockets.Client {
	client, err := oddsockets.NewClient(&oddsockets.Config{
		APIKey:      apiKey,
		ManagerURL:  managerURL,
		UserID:      userID,
		AutoConnect: false,
	})
	if err != nil {
		fail("[%s] failed to create client: %v", userID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		fail("[%s] failed to connect: %v", userID, err)
	}
	return client
}

func workerID(client *oddsockets.Client) string {
	if info := client.GetWorkerInfo(); info != nil {
		if id, ok := info["workerId"].(string); ok {
			return id
		}
	}
	return "unknown"
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func main() {
	apiKey := os.Getenv("ODDSOCKETS_API_KEY")
	if apiKey == "" {
		fail("ODDSOCKETS_API_KEY is not set.")
	}

	fmt.Println("[connect] connecting both clients...")
	alice := mustConnect(apiKey, "alice")
	bob := mustConnect(apiKey, "bob")
	defer alice.Close()
	defer bob.Close()

	fmt.Printf("[alice] worker %s\n", workerID(alice))
	fmt.Printf("[bob]   worker %s\n", workerID(bob))
	fmt.Printf("[connect] alice = %s, bob = %s\n", alice.GetConnectionState(), bob.GetConnectionState())

	channelName := "enh-" + randomHex(5)

	// alice listens for the enhanced broadcasts on her PUBLIC event stream.
	typingCh := make(chan struct{}, 1)
	reactionCh := make(chan struct{}, 1)

	alice.On("user_typing", func(_ oddsockets.EventType, data interface{}) {
		m, _ := data.(map[string]interface{})
		if m == nil || m["userId"] != "bob" {
			return
		}
		fmt.Printf("[alice] received 'user_typing' from bob (channel %v) - broadcast round-trip.\n", m["channel"])
		signal(typingCh)
	})

	alice.On("reaction_added", func(_ oddsockets.EventType, data interface{}) {
		m, _ := data.(map[string]interface{})
		if m == nil || m["emoji"] == nil {
			return
		}
		fmt.Printf("[alice] received 'reaction_added' (%v) from %v - broadcast round-trip.\n", m["emoji"], m["userId"])
		signal(reactionCh)
	})

	// Both clients join the same room.
	ctx := context.Background()
	aliceCh := alice.Channel(channelName)
	bobCh := bob.Channel(channelName)

	aliceMsgs := make(chan *oddsockets.Message, 100)
	bobMsgs := make(chan *oddsockets.Message, 100)

	if err := aliceCh.Subscribe(ctx, aliceMsgs, &oddsockets.SubscribeOptions{EnablePresence: true}); err != nil {
		fail("[alice] failed to subscribe: %v", err)
	}
	if err := bobCh.Subscribe(ctx, bobMsgs, &oddsockets.SubscribeOptions{EnablePresence: true}); err != nil {
		fail("[bob] failed to subscribe: %v", err)
	}
	fmt.Printf("[both] subscribed to %s\n", channelName)

	// Let room membership settle, then fire enhanced actions from bob.
	time.Sleep(500 * time.Millisecond)

	fmt.Println("[bob] Enhanced.StartTyping(bob) ...")
	if err := bob.Enhanced.StartTyping("bob", channelName); err != nil {
		fail("[bob] StartTyping failed: %v", err)
	}

	result, err := bobCh.Publish(ctx, map[string]interface{}{"text": "react to me"}, nil)
	if err != nil {
		fail("[bob] publish failed: %v", err)
	}
	fmt.Printf("[bob] published messageId=%s, Enhanced.AddReaction :thumbsup: ...\n", result.MessageID)

	if err := bob.Enhanced.AddReaction(oddsockets.ReactionParams{
		MessageID: result.MessageID,
		Channel:   channelName,
		Emoji:     ":thumbsup:",
		UserID:    "bob",
		UserName:  "Bob",
	}); err != nil {
		fail("[bob] AddReaction failed: %v", err)
	}

	// Wait until BOTH enhanced broadcasts have reached alice.
	gotTyping, gotReaction := false, false
	deadline := time.After(timeout)
	for !(gotTyping && gotReaction) {
		select {
		case <-typingCh:
			gotTyping = true
		case <-reactionCh:
			gotReaction = true
		case <-deadline:
			fail("\nTIMEOUT - enhanced broadcast not received within %s (typing=%v reaction=%v)", timeout, gotTyping, gotReaction)
		}
	}

	fmt.Println("\nOK - enhanced broadcast receive-path verified (user_typing + reaction_added)")
	alice.Disconnect()
	bob.Disconnect()
	os.Exit(0)
}
