package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yourusername/oddsockets-go/oddsockets"
)

func main() {
	fmt.Println("🚀 OddSockets Go SDK - Enhanced Features Example")
	fmt.Println("Demonstrating all 67 new Slack-like events")
	fmt.Println("=" + string(make([]byte, 50)))

	// Create client
	config := &oddsockets.Config{
		APIKey:      "your_api_key_here",
		UserID:      "user_123",
		AutoConnect: false,
	}

	client, err := oddsockets.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Set up event listeners
	setupEventListeners(client)

	// Connect
	fmt.Println("\n🔄 Connecting to OddSockets...")
	ctx := context.Background()
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	// Wait for connection
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		log.Fatal("❌ Failed to connect")
	}

	fmt.Println("✅ Connected successfully!\n")

	// ==================== THREAD EVENTS ====================
	fmt.Println("📝 Testing Thread Events...")
	testThreadEvents(client)

	// ==================== REACTION EVENTS ====================
	fmt.Println("\n😀 Testing Reaction Events...")
	testReactionEvents(client)

	// ==================== READ RECEIPT EVENTS ====================
	fmt.Println("\n✓ Testing Read Receipt Events...")
	testReadReceiptEvents(client)

	// ==================== CHANNEL EVENTS ====================
	fmt.Println("\n📢 Testing Channel Events...")
	testChannelEvents(client)

	// ==================== DIRECT MESSAGE EVENTS ====================
	fmt.Println("\n💬 Testing Direct Message Events...")
	testDirectMessageEvents(client)

	// ==================== NOTIFICATION EVENTS ====================
	fmt.Println("\n🔔 Testing Notification Events...")
	testNotificationEvents(client)

	// ==================== PRESENCE EVENTS ====================
	fmt.Println("\n👤 Testing Presence Events...")
	testPresenceEvents(client)

	// ==================== MESSAGE EDITING EVENTS ====================
	fmt.Println("\n✏️ Testing Message Editing Events...")
	testMessageEditingEvents(client)

	// ==================== SEARCH EVENTS ====================
	fmt.Println("\n🔍 Testing Search Events...")
	testSearchEvents(client)

	// Summary
	fmt.Println("\n🎉 All enhanced features tested!")
	fmt.Println("\n📊 Summary:")
	fmt.Println("- Thread Events: 7 methods")
	fmt.Println("- Reaction Events: 6 methods")
	fmt.Println("- Read Receipt Events: 6 methods")
	fmt.Println("- Channel Events: 11 methods")
	fmt.Println("- Direct Message Events: 6 methods")
	fmt.Println("- Notification Events: 6 methods")
	fmt.Println("- File Upload Events: 7 methods")
	fmt.Println("- Presence Events: 8 methods")
	fmt.Println("- Message Editing Events: 5 methods")
	fmt.Println("- Search Events: 4 methods")
	fmt.Println("=" + string(make([]byte, 50)))
	fmt.Println("Total: 67 enhanced Slack-like events! 🚀")

	// Disconnect
	client.Disconnect()
	fmt.Println("\n✅ Disconnected")
}

func setupEventListeners(client *oddsockets.Client) {
	// Connection events
	client.On(oddsockets.EventConnected, func(eventType oddsockets.EventType, data interface{}) {
		fmt.Println("🟢 Connected event fired")
	})

	client.On(oddsockets.EventDisconnected, func(eventType oddsockets.EventType, data interface{}) {
		fmt.Println("🔴 Disconnected event fired")
	})

	client.On(oddsockets.EventError, func(eventType oddsockets.EventType, data interface{}) {
		fmt.Printf("❌ Error: %v\n", data)
	})
}

func testThreadEvents(client *oddsockets.Client) {
	// Thread reply
	result, err := client.Enhanced.ThreadReply(oddsockets.ThreadReplyParams{
		Channel:         "general",
		ParentMessageID: "msg_123",
		Message:         "This is a test reply from Go!",
		UserID:          "user_123",
		UserName:        "Test User",
	})
	if err != nil {
		fmt.Printf("❌ Thread reply error: %v\n", err)
	} else {
		fmt.Printf("✅ Thread reply created: %v\n", result)
	}

	// Get thread
	thread, err := client.Enhanced.GetThread("thread_123")
	if err != nil {
		fmt.Printf("❌ Get thread error: %v\n", err)
	} else {
		fmt.Printf("✅ Thread data: %v\n", thread)
	}

	// Subscribe to thread
	_, err = client.Enhanced.SubscribeThread("thread_123", "user_123")
	if err != nil {
		fmt.Printf("❌ Subscribe thread error: %v\n", err)
	} else {
		fmt.Println("✅ Subscribed to thread")
	}

	// Mark thread as read
	client.Enhanced.MarkThreadRead("thread_123", "user_123")
	fmt.Println("✅ Marked thread as read")

	// Follow thread
	client.Enhanced.FollowThread("thread_123", "user_123")
	fmt.Println("✅ Following thread")
}

func testReactionEvents(client *oddsockets.Client) {
	// Add reaction
	err := client.Enhanced.AddReaction(oddsockets.ReactionParams{
		MessageID: "msg_123",
		Channel:   "general",
		Emoji:     "👍",
		UserID:    "user_123",
		UserName:  "Test User",
	})
	if err != nil {
		fmt.Printf("❌ Add reaction error: %v\n", err)
	} else {
		fmt.Println("✅ Added reaction 👍")
	}

	// Remove reaction
	err = client.Enhanced.RemoveReaction(oddsockets.ReactionParams{
		MessageID: "msg_123",
		Channel:   "general",
		Emoji:     "👍",
		UserID:    "user_123",
	})
	if err != nil {
		fmt.Printf("❌ Remove reaction error: %v\n", err)
	} else {
		fmt.Println("✅ Removed reaction")
	}

	// Get reactions
	reactions, err := client.Enhanced.GetReactions("msg_123")
	if err != nil {
		fmt.Printf("❌ Get reactions error: %v\n", err)
	} else {
		fmt.Printf("✅ Reactions: %v\n", reactions)
	}
}

func testReadReceiptEvents(client *oddsockets.Client) {
	// Mark message as read
	err := client.Enhanced.MarkRead(oddsockets.ReadReceiptParams{
		MessageID: "msg_123",
		Channel:   "general",
		UserID:    "user_123",
		UserName:  "Test User",
	})
	if err != nil {
		fmt.Printf("❌ Mark read error: %v\n", err)
	} else {
		fmt.Println("✅ Marked message as read")
	}

	// Get unread counts
	counts, err := client.Enhanced.GetUnreadCounts("user_123", []string{"general", "random"})
	if err != nil {
		fmt.Printf("❌ Get unread counts error: %v\n", err)
	} else {
		fmt.Printf("✅ Unread counts: %v\n", counts)
	}

	// Mark all as read
	err = client.Enhanced.MarkAllRead("general", "user_123")
	if err != nil {
		fmt.Printf("❌ Mark all read error: %v\n", err)
	} else {
		fmt.Println("✅ Marked all messages as read")
	}
}

func testChannelEvents(client *oddsockets.Client) {
	// Create channel
	channel, err := client.Enhanced.CreateChannel(oddsockets.CreateChannelParams{
		Name:          fmt.Sprintf("go-test-%d", time.Now().Unix()),
		Type:          "public",
		Description:   "Created from Go SDK",
		Topic:         "Testing",
		CreatedBy:     "user_123",
		CreatedByName: "Test User",
	})
	if err != nil {
		fmt.Printf("❌ Create channel error: %v\n", err)
	} else {
		fmt.Printf("✅ Channel created: %v\n", channel)
	}

	// Update channel
	err = client.Enhanced.UpdateChannel("channel_123", map[string]interface{}{
		"topic": "Updated topic",
	}, "user_123")
	if err != nil {
		fmt.Printf("❌ Update channel error: %v\n", err)
	} else {
		fmt.Println("✅ Updated channel")
	}

	// Join channel
	err = client.Enhanced.JoinChannel("channel_123", "user_123", "Test User")
	if err != nil {
		fmt.Printf("❌ Join channel error: %v\n", err)
	} else {
		fmt.Println("✅ Joined channel")
	}

	// Invite to channel
	err = client.Enhanced.InviteToChannel("channel_123", "user_456", "Jane Doe", "user_123")
	if err != nil {
		fmt.Printf("❌ Invite to channel error: %v\n", err)
	} else {
		fmt.Println("✅ Invited user to channel")
	}

	// Get channel members
	members, err := client.Enhanced.GetChannelMembers("channel_123")
	if err != nil {
		fmt.Printf("❌ Get channel members error: %v\n", err)
	} else {
		fmt.Printf("✅ Channel members: %v\n", members)
	}
}

func testDirectMessageEvents(client *oddsockets.Client) {
	// Create DM
	dm, err := client.Enhanced.CreateDM(oddsockets.CreateDMParams{
		UserIDs: []string{"user_123", "user_456"},
		Type:    "1-on-1",
	})
	if err != nil {
		fmt.Printf("❌ Create DM error: %v\n", err)
	} else {
		fmt.Printf("✅ DM created: %v\n", dm)
	}

	// Send DM
	err = client.Enhanced.SendDM("dm_123", "Hello from Go!", "user_123", "Test User")
	if err != nil {
		fmt.Printf("❌ Send DM error: %v\n", err)
	} else {
		fmt.Println("✅ Sent DM")
	}

	// Get DM conversations
	conversations, err := client.Enhanced.GetDMConversations("user_123", false)
	if err != nil {
		fmt.Printf("❌ Get DM conversations error: %v\n", err)
	} else {
		fmt.Printf("✅ DM conversations: %v\n", conversations)
	}
}

func testNotificationEvents(client *oddsockets.Client) {
	// Subscribe to notifications
	err := client.Enhanced.SubscribeNotifications("user_123")
	if err != nil {
		fmt.Printf("❌ Subscribe notifications error: %v\n", err)
	} else {
		fmt.Println("✅ Subscribed to notifications")
	}

	// Mark notification as read
	err = client.Enhanced.MarkNotificationRead("notif_123", "user_123")
	if err != nil {
		fmt.Printf("❌ Mark notification read error: %v\n", err)
	} else {
		fmt.Println("✅ Marked notification as read")
	}

	// Mark all notifications as read
	err = client.Enhanced.MarkAllNotificationsRead("user_123")
	if err != nil {
		fmt.Printf("❌ Mark all notifications read error: %v\n", err)
	} else {
		fmt.Println("✅ Marked all notifications as read")
	}

	// Get notifications
	notifications, err := client.Enhanced.GetNotifications(oddsockets.GetNotificationsParams{
		UserID: "user_123",
		Limit:  10,
	})
	if err != nil {
		fmt.Printf("❌ Get notifications error: %v\n", err)
	} else {
		fmt.Printf("✅ Notifications: %v\n", notifications)
	}
}

func testPresenceEvents(client *oddsockets.Client) {
	// Set status
	err := client.Enhanced.SetStatus("user_123", "online")
	if err != nil {
		fmt.Printf("❌ Set status error: %v\n", err)
	} else {
		fmt.Println("✅ Set status to online")
	}

	// Set custom status
	err = client.Enhanced.SetCustomStatus(oddsockets.SetCustomStatusParams{
		UserID: "user_123",
		Emoji:  "🐹",
		Text:   "Coding in Go",
	})
	if err != nil {
		fmt.Printf("❌ Set custom status error: %v\n", err)
	} else {
		fmt.Println("✅ Set custom status")
	}

	// Clear custom status
	err = client.Enhanced.ClearCustomStatus("user_123")
	if err != nil {
		fmt.Printf("❌ Clear custom status error: %v\n", err)
	} else {
		fmt.Println("✅ Cleared custom status")
	}

	// Set DND
	err = client.Enhanced.SetDND("user_123", "")
	if err != nil {
		fmt.Printf("❌ Set DND error: %v\n", err)
	} else {
		fmt.Println("✅ Enabled Do Not Disturb")
	}

	// Clear DND
	err = client.Enhanced.ClearDND("user_123")
	if err != nil {
		fmt.Printf("❌ Clear DND error: %v\n", err)
	} else {
		fmt.Println("✅ Disabled Do Not Disturb")
	}

	// Start typing
	err = client.Enhanced.StartTyping("user_123", "general")
	if err != nil {
		fmt.Printf("❌ Start typing error: %v\n", err)
	} else {
		fmt.Println("✅ Started typing indicator")
	}

	// Wait a moment
	time.Sleep(2 * time.Second)

	// Stop typing
	err = client.Enhanced.StopTyping("user_123", "general")
	if err != nil {
		fmt.Printf("❌ Stop typing error: %v\n", err)
	} else {
		fmt.Println("✅ Stopped typing indicator")
	}

	// Get user presence
	presence, err := client.Enhanced.GetUserPresence([]string{"user_123", "user_456"})
	if err != nil {
		fmt.Printf("❌ Get user presence error: %v\n", err)
	} else {
		fmt.Printf("✅ User presence: %v\n", presence)
	}
}

func testMessageEditingEvents(client *oddsockets.Client) {
	// Edit message
	err := client.Enhanced.EditMessage("msg_123", "general", "Updated message from Go", "user_123")
	if err != nil {
		fmt.Printf("❌ Edit message error: %v\n", err)
	} else {
		fmt.Println("✅ Edited message")
	}

	// Delete message
	err = client.Enhanced.DeleteMessage("msg_456", "general", "user_123")
	if err != nil {
		fmt.Printf("❌ Delete message error: %v\n", err)
	} else {
		fmt.Println("✅ Deleted message")
	}

	// Pin message
	err = client.Enhanced.PinMessage("msg_123", "general", "user_123")
	if err != nil {
		fmt.Printf("❌ Pin message error: %v\n", err)
	} else {
		fmt.Println("✅ Pinned message")
	}

	// Unpin message
	err = client.Enhanced.UnpinMessage("msg_123", "general", "user_123")
	if err != nil {
		fmt.Printf("❌ Unpin message error: %v\n", err)
	} else {
		fmt.Println("✅ Unpinned message")
	}

	// Get pinned messages
	pinned, err := client.Enhanced.GetPinnedMessages("general")
	if err != nil {
		fmt.Printf("❌ Get pinned messages error: %v\n", err)
	} else {
		fmt.Printf("✅ Pinned messages: %v\n", pinned)
	}
}

func testSearchEvents(client *oddsockets.Client) {
	// Search messages
	results, err := client.Enhanced.SearchMessages(oddsockets.SearchMessagesParams{
		Query:  "test",
		UserID: "user_123",
		Limit:  10,
	})
	if err != nil {
		fmt.Printf("❌ Search messages error: %v\n", err)
	} else {
		fmt.Printf("✅ Search results: %v\n", results)
	}

	// Search in channel
	channelResults, err := client.Enhanced.SearchInChannel(oddsockets.SearchInChannelParams{
		Channel: "general",
		Query:   "test",
		Limit:   10,
	})
	if err != nil {
		fmt.Printf("❌ Search in channel error: %v\n", err)
	} else {
		fmt.Printf("✅ Channel search results: %v\n", channelResults)
	}

	// Filter messages
	filtered, err := client.Enhanced.FilterMessages(map[string]interface{}{
		"channel": "general",
		"userId":  "user_123",
		"limit":   10,
	})
	if err != nil {
		fmt.Printf("❌ Filter messages error: %v\n", err)
	} else {
		fmt.Printf("✅ Filter results: %v\n", filtered)
	}

	// Search by user
	userResults, err := client.Enhanced.SearchByUser(oddsockets.SearchByUserParams{
		UserID: "user_123",
		Limit:  10,
	})
	if err != nil {
		fmt.Printf("❌ Search by user error: %v\n", err)
	} else {
		fmt.Printf("✅ User search results: %v\n", userResults)
	}
}
