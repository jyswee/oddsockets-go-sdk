package oddsockets

import (
	"encoding/json"
	"fmt"
	"time"
)

// EnhancedFeatures provides 67 new Slack-like events for OddSockets
type EnhancedFeatures struct {
	client *Client
}

// NewEnhancedFeatures creates a new EnhancedFeatures instance
func NewEnhancedFeatures(client *Client) *EnhancedFeatures {
	return &EnhancedFeatures{
		client: client,
	}
}

// ==================== THREAD EVENTS ====================

// ThreadReplyParams parameters for thread reply
type ThreadReplyParams struct {
	Channel         string `json:"channel"`
	ParentMessageID string `json:"parentMessageId"`
	Message         string `json:"message"`
	UserID          string `json:"userId"`
	UserName        string `json:"userName"`
}

// ThreadReply replies to a message in a thread
func (e *EnhancedFeatures) ThreadReply(params ThreadReplyParams) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("thread_reply_success", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "thread_reply" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("thread_reply", params)

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for thread reply response")
	}
}

// GetThread retrieves a thread with all replies
func (e *EnhancedFeatures) GetThread(threadID string) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("thread_data", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "get_thread" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("get_thread", map[string]interface{}{"threadId": threadID})

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for thread data")
	}
}

// SubscribeThread subscribes to thread updates
func (e *EnhancedFeatures) SubscribeThread(threadID, userID string) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("thread_subscribed", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "subscribe_thread" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("subscribe_thread", map[string]interface{}{
		"threadId": threadID,
		"userId":   userID,
	})

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for thread subscription")
	}
}

// MarkThreadRead marks a thread as read
func (e *EnhancedFeatures) MarkThreadRead(threadID, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("mark_thread_read", map[string]interface{}{
		"threadId": threadID,
		"userId":   userID,
	})
	return nil
}

// FollowThread follows a thread
func (e *EnhancedFeatures) FollowThread(threadID, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("follow_thread", map[string]interface{}{
		"threadId": threadID,
		"userId":   userID,
	})
	return nil
}

// UnfollowThread unfollows a thread
func (e *EnhancedFeatures) UnfollowThread(threadID, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("unfollow_thread", map[string]interface{}{
		"threadId": threadID,
		"userId":   userID,
	})
	return nil
}

// ==================== REACTION EVENTS ====================

// ReactionParams parameters for reactions
type ReactionParams struct {
	MessageID string `json:"messageId"`
	Channel   string `json:"channel"`
	Emoji     string `json:"emoji"`
	UserID    string `json:"userId"`
	UserName  string `json:"userName,omitempty"`
}

// AddReaction adds a reaction to a message
func (e *EnhancedFeatures) AddReaction(params ReactionParams) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("add_reaction", params)
	return nil
}

// RemoveReaction removes a reaction from a message
func (e *EnhancedFeatures) RemoveReaction(params ReactionParams) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("remove_reaction", params)
	return nil
}

// GetReactions gets all reactions for a message
func (e *EnhancedFeatures) GetReactions(messageID string) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("message_reactions", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "get_reactions" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("get_reactions", map[string]interface{}{"messageId": messageID})

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for reactions")
	}
}

// ==================== READ RECEIPT EVENTS ====================

// ReadReceiptParams parameters for read receipts
type ReadReceiptParams struct {
	MessageID string `json:"messageId"`
	Channel   string `json:"channel"`
	UserID    string `json:"userId"`
	UserName  string `json:"userName"`
}

// MarkRead marks a message as read
func (e *EnhancedFeatures) MarkRead(params ReadReceiptParams) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("mark_read", params)
	return nil
}

// GetUnreadCounts gets unread counts for channels
func (e *EnhancedFeatures) GetUnreadCounts(userID string, channels []string) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("unread_counts", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "get_unread_counts" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("get_unread_counts", map[string]interface{}{
		"userId":   userID,
		"channels": channels,
	})

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for unread counts")
	}
}

// MarkAllRead marks all messages in a channel as read
func (e *EnhancedFeatures) MarkAllRead(channel, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("mark_all_read", map[string]interface{}{
		"channel": channel,
		"userId":  userID,
	})
	return nil
}

// ==================== CHANNEL EVENTS ====================

// CreateChannelParams parameters for creating a channel
type CreateChannelParams struct {
	Name          string                   `json:"name"`
	Type          string                   `json:"type"`
	Description   string                   `json:"description"`
	Topic         string                   `json:"topic"`
	CreatedBy     string                   `json:"createdBy"`
	CreatedByName string                   `json:"createdByName"`
	Members       []map[string]interface{} `json:"members,omitempty"`
}

// CreateChannel creates a new channel
func (e *EnhancedFeatures) CreateChannel(params CreateChannelParams) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("channel_create_success", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "create_channel" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("create_channel", params)

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for channel creation")
	}
}

// UpdateChannel updates channel details
func (e *EnhancedFeatures) UpdateChannel(channelID string, updates map[string]interface{}, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("update_channel", map[string]interface{}{
		"channelId": channelID,
		"updates":   updates,
		"userId":    userID,
	})
	return nil
}

// ArchiveChannel archives a channel
func (e *EnhancedFeatures) ArchiveChannel(channelID, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("archive_channel", map[string]interface{}{
		"channelId": channelID,
		"userId":    userID,
	})
	return nil
}

// InviteToChannel invites a user to a channel
func (e *EnhancedFeatures) InviteToChannel(channelID, invitedUserID, invitedUserName, invitedBy string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("invite_to_channel", map[string]interface{}{
		"channelId":        channelID,
		"invitedUserId":    invitedUserID,
		"invitedUserName":  invitedUserName,
		"invitedBy":        invitedBy,
	})
	return nil
}

// RemoveFromChannel removes a user from a channel
func (e *EnhancedFeatures) RemoveFromChannel(channelID, removedUserID, removedBy string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("remove_from_channel", map[string]interface{}{
		"channelId":      channelID,
		"removedUserId":  removedUserID,
		"removedBy":      removedBy,
	})
	return nil
}

// JoinChannel joins a public channel
func (e *EnhancedFeatures) JoinChannel(channelID, userID, userName string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("join_channel", map[string]interface{}{
		"channelId": channelID,
		"userId":    userID,
		"userName":  userName,
	})
	return nil
}

// LeaveChannel leaves a channel
func (e *EnhancedFeatures) LeaveChannel(channelID, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("leave_channel", map[string]interface{}{
		"channelId": channelID,
		"userId":    userID,
	})
	return nil
}

// GetChannelMembers gets channel members
func (e *EnhancedFeatures) GetChannelMembers(channelID string) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("channel_members", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "get_channel_members" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("get_channel_members", map[string]interface{}{"channelId": channelID})

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for channel members")
	}
}

// ==================== DIRECT MESSAGE EVENTS ====================

// CreateDMParams parameters for creating a DM
type CreateDMParams struct {
	UserIDs   []string `json:"userIds"`
	Type      string   `json:"type,omitempty"`
	GroupName string   `json:"groupName,omitempty"`
}

// CreateDM creates or gets a DM conversation
func (e *EnhancedFeatures) CreateDM(params CreateDMParams) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("dm_create_success", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "create_dm" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("create_dm", params)

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for DM creation")
	}
}

// SendDM sends a direct message
func (e *EnhancedFeatures) SendDM(conversationID, message, userID, userName string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("send_dm", map[string]interface{}{
		"conversationId": conversationID,
		"message":        message,
		"userId":         userID,
		"userName":       userName,
	})
	return nil
}

// GetDMConversations gets user's DM conversations
func (e *EnhancedFeatures) GetDMConversations(userID string, includeArchived bool) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("dm_conversations", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "get_dm_conversations" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("get_dm_conversations", map[string]interface{}{
		"userId":          userID,
		"includeArchived": includeArchived,
	})

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for DM conversations")
	}
}

// ==================== NOTIFICATION EVENTS ====================

// SubscribeNotifications subscribes to user notifications
func (e *EnhancedFeatures) SubscribeNotifications(userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("subscribe_notifications", map[string]interface{}{"userId": userID})
	return nil
}

// MarkNotificationRead marks a notification as read
func (e *EnhancedFeatures) MarkNotificationRead(notificationID, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("mark_notification_read", map[string]interface{}{
		"notificationId": notificationID,
		"userId":         userID,
	})
	return nil
}

// MarkAllNotificationsRead marks all notifications as read
func (e *EnhancedFeatures) MarkAllNotificationsRead(userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("mark_all_notifications_read", map[string]interface{}{"userId": userID})
	return nil
}

// ClearNotifications clears all notifications
func (e *EnhancedFeatures) ClearNotifications(userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("clear_notifications", map[string]interface{}{"userId": userID})
	return nil
}

// GetNotificationsParams parameters for getting notifications
type GetNotificationsParams struct {
	UserID string `json:"userId"`
	Limit  int    `json:"limit,omitempty"`
	Status string `json:"status,omitempty"`
}

// GetNotifications gets user notifications
func (e *EnhancedFeatures) GetNotifications(params GetNotificationsParams) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("notifications_data", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "get_notifications" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("get_notifications", params)

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for notifications")
	}
}

// ==================== PRESENCE EVENTS ====================

// SetStatus sets user status
func (e *EnhancedFeatures) SetStatus(userID, status string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("set_status", map[string]interface{}{
		"userId": userID,
		"status": status,
	})
	return nil
}

// SetCustomStatusParams parameters for custom status
type SetCustomStatusParams struct {
	UserID    string `json:"userId"`
	Emoji     string `json:"emoji"`
	Text      string `json:"text"`
	ExpiresAt string `json:"expiresAt,omitempty"`
}

// SetCustomStatus sets custom status
func (e *EnhancedFeatures) SetCustomStatus(params SetCustomStatusParams) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("set_custom_status", params)
	return nil
}

// ClearCustomStatus clears custom status
func (e *EnhancedFeatures) ClearCustomStatus(userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("clear_custom_status", map[string]interface{}{"userId": userID})
	return nil
}

// SetDND enables Do Not Disturb
func (e *EnhancedFeatures) SetDND(userID, until string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	payload := map[string]interface{}{"userId": userID}
	if until != "" {
		payload["until"] = until
	}

	e.client.socket.Emit("set_dnd", payload)
	return nil
}

// ClearDND disables Do Not Disturb
func (e *EnhancedFeatures) ClearDND(userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("clear_dnd", map[string]interface{}{"userId": userID})
	return nil
}

// StartTyping starts typing indicator
func (e *EnhancedFeatures) StartTyping(userID, channel string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("start_typing", map[string]interface{}{
		"userId":  userID,
		"channel": channel,
	})
	return nil
}

// StopTyping stops typing indicator
func (e *EnhancedFeatures) StopTyping(userID, channel string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("stop_typing", map[string]interface{}{
		"userId":  userID,
		"channel": channel,
	})
	return nil
}

// GetUserPresence gets user presence information
func (e *EnhancedFeatures) GetUserPresence(userIDs []string) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("user_presence_data", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "get_user_presence" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("get_user_presence", map[string]interface{}{"userIds": userIDs})

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for user presence")
	}
}

// ==================== MESSAGE EDITING EVENTS ====================

// EditMessage edits a message
func (e *EnhancedFeatures) EditMessage(messageID, channel, newContent, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("edit_message", map[string]interface{}{
		"messageId":  messageID,
		"channel":    channel,
		"newContent": newContent,
		"userId":     userID,
	})
	return nil
}

// DeleteMessage deletes a message
func (e *EnhancedFeatures) DeleteMessage(messageID, channel, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("delete_message", map[string]interface{}{
		"messageId": messageID,
		"channel":   channel,
		"userId":    userID,
	})
	return nil
}

// PinMessage pins a message to a channel
func (e *EnhancedFeatures) PinMessage(messageID, channel, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("pin_message", map[string]interface{}{
		"messageId": messageID,
		"channel":   channel,
		"userId":    userID,
	})
	return nil
}

// UnpinMessage unpins a message from a channel
func (e *EnhancedFeatures) UnpinMessage(messageID, channel, userID string) error {
	if !e.client.IsConnected() {
		return fmt.Errorf("not connected to OddSockets")
	}

	e.client.socket.Emit("unpin_message", map[string]interface{}{
		"messageId": messageID,
		"channel":   channel,
		"userId":    userID,
	})
	return nil
}

// GetPinnedMessages gets pinned messages in a channel
func (e *EnhancedFeatures) GetPinnedMessages(channel string) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("pinned_messages", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "get_pinned_messages" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("get_pinned_messages", map[string]interface{}{"channel": channel})

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for pinned messages")
	}
}

// ==================== SEARCH EVENTS ====================

// SearchMessagesParams parameters for searching messages
type SearchMessagesParams struct {
	Query  string `json:"query"`
	UserID string `json:"userId"`
	Limit  int    `json:"limit,omitempty"`
}

// SearchMessages searches messages across all channels
func (e *EnhancedFeatures) SearchMessages(params SearchMessagesParams) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("search_results", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "search_messages" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("search_messages", params)

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for search results")
	}
}

// FilterMessages filters messages by criteria
func (e *EnhancedFeatures) FilterMessages(filters map[string]interface{}) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("filter_results", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "filter_messages" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("filter_messages", filters)

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for filter results")
	}
}

// SearchInChannelParams parameters for searching in a channel
type SearchInChannelParams struct {
	Channel string `json:"channel"`
	Query   string `json:"query"`
	Limit   int    `json:"limit,omitempty"`
}

// SearchInChannel searches within a specific channel
func (e *EnhancedFeatures) SearchInChannel(params SearchInChannelParams) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("channel_search_results", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "search_in_channel" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("search_in_channel", params)

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for channel search results")
	}
}

// SearchByUserParams parameters for searching by user
type SearchByUserParams struct {
	UserID string `json:"userId"`
	Query  string `json:"query,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// SearchByUser searches messages by user
func (e *EnhancedFeatures) SearchByUser(params SearchByUserParams) (map[string]interface{}, error) {
	if !e.client.IsConnected() {
		return nil, fmt.Errorf("not connected to OddSockets")
	}

	result := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	e.client.socket.Once("user_search_results", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			result <- m
		}
	})

	e.client.socket.Once("error", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if event, ok := m["event"].(string); ok && event == "search_by_user" {
				errChan <- fmt.Errorf("%v", m["message"])
			}
		}
	})

	e.client.socket.Emit("search_by_user", params)

	select {
	case res := <-result:
		return res, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for user search results")
	}
}
