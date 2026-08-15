# OddSockets Go SDK

[![Go Reference](https://pkg.go.dev/badge/github.com/jyswee/oddsockets-go-sdk.svg)](https://pkg.go.dev/github.com/jyswee/oddsockets-go-sdk)
[![Go Report Card](https://goreportcard.com/badge/github.com/jyswee/oddsockets-go-sdk)](https://goreportcard.com/report/github.com/jyswee/oddsockets-go-sdk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Official Go SDK for OddSockets real-time messaging platform.

## Features

- **High Performance**: Optimized for Go's concurrency model with goroutines
- **Channels & Context**: Native Go patterns with context cancellation
- **Type Safety**: Strong typing with Go structs and interfaces
- **Enhanced Surface**: Slack-like reactions, threads, typing, presence and more
- **Cost Effective**: No per-message pricing, no message size limits
- **Cloud Native**: Perfect for microservices and Kubernetes deployments

## 📦 Installation

```bash
go get github.com/jyswee/oddsockets-go-sdk
```

## 🏃‍♂️ Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/jyswee/oddsockets-go-sdk/oddsockets"
)

func main() {
    // Create client
    client, err := oddsockets.NewClient(&oddsockets.Config{
        APIKey:     "ak_live_1234567890abcdef",
        ManagerURL: "https://connect.oddsockets.tyga.network",
        UserID:     "go-demo-user",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Connect to OddSockets
    ctx := context.Background()
    if err := client.Connect(ctx); err != nil {
        log.Fatal(err)
    }

    // Create channel
    channel := client.Channel("my-channel")

    // Subscribe to messages
    messages := make(chan *oddsockets.Message, 100)
    if err := channel.Subscribe(ctx, messages, &oddsockets.SubscribeOptions{
        EnablePresence: true,
        RetainHistory:  true,
    }); err != nil {
        log.Fatal(err)
    }

    // Handle messages in goroutine
    go func() {
        for msg := range messages {
            fmt.Printf("Received: %+v\n", msg.Data)
        }
    }()

    // Publish a message
    if err := channel.Publish(ctx, "Hello from Go! 🐹", nil); err != nil {
        log.Fatal(err)
    }

    // Keep alive
    time.Sleep(5 * time.Second)
}
```

### Context and Cancellation

```go
package main

import (
    "context"
    "time"

    "github.com/jyswee/oddsockets-go-sdk/oddsockets"
)

func main() {
    client, _ := oddsockets.NewClient(&oddsockets.Config{
        APIKey: "ak_live_1234567890abcdef",
    })
    defer client.Close()

    // Context with timeout
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // Connect with context
    client.Connect(ctx)

    channel := client.Channel("timed-channel")
    messages := make(chan *oddsockets.Message, 10)

    // Subscribe with context cancellation
    go func() {
        channel.Subscribe(ctx, messages, nil)
    }()

    // Context will automatically cancel subscription after timeout
}
```

## Enhanced Features

Beyond core pub/sub, OddSockets ships a Slack-like **enhanced surface** — reactions,
typing indicators, threads, read receipts, presence/status, notifications, DMs,
channel management, message editing and search. It lives on the exported
`client.Enhanced` field. The pattern is always the same:

1. **Send** an action with a `client.Enhanced.*` method.
2. **Receive** the paired broadcast with `client.On("<event>", handler)`.

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/jyswee/oddsockets-go-sdk/oddsockets"
)

func main() {
    client, err := oddsockets.NewClient(&oddsockets.Config{
        APIKey: "ak_live_1234567890abcdef",
        UserID: "alice",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()
    if err := client.Connect(ctx); err != nil {
        log.Fatal(err)
    }

    // Receive-path: broadcasts from other users on the channel
    client.On("user_typing", func(_ oddsockets.EventType, data interface{}) {
        m, _ := data.(map[string]interface{})
        fmt.Printf("%v is typing\n", m["userId"])
    })
    client.On("reaction_added", func(_ oddsockets.EventType, data interface{}) {
        m, _ := data.(map[string]interface{})
        fmt.Printf("%v reacted %v\n", m["userId"], m["emoji"])
    })

    channel := client.Channel("room-42")
    msgs := make(chan *oddsockets.Message, 100)
    channel.Subscribe(ctx, msgs, &oddsockets.SubscribeOptions{EnablePresence: true})

    // Send-path: enhanced actions over the live socket
    client.Enhanced.StartTyping("alice", "room-42")
    client.Enhanced.AddReaction(oddsockets.ReactionParams{
        MessageID: "msg-1",
        Channel:   "room-42",
        Emoji:     ":thumbsup:",
        UserID:    "alice",
        UserName:  "Alice",
    })
    client.Enhanced.ThreadReply(oddsockets.ThreadReplyParams{
        Channel:         "room-42",
        ParentMessageID: "msg-1",
        Message:         "Replying in the thread",
        UserID:          "alice",
        UserName:        "Alice",
    })
}
```

Each area exposes methods on `client.Enhanced`; the worker broadcasts the paired
events which you handle with `client.On(...)`. Query methods (`Get*`, `Search*`)
return `(map[string]interface{}, error)` with the worker response.

| Area | Requests (`client.Enhanced.*`) | Broadcast events (`client.On`) |
|------|--------------------------------|--------------------------------|
| Typing | `StartTyping`, `StopTyping` | `user_typing`, `user_stopped_typing` |
| Reactions | `AddReaction`, `RemoveReaction`, `GetReactions` | `reaction_added`, `reaction_removed` |
| Threads | `ThreadReply`, `GetThread`, `SubscribeThread`, `FollowThread`, `MarkThreadRead` | `thread_reply`, `thread_subscribed`, `thread_followed`, `thread_read_updated` |
| Read receipts | `MarkRead`, `MarkAllRead`, `GetUnreadCounts` | `user_read`, `unread_count_updated`, `all_marked_read` |
| Messages | `EditMessage`, `DeleteMessage`, `PinMessage`, `UnpinMessage`, `GetPinnedMessages`, `SearchMessages` | `message_edited`, `message_deleted`, `message_pinned`, `message_unpinned` |
| Presence & status | `SetStatus`, `SetCustomStatus`, `SetDND`, `GetUserPresence` | `user_status_changed`, `custom_status_updated`, `dnd_status_changed` |
| Channels | `CreateChannel`, `UpdateChannel`, `ArchiveChannel`, `InviteToChannel`, `JoinChannel`, `LeaveChannel` | `channel_created`, `channel_updated`, `user_invited`, `user_joined_channel`, `user_left_channel` |
| DMs | `CreateDM`, `SendDM`, `GetDMConversations` | `dm_created`, `dm_received` |
| Notifications | `SubscribeNotifications`, `GetNotifications`, `MarkNotificationRead`, `ClearNotifications` | `notification`, `notification_read`, `notifications_cleared` |
| Search | `SearchMessages`, `SearchInChannel`, `SearchByUser`, `FilterMessages` | `(map, error)` results |

For any worker event not wrapped above, subscribe with the raw
`client.On("<event>", handler)` API — all enhanced broadcasts are forwarded onto
the client event stream.

## Examples

Explore the runnable examples:

- **[Basic Usage](examples/basic/main.go)** - Simple pub/sub messaging
- **[Enhanced Features](examples/enhanced/main.go)** - Two-client enhanced-events regression

## Configuration

### Client Options

```go
config := &oddsockets.Config{
    APIKey:            "your-api-key",        // Required: Your OddSockets API key
    ManagerURL:        "manager-url",         // Optional: Manager URL
    UserID:            "user-id",             // Optional: User identifier
    AutoConnect:       true,                  // Optional: Auto-connect on creation
    ReconnectAttempts: 5,                     // Optional: Max reconnection attempts
    HeartbeatInterval: 30 * time.Second,     // Optional: Heartbeat interval
    Timeout:           10 * time.Second,     // Optional: Request timeout
}
```

### Channel Options

```go
// Subscribe with options
err := channel.Subscribe(ctx, messages, &oddsockets.SubscribeOptions{
    EnablePresence:    true,                  // Enable presence tracking
    RetainHistory:     true,                  // Retain message history
    FilterExpression:  "user.premium == true", // Message filter expression
})

// Publish with options
err := channel.Publish(ctx, message, &oddsockets.PublishOptions{
    TTL:             3600,                    // Time to live (seconds)
    Metadata:        map[string]interface{}{"priority": "high"}, // Additional metadata
    StoreInHistory:  true,                    // Store in message history
})
```

## Go Support

- Go 1.19+
- Goroutines and channels
- Context cancellation
- Structured concurrency

## Testing

```bash
# Run tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run benchmarks
go test -bench=. ./...

# Run integration tests
go test -tags=integration ./...
```

## Building

```bash
# Get dependencies
go mod tidy

# Build
go build ./...

# Install
go install ./...

# Cross-compile
GOOS=linux GOARCH=amd64 go build -o oddsockets-linux ./cmd/example
```

## Performance

OddSockets Go SDK delivers superior performance:

- **Low latency** real-time delivery
- **99.9% uptime** with automatic failover
- **Unlimited message size** - no artificial limits
- **High throughput** - handle millions of messages with goroutines

## Security

- **End-to-end encryption** available
- **API key authentication** with fine-grained permissions
- **Rate limiting** and abuse protection
- **GDPR compliant** data handling

## Framework Integrations

### Gin Web Framework

```go
package main

import (
    "github.com/gin-gonic/gin"
    "github.com/jyswee/oddsockets-go-sdk/oddsockets"
)

func main() {
    client, _ := oddsockets.NewClient(&oddsockets.Config{
        APIKey: "ak_live_1234567890abcdef",
    })
    defer client.Close()

    r := gin.Default()
    
    r.POST("/send-message", func(c *gin.Context) {
        var req struct {
            Channel string      `json:"channel"`
            Message interface{} `json:"message"`
        }
        
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(400, gin.H{"error": err.Error()})
            return
        }
        
        channel := client.Channel(req.Channel)
        if err := channel.Publish(c.Request.Context(), req.Message, nil); err != nil {
            c.JSON(500, gin.H{"error": err.Error()})
            return
        }
        
        c.JSON(200, gin.H{"status": "sent"})
    })
    
    r.Run(":8080")
}
```

### gRPC Service

```go
package main

import (
    "context"
    
    "github.com/jyswee/oddsockets-go-sdk/oddsockets"
    "google.golang.org/grpc"
)

type MessageService struct {
    client *oddsockets.Client
}

func (s *MessageService) SendMessage(ctx context.Context, req *SendMessageRequest) (*SendMessageResponse, error) {
    channel := s.client.Channel(req.Channel)
    
    if err := channel.Publish(ctx, req.Message, nil); err != nil {
        return nil, err
    }
    
    return &SendMessageResponse{Success: true}, nil
}
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: oddsockets-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: oddsockets-service
  template:
    metadata:
      labels:
        app: oddsockets-service
    spec:
      containers:
      - name: service
        image: your-registry/oddsockets-service:latest
        env:
        - name: ODDSOCKETS_API_KEY
          valueFrom:
            secretKeyRef:
              name: oddsockets-secret
              key: api-key
        - name: ODDSOCKETS_MANAGER_URL
          value: "https://connect.oddsockets.tyga.network"
```

## Other SDKs

OddSockets is available in multiple languages:

- **[JavaScript SDK](../javascript/)** - Browser + Node.js, TypeScript ready
- **[Python SDK](../python/)** - AsyncIO support, Django/Flask integrations
- **[Java SDK](../java/)** - Enterprise-ready, Spring Boot integration
- **[C# SDK](../csharp/)** - .NET Core/Framework, Azure integrations
- **[Swift SDK](../swift/)** - iOS native, Combine framework
- **[Kotlin SDK](../kotlin/)** - Android native, coroutines support

## Get a Free API Key

AI agents can sign up with a verified email in two steps — no dashboard, no human required.

**Step 1:** Request a verification code
```bash
curl -X POST https://oddsockets.com/api/agent-signup \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "agentName": "my-agent", "platform": "go"}'
```

**Step 2:** Verify the 6-digit code from your email and get your API key
```bash
curl -X POST https://oddsockets.com/api/agent-signup/verify \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "code": "123456", "agentName": "my-agent"}'
```

## Plans

| | Free | Starter | Pro |
|---|---|---|---|
| **Price** | $0/mo | $49.99/mo | $299/mo |
| **MAU** | 100 | 1,000 | 50,000 |
| **Concurrent connections** | 50 | 1,000 | Unlimited |
| **Messages/day** | 10,000 | 4,320,000 | Unlimited |
| **Messages/minute** | 100 | 3,000 | Unlimited |
| **Channels** | 10 | Unlimited | Unlimited |
| **Storage** | 100MB (24h) | 50GB (6 months) | Unlimited |

All limits are enforced in real time.

## Get Accredited

<a href="https://tyga.games/accreditation"><img src="https://prodmedia.tyga.host/public/tyga.cloud/landing/tyga.games/tygagames-black-words.svg" alt="tyga.games accreditation" height="44"></a>

Prove you can build and operate real-time features on OddSockets — channels, presence, pub/sub, delivery guarantees and production liveops — on the stack itself. Three tiers (**TCU / TCA / TCP**), certified through **tyga.games** and delivered on ClassaaS.

[**Get accredited on tyga.games →**](https://tyga.games/accreditation)

## Support

- [Documentation](https://docs.oddsockets.com/sdks/go)
- [Issue Tracker](https://github.com/jyswee/oddsockets-go-sdk/issues)
- [Email Support](mailto:support@oddsockets.com)

## License

MIT License - Copyright (c) 2026 Joe Wee, Tyga.Cloud Ltd. See [LICENSE](LICENSE) for details.
