# OddSockets Go SDK

[![Go Reference](https://pkg.go.dev/badge/github.com/jyswee/oddsockets-go-sdk.svg)](https://pkg.go.dev/github.com/jyswee/oddsockets-go-sdk)
[![Go Report Card](https://goreportcard.com/badge/github.com/jyswee/oddsockets-go-sdk)](https://goreportcard.com/report/github.com/jyswee/oddsockets-go-sdk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Official Go SDK for OddSockets real-time messaging platform.

## Features

- **High Performance**: Optimized for Go's concurrency model with goroutines
- **Channels & Context**: Native Go patterns with context cancellation
- **Type Safety**: Strong typing with Go structs and interfaces
- **PubNub Compatible**: Drop-in replacement for PubNub Go SDK
- **High Performance**: 50% lower latency than PubNub
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
        ManagerURL: "https://manager1.oddsockets.tyga.network",
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

### PubNub Migration

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/jyswee/oddsockets-go-sdk/pubnub"
)

func main() {
    // Drop-in replacement for PubNub
    config := pubnub.NewConfig()
    config.PublishKey = "ak_live_1234567890abcdef"
    config.SubscribeKey = "ak_live_1234567890abcdef"
    config.UserID = "user123"

    pn := pubnub.NewPubNub(config)
    defer pn.Destroy()

    // Subscribe
    listener := &pubnub.SubscribeCallback{
        Message: func(pn *pubnub.PubNub, message pubnub.MessageResult) {
            fmt.Printf("Message: %+v\n", message.Message)
        },
    }

    pn.AddListener(listener)
    pn.Subscribe().Channels([]string{"my-channel"}).Execute()

    // Publish
    pn.Publish().Channel("my-channel").Message("Hello from Go!").Execute()
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

## Documentation

- **[API Reference](docs/api-reference.md)** - Complete API documentation
- **[Getting Started](docs/getting-started.md)** - Detailed setup guide
- **[Migration Guide](docs/migration-guide.md)** - Migrate from PubNub
- **[Troubleshooting](docs/troubleshooting.md)** - Common issues and solutions

## Examples

Explore our comprehensive examples:

- **[Basic Usage](examples/basic/main.go)** - Simple messaging
- **[PubNub Migration](examples/pubnub/main.go)** - Migration example
- **[Microservices](examples/microservices/main.go)** - Service-to-service messaging
- **[Kubernetes](examples/kubernetes/)** - Cloud-native deployment

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

- **50% lower latency** compared to PubNub
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
          value: "https://manager1.oddsockets.tyga.network"
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

## Support

- [Documentation](https://docs.oddsockets.com/sdks/go)
- [Issue Tracker](https://github.com/jyswee/oddsockets-go-sdk/issues)
- [Email Support](mailto:support@oddsockets.com)

## License

MIT License - Copyright (c) 2026 Joe Wee, Tyga.Cloud Ltd. See [LICENSE](LICENSE) for details.
