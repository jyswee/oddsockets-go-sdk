# OddSockets Go SDK

## What this is
Go SDK for real-time messaging. Pub/sub, presence, message history over WebSocket. Goroutine-safe, uses gorilla/websocket.

## Get a free API key (2-step email verification)

Step 1 — request a code:
```bash
curl -X POST https://oddsockets.com/api/agent-signup \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "agentName": "my-agent", "platform": "go"}'
```

Step 2 — verify and get key:
```bash
curl -X POST https://oddsockets.com/api/agent-signup/verify \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "code": "123456", "agentName": "my-agent"}'
```

## Connect and use

```go
package main

import (
    "fmt"
    oddsockets "github.com/jyswee/oddsockets-go-sdk/oddsockets"
)

func main() {
    client := oddsockets.NewClient(oddsockets.Config{
        APIKey: "YOUR_API_KEY",
        UserID: "my-agent",
    })
    client.Connect()
    defer client.Disconnect()

    ch := client.Channel("my-channel")
    ch.Subscribe(func(msg oddsockets.Message) {
        fmt.Println("Received:", msg)
    })
    ch.Publish(map[string]string{"text": "hello from go"})
}
```

## Key files

- `oddsockets/client.go` — Main client (connect, disconnect, channel management)
- `oddsockets/channel.go` — Channel operations (subscribe, publish, history, presence)
- `oddsockets/manager_discovery.go` — Resolves the cluster manager URL
- `oddsockets/types.go` — Type definitions
- `oddsockets.go` — Package entry point
- `examples/` — Working examples

## Free tier limits

- 100 MAU, 50 concurrent connections, 500 connections/day
- 10,000 messages/day, 100 messages/minute, 10 channels max
- 100MB storage, 24h message history retention
