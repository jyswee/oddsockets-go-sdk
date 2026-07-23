# OddSockets Go SDK - Round-trip Demo

A minimal, runnable program that connects to OddSockets, subscribes to a channel, publishes a message, and verifies it receives its own message back in real time.

This demo targets the OddSockets manager at `connect.oddsockets.tyga.network`, which transparently assigns a worker for the connection.

## Get a free API key

Signup is a two-step email verification with no card required.

1. Request a signup code:

   ```bash
   curl -X POST https://oddsockets.com/api/agent-signup \
     -H 'Content-Type: application/json' \
     -d '{"email":"you@example.com"}'
   ```

2. Verify the code you receive by email to receive your API key:

   ```bash
   curl -X POST https://oddsockets.com/api/agent-signup/verify \
     -H 'Content-Type: application/json' \
     -d '{"email":"you@example.com","code":"CODE_FROM_EMAIL"}'
   ```

Then export the key (never hardcode it):

```bash
export ODDSOCKETS_API_KEY=ak_live_your_key_here
```

## Build and run

Requires Go 1.19 or newer. From this `demo/` directory:

```bash
# Run directly
go run .

# Or build a binary and run it
go build -o demo .
./demo
```

The demo module uses a `replace` directive pointing at the parent SDK in this repo, so it works right after cloning without publishing the SDK.

## What it shows

- Creating a client with an API key and the OddSockets manager URL
- Connecting to OddSockets (manager transparently assigns a worker)
- Subscribing to a unique per-run channel named `demo-<random>`
- Publishing a structured message `{text, nonce}`
- Receiving your own message back and matching it by its `nonce`
- Printing `OK - round-trip verified` on success, with a 15-second timeout that exits non-zero otherwise
