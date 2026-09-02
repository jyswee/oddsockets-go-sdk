# Two-client OddSockets Go SDK demo, built straight from the SDK sources.
#
# Build (from the repo root, so the demo module's `replace => ../` can see the
# SDK source):
#   docker build -f demo/Dockerfile -t oddsockets-go-demo .
#
# Basic pub/sub round-trip (default entrypoint):
#   docker run --rm -e ODDSOCKETS_API_KEY=ak_your_key_here oddsockets-go-demo
#
# Enhanced-events two-client receive-path regression:
#   docker run --rm -e ODDSOCKETS_API_KEY=ak_your_key_here \
#     oddsockets-go-demo /usr/local/bin/enhanced
FROM golang:1.22

WORKDIR /app
COPY . .

WORKDIR /app/demo
RUN go build -o /usr/local/bin/demo . \
 && go build -o /usr/local/bin/enhanced ./enhanced \
 && go build -o /usr/local/bin/challenge ./challenge

ENTRYPOINT ["/usr/local/bin/demo"]
