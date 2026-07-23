module github.com/jyswee/oddsockets-go-sdk/demo

go 1.19

require github.com/jyswee/oddsockets-go-sdk v0.0.0

// The SDK is not published to a module proxy, so resolve it from the parent
// directory in this repo. This makes the demo clone-and-run.
replace github.com/jyswee/oddsockets-go-sdk => ../

require (
	github.com/google/uuid v1.3.1 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
)
