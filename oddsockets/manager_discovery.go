package oddsockets

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// DefaultManagerURL is the hosted OddSockets manager endpoint.
//
// It only applies when no manager URL has been configured at all. It is never
// used to recover from a configured manager that is unreachable: silently
// redirecting a self-hosted or QA deployment at production would make a broken
// setup look healthy and would send traffic to the wrong cluster.
const DefaultManagerURL = "https://connect.oddsockets.tyga.network"

// ManagerURLEnvVar is the environment variable consulted when no manager URL is
// supplied in code.
const ManagerURLEnvVar = "ODDSOCKETS_MANAGER_URL"

// ManagerDiscovery resolves the manager endpoint used for worker assignment.
type ManagerDiscovery struct {
	managerURL string
}

// NewManagerDiscovery creates a manager discovery instance bound to the given
// manager URL. Precedence is: managerURL argument, then ODDSOCKETS_MANAGER_URL,
// then DefaultManagerURL.
func NewManagerDiscovery(managerURL string) (*ManagerDiscovery, error) {
	resolved, err := ResolveManagerURL(managerURL)
	if err != nil {
		return nil, err
	}

	return &ManagerDiscovery{managerURL: resolved}, nil
}

// ResolveManagerURL applies the manager URL precedence rules and validates the
// result. The returned URL has any trailing slashes removed.
func ResolveManagerURL(managerURL string) (string, error) {
	candidate := strings.TrimSpace(managerURL)
	if candidate == "" {
		candidate = strings.TrimSpace(os.Getenv(ManagerURLEnvVar))
	}
	if candidate == "" {
		candidate = DefaultManagerURL
	}

	normalized := strings.TrimRight(candidate, "/")

	parsed, err := url.Parse(normalized)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", fmt.Errorf("Invalid managerUrl: %s", candidate)
	}

	return normalized, nil
}

// ManagerURL returns the resolved manager URL used for worker assignment.
func (md *ManagerDiscovery) ManagerURL() string {
	return md.managerURL
}

// DiscoverManagerURL returns the configured manager URL. The manager itself
// handles routing and load balancing across workers.
func (md *ManagerDiscovery) DiscoverManagerURL(apiKey string) (string, error) {
	return md.managerURL, nil
}

// ClearCache clears any cached discovery data (no-op for compatibility)
func (md *ManagerDiscovery) ClearCache() {
	// No cache to clear in simplified version
}

// generateClientIdentifier creates a consistent client identifier for session stickiness
func generateClientIdentifier(apiKey, userID string) string {
	// Create a consistent identifier based on API key and user ID
	baseID := userID
	if baseID == "" {
		baseID = "default"
	}

	// Simple hash function for API key
	hash := sha256.Sum256([]byte(apiKey))
	apiKeyHash := hex.EncodeToString(hash[:])[:8]

	return fmt.Sprintf("%s_%s", apiKeyHash, baseID)
}
