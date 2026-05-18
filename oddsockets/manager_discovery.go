package oddsockets

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// ManagerDiscovery handles discovery of the optimal manager URL
type ManagerDiscovery struct {
	managerURL string
}

// NewManagerDiscovery creates a new manager discovery instance
func NewManagerDiscovery() *ManagerDiscovery {
	return &ManagerDiscovery{
		managerURL: "https://manager1.oddsockets.tyga.network",
	}
}

// DiscoverManagerURL returns the manager URL
// Always returns the main endpoint which handles all routing and load balancing transparently
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
