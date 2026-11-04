package usage

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	ObservePath    = "/v1/observe"
	MaxModelBytes  = 256
	MaxLatency     = time.Hour
	MaxUsageTokens = 1e9
)

var credentialPattern = regexp.MustCompile(`^([0-9a-f]{12}|anonymous)$`)

func ValidCredential(credential string) bool {
	return credentialPattern.MatchString(credential)
}

func ValidProvider(provider string) bool {
	return provider == "openai" || provider == "anthropic"
}

type Observation struct {
	Provider     string            `json:"provider"`
	Model        string            `json:"model"`
	Credential   string            `json:"credential"`
	Status       int               `json:"status"`
	Headers      map[string]string `json:"headers"`
	Usage        *float64          `json:"usage,omitempty"`
	LatencyMS    float64           `json:"latency_ms"`
	HeadersAgeMS float64           `json:"headers_age_ms,omitempty"`
	Complete     bool              `json:"complete"`
}

func (o Observation) Validate() error {
	switch {
	case !ValidProvider(o.Provider):
		return errors.New("provider must be openai or anthropic")
	case o.Model == "" || len(o.Model) > MaxModelBytes:
		return fmt.Errorf("model is required and at most %d bytes", MaxModelBytes)
	case !ValidCredential(o.Credential):
		return errors.New("credential must be the 12-character hash or anonymous")
	case o.Status < 100 || o.Status > 599:
		return errors.New("status must be an HTTP status code")
	case o.LatencyMS < 0 || o.LatencyMS > float64(MaxLatency/time.Millisecond):
		return errors.New("latency_ms must be between 0 and one hour")
	case o.HeadersAgeMS < 0 || o.HeadersAgeMS > float64(MaxLatency/time.Millisecond):
		return errors.New("headers_age_ms must be between 0 and one hour")
	case o.Usage != nil && (*o.Usage < 0 || *o.Usage > MaxUsageTokens):
		return errors.New("usage must be between 0 and 1e9 tokens")
	}
	return nil
}
