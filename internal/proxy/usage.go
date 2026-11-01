package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"

	"github.com/dimonchik03/quotaware/internal/headers"
)

type usagePayload struct {
	PromptTokens        int `json:"prompt_tokens"`
	CompletionTokens    int `json:"completion_tokens"`
	InputTokens         int `json:"input_tokens"`
	OutputTokens        int `json:"output_tokens"`
	CacheCreationTokens int `json:"cache_creation_input_tokens"`
}

func (u usagePayload) tokens(provider headers.Provider) float64 {
	if provider == headers.Anthropic {
		return float64(u.InputTokens + u.CacheCreationTokens + u.OutputTokens)
	}
	return float64(u.PromptTokens + u.CompletionTokens)
}

func extractUsage(provider headers.Provider, contentType string, body []byte) (float64, bool) {
	if strings.HasPrefix(strings.ToLower(contentType), "text/event-stream") {
		return streamUsage(provider, body)
	}
	var envelope struct {
		Usage *usagePayload `json:"usage"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Usage == nil {
		return 0, false
	}
	tokens := envelope.Usage.tokens(provider)
	return tokens, tokens > 0
}

func streamUsage(provider headers.Provider, body []byte) (float64, bool) {
	var merged usagePayload
	found := false
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64*1024), len(body)+1)
	for scanner.Scan() {
		payload, ok := strings.CutPrefix(scanner.Text(), "data:")
		payload = strings.TrimSpace(payload)
		if !ok || payload == "" || payload == "[DONE]" {
			continue
		}
		if provider == headers.Anthropic {
			found = mergeAnthropicEvent(&merged, []byte(payload)) || found
			continue
		}
		var chunk struct {
			Usage *usagePayload `json:"usage"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err == nil && chunk.Usage != nil {
			merged, found = *chunk.Usage, true
		}
	}
	tokens := merged.tokens(provider)
	return tokens, found && tokens > 0
}

func mergeAnthropicEvent(merged *usagePayload, payload []byte) bool {
	var event struct {
		Type    string `json:"type"`
		Message *struct {
			Usage *usagePayload `json:"usage"`
		} `json:"message"`
		Usage *usagePayload `json:"usage"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		return false
	}
	switch {
	case event.Type == "message_start" && event.Message != nil && event.Message.Usage != nil:
		merged.InputTokens = event.Message.Usage.InputTokens
		merged.CacheCreationTokens = event.Message.Usage.CacheCreationTokens
		merged.OutputTokens = event.Message.Usage.OutputTokens
		return true
	case event.Type == "message_delta" && event.Usage != nil:
		merged.OutputTokens = event.Usage.OutputTokens
		return true
	}
	return false
}
