package fakeprovider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func chatCompletion(c call) map[string]any {
	return map[string]any{
		"id":      fmt.Sprintf("chatcmpl-%06d", c.id),
		"object":  "chat.completion",
		"created": c.now.Unix(),
		"model":   c.request.Model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": strings.Join(c.words, " ")},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{
			"prompt_tokens":     c.input,
			"completion_tokens": c.output,
			"total_tokens":      c.input + c.output,
		},
	}
}

func message(c call) map[string]any {
	return map[string]any{
		"id":            fmt.Sprintf("msg_%06d", c.id),
		"type":          "message",
		"role":          "assistant",
		"model":         c.request.Model,
		"content":       []map[string]any{{"type": "text", "text": strings.Join(c.words, " ")}},
		"stop_reason":   "end_turn",
		"stop_sequence": nil,
		"usage": map[string]any{
			"input_tokens":                c.input,
			"output_tokens":               c.output,
			"cache_creation_input_tokens": 0,
			"cache_read_input_tokens":     0,
		},
	}
}

func openAIRateLimitError(model string) map[string]any {
	return map[string]any{"error": map[string]any{
		"message": fmt.Sprintf("Rate limit reached for %s. Please try again later.", model),
		"type":    "requests",
		"param":   nil,
		"code":    "rate_limit_exceeded",
	}}
}

func anthropicRateLimitError() map[string]any {
	return map[string]any{"type": "error", "error": map[string]any{
		"type":    "rate_limit_error",
		"message": "This request would exceed the rate limit for your organization.",
	}}
}

func (s *Server) streamChatCompletion(w http.ResponseWriter, c call) {
	stream := newStream(w)
	chunk := func(delta map[string]any, finish any) map[string]any {
		return map[string]any{
			"id":      fmt.Sprintf("chatcmpl-%06d", c.id),
			"object":  "chat.completion.chunk",
			"created": c.now.Unix(),
			"model":   c.request.Model,
			"choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}},
		}
	}
	stream.data(chunk(map[string]any{"role": "assistant", "content": ""}, nil))
	for _, word := range c.words {
		stream.data(chunk(map[string]any{"content": word + " "}, nil))
	}
	stream.data(chunk(map[string]any{}, "stop"))
	if c.request.StreamOptions != nil && c.request.StreamOptions.IncludeUsage {
		stream.data(map[string]any{
			"id":      fmt.Sprintf("chatcmpl-%06d", c.id),
			"object":  "chat.completion.chunk",
			"created": c.now.Unix(),
			"model":   c.request.Model,
			"choices": []any{},
			"usage": map[string]any{
				"prompt_tokens":     c.input,
				"completion_tokens": c.output,
				"total_tokens":      c.input + c.output,
			},
		})
	}
	stream.raw("data: [DONE]\n\n")
}

func (s *Server) streamMessage(w http.ResponseWriter, c call) {
	stream := newStream(w)
	stream.event("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            fmt.Sprintf("msg_%06d", c.id),
			"type":          "message",
			"role":          "assistant",
			"model":         c.request.Model,
			"content":       []any{},
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage": map[string]any{
				"input_tokens":                c.input,
				"output_tokens":               1,
				"cache_creation_input_tokens": 0,
				"cache_read_input_tokens":     0,
			},
		},
	})
	stream.event("content_block_start", map[string]any{
		"type": "content_block_start", "index": 0,
		"content_block": map[string]any{"type": "text", "text": ""},
	})
	for _, word := range c.words {
		stream.event("content_block_delta", map[string]any{
			"type": "content_block_delta", "index": 0,
			"delta": map[string]any{"type": "text_delta", "text": word + " "},
		})
	}
	stream.event("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
	stream.event("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": c.output},
	})
	stream.event("message_stop", map[string]any{"type": "message_stop"})
}

type stream struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

func newStream(w http.ResponseWriter) *stream {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	return &stream{w: w, flusher: flusher}
}

func (s *stream) event(name string, payload any) {
	s.raw("event: " + name + "\n")
	s.data(payload)
}

func (s *stream) data(payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return
	}
	s.raw("data: " + string(encoded) + "\n\n")
}

func (s *stream) raw(text string) {
	_, _ = fmt.Fprint(s.w, text)
	if s.flusher != nil {
		s.flusher.Flush()
	}
}
