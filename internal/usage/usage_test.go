package usage_test

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"

	"github.com/dimonchik03/quotaware/internal/headers"
	"github.com/dimonchik03/quotaware/internal/usage"
)

func TestExtractUsage(t *testing.T) {
	cases := []struct {
		name        string
		provider    headers.Provider
		contentType string
		body        string
		wantTokens  float64
		wantOK      bool
	}{
		{
			name:        "OpenAI JSON counts prompt plus completion tokens",
			provider:    headers.OpenAI,
			contentType: "application/json",
			body:        `{"id":"chatcmpl-1","usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150,"prompt_tokens_details":{"cached_tokens":80}}}`,
			wantTokens:  150,
			wantOK:      true,
		},
		{
			name:        "an OpenAI stream takes the final usage chunk and ignores null usage on the way",
			provider:    headers.OpenAI,
			contentType: "text/event-stream; charset=utf-8",
			body: strings.Join([]string{
				`data: {"choices":[{"delta":{"content":"a"}}],"usage":null}`,
				``,
				`data: {"choices":[{"delta":{"content":"b"}}],"usage":null}`,
				``,
				`data: {"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":2}}`,
				``,
				`data: [DONE]`,
				``,
			}, "\n"),
			wantTokens: 22,
			wantOK:     true,
		},
		{
			name:        "an OpenAI stream without a usage chunk reports nothing",
			provider:    headers.OpenAI,
			contentType: "text/event-stream",
			body:        "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: [DONE]\n\n",
			wantOK:      false,
		},
		{
			name:        "Anthropic JSON counts uncached input, cache writes and output",
			provider:    headers.Anthropic,
			contentType: "application/json",
			body:        `{"id":"msg_1","usage":{"input_tokens":10,"cache_creation_input_tokens":7,"cache_read_input_tokens":500,"output_tokens":5}}`,
			wantTokens:  22,
			wantOK:      true,
		},
		{
			name:        "an Anthropic stream merges message_start input with message_delta output",
			provider:    headers.Anthropic,
			contentType: "text/event-stream",
			body: strings.Join([]string{
				`event: message_start`,
				`data: {"type":"message_start","message":{"usage":{"input_tokens":30,"cache_creation_input_tokens":4,"output_tokens":1}}}`,
				``,
				`event: content_block_delta`,
				`data: {"type":"content_block_delta","delta":{"text":"hi"}}`,
				``,
				`event: message_delta`,
				`data: {"type":"message_delta","usage":{"output_tokens":12}}`,
				``,
				`event: message_stop`,
				`data: {"type":"message_stop"}`,
				``,
			}, "\n"),
			wantTokens: 46,
			wantOK:     true,
		},
		{
			name:        "a body without usage reports nothing",
			provider:    headers.OpenAI,
			contentType: "application/json",
			body:        `{"error":{"message":"boom"}}`,
			wantOK:      false,
		},
		{
			name:        "a body that is not JSON reports nothing",
			provider:    headers.OpenAI,
			contentType: "application/json",
			body:        `<html>`,
			wantOK:      false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tokens, ok := usage.Extract(tc.provider, tc.contentType, []byte(tc.body))

			if ok != tc.wantOK || tokens != tc.wantTokens {
				t.Fatalf("tokens = %v ok = %v, want %v %v", tokens, ok, tc.wantTokens, tc.wantOK)
			}
		})
	}
}

func TestDecompress(t *testing.T) {
	payload := []byte(`{"usage":{"prompt_tokens":1,"completion_tokens":2}}`)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	t.Run("passes a body through under every encoding that means none", func(t *testing.T) {
		for _, encoding := range []string{"", "identity", " Identity "} {
			body, ok := usage.Decompress(encoding, payload, 1024)
			if !ok || !bytes.Equal(body, payload) {
				t.Fatalf("%q: body = %q ok = %v, want the body unchanged", encoding, body, ok)
			}
		}
	})

	t.Run("inflates a gzip body", func(t *testing.T) {
		body, ok := usage.Decompress("gzip", compressed.Bytes(), 1024)
		if !ok || !bytes.Equal(body, payload) {
			t.Fatalf("body = %q ok = %v", body, ok)
		}
	})

	t.Run("refuses a gzip body that inflates past the limit", func(t *testing.T) {
		if _, ok := usage.Decompress("gzip", compressed.Bytes(), 10); ok {
			t.Fatal("expected the inflated body to be refused")
		}
	})

	t.Run("refuses an encoding it cannot inflate", func(t *testing.T) {
		if _, ok := usage.Decompress("br", payload, 1024); ok {
			t.Fatal("expected brotli to be refused")
		}
	})
}
