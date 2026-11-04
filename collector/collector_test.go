package collector_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/collector"
	"github.com/dimonchik03/quotaware/internal/usage"
)

type sink struct {
	mu           sync.Mutex
	observations []collector.Observation
	tokens       []string
	release      chan struct{}
	server       *httptest.Server
}

func newSink(t *testing.T, block bool) *sink {
	t.Helper()
	s := &sink{release: make(chan struct{})}
	if !block {
		close(s.release)
	}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-s.release
		if r.URL.Path != usage.ObservePath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var observation collector.Observation
		if err := json.NewDecoder(r.Body).Decode(&observation); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.observations = append(s.observations, observation)
		s.tokens = append(s.tokens, r.Header.Get("Authorization"))
		s.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *sink) all() []collector.Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]collector.Observation{}, s.observations...)
}

func provider(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func rateLimited(w http.ResponseWriter) {
	w.Header().Set("x-ratelimit-limit-requests", "60")
	w.Header().Set("x-ratelimit-remaining-requests", "59")
	w.Header().Set("x-ratelimit-reset-requests", "1s")
}

func openAI(w http.ResponseWriter, _ *http.Request) {
	rateLimited(w)
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"usage":{"prompt_tokens":100,"completion_tokens":50}}`)
}

func openAIStream(w http.ResponseWriter, _ *http.Request) {
	rateLimited(w)
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}],\"usage\":null}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
}

func newCollector(t *testing.T, quotaware string, mutate func(*collector.Config)) *collector.Collector {
	t.Helper()
	cfg := collector.DefaultConfig(quotaware, "openai")
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := collector.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func call(client *http.Client, url, body string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer sk-collector")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return response, nil
}

func mustCall(t *testing.T, client *http.Client, url, body string) *http.Response {
	t.Helper()
	response, err := call(client, url, body)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

const chat = `{"model":"gpt-4.1-mini","messages":[]}`

func TestCollector(t *testing.T) {
	t.Run("observes a provider call and posts it with the token", func(t *testing.T) {
		s := newSink(t, false)
		upstream := provider(t, openAI)
		c := newCollector(t, s.server.URL, func(cfg *collector.Config) { cfg.Token = "secret" })
		client := &http.Client{Transport: c.Wrap(nil)}

		response := mustCall(t, client, upstream.URL, chat)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}

		if response.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", response.StatusCode)
		}
		observations := s.all()
		if len(observations) != 1 {
			t.Fatalf("observations = %d, want 1", len(observations))
		}
		o := observations[0]
		if o.Provider != "openai" || o.Model != "gpt-4.1-mini" || o.Status != 200 || !o.Complete {
			t.Fatalf("observation = %+v", o)
		}
		if o.Headers["x-ratelimit-remaining-requests"] != "59" || o.Usage == nil || *o.Usage != 150 || o.LatencyMS <= 0 {
			t.Fatalf("observation = %+v, want the rate-limit headers, 150 tokens and a latency", o)
		}
		if len(o.Credential) != 12 || strings.Contains(o.Credential, "sk-") || s.tokens[0] != "Bearer secret" {
			t.Fatalf("credential = %q token = %q, want a hash and the bearer token", o.Credential, s.tokens[0])
		}
	})

	t.Run("reads usage from a stream that includes it", func(t *testing.T) {
		s := newSink(t, false)
		upstream := provider(t, openAIStream)
		c := newCollector(t, s.server.URL, nil)
		client := &http.Client{Transport: c.Wrap(nil)}

		mustCall(t, client, upstream.URL, `{"model":"gpt-4.1-mini","stream":true,"stream_options":{"include_usage":true},"messages":[]}`)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}

		if o := s.all()[0]; o.Usage == nil || *o.Usage != 22 || !o.Complete {
			t.Fatalf("observation = %+v, want 22 tokens from the final chunk", o)
		}
	})

	t.Run("reports a 429 with its retry-after and no usage", func(t *testing.T) {
		s := newSink(t, false)
		upstream := provider(t, func(w http.ResponseWriter, _ *http.Request) {
			rateLimited(w)
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":{"type":"rate_limit"}}`)
		})
		c := newCollector(t, s.server.URL, nil)
		client := &http.Client{Transport: c.Wrap(nil)}

		response := mustCall(t, client, upstream.URL, chat)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}

		o := s.all()[0]
		if response.StatusCode != http.StatusTooManyRequests || o.Status != http.StatusTooManyRequests || o.Headers["retry-after"] != "7" || o.Usage != nil || !o.Complete {
			t.Fatalf("observation = %+v, want the 429, its retry-after, no usage and a complete body", o)
		}
	})

	t.Run("reports how long before the end of a stream its headers arrived", func(t *testing.T) {
		s := newSink(t, false)
		upstream := provider(t, func(w http.ResponseWriter, _ *http.Request) {
			rateLimited(w)
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			time.Sleep(60 * time.Millisecond)
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		})
		c := newCollector(t, s.server.URL, nil)
		client := &http.Client{Transport: c.Wrap(nil)}

		mustCall(t, client, upstream.URL, chat)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}

		o := s.all()[0]
		if o.HeadersAgeMS < 50 || o.LatencyMS < 50 {
			t.Fatalf("headers age = %v ms latency = %v ms, want the 60 ms the body took after the headers", o.HeadersAgeMS, o.LatencyMS)
		}
	})

	t.Run("leaves other requests alone", func(t *testing.T) {
		s := newSink(t, false)
		upstream := provider(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `[]`) })
		c := newCollector(t, s.server.URL, nil)
		client := &http.Client{Transport: c.Wrap(nil)}

		response, err := client.Get(upstream.URL + "/v1/models")
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}

		if len(s.all()) != 0 || c.Unobserved() != 0 {
			t.Fatalf("observations = %d unobserved = %d, want none", len(s.all()), c.Unobserved())
		}
	})

	t.Run("forwards an oversized request unobserved", func(t *testing.T) {
		s := newSink(t, false)
		var received int
		upstream := provider(t, func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			received = len(body)
			openAI(w, r)
		})
		c := newCollector(t, s.server.URL, func(cfg *collector.Config) { cfg.MaxRequestBytes = 64 })
		client := &http.Client{Transport: c.Wrap(nil)}
		body := `{"model":"gpt-4.1-mini","messages":[{"content":"` + strings.Repeat("x", 200) + `"}]}`

		response := mustCall(t, client, upstream.URL, body)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}

		if response.StatusCode != http.StatusOK || received != len(body) {
			t.Fatalf("status = %d received = %d, want the full body forwarded", response.StatusCode, received)
		}
		if len(s.all()) != 0 || c.Unobserved() != 1 {
			t.Fatalf("observations = %d unobserved = %d, want the call skipped and counted", len(s.all()), c.Unobserved())
		}
	})

	t.Run("counts a failed upstream call as unobserved and returns the error", func(t *testing.T) {
		s := newSink(t, false)
		closed := httptest.NewServer(http.NotFoundHandler())
		closed.Close()
		c := newCollector(t, s.server.URL, nil)
		client := &http.Client{Transport: c.Wrap(nil)}

		_, err := call(client, closed.URL, chat)
		if closeErr := c.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}

		if err == nil || c.Unobserved() != 1 || len(s.all()) != 0 {
			t.Fatalf("err = %v unobserved = %d, want the transport error surfaced and counted", err, c.Unobserved())
		}
	})

	t.Run("drops observations instead of blocking the caller when the queue is full", func(t *testing.T) {
		s := newSink(t, true)
		upstream := provider(t, openAI)
		c := newCollector(t, s.server.URL, func(cfg *collector.Config) { cfg.QueueSize = 1 })
		client := &http.Client{Transport: c.Wrap(nil)}

		failures := make(chan error, 1)
		go func() {
			for range 4 {
				if _, err := call(client, upstream.URL, chat); err != nil {
					failures <- err
					return
				}
			}
			close(failures)
		}()
		select {
		case err, open := <-failures:
			if open {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("calls blocked on a full observation queue")
		}
		close(s.release)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}

		if c.Dropped() == 0 {
			t.Fatal("expected dropped observations")
		}
	})

	t.Run("counts deliveries quotaware rejects and closes more than once", func(t *testing.T) {
		rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadRequest) }))
		t.Cleanup(rejecting.Close)
		upstream := provider(t, openAI)
		c := newCollector(t, rejecting.URL, nil)
		client := &http.Client{Transport: c.Wrap(nil)}

		mustCall(t, client, upstream.URL, chat)
		if err := errors.Join(c.Close(), c.Close()); err != nil {
			t.Fatal(err)
		}

		if c.Failed() != 1 {
			t.Fatalf("failed = %d, want 1", c.Failed())
		}
	})

	t.Run("close returns within its deadline when quotaware hangs and says it lost observations", func(t *testing.T) {
		s := newSink(t, true)
		upstream := provider(t, openAI)
		c := newCollector(t, s.server.URL, func(cfg *collector.Config) {
			cfg.PostTimeout = time.Second
			cfg.CloseTimeout = 300 * time.Millisecond
		})
		client := &http.Client{Transport: c.Wrap(nil)}
		mustCall(t, client, upstream.URL, chat)
		mustCall(t, client, upstream.URL, chat)

		started := time.Now()
		err := c.Close()
		close(s.release)

		if !errors.Is(err, collector.ErrUndelivered) {
			t.Fatalf("err = %v, want ErrUndelivered", err)
		}

		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("close took %v, want it bounded by the timeouts", elapsed)
		}
	})

	t.Run("counts an observation made after close as dropped", func(t *testing.T) {
		s := newSink(t, false)
		upstream := provider(t, openAI)
		c := newCollector(t, s.server.URL, nil)
		client := &http.Client{Transport: c.Wrap(nil)}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}

		mustCall(t, client, upstream.URL, chat)

		if c.Dropped() != 1 || len(s.all()) != 0 {
			t.Fatalf("dropped = %d delivered = %d, want the late observation dropped", c.Dropped(), len(s.all()))
		}
	})

	t.Run("closes the request body when reading it fails", func(t *testing.T) {
		c := newCollector(t, "http://127.0.0.1:1", nil)
		body := &failingBody{}
		request, err := http.NewRequest(http.MethodPost, "http://provider.test/v1/chat/completions", body)
		if err != nil {
			t.Fatal(err)
		}

		_, err = c.Wrap(nil).RoundTrip(request)

		if err == nil || !body.closed {
			t.Fatalf("err = %v closed = %v, want the read error and the body closed", err, body.closed)
		}
	})

	t.Run("rejects an unknown provider", func(t *testing.T) {
		if _, err := collector.New(collector.DefaultConfig("http://quotaware:9100", "gemini")); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func ExampleNew() {
	c, err := collector.New(collector.DefaultConfig("http://quotaware:9100", "openai"))
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()
	client := &http.Client{Transport: c.Wrap(http.DefaultTransport)}
	fmt.Println(client.Transport != nil)
	// Output: true
}

type failingBody struct {
	closed bool
}

func (b *failingBody) Read([]byte) (int, error) {
	return 0, errors.New("client went away")
}

func (b *failingBody) Close() error {
	b.closed = true
	return nil
}

func TestHeaderAgeIncludesTimeInTheDeliveryQueue(t *testing.T) {
	s := newSink(t, true)
	upstream := provider(t, openAI)
	c := newCollector(t, s.server.URL, nil)
	client := &http.Client{Transport: c.Wrap(nil)}
	mustCall(t, client, upstream.URL, chat)
	mustCall(t, client, upstream.URL, chat)
	time.Sleep(50 * time.Millisecond)
	close(s.release)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	observations := s.all()
	if len(observations) != 2 || observations[1].HeadersAgeMS < 50 {
		t.Fatalf("delivered observations = %+v", observations)
	}
}
