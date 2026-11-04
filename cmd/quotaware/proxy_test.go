package main

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNewServerBoundsHeaderReadsAndIdleConnections(t *testing.T) {
	server := newServer(endpoint{address: ":0", handler: http.NotFoundHandler()})

	if server.ReadHeaderTimeout != 10*time.Second || server.IdleTimeout != 2*time.Minute {
		t.Fatalf("timeouts = header %v idle %v, want 10s and 2m", server.ReadHeaderTimeout, server.IdleTimeout)
	}
	if server.WriteTimeout != 0 || server.ReadTimeout != 0 {
		t.Fatalf("write timeout = %v read timeout = %v, want none so proxied bodies and streams are never cut", server.WriteTimeout, server.ReadTimeout)
	}
}

func TestNewServerBoundsTheWholeRequestReadOnTheAdminPort(t *testing.T) {
	server := newServer(endpoint{address: ":0", handler: http.NotFoundHandler(), readTimeout: 30 * time.Second})

	if server.ReadTimeout != 30*time.Second {
		t.Fatalf("read timeout = %v, want 30s so a slow observation cannot hold a connection", server.ReadTimeout)
	}
}

func TestObserverFlags(t *testing.T) {
	t.Run("accepts origins and fills the defaults", func(t *testing.T) {
		options, err := parseObserverFlags("proxy", []string{"-openai-upstream", "http://fakeprovider:8081", "-observe-insecure"}, &bytes.Buffer{}, false)

		if err != nil {
			t.Fatal(err)
		}
		if options.proxy.OpenAI.String() != "http://fakeprovider:8081" || options.proxy.Anthropic.Host != "api.anthropic.com" {
			t.Fatalf("upstreams = %v / %v", options.proxy.OpenAI, options.proxy.Anthropic)
		}
		if options.listen != ":8080" || options.admin != ":9100" || options.alpha != 0.2 {
			t.Fatalf("options = %+v, want the documented defaults", options)
		}
		if options.limits.Buckets != 10000 || options.limits.Consumers != 1000 {
			t.Fatalf("limits = %+v, want the documented defaults", options.limits)
		}
	})

	t.Run("refuses to start without an observe token unless insecure is explicit", func(t *testing.T) {
		t.Setenv("QUOTAWARE_OBSERVE_TOKEN", "")

		_, err := parseObserverFlags("proxy", nil, &bytes.Buffer{}, false)

		if err == nil || !strings.Contains(err.Error(), "observe-insecure") {
			t.Fatalf("err = %v, want a refusal naming the insecure flag", err)
		}
	})

	t.Run("accepts a token from the flag and reads the limits", func(t *testing.T) {
		options, err := parseObserverFlags("proxy", []string{"-observe-token", "secret", "-max-buckets", "50", "-max-scaled-objects", "5"}, &bytes.Buffer{}, false)

		if err != nil {
			t.Fatal(err)
		}
		if options.observeToken != "secret" || options.limits.Buckets != 50 || options.limits.Consumers != 5 {
			t.Fatalf("options = %+v, want the token and the limits", options)
		}
	})

	t.Run("reads the observe token from the environment when the flag is absent", func(t *testing.T) {
		t.Setenv("QUOTAWARE_OBSERVE_TOKEN", "from-env")

		options, err := parseObserverFlags("proxy", nil, &bytes.Buffer{}, false)

		if err != nil {
			t.Fatal(err)
		}
		if options.observeToken != "from-env" {
			t.Fatalf("observe token = %q, want the environment value", options.observeToken)
		}
	})

	t.Run("prefers the observe token flag over the environment", func(t *testing.T) {
		t.Setenv("QUOTAWARE_OBSERVE_TOKEN", "from-env")

		options, err := parseObserverFlags("proxy", []string{"-observe-token", "from-flag"}, &bytes.Buffer{}, false)

		if err != nil {
			t.Fatal(err)
		}
		if options.observeToken != "from-flag" {
			t.Fatalf("observe token = %q, want the flag value", options.observeToken)
		}
	})

	t.Run("lets an explicitly empty observe token flag override the environment", func(t *testing.T) {
		t.Setenv("QUOTAWARE_OBSERVE_TOKEN", "from-env")

		options, err := parseObserverFlags("proxy", []string{"-observe-token=", "-observe-insecure"}, &bytes.Buffer{}, false)

		if err != nil {
			t.Fatal(err)
		}
		if options.observeToken != "" {
			t.Fatalf("observe token = %q, want the empty flag value", options.observeToken)
		}
	})

	t.Run("never prints the observe token from the environment in the usage of a wrong flag", func(t *testing.T) {
		t.Setenv("QUOTAWARE_OBSERVE_TOKEN", "do-not-print-me")
		var stderr bytes.Buffer

		code := run([]string{"all", "-no-such-flag"}, strings.NewReader(""), &bytes.Buffer{}, &stderr)

		if code != 2 || strings.Contains(stderr.String(), "do-not-print-me") {
			t.Fatalf("exit = %d, stderr = %q, want 2 without the token", code, stderr.String())
		}
	})

	t.Run("never prints the observe token from the environment in the help text", func(t *testing.T) {
		t.Setenv("QUOTAWARE_OBSERVE_TOKEN", "do-not-print-me")
		var stderr bytes.Buffer

		code := run([]string{"proxy", "-h"}, strings.NewReader(""), &bytes.Buffer{}, &stderr)

		if code != 0 || strings.Contains(stderr.String(), "do-not-print-me") || !strings.Contains(stderr.String(), "QUOTAWARE_OBSERVE_TOKEN") {
			t.Fatalf("exit = %d, stderr = %q, want 0 naming the variable but not its value", code, stderr.String())
		}
	})

	t.Run("reads the body timeout flag", func(t *testing.T) {
		options, err := parseObserverFlags("proxy", []string{"-read-body-timeout", "5s", "-observe-insecure"}, &bytes.Buffer{}, false)

		if err != nil {
			t.Fatal(err)
		}
		if options.proxy.ReadBodyTimeout != 5*time.Second {
			t.Fatalf("read body timeout = %v, want 5s", options.proxy.ReadBodyTimeout)
		}
	})

	t.Run("adds the grpc address for the all command", func(t *testing.T) {
		options, err := parseObserverFlags("all", []string{"-grpc", ":9999", "-observe-insecure"}, &bytes.Buffer{}, true)

		if err != nil {
			t.Fatal(err)
		}
		if options.grpc != ":9999" || options.pushInterval <= 0 {
			t.Fatalf("options = %+v, want the grpc address and a push interval", options)
		}
	})

	t.Run("reads the redis addresses queue mode may use", func(t *testing.T) {
		options, err := parseObserverFlags("all", []string{"-redis-address", "redis:6379, redis-b:6379", "-observe-insecure"}, &bytes.Buffer{}, true)

		if err != nil {
			t.Fatal(err)
		}
		if len(options.redisAddresses) != 2 || options.redisAddresses[1] != "redis-b:6379" {
			t.Fatalf("redis addresses = %v, want both trimmed entries", options.redisAddresses)
		}
	})

	t.Run("rejects the grpc flag for the proxy command", func(t *testing.T) {
		_, err := parseObserverFlags("proxy", []string{"-grpc", ":9999"}, &bytes.Buffer{}, false)

		if err == nil {
			t.Fatal("expected an unknown flag error")
		}
	})

	t.Run("rejects an upstream with a path", func(t *testing.T) {
		_, err := parseObserverFlags("proxy", []string{"-anthropic-upstream", "https://api.anthropic.com/v1"}, &bytes.Buffer{}, false)

		if err == nil || !strings.Contains(err.Error(), "anthropic-upstream") {
			t.Fatalf("err = %v, want it to name the flag", err)
		}
	})

	t.Run("rejects an upstream without a scheme", func(t *testing.T) {
		_, err := parseObserverFlags("proxy", []string{"-openai-upstream", "fakeprovider:8081", "-observe-insecure"}, &bytes.Buffer{}, false)

		if err == nil || !strings.Contains(err.Error(), "scheme") {
			t.Fatalf("err = %v, want a complaint about the scheme", err)
		}
	})

	t.Run("rejects an upstream without a host", func(t *testing.T) {
		_, err := parseObserverFlags("proxy", []string{"-openai-upstream", "http://", "-observe-insecure"}, &bytes.Buffer{}, false)

		if err == nil || !strings.Contains(err.Error(), "host") {
			t.Fatalf("err = %v, want a complaint about the host", err)
		}
	})

	t.Run("rejects the same address for the proxy and the admin port", func(t *testing.T) {
		_, err := parseObserverFlags("proxy", []string{"-listen", ":8080", "-admin", ":8080"}, &bytes.Buffer{}, false)

		if err == nil || !strings.Contains(err.Error(), "differ") {
			t.Fatalf("err = %v, want a complaint about identical addresses", err)
		}
	})

	t.Run("rejects a grpc address that collides with the admin port", func(t *testing.T) {
		_, err := parseObserverFlags("all", []string{"-grpc", ":9100"}, &bytes.Buffer{}, true)

		if err == nil || !strings.Contains(err.Error(), "grpc") {
			t.Fatalf("err = %v, want a complaint about the grpc address", err)
		}
	})

	t.Run("exits with 2 on an invalid alpha", func(t *testing.T) {
		var stderr bytes.Buffer

		code := run([]string{"all", "-ewma-alpha", "2", "-observe-insecure"}, strings.NewReader(""), &bytes.Buffer{}, &stderr)

		if code != 2 || !strings.Contains(stderr.String(), "alpha") {
			t.Fatalf("exit = %d, stderr = %q, want 2 naming alpha", code, stderr.String())
		}
	})
}

func TestListeningAttributesRedactUpstreamPasswords(t *testing.T) {
	options, err := parseObserverFlags("proxy", []string{"-openai-upstream", "https://user:s3cret@gateway.internal", "-observe-insecure"}, &bytes.Buffer{}, false)
	if err != nil {
		t.Fatal(err)
	}

	attributes := fmt.Sprint(listeningAttributes("proxy", options))

	if strings.Contains(attributes, "s3cret") || !strings.Contains(attributes, "gateway.internal") {
		t.Fatalf("attributes = %s, want the upstream logged without its password", attributes)
	}
}
