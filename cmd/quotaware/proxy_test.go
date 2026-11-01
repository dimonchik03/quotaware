package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestProxyFlags(t *testing.T) {
	t.Run("accepts origins and fills the defaults", func(t *testing.T) {
		options, err := parseProxyFlags([]string{"-openai-upstream", "http://fakeprovider:8081"}, &bytes.Buffer{})

		if err != nil {
			t.Fatal(err)
		}
		if options.proxy.OpenAI.String() != "http://fakeprovider:8081" || options.proxy.Anthropic.Host != "api.anthropic.com" {
			t.Fatalf("upstreams = %v / %v", options.proxy.OpenAI, options.proxy.Anthropic)
		}
		if options.listen != ":8080" || options.admin != ":9100" || options.alpha != 0.2 {
			t.Fatalf("options = %+v, want the documented defaults", options)
		}
	})

	t.Run("rejects an upstream with a path", func(t *testing.T) {
		_, err := parseProxyFlags([]string{"-anthropic-upstream", "https://api.anthropic.com/v1"}, &bytes.Buffer{})

		if err == nil || !strings.Contains(err.Error(), "anthropic-upstream") {
			t.Fatalf("err = %v, want it to name the flag", err)
		}
	})

	t.Run("rejects an upstream without a host", func(t *testing.T) {
		_, err := parseProxyFlags([]string{"-openai-upstream", "fakeprovider:8081"}, &bytes.Buffer{})

		if err == nil {
			t.Fatal("expected an error for a bare host without scheme")
		}
	})

	t.Run("rejects the same address for the proxy and the admin port", func(t *testing.T) {
		_, err := parseProxyFlags([]string{"-listen", ":8080", "-admin", ":8080"}, &bytes.Buffer{})

		if err == nil || !strings.Contains(err.Error(), "differ") {
			t.Fatalf("err = %v, want a complaint about identical addresses", err)
		}
	})

	t.Run("exits with 2 on an invalid alpha", func(t *testing.T) {
		var stderr bytes.Buffer

		code := run([]string{"proxy", "-ewma-alpha", "2"}, strings.NewReader(""), &bytes.Buffer{}, &stderr)

		if code != 2 || !strings.Contains(stderr.String(), "alpha") {
			t.Fatalf("exit = %d, stderr = %q, want 2 naming alpha", code, stderr.String())
		}
	})
}
