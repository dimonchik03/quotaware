# quotaware-httpx

httpx event hooks that post each OpenAI or Anthropic call's rate-limit headers,
usage, latency and status to quotaware's `/v1/observe`, so quotaware can size worker
deployments without proxying the calls.

    import os

    from openai import AsyncOpenAI, DefaultAsyncHttpxClient, DefaultHttpxClient, OpenAI
    from quotaware_httpx import QuotawareHook

    hook = QuotawareHook("http://quotaware:9100", "openai", token=os.environ["QUOTAWARE_OBSERVE_TOKEN"])
    client = OpenAI(http_client=DefaultHttpxClient(event_hooks=hook.hooks()))
    async_client = AsyncOpenAI(http_client=DefaultAsyncHttpxClient(event_hooks=hook.async_hooks()))

The hook reads bodies as the caller consumes them. It captures up to 4 MiB and sends
headers even when usage is unavailable. Header age includes time spent waiting for
delivery. Call `hook.close()` when shutting down to drain queued observations.
[Collector setup and metrics](https://github.com/dimonchik03/quotaware#without-the-proxy-hop).
