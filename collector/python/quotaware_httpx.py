import hashlib
import json
import queue
import threading
import time

import httpx

PATHS = {"openai": "/v1/chat/completions", "anthropic": "/v1/messages"}
OBSERVE_PATH = "/v1/observe"


def credential_hash(headers):
    lowered = {name.lower(): value for name, value in headers.items()}
    credential = lowered.get("authorization") or lowered.get("x-api-key") or ""
    if not credential:
        return "anonymous"
    return hashlib.sha256(credential.encode()).hexdigest()[:12]


def tokens(provider, payload):
    usage = payload.get("usage") if isinstance(payload, dict) else None
    if not isinstance(usage, dict):
        return None
    if provider == "anthropic":
        names = ("input_tokens", "cache_creation_input_tokens", "output_tokens")
    else:
        names = ("prompt_tokens", "completion_tokens")
    total = sum(usage.get(name) or 0 for name in names)
    return float(total) if total > 0 else None


def rate_limit_headers(headers):
    picked = {}
    for name, value in headers.items():
        lower = name.lower()
        if lower.startswith("x-ratelimit-") or lower.startswith("anthropic-ratelimit-") or lower == "retry-after":
            picked[lower] = value
    return picked


class ObservedStream(httpx.SyncByteStream):
    def __init__(self, inner, on_close):
        self._inner = inner
        self._on_close = on_close
        self._complete = False
        self._closed = False

    def __iter__(self):
        for chunk in self._inner:
            yield chunk
        self._complete = True

    def close(self):
        try:
            self._inner.close()
        finally:
            if not self._closed:
                self._closed = True
                self._on_close(self._complete)


class AsyncObservedStream(httpx.AsyncByteStream):
    def __init__(self, inner, on_close):
        self._inner = inner
        self._on_close = on_close
        self._complete = False
        self._closed = False

    async def __aiter__(self):
        async for chunk in self._inner:
            yield chunk
        self._complete = True

    async def aclose(self):
        try:
            await self._inner.aclose()
        finally:
            if not self._closed:
                self._closed = True
                self._on_close(self._complete)


def _is_stream(response):
    return response.headers.get("content-type", "").startswith("text/event-stream")


def _succeeded(response):
    return 200 <= response.status_code < 300


class QuotawareHook:
    def __init__(self, quotaware, provider, token=None, queue_size=256, timeout=5.0):
        if provider not in PATHS:
            raise ValueError("provider must be openai or anthropic")
        url = httpx.URL(quotaware.rstrip("/") + OBSERVE_PATH)
        if url.scheme not in ("http", "https") or not url.host:
            raise ValueError("quotaware must be an http(s) origin such as http://quotaware:9100")
        self.endpoint = str(url)
        self.provider = provider
        self.token = token
        self.timeout = timeout
        self.dropped = 0
        self.failed = 0
        self.errors = 0
        self._queue = queue.Queue(maxsize=queue_size)
        self._thread = threading.Thread(target=self._deliver, name="quotaware-collector", daemon=True)
        self._thread.start()

    def hooks(self):
        return {"request": [self.on_request], "response": [self.on_response]}

    def async_hooks(self):
        return {"request": [self.on_async_request], "response": [self.on_async_response]}

    def on_request(self, request):
        request.extensions["quotaware_started"] = time.monotonic()

    async def on_async_request(self, request):
        self.on_request(request)

    def on_response(self, response):
        try:
            report = self._report_for(response)
            if report is None:
                return
            if _is_stream(response):
                response.stream = ObservedStream(response.stream, report)
                return
            if _succeeded(response):
                response.read()
            report(True, self._usage(response))
        except Exception:
            self.errors += 1

    async def on_async_response(self, response):
        try:
            report = self._report_for(response)
            if report is None:
                return
            if _is_stream(response):
                response.stream = AsyncObservedStream(response.stream, report)
                return
            if _succeeded(response):
                await response.aread()
            report(True, self._usage(response))
        except Exception:
            self.errors += 1

    def _report_for(self, response):
        request = response.request
        if request.method != "POST" or not request.url.path.endswith(PATHS[self.provider]):
            return None
        body = json.loads(request.content or b"{}")
        model = body.get("model") if isinstance(body, dict) else None
        if not model:
            return None
        started = request.extensions.get("quotaware_started")
        observation = {
            "provider": self.provider,
            "model": model,
            "credential": credential_hash(request.headers),
            "status": response.status_code,
            "headers": rate_limit_headers(response.headers),
        }
        return lambda complete, usage=None: self._enqueue(observation, started, complete, usage)

    def _usage(self, response):
        if not _succeeded(response):
            return None
        return tokens(self.provider, response.json())

    def _enqueue(self, observation, started, complete, usage):
        observation = {
            **observation,
            "usage": usage,
            "latency_ms": (time.monotonic() - started) * 1000 if started else 0.0,
            "complete": complete,
        }
        try:
            self._queue.put_nowait(observation)
        except queue.Full:
            self.dropped += 1

    def _deliver(self):
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        with httpx.Client(timeout=self.timeout, headers=headers) as client:
            while True:
                item = self._queue.get()
                if item is None:
                    return
                try:
                    reply = client.post(self.endpoint, json=item)
                    if reply.status_code >= 300:
                        self.failed += 1
                except Exception:
                    self.failed += 1

    def close(self):
        self._queue.put(None)
        self._thread.join(timeout=self.timeout + 1)
