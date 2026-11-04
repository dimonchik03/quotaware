import hashlib
import json
import logging
import os
import queue
import threading
import time
import weakref
import zlib

import httpx

PATHS = {"openai": "/v1/chat/completions", "anthropic": "/v1/messages"}
OBSERVE_PATH = "/v1/observe"
MAX_CAPTURE_BYTES = 4 << 20
MAX_REQUEST_BYTES = 8 << 20
LOGGER = logging.getLogger("quotaware_httpx")


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


def stream_tokens(provider, body):
    merged = {}
    found = False
    for line in body.splitlines():
        if not line.startswith(b"data:"):
            continue
        payload = line[len(b"data:"):].strip()
        if not payload or payload == b"[DONE]":
            continue
        try:
            event = json.loads(payload)
        except ValueError:
            continue
        if not isinstance(event, dict):
            continue
        if provider != "anthropic":
            if isinstance(event.get("usage"), dict):
                merged, found = event["usage"], True
            continue
        message = event.get("message")
        if event.get("type") == "message_start" and isinstance(message, dict) and isinstance(message.get("usage"), dict):
            started = message["usage"]
            merged = {name: started.get(name) for name in ("input_tokens", "cache_creation_input_tokens", "output_tokens")}
            found = True
        elif event.get("type") == "message_delta" and isinstance(event.get("usage"), dict):
            merged["output_tokens"] = event["usage"].get("output_tokens")
            found = True
    return tokens(provider, {"usage": merged}) if found else None


def rate_limit_headers(headers):
    picked = {}
    for name, value in headers.multi_items():
        lower = name.lower()
        if lower in picked:
            continue
        if lower.startswith("x-ratelimit-") or lower.startswith("anthropic-ratelimit-") or lower == "retry-after":
            picked[lower] = value
    return picked


def _decoded(encoding, body):
    encoding = encoding.strip().lower()
    if encoding == "":
        return body
    if encoding != "gzip":
        return None
    inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
    decoded = inflater.decompress(body, MAX_CAPTURE_BYTES + 1)
    return None if len(decoded) > MAX_CAPTURE_BYTES else decoded


class _Capture:
    def __init__(self):
        self._chunks = []
        self._size = 0
        self._overflow = False

    def add(self, chunk):
        if self._overflow:
            return
        self._size += len(chunk)
        if self._size > MAX_CAPTURE_BYTES:
            self._overflow, self._chunks = True, []
            return
        self._chunks.append(chunk)

    def body(self):
        return None if self._overflow else b"".join(self._chunks)


class ObservedStream(httpx.SyncByteStream):
    def __init__(self, inner, on_close):
        self._inner = inner
        self._on_close = on_close
        self._capture = _Capture()
        self._complete = False
        self._closed = False

    def __iter__(self):
        for chunk in self._inner:
            self._capture.add(chunk)
            yield chunk
        self._complete = True

    def close(self):
        try:
            self._inner.close()
        finally:
            if not self._closed:
                self._closed = True
                self._on_close(self._complete, self._capture.body())


class AsyncObservedStream(httpx.AsyncByteStream):
    def __init__(self, inner, on_close):
        self._inner = inner
        self._on_close = on_close
        self._capture = _Capture()
        self._complete = False
        self._closed = False

    async def __aiter__(self):
        async for chunk in self._inner:
            self._capture.add(chunk)
            yield chunk
        self._complete = True

    async def aclose(self):
        try:
            await self._inner.aclose()
        finally:
            if not self._closed:
                self._closed = True
                self._on_close(self._complete, self._capture.body())


def _is_stream(response):
    return response.headers.get("content-type", "").lower().startswith("text/event-stream")


def _succeeded(response):
    return 200 <= response.status_code < 300


def _restart_in_child(reference):
    hook = reference()
    if hook is None or hook._closed:
        return
    hook._counts = dict.fromkeys(hook._counts, 0)
    hook._start()


class QuotawareHook:
    def __init__(self, quotaware, provider, token=None, queue_size=256, timeout=5.0, max_request_bytes=MAX_REQUEST_BYTES):
        if provider not in PATHS:
            raise ValueError("provider must be openai or anthropic")
        url = httpx.URL(quotaware.rstrip("/") + OBSERVE_PATH)
        if url.scheme not in ("http", "https") or not url.host:
            raise ValueError("quotaware must be an http(s) origin such as http://quotaware:9100")
        self.endpoint = str(url)
        self.provider = provider
        self.token = token
        self.timeout = timeout
        self.max_request_bytes = max_request_bytes
        self._queue_size = queue_size
        self._counts = {"dropped": 0, "failed": 0, "errors": 0, "unobserved": 0}
        self._start()
        if hasattr(os, "register_at_fork"):
            reference = weakref.ref(self)
            os.register_at_fork(after_in_child=lambda: _restart_in_child(reference))

    def _start(self):
        self._lock = threading.Lock()
        self._closed = False
        self._failing = False
        self._queue = queue.Queue(maxsize=self._queue_size)
        self._thread = threading.Thread(target=self._deliver, name="quotaware-collector", daemon=True)
        self._thread.start()

    @property
    def dropped(self):
        return self._counts["dropped"]

    @property
    def failed(self):
        return self._counts["failed"]

    @property
    def errors(self):
        return self._counts["errors"]

    @property
    def unobserved(self):
        return self._counts["unobserved"]

    def _count(self, name):
        with self._lock:
            self._counts[name] += 1
            return self._counts[name]

    def hooks(self):
        return {"request": [self.on_request], "response": [self.on_response]}

    def async_hooks(self):
        return {"request": [self.on_async_request], "response": [self.on_async_response]}

    def on_request(self, request):
        request.extensions["quotaware_started"] = time.monotonic()

    async def on_async_request(self, request):
        self.on_request(request)

    def on_response(self, response):
        report = self._guarded(self._report_for, response)
        if report is None:
            return
        if _is_stream(response):
            response.stream = ObservedStream(response.stream, self._stream_reporter(response, report))
            return
        if _succeeded(response):
            try:
                response.read()
            except Exception:
                report(False)
                raise
        report(True, self._guarded(self._usage, response))

    async def on_async_response(self, response):
        report = self._guarded(self._report_for, response)
        if report is None:
            return
        if _is_stream(response):
            response.stream = AsyncObservedStream(response.stream, self._stream_reporter(response, report))
            return
        if _succeeded(response):
            try:
                await response.aread()
            except Exception:
                report(False)
                raise
        report(True, self._guarded(self._usage, response))

    def _guarded(self, step, response):
        try:
            return step(response)
        except Exception:
            if self._count("errors") == 1:
                LOGGER.warning("quotaware hook failed; later failures are only counted", exc_info=True)
            return None

    def _report_for(self, response):
        request = response.request
        if request.method != "POST" or not request.url.path.endswith(PATHS[self.provider]):
            return None
        model = self._model(request.content)
        if model is None:
            self._count("unobserved")
            return None
        started = request.extensions.get("quotaware_started")
        headers_at = time.monotonic()
        observation = {
            "provider": self.provider,
            "model": model,
            "credential": credential_hash(request.headers),
            "status": response.status_code,
            "headers": rate_limit_headers(response.headers),
        }
        return lambda complete, usage=None: self._enqueue(observation, started, headers_at, complete, usage)

    def _model(self, content):
        if len(content) > self.max_request_bytes:
            return None
        try:
            body = json.loads(content or b"{}")
        except ValueError:
            return None
        model = body.get("model") if isinstance(body, dict) else None
        return model if isinstance(model, str) and model else None

    def _stream_reporter(self, response, report):
        succeeded = _succeeded(response)
        encoding = response.headers.get("content-encoding", "")

        def on_close(complete, body):
            usage = None
            if complete and succeeded and body is not None:
                usage = self._guarded(lambda _: self._stream_usage(encoding, body), response)
            report(complete, usage)

        return on_close

    def _stream_usage(self, encoding, body):
        decoded = _decoded(encoding, body)
        return None if decoded is None else stream_tokens(self.provider, decoded)

    def _usage(self, response):
        if not _succeeded(response):
            return None
        try:
            payload = response.json()
        except ValueError:
            return None
        return tokens(self.provider, payload)

    def _enqueue(self, observation, started, headers_at, complete, usage):
        now = time.monotonic()
        observation = {
            **observation,
            "usage": usage,
            "latency_ms": (now - started) * 1000 if started else 0.0,
            "headers_age_ms": (now - headers_at) * 1000,
            "complete": complete,
        }
        if self._closed:
            self._count("dropped")
            return
        try:
            self._queue.put_nowait(observation)
        except queue.Full:
            self._count("dropped")

    def _deliver(self):
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        with httpx.Client(timeout=self.timeout, headers=headers) as client:
            while True:
                item = self._queue.get()
                if item is None:
                    return
                self._post(client, item)

    def _post(self, client, item):
        try:
            reply = client.post(self.endpoint, json=item)
        except Exception as error:
            self._delivery_failed(repr(error))
            return
        if reply.status_code >= 300:
            self._delivery_failed(f"quotaware answered {reply.status_code}")
            return
        if self._failing:
            self._failing = False
            LOGGER.info("quotaware observations delivered again")

    def _delivery_failed(self, reason):
        self._count("failed")
        if not self._failing:
            self._failing = True
            LOGGER.warning("quotaware observations not delivered: %s", reason)

    def close(self):
        with self._lock:
            if self._closed:
                return 0
            self._closed = True
        deadline = time.monotonic() + self.timeout + 1
        sentinel = 1
        try:
            self._queue.put(None, timeout=self.timeout)
        except queue.Full:
            sentinel = 0
        self._thread.join(timeout=max(deadline - time.monotonic(), 0))
        undelivered = max(self._queue.qsize() - sentinel, 0)
        if undelivered:
            LOGGER.warning("quotaware collector closed with %d observations undelivered", undelivered)
        return undelivered
