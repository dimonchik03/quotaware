import asyncio
import gzip
import hashlib
import json
import logging
import os
import sys
import threading
import time
import weakref
import zlib
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest

import quotaware_httpx
from quotaware_httpx import QuotawareHook, credential_hash, tokens


class Sink(BaseHTTPRequestHandler):
    received = []
    status = 202
    delay = 0.0
    gate = None

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        Sink.received.append((dict(self.headers), json.loads(self.rfile.read(length))))
        if Sink.gate is not None:
            Sink.gate.wait(5)
        time.sleep(Sink.delay)
        self.send_response(Sink.status)
        self.end_headers()

    def log_message(self, *args):
        return


@pytest.fixture
def sink():
    Sink.received = []
    Sink.status = 202
    Sink.delay = 0.0
    server = HTTPServer(("127.0.0.1", 0), Sink)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


RATE_LIMIT_HEADERS = {
    "x-ratelimit-limit-requests": "60",
    "x-ratelimit-remaining-requests": "59",
    "x-ratelimit-reset-requests": "1s",
}


def fake_openai(request):
    return httpx.Response(200, headers={**RATE_LIMIT_HEADERS, "content-type": "application/json"}, json={"usage": {"prompt_tokens": 100, "completion_tokens": 50}})


def fake_stream(request):
    return httpx.Response(200, headers={**RATE_LIMIT_HEADERS, "content-type": "text/event-stream"}, stream=httpx.ByteStream(b"data: {}\n\ndata: [DONE]\n\n"))


def fake_broken(request):
    return httpx.Response(200, headers={**RATE_LIMIT_HEADERS, "content-type": "application/json"}, content=b"not json")


def client_with(hook, transport):
    return httpx.Client(base_url="https://provider.test", transport=httpx.MockTransport(transport), event_hooks=hook.hooks())


def chat(client):
    return client.post("/v1/chat/completions", json={"model": "gpt-4.1-mini", "messages": []}, headers={"Authorization": "Bearer sk-test"})


def test_posts_an_observation_for_a_chat_completion(sink):
    hook = QuotawareHook(sink, "openai", token="secret")
    response = chat(client_with(hook, fake_openai))
    hook.close()

    assert response.status_code == 200
    assert len(Sink.received) == 1
    headers, observation = Sink.received[0]
    assert headers["Authorization"] == "Bearer secret"
    assert observation["provider"] == "openai"
    assert observation["model"] == "gpt-4.1-mini"
    assert observation["status"] == 200
    assert observation["headers"]["x-ratelimit-remaining-requests"] == "59"
    assert observation["usage"] == 150
    assert observation["complete"] is True
    assert observation["credential"] == hashlib.sha256(b"Bearer sk-test").hexdigest()[:12]


def test_observes_a_stream_once_it_is_closed_and_measures_to_the_end(sink):
    hook = QuotawareHook(sink, "openai")
    client = client_with(hook, fake_stream)
    with client.stream("POST", "/v1/chat/completions", json={"model": "gpt-4.1-mini", "messages": []}) as response:
        time.sleep(0.05)
        chunks = list(response.iter_bytes())
        assert Sink.received == []
    hook.close()

    assert b"".join(chunks).endswith(b"[DONE]\n\n")
    _, observation = Sink.received[0]
    assert observation["complete"] is True
    assert observation["usage"] is None
    assert observation["latency_ms"] >= 50


def test_marks_a_stream_the_caller_abandoned_as_incomplete(sink):
    hook = QuotawareHook(sink, "openai")
    client = client_with(hook, fake_stream)
    with client.stream("POST", "/v1/chat/completions", json={"model": "gpt-4.1-mini", "messages": []}):
        pass
    hook.close()

    _, observation = Sink.received[0]
    assert observation["complete"] is False


def test_a_stream_read_in_one_go_is_observed_as_complete(sink):
    hook = QuotawareHook(sink, "openai")
    chat(client_with(hook, fake_stream))
    hook.close()

    _, observation = Sink.received[0]
    assert observation["complete"] is True
    assert observation["usage"] is None


def test_ignores_other_requests(sink):
    hook = QuotawareHook(sink, "openai")
    client_with(hook, fake_openai).get("/v1/models")
    hook.close()

    assert Sink.received == []


def test_a_2xx_body_that_is_not_json_still_reports_its_headers(sink):
    hook = QuotawareHook(sink, "openai")
    response = chat(client_with(hook, fake_broken))
    hook.close()

    assert response.status_code == 200
    assert hook.errors == 0
    _, observation = Sink.received[0]
    assert observation["usage"] is None
    assert observation["complete"] is True
    assert observation["headers"]["x-ratelimit-remaining-requests"] == "59"


def test_counts_rejected_deliveries(sink):
    Sink.status = 400
    hook = QuotawareHook(sink, "openai")
    chat(client_with(hook, fake_openai))
    hook.close()

    assert hook.failed == 1


def test_drops_when_the_queue_is_full(sink):
    Sink.delay = 0.5
    hook = QuotawareHook(sink, "openai", queue_size=1)
    client = client_with(hook, fake_openai)
    for _ in range(5):
        chat(client)
    hook.close()

    assert hook.dropped >= 3
    assert hook.dropped + len(Sink.received) == 5
    assert hook.failed == 0


def async_client_with(hook, transport):
    return httpx.AsyncClient(base_url="https://provider.test", transport=httpx.MockTransport(transport), event_hooks=hook.async_hooks())


async def async_chat(client):
    return await client.post("/v1/chat/completions", json={"model": "gpt-4.1-mini", "messages": []}, headers={"Authorization": "Bearer sk-test"})


def test_async_hooks_post_an_observation_for_a_chat_completion(sink):
    hook = QuotawareHook(sink, "openai", token="secret")

    async def call():
        async with async_client_with(hook, fake_openai) as client:
            return await async_chat(client)

    response = asyncio.run(call())
    hook.close()

    assert response.status_code == 200
    assert hook.errors == 0
    _, observation = Sink.received[0]
    assert observation["model"] == "gpt-4.1-mini"
    assert observation["usage"] == 150
    assert observation["complete"] is True
    assert observation["credential"] == hashlib.sha256(b"Bearer sk-test").hexdigest()[:12]


def test_async_hooks_observe_a_stream_once_it_is_closed_and_measure_to_the_end(sink):
    hook = QuotawareHook(sink, "openai")

    async def call():
        async with async_client_with(hook, fake_stream) as client:
            async with client.stream("POST", "/v1/chat/completions", json={"model": "gpt-4.1-mini", "messages": []}) as response:
                await asyncio.sleep(0.05)
                chunks = [chunk async for chunk in response.aiter_bytes()]
                assert Sink.received == []
                return chunks

    chunks = asyncio.run(call())
    hook.close()

    assert b"".join(chunks).endswith(b"[DONE]\n\n")
    _, observation = Sink.received[0]
    assert observation["complete"] is True
    assert observation["usage"] is None
    assert observation["latency_ms"] >= 50


def test_async_hooks_observe_a_stream_read_in_one_go_as_complete(sink):
    hook = QuotawareHook(sink, "openai")

    async def call():
        async with async_client_with(hook, fake_stream) as client:
            return await async_chat(client)

    response = asyncio.run(call())
    hook.close()

    assert response.content.endswith(b"[DONE]\n\n")
    assert hook.errors == 0
    _, observation = Sink.received[0]
    assert observation["complete"] is True
    assert observation["usage"] is None


def test_async_hooks_mark_a_stream_the_caller_abandoned_as_incomplete(sink):
    hook = QuotawareHook(sink, "openai")

    async def call():
        async with async_client_with(hook, fake_stream) as client:
            async with client.stream("POST", "/v1/chat/completions", json={"model": "gpt-4.1-mini", "messages": []}):
                pass

    asyncio.run(call())
    hook.close()

    _, observation = Sink.received[0]
    assert observation["complete"] is False
    assert observation["usage"] is None


def test_async_hooks_report_the_headers_of_a_2xx_body_that_is_not_json(sink):
    hook = QuotawareHook(sink, "openai")

    async def call():
        async with async_client_with(hook, fake_broken) as client:
            return await async_chat(client)

    response = asyncio.run(call())
    hook.close()

    assert response.status_code == 200
    assert hook.errors == 0
    _, observation = Sink.received[0]
    assert observation["usage"] is None
    assert observation["headers"]["x-ratelimit-remaining-requests"] == "59"


def test_counts_tokens_per_provider_and_tolerates_nulls():
    assert tokens("openai", {"usage": {"prompt_tokens": 10, "completion_tokens": 5}}) == 15
    assert tokens("anthropic", {"usage": {"input_tokens": 10, "cache_creation_input_tokens": None, "cache_read_input_tokens": None, "output_tokens": 5}}) == 15
    assert tokens("anthropic", {"usage": {"input_tokens": 10, "cache_creation_input_tokens": 7, "cache_read_input_tokens": 500, "output_tokens": 5}}) == 22
    assert tokens("openai", {"error": {}}) is None


def test_hashes_the_credential_like_the_proxy():
    assert credential_hash({"Authorization": "Bearer sk-test"}) == hashlib.sha256(b"Bearer sk-test").hexdigest()[:12]
    assert credential_hash({"x-api-key": "sk-ant"}) == hashlib.sha256(b"sk-ant").hexdigest()[:12]
    assert credential_hash({}) == "anonymous"


def test_rejects_an_unknown_provider_and_a_bad_endpoint():
    with pytest.raises(ValueError):
        QuotawareHook("http://quotaware:9100", "gemini")
    with pytest.raises(ValueError):
        QuotawareHook("not a url at all", "openai")


OPENAI_STREAM = b'data: {"choices":[]}\n\ndata: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}\n\ndata: [DONE]\n\n'
ANTHROPIC_STREAM = (
    b'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":10,"cache_creation_input_tokens":2,"output_tokens":1}}}\n\n'
    b'event: message_delta\ndata: {"type":"message_delta","usage":{"output_tokens":7}}\n\n'
)


def streaming(body, content_type="text/event-stream", encoding=None):
    headers = {**RATE_LIMIT_HEADERS, "content-type": content_type}
    if encoding:
        headers["content-encoding"] = encoding
    return lambda request: httpx.Response(200, headers=headers, stream=httpx.ByteStream(body))


def test_reads_usage_from_an_openai_stream_that_includes_it(sink):
    hook = QuotawareHook(sink, "openai")
    chat(client_with(hook, streaming(OPENAI_STREAM)))
    hook.close()

    _, observation = Sink.received[0]
    assert observation["usage"] == 15


def test_recognises_a_stream_whatever_the_case_of_its_content_type(sink):
    hook = QuotawareHook(sink, "openai")
    chat(client_with(hook, streaming(OPENAI_STREAM, content_type="Text/Event-Stream")))
    hook.close()

    _, observation = Sink.received[0]
    assert observation["usage"] == 15


def test_reads_usage_from_a_gzip_stream(sink):
    hook = QuotawareHook(sink, "openai")
    response = chat(client_with(hook, streaming(gzip.compress(OPENAI_STREAM), encoding="gzip")))
    hook.close()

    assert response.content == OPENAI_STREAM
    _, observation = Sink.received[0]
    assert observation["usage"] == 15


def test_async_hooks_read_usage_from_an_anthropic_stream(sink):
    hook = QuotawareHook(sink, "anthropic")
    transport = httpx.MockTransport(streaming(ANTHROPIC_STREAM))

    async def call():
        async with httpx.AsyncClient(base_url="https://provider.test", transport=transport, event_hooks=hook.async_hooks()) as client:
            return await client.post("/v1/messages", json={"model": "claude-sonnet-4-5", "messages": []}, headers={"x-api-key": "sk-ant"})

    asyncio.run(call())
    hook.close()

    _, observation = Sink.received[0]
    assert observation["usage"] == 19


class BreakingStream(httpx.SyncByteStream):
    def __iter__(self):
        yield b'{"usage":'
        raise httpx.ReadError("connection reset")


def breaking(request):
    return httpx.Response(200, headers={"content-type": "application/json"}, stream=BreakingStream())


def test_a_body_that_breaks_reaches_the_caller_with_its_own_error(sink):
    hook = QuotawareHook(sink, "openai")

    with pytest.raises(httpx.ReadError):
        chat(client_with(hook, breaking))
    hook.close()

    _, observation = Sink.received[0]
    assert observation["complete"] is False


def wait_for(condition):
    deadline = time.monotonic() + 5
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert condition()


def test_logs_once_when_deliveries_start_failing_and_once_when_they_recover(sink, caplog):
    caplog.set_level(logging.INFO, logger="quotaware_httpx")
    Sink.status = 401
    hook = QuotawareHook(sink, "openai")
    client = client_with(hook, fake_openai)
    chat(client)
    chat(client)
    wait_for(lambda: hook.failed == 2)
    Sink.status = 202
    chat(client)
    hook.close()

    messages = [record.getMessage() for record in caplog.records]
    assert sum("not delivered" in message for message in messages) == 1
    assert sum("delivered again" in message for message in messages) == 1


def test_close_is_idempotent_and_counts_later_observations_as_dropped(sink):
    hook = QuotawareHook(sink, "openai")
    client = client_with(hook, fake_openai)

    assert hook.close() == 0
    assert hook.close() == 0
    chat(client)

    assert hook.dropped == 1
    assert Sink.received == []


@pytest.mark.skipif(not hasattr(os, "fork") or sys.platform == "darwin", reason="needs os.fork; macOS aborts a forked child of a threaded process that touches the Objective-C runtime")
def test_keeps_reporting_from_a_process_forked_after_the_hook_was_created(sink):
    hook = QuotawareHook(sink, "openai")
    pid = os.fork()
    if pid == 0:
        code = 1
        try:
            chat(client_with(hook, fake_openai))
            code = 0 if hook.close() == 0 else 1
        finally:
            os._exit(code)
    _, status = os.waitpid(pid, 0)
    hook.close()

    assert os.waitstatus_to_exitcode(status) == 0
    wait_for(lambda: len(Sink.received) == 1)


def rate_limited(request):
    return httpx.Response(429, headers={**RATE_LIMIT_HEADERS, "retry-after": "7", "content-type": "application/json"}, json={"error": {"type": "rate_limit"}})


def test_reports_a_429_with_its_retry_after_and_no_usage(sink):
    hook = QuotawareHook(sink, "openai")
    response = chat(client_with(hook, rate_limited))
    hook.close()

    assert response.status_code == 429
    _, observation = Sink.received[0]
    assert observation["status"] == 429
    assert observation["headers"]["retry-after"] == "7"
    assert observation["headers"]["x-ratelimit-remaining-requests"] == "59"
    assert observation["complete"] is True
    assert observation["usage"] is None


class AsyncBreakingStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b'{"usage":'
        raise httpx.ReadError("connection reset")


def test_async_hooks_let_a_broken_body_reach_the_caller_with_its_own_error(sink):
    hook = QuotawareHook(sink, "openai")

    def broken(request):
        return httpx.Response(200, headers={"content-type": "application/json"}, stream=AsyncBreakingStream())

    async def call():
        async with async_client_with(hook, broken) as client:
            return await async_chat(client)

    with pytest.raises(httpx.ReadError):
        asyncio.run(call())
    hook.close()

    _, observation = Sink.received[0]
    assert observation["complete"] is False


def test_reports_how_long_before_the_end_of_a_stream_its_headers_arrived(sink):
    hook = QuotawareHook(sink, "openai")
    client = client_with(hook, streaming(OPENAI_STREAM))
    with client.stream("POST", "/v1/chat/completions", json={"model": "gpt-4.1-mini", "messages": []}) as response:
        time.sleep(0.05)
        response.read()
    hook.close()

    _, observation = Sink.received[0]
    assert observation["headers_age_ms"] >= 40
    assert observation["latency_ms"] >= 40


def test_leaves_a_request_body_over_the_limit_unobserved(sink):
    hook = QuotawareHook(sink, "openai", max_request_bytes=64)
    client = client_with(hook, fake_openai)

    response = client.post("/v1/chat/completions", json={"model": "gpt-4.1-mini", "messages": [{"role": "user", "content": "x" * 100}]})
    hook.close()

    assert response.status_code == 200
    assert hook.unobserved == 1
    assert hook.errors == 0
    assert Sink.received == []


def test_leaves_a_request_that_is_not_json_unobserved_without_an_error(sink):
    hook = QuotawareHook(sink, "openai")
    client = client_with(hook, fake_openai)

    client.post("/v1/chat/completions", content=b"not json")
    hook.close()

    assert hook.unobserved == 1
    assert hook.errors == 0


def test_leaves_a_request_whose_model_is_not_a_string_unobserved(sink):
    hook = QuotawareHook(sink, "openai")
    client = client_with(hook, fake_openai)

    client.post("/v1/chat/completions", json={"model": 4, "messages": []})
    hook.close()

    assert hook.unobserved == 1
    assert Sink.received == []


def test_reports_the_first_value_of_a_repeated_header(sink):
    hook = QuotawareHook(sink, "openai")

    def repeated(request):
        return httpx.Response(200, headers=[("content-type", "application/json"), ("x-ratelimit-remaining-requests", "59"), ("x-ratelimit-remaining-requests", "58")], json={})

    chat(client_with(hook, repeated))
    hook.close()

    _, observation = Sink.received[0]
    assert observation["headers"]["x-ratelimit-remaining-requests"] == "59"


def test_works_where_the_platform_has_no_fork_hook(sink, monkeypatch):
    monkeypatch.delattr(os, "register_at_fork")

    hook = QuotawareHook(sink, "openai")
    chat(client_with(hook, fake_openai))
    hook.close()

    assert len(Sink.received) == 1


def test_a_hook_closed_before_a_fork_stays_closed_in_the_child(sink):
    hook = QuotawareHook(sink, "openai")
    hook.close()

    quotaware_httpx._restart_in_child(weakref.ref(hook))
    chat(client_with(hook, fake_openai))

    assert hook.dropped == 1
    assert Sink.received == []


def test_close_returns_what_is_still_queued_even_without_a_delivery_thread(sink):
    hook = QuotawareHook(sink, "openai")
    hook._queue.put(None)
    hook._thread.join()
    hook._queue.put_nowait({"provider": "openai"})

    assert hook.close() == 1


def test_an_observed_stream_reports_only_when_it_is_closed():
    reports = []
    stream = quotaware_httpx.ObservedStream(httpx.ByteStream(b"data: [DONE]\n\n"), lambda complete, body: reports.append(complete))

    chunks = list(stream)
    before_close = list(reports)
    stream.close()

    assert chunks == [b"data: [DONE]\n\n"]
    assert before_close == []
    assert reports == [True]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_streamed_json_is_read_lazily_and_usage_capture_is_bounded(sink, monkeypatch, asynchronous):
    hook = QuotawareHook(sink, "openai")
    monkeypatch.setattr(quotaware_httpx, "MAX_CAPTURE_BYTES", 64)
    body = json.dumps({"usage": {"prompt_tokens": 5}, "text": "x" * 100}).encode()
    reads = []
    class Body(httpx.SyncByteStream, httpx.AsyncByteStream):
        def __iter__(self):
            reads.append(True)
            yield body

        async def __aiter__(self):
            for chunk in self:
                yield chunk

    def transport(request):
        return httpx.Response(200, headers={"content-type": "application/json"}, stream=Body())

    async def consume():
        async with async_client_with(hook, transport) as client:
            async with client.stream("POST", "/v1/chat/completions", json={"model": "m"}) as response:
                assert reads == []
                assert await response.aread() == body

    try:
        if asynchronous:
            asyncio.run(consume())
        else:
            with client_with(hook, transport) as client:
                with client.stream("POST", "/v1/chat/completions", json={"model": "m"}) as response:
                    assert reads == []
                    assert response.read() == body
    finally:
        hook.close()
    assert Sink.received[0][1]["complete"] is True
    assert Sink.received[0][1]["usage"] is None


def test_backlogged_delivery_includes_queue_time_in_header_age(sink, monkeypatch):
    release = threading.Event()
    monkeypatch.setattr(Sink, "gate", release)
    hook = QuotawareHook(sink, "openai")
    try:
        with client_with(hook, fake_openai) as client:
            chat(client)
            wait_for(lambda: len(Sink.received) == 1)
            chat(client)
            time.sleep(0.05)
    finally:
        release.set()
        hook.close()
    assert Sink.received[1][1]["headers_age_ms"] >= 45


def test_full_queue_at_close_eventually_stops_the_thread(sink, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    hook = QuotawareHook(sink, "openai", queue_size=1, timeout=0.05)
    post = httpx.Client.post

    def slow_sink(client, url, *args, **kwargs):
        if url == hook.endpoint:
            entered.set()
            release.wait(5)
            return httpx.Response(202)
        return post(client, url, *args, **kwargs)

    monkeypatch.setattr(httpx.Client, "post", slow_sink)
    with client_with(hook, fake_openai) as client:
        chat(client)
        assert entered.wait(2)
        chat(client)
    try:
        assert hook.close() == 1
    finally:
        release.set()
    hook._thread.join(2)
    assert not hook._thread.is_alive()

@pytest.mark.parametrize("encoding", ["gzip", "deflate", "br", "zstd"])
def test_captures_bounded_compressed_json_and_leaves_oversize_usage_unknown(sink, monkeypatch, encoding):
    compress = {"gzip": gzip.compress, "deflate": zlib.compress}.get(encoding)
    if encoding == "br":
        compress = pytest.importorskip("brotli", minversion="1.2.0").compress
    if encoding == "zstd":
        compress = pytest.importorskip("zstandard").ZstdCompressor().compress
    hook = QuotawareHook(sink, "openai")
    monkeypatch.setattr(quotaware_httpx, "MAX_CAPTURE_BYTES", 65536)
    for size in (32768, 131072):
        body = json.dumps({"text": "x" * size, "usage": {"prompt_tokens": 5}}).encode()
        def transport(request):
            return httpx.Response(200, headers={"content-type": "application/json", "content-encoding": encoding},
                                  stream=httpx.ByteStream(compress(body)))
        with client_with(hook, transport) as client:
            assert chat(client).content == body
    hook.close()
    assert [message["usage"] for _, message in Sink.received] == [5.0, None]
