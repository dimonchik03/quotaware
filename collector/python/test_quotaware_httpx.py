import asyncio
import gzip
import hashlib
import json
import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest

from quotaware_httpx import QuotawareHook, credential_hash, tokens


class Sink(BaseHTTPRequestHandler):
    received = []
    status = 202
    delay = 0.0

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        Sink.received.append((dict(self.headers), json.loads(self.rfile.read(length))))
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
    return httpx.Response(200, headers={"content-type": "application/json"}, content=b"not json")


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
