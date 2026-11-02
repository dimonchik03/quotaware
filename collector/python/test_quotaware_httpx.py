import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest

from quotaware_httpx import QuotawareHook, credential_hash, tokens


class Sink(BaseHTTPRequestHandler):
    received = []
    status = 202

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        Sink.received.append((dict(self.headers), json.loads(self.rfile.read(length))))
        self.send_response(Sink.status)
        self.end_headers()

    def log_message(self, *args):
        return


@pytest.fixture
def sink():
    Sink.received = []
    Sink.status = 202
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
    return httpx.Response(200, headers={**RATE_LIMIT_HEADERS, "content-type": "text/event-stream"}, content=b"data: {}\n\ndata: [DONE]\n\n")


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


def test_records_a_stream_as_complete_without_usage(sink):
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


def test_a_broken_response_never_fails_the_call(sink):
    hook = QuotawareHook(sink, "openai")
    response = chat(client_with(hook, fake_broken))
    hook.close()

    assert response.status_code == 200
    assert hook.errors == 1
    assert Sink.received == []


def test_counts_rejected_deliveries(sink):
    Sink.status = 400
    hook = QuotawareHook(sink, "openai")
    chat(client_with(hook, fake_openai))
    hook.close()

    assert hook.failed == 1


def test_drops_when_the_queue_is_full(sink):
    hook = QuotawareHook("http://127.0.0.1:9", "openai", queue_size=1)
    client = client_with(hook, fake_openai)
    for _ in range(5):
        chat(client)
    hook.close()

    assert hook.dropped >= 1
    assert hook.failed >= 1


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
