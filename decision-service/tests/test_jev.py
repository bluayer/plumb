import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from plumb_decision.plugins import Rejected, Unavailable
from plumb_decision.plugins.jev import JevPlugin

QUESTIONS = {
    "action": {"type": "choice", "instructions": "?", "criteria": {"wait": "", "move": ""}},
    "urgency": {"type": "score", "criteria": ["low", "high"]},
    "transient": {"type": "noul", "instructions": "?"},
}


class FakeJev(BaseHTTPRequestHandler):
    status = 200
    seen = []

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        FakeJev.seen.append((self.path, dict(self.headers), body))
        if FakeJev.status != 200:
            self.send_response(FakeJev.status)
            self.end_headers()
            self.wfile.write(b'{"detail":"nope"}')
            return
        # Shape from typesafe-sdk 0.7.1 _schemas/models.py.
        payload = {"model": body["model"], "usage": {"input_tokens": 10, "output_tokens": 0}, "answers": {
            "action": {"type": "choice", "choice": "move", "confidence": 0.7, "probabilities": {"wait": 0.3, "move": 0.7}},
            "urgency": {"type": "score", "score": 0.9, "confidence": 0.6, "legend": {"0": "low", "1": "high"},
                        "probabilities": {"0": 0.1, "1": 0.9}},
            "transient": {"type": "noul", "noul": 0.25},
        }}
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def jev_url():
    FakeJev.status, FakeJev.seen = 200, []
    srv = HTTPServer(("127.0.0.1", 0), FakeJev)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


def test_request_and_answers(jev_url, monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "sk-test")
    p = JevPlugin("jev", {"base_url": jev_url, "model": "jev-latest"})
    assert p.ready() and not p.capabilities.remote
    answers = p.predict({"v": 1}, QUESTIONS)
    assert answers["action"]["probabilities"]["move"] == 0.7
    assert answers["transient"]["noul"] == 0.25
    path, headers, body = FakeJev.seen[0]
    assert path == "/v1/systemone"
    assert headers["Authorization"] == "Bearer sk-test"
    assert body == {"state": {"v": 1}, "model": "jev-latest", "questions": QUESTIONS}


def test_not_ready_without_key(monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    p = JevPlugin("jev", {})
    assert not p.ready()
    assert p.capabilities.remote  # hosted API leaves the cluster


def test_keyless_self_hosted(jev_url):
    p = JevPlugin("laya-serve", {"base_url": jev_url, "api_key_env": ""})
    assert p.ready()
    p.predict({}, QUESTIONS)
    assert "Authorization" not in FakeJev.seen[0][1]


@pytest.mark.parametrize("status,exc", [(429, Unavailable), (503, Unavailable), (401, Rejected), (422, Rejected)])
def test_http_errors(jev_url, monkeypatch, status, exc):
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    FakeJev.status = status
    with pytest.raises(exc):
        JevPlugin("jev", {"base_url": jev_url}).predict({}, QUESTIONS)


def test_unreachable_is_unavailable(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    with pytest.raises(Unavailable):
        JevPlugin("jev", {"base_url": "http://127.0.0.1:1", "timeout": 0.5}).predict({}, QUESTIONS)


@pytest.mark.parametrize("url", ["http://api.example.com", "ftp://x", "not a url"])
def test_rejects_unsafe_urls(url):
    with pytest.raises(ValueError):
        JevPlugin("jev", {"base_url": url})


def test_in_cluster_http_allowed_but_not_remote():
    p = JevPlugin("laya", {"base_url": "http://laya-serve.ml.svc.cluster.local:8000", "allow_http": True})
    assert not p.capabilities.remote
