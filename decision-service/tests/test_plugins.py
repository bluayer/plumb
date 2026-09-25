import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from plumb_decision import plugins
from plumb_decision.plugins import JevPlugin, Rejected, Unavailable

QUESTIONS = {
    "action": {"type": "choice", "instructions": "?", "criteria": {"wait": "", "move": ""}},
    "urgency": {"type": "score", "criteria": ["low", "high"]},
    "transient": {"type": "noul", "instructions": "?"},
}


def test_registry():
    assert isinstance(plugins.create("u", "uniform", {}), plugins.UniformPlugin)
    assert plugins.create("f", "conftest:FakePlugin", {}).instance == "f"
    for bad in ["nope", "json:dumps", "json:JSONDecoder"]:
        with pytest.raises(ValueError):
            plugins.create("x", bad, {})


class FakeJev(BaseHTTPRequestHandler):
    status, seen = 200, []

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        FakeJev.seen.append((self.path, dict(self.headers), body))
        # Response shape from typesafe-sdk 0.7.1 _schemas/models.py.
        payload = {"model": body["model"], "usage": {"input_tokens": 1, "output_tokens": 0}, "answers": {
            "action": {"type": "choice", "choice": "move", "confidence": 0.7, "probabilities": {"wait": 0.3, "move": 0.7}},
            "transient": {"type": "noul", "noul": 0.25}}}
        data = json.dumps(payload if FakeJev.status == 200 else {"detail": "nope"}).encode()
        self.send_response(FakeJev.status)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def jev_url():
    FakeJev.status, FakeJev.seen = 200, []
    srv = HTTPServer(("127.0.0.1", 0), FakeJev)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


def test_jev_request_and_answers(jev_url, monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "sk-test")
    p = JevPlugin("jev", {"base_url": jev_url})
    assert p.ready() and not p.remote
    assert p.predict({"v": 1}, QUESTIONS)["transient"]["noul"] == 0.25
    path, headers, body = FakeJev.seen[0]
    assert path == "/v1/systemone" and headers["Authorization"] == "Bearer sk-test"
    assert body == {"state": {"v": 1}, "model": "jev-latest", "questions": QUESTIONS}


def test_jev_keys(jev_url, monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    hosted = JevPlugin("jev", {})
    assert not hosted.ready() and hosted.remote
    JevPlugin("self-hosted", {"base_url": jev_url, "api_key_env": ""}).predict({}, QUESTIONS)
    assert "Authorization" not in FakeJev.seen[0][1]


@pytest.mark.parametrize("status,exc", [(429, Unavailable), (503, Unavailable), (401, Rejected), (422, Rejected)])
def test_jev_http_errors(jev_url, status, exc):
    FakeJev.status = status
    with pytest.raises(exc):
        JevPlugin("jev", {"base_url": jev_url, "api_key_env": ""}).predict({}, QUESTIONS)


def test_jev_unreachable():
    with pytest.raises(Unavailable):
        JevPlugin("jev", {"base_url": "http://127.0.0.1:1", "timeout": 0.5, "api_key_env": ""}).predict({}, QUESTIONS)


@pytest.mark.parametrize("url", ["http://api.example.com", "ftp://x", "not a url"])
def test_jev_rejects_unsafe_urls(url):
    with pytest.raises(ValueError):
        JevPlugin("jev", {"base_url": url})


def test_jev_in_cluster_is_not_remote():
    assert not JevPlugin("l", {"base_url": "http://laya-serve.ml.svc.cluster.local:8000", "allow_http": True}).remote


def test_uniform_is_never_confident():
    from plumb_decision.answers import answer
    from plumb_decision.server import build_questions

    qs = build_questions("ice_event", ["b", "c"], 20)
    raw = plugins.UniformPlugin("u", {}).predict({}, qs)
    assert all(answer(qid, q, raw[qid], {})["confidence"] <= 0.5 + 1e-9 for qid, q in qs.items())
