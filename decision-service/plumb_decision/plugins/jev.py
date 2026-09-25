"""TypeSafe Jev, or any server speaking the /v1/systemone protocol (including laya-serve).

Protocol, from typesafe-sdk 0.7.1 (typesafe_sdk/constants.py, _core/constants.py,
_core/endpoints.py, _core/transport.py, _schemas/models.py):

    POST {base_url}/v1/systemone
    Authorization: Bearer <api key>
    {"state": <json>, "model": "jev-latest", "questions": {...}}
    → {"model": "...", "answers": {"<id>": {"type": "choice"|"score"|"noul", ...}}, "usage": {...}}

Choice and score answers carry "probabilities"; noul answers carry "noul" = P(true).
laya-serve implements the same route, so this plugin also reaches a self-hosted Laya.

This plugin sends the compressed cluster state to the configured endpoint. For the
hosted API that leaves the cluster; enable it deliberately.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict

from .base import Capabilities, ModelPlugin, Rejected, Unavailable

DEFAULT_BASE_URL = "https://api.typesafe.ai"
DEFAULT_MODEL = "jev-latest"
API_KEY_ENV = "TYPESAFE_API_KEY"
SYSTEM_ONE_PATH = "/v1/systemone"
MODELS_PATH = "/v1/models"
RETRYABLE_STATUSES = {408, 429} | set(range(500, 600))
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


class JevPlugin(ModelPlugin):
    """options:
      base_url:     default https://api.typesafe.ai (TYPESAFE_BASE_URL also honoured)
      model:        default jev-latest (TYPESAFE_DEFAULT_MODEL also honoured)
      api_key_env:  env var holding the key (default TYPESAFE_API_KEY); may be empty for a
                    keyless self-hosted server
      timeout:      seconds per request (default 3)
      max_choices:  default 20 to match the agent; Jev accepts more
      allow_http:   allow plain http to non-local hosts (default false)
    """

    plugin_name = "jev"

    def __init__(self, instance, options):
        super().__init__(instance, options)
        self._base = (self.options.get("base_url") or os.environ.get("TYPESAFE_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self._model = self.options.get("model") or os.environ.get("TYPESAFE_DEFAULT_MODEL") or DEFAULT_MODEL
        self._key_env = self.options.get("api_key_env", API_KEY_ENV)
        self._timeout = float(self.options.get("timeout", 3.0))
        self._max_choices = int(self.options.get("max_choices", 20))
        u = urllib.parse.urlparse(self._base)
        if u.scheme not in ("http", "https") or not u.hostname:
            raise ValueError(f"{instance}: invalid base_url {self._base!r}")
        self._remote = u.hostname not in _LOCAL_HOSTS and not u.hostname.endswith(".svc") and not u.hostname.endswith(".cluster.local")
        if u.scheme == "http" and u.hostname not in _LOCAL_HOSTS and not self.options.get("allow_http", False):
            raise ValueError(f"{instance}: refusing plain http to {u.hostname}; use https or set allow_http")

    @property
    def model_id(self) -> str:
        return self._model

    @property
    def capabilities(self) -> Capabilities:
        return Capabilities(max_choices=self._max_choices, max_state_bytes=16384, remote=self._remote)

    def _api_key(self) -> str:
        return os.environ.get(self._key_env, "").strip() if self._key_env else ""

    def ready(self) -> bool:
        # Hosted API: ready when credentials are present (or none are required).
        return not self._key_env or bool(self._api_key())

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "plumb-decision"}
        key = self._api_key()
        if key:
            h["Authorization"] = f"Bearer {key}"
        return h

    def predict(self, state: Any, questions: Dict[str, dict]) -> Dict[str, dict]:
        body = json.dumps({"state": state, "model": self._model, "questions": questions}).encode()
        req = urllib.request.Request(self._base + SYSTEM_ONE_PATH, data=body, headers=self._headers(), method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:  # noqa: S310 - scheme checked in __init__
                payload = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail = e.read(200).decode("utf-8", "replace")
            if e.code in RETRYABLE_STATUSES:
                raise Unavailable(f"{self.instance}: HTTP {e.code}: {detail}") from None
            raise Rejected(f"{self.instance}: HTTP {e.code}: {detail}") from None
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            raise Unavailable(f"{self.instance}: {e}") from None
        except ValueError as e:
            raise Unavailable(f"{self.instance}: invalid JSON response: {e}") from None
        answers = payload.get("answers") if isinstance(payload, dict) else None
        if not isinstance(answers, dict):
            raise Unavailable(f"{self.instance}: response has no answers object")
        return answers
