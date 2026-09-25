"""Decision model plugins.

A plugin adapts one model to Plumb's typed questions. predict(state, questions) returns
raw answers keyed by question id:

    choice: {"type": "choice", "probabilities": {"<label>": p, ...}}
    score:  {"type": "score",  "probabilities": {"0": p, "1": p, ...}}
    noul:   {"type": "noul",   "noul": P(true)}

The server validates, renormalizes and calibrates them (answers.py). Add a model by
subclassing ModelPlugin and naming it in the models config as "pkg.module:Class", or by
publishing it under the "plumb.decision.plugins" entry point group.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from importlib.metadata import entry_points
from typing import Any, Dict, Mapping, Optional, Type

log = logging.getLogger(__name__)


class NotReady(RuntimeError):
    """Still loading, or failed to load. Maps to UNAVAILABLE."""


class Unavailable(RuntimeError):
    """Transient failure (timeout, 429, 5xx). Maps to UNAVAILABLE."""


class Rejected(RuntimeError):
    """The model refused the request (credentials, input). Maps to FAILED_PRECONDITION."""


class ModelPlugin:
    max_choices = 20  # options per choice question
    max_state_bytes = 4096  # keeps the state inside the model's context
    remote = False  # True when predict() sends state outside the cluster

    def __init__(self, instance: str, options: Mapping[str, Any]):
        self.instance, self.options = instance, dict(options)
        self.model_id = self.options.get("model", type(self).__name__)

    def start(self) -> None:
        """Begin loading; must not block for long."""

    def ready(self) -> bool:
        return True

    def predict(self, state: Any, questions: Dict[str, dict]) -> Dict[str, dict]:
        raise NotImplementedError


class LayaPlugin(ModelPlugin):
    """Local Laya checkpoint (options: model, device, max_len), loaded in the background.
    The base checkpoint is near random on this domain; point `model` at the fine-tuned one."""

    def __init__(self, instance, options):
        super().__init__(instance, options)
        self.model_id = self.options.get("model", "convaiinnovations/laya")
        self._agent, self._error = None, None

    def start(self) -> None:
        threading.Thread(target=self._load, daemon=True).start()

    def _load(self) -> None:
        try:
            import laya  # torch; deferred on purpose

            self._agent = laya.load(self.model_id, device=self.options.get("device"))
            log.info("%s: loaded %s", self.instance, self.model_id)
        except Exception as e:  # surfaced through predict()
            log.exception("%s: loading %s failed", self.instance, self.model_id)
            self._error = e

    def ready(self) -> bool:
        return self._agent is not None

    def predict(self, state, questions):
        if self._agent is None:
            raise NotReady(f"model not loaded: {self._error or 'loading'}")
        kw = {"max_len": self.options["max_len"]} if self.options.get("max_len") else {}
        return self._agent.system_one(state, questions, **kw)["answers"]


class JevPlugin(ModelPlugin):
    """TypeSafe Jev, or any /v1/systemone server (e.g. a self-hosted laya-serve).

    Protocol from typesafe-sdk 0.7.1 (constants.py, _core/endpoints.py, _schemas/models.py):
    POST {base_url}/v1/systemone, "Authorization: Bearer <key>",
    {"state", "model", "questions"} → {"model", "answers", "usage"}.

    options: base_url (https://api.typesafe.ai), model (jev-latest), api_key_env
    (TYPESAFE_API_KEY; "" for a keyless server), timeout (3s), allow_http (false).
    The hosted API receives the compressed cluster state; enable it deliberately.
    """

    max_state_bytes = 16384

    def __init__(self, instance, options):
        super().__init__(instance, options)
        self.base = (self.options.get("base_url") or "https://api.typesafe.ai").rstrip("/")
        self.model_id = self.options.get("model", "jev-latest")
        self.key_env = self.options.get("api_key_env", "TYPESAFE_API_KEY")
        self.timeout = float(self.options.get("timeout", 3))
        host = urllib.parse.urlparse(self.base).hostname or ""
        local = host in ("localhost", "127.0.0.1", "::1")
        if not host or not self.base.startswith(("http://", "https://")):
            raise ValueError(f"{instance}: invalid base_url {self.base!r}")
        if self.base.startswith("http://") and not local and not self.options.get("allow_http"):
            raise ValueError(f"{instance}: refusing plain http to {host}; use https or set allow_http")
        self.remote = not (local or host.endswith((".svc", ".cluster.local")))

    def ready(self) -> bool:
        return not self.key_env or bool(os.environ.get(self.key_env))

    def predict(self, state, questions):
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.key_env and os.environ.get(self.key_env):
            headers["Authorization"] = f"Bearer {os.environ[self.key_env]}"
        body = json.dumps({"state": state, "model": self.model_id, "questions": questions}).encode()
        req = urllib.request.Request(self.base + "/v1/systemone", data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310 - scheme checked above
                answers = json.loads(resp.read()).get("answers")
        except urllib.error.HTTPError as e:
            msg = f"{self.instance}: HTTP {e.code}: {e.read(200).decode('utf-8', 'replace')}"
            raise (Unavailable if e.code in (408, 429) or e.code >= 500 else Rejected)(msg) from None
        except (OSError, ValueError, AttributeError) as e:  # network, timeout, bad JSON
            raise Unavailable(f"{self.instance}: {e}") from None
        if not isinstance(answers, dict):
            raise Unavailable(f"{self.instance}: response has no answers object")
        return answers


class UniformPlugin(ModelPlugin):
    """No model: uniform answers, so confidence is minimal and the agent never accepts it.
    For wiring tests and clusters without a GPU or network."""

    def predict(self, state, questions):
        out = {}
        for qid, q in questions.items():
            if q["type"] == "noul":
                out[qid] = {"type": "noul", "noul": 0.5}
            else:
                labels = list(q["criteria"]) if q["type"] == "choice" else [str(i) for i in range(len(q["criteria"]))]
                out[qid] = {"type": q["type"], "probabilities": {k: 1 / len(labels) for k in labels}}
        return out


BUILTINS: Dict[str, Type[ModelPlugin]] = {"laya": LayaPlugin, "jev": JevPlugin, "uniform": UniformPlugin}


def create(instance: str, plugin: str, options: Mapping[str, Any]) -> ModelPlugin:
    """Resolves a built-in, an entry point, or "pkg.module:Class"."""
    cls: Optional[Any] = BUILTINS.get(plugin)
    if cls is None:
        eps = [ep for ep in entry_points(group="plumb.decision.plugins") if ep.name == plugin]
        if eps:
            cls = eps[0].load()
        elif ":" in plugin:
            module, _, attr = plugin.partition(":")
            cls = getattr(importlib.import_module(module), attr, None)
    if not (isinstance(cls, type) and issubclass(cls, ModelPlugin)):
        raise ValueError(f"unknown model plugin {plugin!r} (built-ins: {', '.join(BUILTINS)})")
    return cls(instance, options)
