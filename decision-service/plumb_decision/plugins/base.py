"""The model plugin contract.

A plugin adapts one decision model (a local checkpoint, a hosted API, ...) to Plumb's
typed questions. It receives the compressed state and a Laya/Jev-style question dict
and returns *raw* answers in this normalized shape, keyed by question id:

    choice: {"type": "choice", "probabilities": {"<label>": p, ...}}
    score:  {"type": "score",  "probabilities": {"0": p, "1": p, ...}}
    noul:   {"type": "noul",   "noul": P(true)}

Extra keys are ignored. The server validates every answer against the question
(labels, levels, finite probabilities), renormalizes, applies the instance's
temperature calibration and derives choice/score/confidence itself, so plugins
never need to and a misbehaving plugin cannot leak malformed answers to the agent.

To add a model, subclass ModelPlugin and either
  * reference it from the models config as "plugin": "my_pkg.module:MyPlugin", or
  * publish it under the "plumb.decision.plugins" entry point group.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Mapping


@dataclass(frozen=True)
class Capabilities:
    # Largest number of options one choice question may have.
    max_choices: int = 20
    # Largest state_json accepted (bytes). Keeps inputs inside the model's context.
    max_state_bytes: int = 4096
    # True when predict() sends the state outside the cluster (hosted APIs).
    remote: bool = False


class PluginError(RuntimeError):
    """Base class for errors a plugin raises on purpose."""


class NotReady(PluginError):
    """The model is still loading or failed to load. Maps to UNAVAILABLE."""


class Unavailable(PluginError):
    """A transient failure (timeout, 429, 5xx). Maps to UNAVAILABLE."""


class Rejected(PluginError):
    """The model refused the request (bad credentials, invalid input). Maps to FAILED_PRECONDITION."""


class ModelPlugin(abc.ABC):
    #: Name used in the models config ("plugin": "<name>").
    plugin_name: ClassVar[str] = ""

    def __init__(self, instance: str, options: Mapping[str, Any]):
        self.instance = instance
        self.options = dict(options)

    @property
    @abc.abstractmethod
    def model_id(self) -> str:
        """Provider-side model identifier, reported with every response."""

    @property
    def capabilities(self) -> Capabilities:
        return Capabilities()

    def start(self) -> None:
        """Begin loading or connecting. Must not block for long; load heavy models in a thread."""

    def ready(self) -> bool:
        return True

    @abc.abstractmethod
    def predict(self, state: Any, questions: Dict[str, dict]) -> Dict[str, dict]:
        """Returns raw answers keyed by question id (see module docstring)."""

    def close(self) -> None:
        """Release resources."""
