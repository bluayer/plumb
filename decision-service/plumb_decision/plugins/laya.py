"""Laya (convaiinnovations/laya): local, non-autoregressive System 1 model."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

from .base import Capabilities, ModelPlugin, NotReady

log = logging.getLogger(__name__)


class LayaPlugin(ModelPlugin):
    """Loads a Laya checkpoint in the background so the server starts immediately.

    options:
      model:    checkpoint id or path (default convaiinnovations/laya); point it at the
                fine-tuned checkpoint once it exists — the base model is near random here.
      device:   torch device (default auto)
      max_len:  token limit override (default: checkpoint's own, 512 or 1024)
    """

    plugin_name = "laya"

    def __init__(self, instance, options):
        super().__init__(instance, options)
        self._model = self.options.get("model", "convaiinnovations/laya")
        self._device: Optional[str] = self.options.get("device")
        self._max_len: Optional[int] = self.options.get("max_len")
        self._agent = None
        self._error: Optional[BaseException] = None

    @property
    def model_id(self) -> str:
        return self._model

    @property
    def capabilities(self) -> Capabilities:
        # Choice questions are limited to 20 options; the state budget matches the
        # agent's 320-token summary plus headroom, far below the 512-token context.
        return Capabilities(max_choices=20, max_state_bytes=4096, remote=False)

    def start(self) -> None:
        threading.Thread(target=self._load, name=f"laya-load-{self.instance}", daemon=True).start()

    def _load(self) -> None:
        try:
            import laya  # heavy import (torch); deferred on purpose

            start = time.monotonic()
            self._agent = laya.load(self._model, device=self._device)
            log.info("%s: loaded %s in %.1fs", self.instance, self._model, time.monotonic() - start)
        except BaseException as e:  # noqa: BLE001 - surfaced through ready()/predict()
            log.exception("%s: loading %s failed", self.instance, self._model)
            self._error = e

    def ready(self) -> bool:
        return self._agent is not None

    def predict(self, state: Any, questions: Dict[str, dict]) -> Dict[str, dict]:
        agent = self._agent
        if agent is None:
            raise NotReady(f"model not loaded: {self._error or 'loading'}")
        kwargs = {"max_len": self._max_len} if self._max_len else {}
        # Laya answers already carry "probabilities" (choice/score) and "noul".
        return agent.system_one(state, questions, **kwargs)["answers"]
