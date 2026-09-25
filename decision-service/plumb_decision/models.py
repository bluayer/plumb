"""Model instances configured for the decision service.

Config (JSON; path from --models-config or PLUMB_MODELS_CONFIG):

    {
      "default": "laya",
      "models": {
        "laya": {"plugin": "laya", "options": {"model": "convaiinnovations/laya"},
                 "temperatures": {"choice": 1.0, "score": 1.0, "noul": 1.0}},
        "jev":  {"plugin": "jev",  "options": {"model": "jev-latest", "timeout": 3}}
      }
    }

"plugin" is a built-in name, an entry point name or "package.module:Class".
Temperatures are per instance because each model is calibrated separately.
Without a config file a single Laya instance is built from the LAYA_* variables.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from . import calibration, plugins

log = logging.getLogger(__name__)


@dataclass
class Instance:
    name: str
    plugin_name: str
    plugin: plugins.ModelPlugin
    temperatures: calibration.Temperatures


class ModelSet:
    def __init__(self, instances: List[Instance], default: str):
        self._by_name: Dict[str, Instance] = {i.name: i for i in instances}
        if not self._by_name:
            raise ValueError("no model instances configured")
        if default not in self._by_name:
            raise ValueError(f"default model {default!r} is not configured")
        self.default = default

    def get(self, name: str = "") -> Optional[Instance]:
        return self._by_name.get(name or self.default)

    def __iter__(self):
        return iter(self._by_name.values())

    def start(self) -> None:
        for i in self:
            log.info("starting model %s (plugin %s, model %s, remote=%s)", i.name, i.plugin_name, i.plugin.model_id,
                     i.plugin.capabilities.remote)
            i.plugin.start()

    def close(self) -> None:
        for i in self:
            try:
                i.plugin.close()
            except Exception:  # noqa: BLE001
                log.exception("closing %s", i.name)


def _temperatures(raw) -> calibration.Temperatures:
    if raw is None:
        return calibration.Temperatures()
    if isinstance(raw, str):
        return calibration.Temperatures.load(raw)
    return calibration.Temperatures.from_dict(raw)


def from_dict(cfg: dict) -> ModelSet:
    models = cfg.get("models")
    if not isinstance(models, dict) or not models:
        raise ValueError("models config needs a non-empty 'models' object")
    instances = []
    for name, spec in models.items():
        if not isinstance(spec, dict) or not spec.get("plugin"):
            raise ValueError(f"model {name!r} needs a 'plugin'")
        if spec.get("enabled", True) is False:
            continue
        instances.append(Instance(name=name, plugin_name=spec["plugin"],
                                  plugin=plugins.create(name, spec["plugin"], spec.get("options", {})),
                                  temperatures=_temperatures(spec.get("temperatures"))))
    default = cfg.get("default") or (instances[0].name if instances else "")
    return ModelSet(instances, default)


def load(path: Optional[str]) -> ModelSet:
    if path:
        with open(path, encoding="utf-8") as f:
            return from_dict(json.load(f))
    # Backwards-compatible single-Laya setup.
    options = {"model": os.environ.get("LAYA_MODEL", "convaiinnovations/laya")}
    if os.environ.get("LAYA_DEVICE"):
        options["device"] = os.environ["LAYA_DEVICE"]
    if os.environ.get("LAYA_MAX_LEN"):
        options["max_len"] = int(os.environ["LAYA_MAX_LEN"])
    return from_dict({"default": "laya", "models": {"laya": {
        "plugin": "laya", "options": options, "temperatures": os.environ.get("DECISION_TEMPERATURES")}}})
