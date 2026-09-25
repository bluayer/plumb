"""Model plugin registry.

Plugins are resolved, in order, from:
  1. built-ins: laya, jev, uniform
  2. the "plumb.decision.plugins" entry point group (third-party packages)
  3. an import path "package.module:ClassName"
"""

from __future__ import annotations

import importlib
from importlib.metadata import entry_points
from typing import Any, Dict, Mapping, Type

from .base import Capabilities, ModelPlugin, NotReady, PluginError, Rejected, Unavailable
from .jev import JevPlugin
from .laya import LayaPlugin
from .uniform import UniformPlugin

ENTRY_POINT_GROUP = "plumb.decision.plugins"

BUILTINS: Dict[str, Type[ModelPlugin]] = {p.plugin_name: p for p in (LayaPlugin, JevPlugin, UniformPlugin)}

__all__ = [
    "BUILTINS",
    "Capabilities",
    "ModelPlugin",
    "NotReady",
    "PluginError",
    "Rejected",
    "Unavailable",
    "create",
    "resolve",
]


def resolve(name: str) -> Type[ModelPlugin]:
    if name in BUILTINS:
        return BUILTINS[name]
    for ep in entry_points(group=ENTRY_POINT_GROUP):
        if ep.name == name:
            return _check(ep.load(), name)
    if ":" in name:
        module, _, attr = name.partition(":")
        return _check(getattr(importlib.import_module(module), attr), name)
    raise ValueError(f"unknown model plugin {name!r} (built-ins: {', '.join(sorted(BUILTINS))})")


def _check(cls: Any, name: str) -> Type[ModelPlugin]:
    if not (isinstance(cls, type) and issubclass(cls, ModelPlugin)):
        raise ValueError(f"{name!r} is not a ModelPlugin subclass")
    return cls


def create(instance: str, plugin: str, options: Mapping[str, Any]) -> ModelPlugin:
    return resolve(plugin)(instance, options)
