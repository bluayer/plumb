import json

import pytest

from plumb_decision import models, plugins
from plumb_decision.plugins.uniform import UniformPlugin


def test_builtins_resolve():
    assert set(plugins.BUILTINS) == {"laya", "jev", "uniform"}
    assert plugins.resolve("uniform") is UniformPlugin


def test_import_path_resolves():
    assert plugins.resolve("fakes:FakePlugin").plugin_name == "fake"


@pytest.mark.parametrize("name", ["nope", "json:dumps", "json:JSONDecoder"])
def test_bad_plugins(name):
    with pytest.raises((ValueError, AttributeError)):
        plugins.resolve(name)


def test_models_config(tmp_path):
    cfg = {
        "default": "u2",
        "models": {
            "u1": {"plugin": "uniform", "temperatures": {"choice": 2.0}},
            "u2": {"plugin": "fakes:FakePlugin", "options": {"model": "custom"}},
            "off": {"plugin": "uniform", "enabled": False},
        },
    }
    p = tmp_path / "models.json"
    p.write_text(json.dumps(cfg))
    ms = models.load(str(p))
    assert ms.default == "u2"
    assert ms.get().plugin.model_id == "custom"
    assert ms.get("u1").temperatures.choice == 2.0
    assert ms.get("off") is None


@pytest.mark.parametrize("cfg", [
    {},
    {"models": {"x": {}}},
    {"models": {"x": {"plugin": "uniform"}}, "default": "y"},
    {"models": {"x": {"plugin": "uniform", "temperatures": {"choice": 999}}}},
])
def test_bad_configs(cfg):
    with pytest.raises(ValueError):
        models.from_dict(cfg)


def test_legacy_env_builds_laya(monkeypatch):
    monkeypatch.setenv("LAYA_MODEL", "my/ckpt")
    ms = models.load(None)
    assert ms.default == "laya" and ms.get().plugin.model_id == "my/ckpt"
    assert not ms.get().plugin.capabilities.remote


def test_uniform_has_minimum_confidence():
    from plumb_decision import answers, calibration, questions

    qs = questions.build(questions.load("ice_event"), ["b", "c"])
    raw = UniformPlugin("u", {}).predict({}, qs)
    for qid, q in qs.items():
        a = calibration.calibrate(qid, answers.normalize(qid, q, raw[qid]), calibration.Temperatures())
        assert a["confidence"] <= 0.5 + 1e-9


def test_example_config_and_plugin(monkeypatch):
    import os
    import sys

    from plumb_decision import answers, questions

    examples = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "examples")
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    ms = models.load(os.path.join(examples, "models.json"))
    assert {i.name for i in ms} == {"laya", "jev"}
    assert ms.get("jev").plugin.capabilities.remote

    monkeypatch.syspath_prepend(examples)
    p = plugins.create("mine", "my_plugin:KeywordPlugin", {"word": "quota"})
    qs = questions.build(questions.load("ice_event"), ["b"])
    raw = p.predict({"ev": {"kind": "quota"}}, qs)
    for qid, q in qs.items():
        answers.normalize(qid, q, raw[qid])
    sys.modules.pop("my_plugin", None)
