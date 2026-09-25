import math

import pytest

from plumb_decision.answers import InvalidAnswer, answer, check_temperatures

CHOICE = {"type": "choice", "criteria": {"a": "", "b": ""}}
SCORE = {"type": "score", "criteria": ["x", "y", "z"]}
NOUL = {"type": "noul"}


def test_choice_renormalizes_and_fills_missing():
    a = answer("q", CHOICE, {"type": "choice", "probabilities": {"a": 2.0}}, {})
    assert a["choice"] == "a" and a["probabilities"]["b"] < 1e-9 and math.isclose(a["confidence"], 1.0)


def test_score_expectation_and_int_keys():
    a = answer("q", SCORE, {"type": "score", "probabilities": {1: 0.5, 2: 0.5}}, {})
    assert math.isclose(a["score"], 1.5)


def test_temperature_softens_and_question_overrides_type():
    raw = {"type": "choice", "probabilities": {"a": 0.9, "b": 0.1}}
    assert answer("q", CHOICE, raw, {"choice": 2.0})["confidence"] < 0.9
    assert answer("q", CHOICE, raw, {"choice": 2.0, "q": 1.0})["confidence"] == pytest.approx(0.9)
    assert answer("t", NOUL, {"type": "noul", "noul": 0.8}, {"t": 0.5})["noul"] > 0.8


@pytest.mark.parametrize("q,raw", [
    (CHOICE, None),
    (CHOICE, {"type": "score", "probabilities": {"a": 1}}),
    (CHOICE, {"type": "choice"}),
    (CHOICE, {"type": "choice", "probabilities": {"c": 1}}),
    (CHOICE, {"type": "choice", "probabilities": {"a": float("nan")}}),
    (CHOICE, {"type": "choice", "probabilities": {"a": -1, "b": 2}}),
    (CHOICE, {"type": "choice", "probabilities": {"a": 0, "b": 0}}),
    (NOUL, {"type": "noul", "noul": 1.5}),
    (NOUL, {"type": "noul"}),
])
def test_rejects_malformed(q, raw):
    with pytest.raises(InvalidAnswer):
        answer("q", q, raw, {})


def test_temperature_range():
    with pytest.raises(ValueError):
        check_temperatures({"choice": 999})
