import math

import pytest

from plumb_decision import answers

CHOICE = {"type": "choice", "criteria": {"a": "", "b": ""}}
SCORE = {"type": "score", "criteria": ["x", "y", "z"]}


def test_choice_renormalizes_and_fills_missing():
    out = answers.normalize("q", CHOICE, {"type": "choice", "probabilities": {"a": 2.0}})
    assert out["probabilities"] == {"a": 1.0, "b": 0.0}


def test_score_accepts_int_keys():
    out = answers.normalize("q", SCORE, {"type": "score", "probabilities": {0: 0.5, 1: 0.5}})
    assert math.isclose(sum(out["probabilities"].values()), 1.0)


@pytest.mark.parametrize("q,raw", [
    (CHOICE, None),
    (CHOICE, {"type": "score", "probabilities": {"a": 1}}),
    (CHOICE, {"type": "choice"}),
    (CHOICE, {"type": "choice", "probabilities": {"c": 1}}),
    (CHOICE, {"type": "choice", "probabilities": {"a": float("nan")}}),
    (CHOICE, {"type": "choice", "probabilities": {"a": -1, "b": 2}}),
    (CHOICE, {"type": "choice", "probabilities": {"a": 0, "b": 0}}),
    ({"type": "noul"}, {"type": "noul", "noul": 1.5}),
    ({"type": "noul"}, {"type": "noul"}),
])
def test_rejects(q, raw):
    with pytest.raises(answers.InvalidAnswer):
        answers.normalize("q", q, raw)
