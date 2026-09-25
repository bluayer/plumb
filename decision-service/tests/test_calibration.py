import math

import pytest

from plumb_decision import calibration


def test_identity_temperature_keeps_probabilities():
    out = calibration.calibrate("action", {"type": "choice", "probabilities": {"a": 0.7, "b": 0.3}}, calibration.Temperatures())
    assert out["choice"] == "a"
    assert math.isclose(out["confidence"], 0.7)


def test_higher_temperature_softens():
    temps = calibration.Temperatures(choice=2.0)
    out = calibration.calibrate("action", {"type": "choice", "probabilities": {"a": 0.9, "b": 0.1}}, temps)
    assert out["choice"] == "a"
    assert out["confidence"] < 0.9
    assert math.isclose(sum(out["probabilities"].values()), 1.0)


def test_per_question_override():
    temps = calibration.Temperatures(noul=1.0, by_question={"transient": 0.5})
    out = calibration.calibrate("transient", {"type": "noul", "noul": 0.8}, temps)
    assert out["noul"] > 0.8


def test_score_expectation():
    out = calibration.calibrate("urgency", {"type": "score", "probabilities": {"0": 0.0, "1": 0.5, "2": 0.5, "3": 0.0}}, calibration.Temperatures())
    assert math.isclose(out["score"], 1.5, abs_tol=1e-6)


def test_rejects_absurd_temperature():
    with pytest.raises(ValueError):
        calibration.Temperatures(choice=100).for_question("x", "choice")


def test_load_file(tmp_path):
    p = tmp_path / "t.json"
    p.write_text('{"choice": 1.5, "by_question": {"action": 2}}')
    t = calibration.Temperatures.load(str(p))
    assert t.for_question("ice_kind", "choice") == 1.5
    assert t.for_question("action", "choice") == 2.0
    assert calibration.Temperatures.load(None).choice == 1.0
