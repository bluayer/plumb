"""Turns raw plugin output into calibrated answers. Plugin output is never trusted: every
answer is checked against its question, renormalized, then temperature-scaled."""

from __future__ import annotations

import math
from typing import Dict, Mapping


class InvalidAnswer(ValueError):
    pass


def check_temperatures(temps: Mapping[str, float]) -> Dict[str, float]:
    """Temperatures by question id or type ("choice", "score", "noul"); default 1."""
    out = {k: float(v) for k, v in temps.items()}
    for k, t in out.items():
        if not 0.05 <= t <= 20:
            raise ValueError(f"temperature {t} for {k!r} outside [0.05, 20]")
    return out


def _probs(qid: str, raw: dict, labels: list) -> Dict[str, float]:
    p = raw.get("probabilities")
    if not isinstance(p, dict):
        raise InvalidAnswer(f"{qid}: probabilities missing")
    p = {str(k): v for k, v in p.items()}
    if unknown := set(p) - set(labels):
        raise InvalidAnswer(f"{qid}: unknown options {sorted(unknown)}")
    try:
        out = {k: float(p.get(k, 0.0)) for k in labels}
    except (TypeError, ValueError):
        raise InvalidAnswer(f"{qid}: probability is not a number") from None
    if not all(math.isfinite(v) and v >= 0 for v in out.values()) or sum(out.values()) <= 0:
        raise InvalidAnswer(f"{qid}: probabilities {out} are not a distribution")
    return out


def _scale(probs: Dict[str, float], t: float) -> Dict[str, float]:
    """p_i ∝ p_i^(1/T), i.e. logits divided by T."""
    logs = {k: math.log(max(v, 1e-12)) / t for k, v in probs.items()}
    top = max(logs.values())
    exp = {k: math.exp(v - top) for k, v in logs.items()}
    total = sum(exp.values())
    return {k: v / total for k, v in exp.items()}


def answer(qid: str, question: dict, raw: object, temps: Mapping[str, float]) -> dict:
    """Returns {type, probabilities, confidence} plus choice, score or noul."""
    qtype = question["type"]
    if not isinstance(raw, dict) or raw.get("type") != qtype:
        raise InvalidAnswer(f"{qid}: expected a {qtype} answer, got {raw!r:.80}")
    if qtype == "noul":
        try:
            n = float(raw.get("noul"))
        except (TypeError, ValueError):
            raise InvalidAnswer(f"{qid}: noul is not a number") from None
        if not 0 <= n <= 1:
            raise InvalidAnswer(f"{qid}: noul {n} outside [0, 1]")
        probs = {"false": 1 - n, "true": n}
    else:
        labels = [str(k) for k in question["criteria"]] if qtype == "choice" else [str(i) for i in range(len(question["criteria"]))]
        probs = _probs(qid, raw, labels)
    probs = _scale(probs, temps.get(qid, temps.get(qtype, 1.0)))
    out = {"type": qtype, "probabilities": probs, "confidence": max(probs.values())}
    if qtype == "noul":
        out["noul"] = probs["true"]
    elif qtype == "choice":
        out["choice"] = max(probs, key=probs.get)
    else:
        out["score"] = sum(int(k) * v for k, v in probs.items())
    return out
