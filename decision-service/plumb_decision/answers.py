"""Validation of plugin output against the question it answers."""

from __future__ import annotations

import math
from typing import Dict


class InvalidAnswer(ValueError):
    pass


def _probs(qid: str, raw: dict, expected: list) -> Dict[str, float]:
    p = raw.get("probabilities")
    if not isinstance(p, dict):
        raise InvalidAnswer(f"{qid}: probabilities missing")
    out: Dict[str, float] = {}
    for k in expected:
        v = p.get(k, p.get(int(k)) if k.isdigit() else None)
        if v is None:
            v = 0.0
        try:
            f = float(v)
        except (TypeError, ValueError):
            raise InvalidAnswer(f"{qid}: probability for {k!r} is not a number") from None
        if not math.isfinite(f) or f < 0:
            raise InvalidAnswer(f"{qid}: probability for {k!r} is {v!r}")
        out[k] = f
    unknown = {str(k) for k in p} - set(expected)
    if unknown:
        raise InvalidAnswer(f"{qid}: unknown options {sorted(unknown)}")
    total = sum(out.values())
    if total <= 0:
        raise InvalidAnswer(f"{qid}: probabilities sum to {total}")
    return {k: v / total for k, v in out.items()}


def normalize(qid: str, question: dict, raw: object) -> dict:
    """Returns {"type", "probabilities"} for choice/score or {"type", "noul"} for noul."""
    if not isinstance(raw, dict):
        raise InvalidAnswer(f"{qid}: answer is not an object")
    qtype = question["type"]
    if raw.get("type") != qtype:
        raise InvalidAnswer(f"{qid}: answer type {raw.get('type')!r}, want {qtype!r}")
    if qtype == "choice":
        return {"type": "choice", "probabilities": _probs(qid, raw, [str(k) for k in question["criteria"]])}
    if qtype == "score":
        return {"type": "score", "probabilities": _probs(qid, raw, [str(i) for i in range(len(question["criteria"]))])}
    try:
        n = float(raw.get("noul"))
    except (TypeError, ValueError):
        raise InvalidAnswer(f"{qid}: noul is not a number") from None
    if not (math.isfinite(n) and 0.0 <= n <= 1.0):
        raise InvalidAnswer(f"{qid}: noul {n} outside [0, 1]")
    return {"type": "noul", "noul": n}
