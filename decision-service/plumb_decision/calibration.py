"""Per-question-type temperature scaling applied on top of Laya's probabilities.

Laya ships temperatures fitted on its own training distribution. After domain
fine-tuning, fit one scalar per question type (or per question id) on held-out
decisions and put it in a JSON file:

    {"choice": 1.3, "score": 1.1, "noul": 0.9, "by_question": {"action": 1.5}}
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from typing import Dict, Mapping


@dataclass
class Temperatures:
    choice: float = 1.0
    score: float = 1.0
    noul: float = 1.0
    by_question: Dict[str, float] = field(default_factory=dict)

    def for_question(self, qid: str, qtype: str) -> float:
        t = self.by_question.get(qid, getattr(self, qtype, 1.0))
        if not (0.05 <= t <= 20.0):
            raise ValueError(f"temperature {t} for {qid} outside [0.05, 20]")
        return t

    @classmethod
    def load(cls, path: str | None) -> "Temperatures":
        if not path or not os.path.exists(path):
            return cls()
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    @classmethod
    def from_dict(cls, raw: Mapping) -> "Temperatures":
        t = cls(
            choice=float(raw.get("choice", 1.0)),
            score=float(raw.get("score", 1.0)),
            noul=float(raw.get("noul", 1.0)),
            by_question={k: float(v) for k, v in raw.get("by_question", {}).items()},
        )
        for qtype in ("choice", "score", "noul"):
            t.for_question("_", qtype)  # validates the range
        for qid in t.by_question:
            t.for_question(qid, "choice")
        return t


def rescale(probs: Mapping[str, float], temperature: float) -> Dict[str, float]:
    """p_i ∝ p_i^(1/T): equivalent to dividing logits by T."""
    if temperature == 1.0:
        total = sum(probs.values()) or 1.0
        return {k: v / total for k, v in probs.items()}
    logs = {k: math.log(max(v, 1e-12)) / temperature for k, v in probs.items()}
    m = max(logs.values())
    exp = {k: math.exp(v - m) for k, v in logs.items()}
    total = sum(exp.values())
    return {k: v / total for k, v in exp.items()}


def calibrate(qid: str, answer: dict, temps: Temperatures) -> dict:
    """Takes a normalized answer (see answers.normalize) and returns
    {type, choice, score, noul, confidence, probabilities} after scaling."""
    qtype = answer["type"]
    t = temps.for_question(qid, qtype)
    if qtype == "noul":
        p_true = float(answer["noul"])
        probs = rescale({"false": 1.0 - p_true, "true": p_true}, t)
        return {"type": "noul", "noul": probs["true"], "confidence": max(probs.values()), "probabilities": probs}
    probs = rescale({str(k): float(v) for k, v in answer["probabilities"].items()}, t)
    out = {"type": qtype, "probabilities": probs, "confidence": max(probs.values())}
    if qtype == "choice":
        out["choice"] = max(probs, key=probs.get)
    else:
        out["score"] = sum(int(k) * v for k, v in probs.items())
    return out
