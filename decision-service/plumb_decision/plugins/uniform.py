"""A model-free plugin for wiring tests and local clusters without a GPU or network.

It answers every question with a uniform distribution, so calibrated confidence is at
its minimum and the agent's confidence gate never accepts it.
"""

from __future__ import annotations

from typing import Any, Dict

from .base import ModelPlugin


class UniformPlugin(ModelPlugin):
    plugin_name = "uniform"

    @property
    def model_id(self) -> str:
        return "uniform"

    def predict(self, state: Any, questions: Dict[str, dict]) -> Dict[str, dict]:
        out = {}
        for qid, q in questions.items():
            if q["type"] == "choice":
                n = len(q["criteria"])
                out[qid] = {"type": "choice", "probabilities": {k: 1.0 / n for k in q["criteria"]}}
            elif q["type"] == "score":
                n = len(q["criteria"])
                out[qid] = {"type": "score", "probabilities": {str(i): 1.0 / n for i in range(n)}}
            else:
                out[qid] = {"type": "noul", "noul": 0.5}
        return out
