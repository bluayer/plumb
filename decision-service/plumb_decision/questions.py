"""Question sets sent to Laya. Definitions live in ../questions/*.json."""

from __future__ import annotations

import copy
import json
import os
from typing import Dict, List, Sequence

QUESTIONS_DIR = os.environ.get(
    "DECISION_QUESTIONS_DIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "questions"),
)

# Default limit on options per choice question (Laya's). Each model plugin reports its
# own limit; larger sets must be split hierarchically (region -> zone -> instance type).
MAX_CHOICES = 20


class QuestionSetError(ValueError):
    pass


def load(name: str, directory: str = QUESTIONS_DIR) -> dict:
    if not name or "/" in name or name.startswith("."):
        raise QuestionSetError(f"invalid question set name {name!r}")
    path = os.path.join(directory, f"{name}.json")
    if not os.path.exists(path):
        raise QuestionSetError(f"unknown question set {name!r}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def build(qset: dict, region_choices: Sequence[str], max_choices: int = MAX_CHOICES) -> Dict[str, dict]:
    """Returns the Laya questions dict, adding target_region when regions are given."""
    questions = copy.deepcopy(qset["questions"])
    for qid, q in questions.items():
        crit = q.get("criteria")
        if q["type"] == "choice" and len(crit) > max_choices:
            raise QuestionSetError(f"{qid} has {len(crit)} options, limit is {max_choices}")
    tr = qset.get("target_region")
    choices: List[str] = list(dict.fromkeys(region_choices))
    if tr and choices:
        if len(choices) > max_choices:
            raise QuestionSetError(f"{len(choices)} regions exceed the {max_choices}-option limit")
        if len(choices) == 1:
            # A single-option choice carries no information; keep the question well-formed.
            choices.append("none")
        questions["target_region"] = {
            "type": "choice",
            "instructions": tr["instructions"],
            "criteria": {c: f"region {c}" for c in choices},
        }
    return questions
