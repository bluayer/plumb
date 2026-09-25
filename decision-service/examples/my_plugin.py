"""A minimal custom model plugin. Use it with

    {"models": {"mine": {"plugin": "my_plugin:KeywordPlugin", "options": {"word": "quota"}}}}

and PYTHONPATH pointing at this directory (or package it with an entry point in the
"plumb.decision.plugins" group).
"""

import json

from plumb_decision.plugins import ModelPlugin


class KeywordPlugin(ModelPlugin):
    plugin_name = "keyword"

    @property
    def model_id(self):
        return "keyword-v1"

    def predict(self, state, questions):
        hit = self.options.get("word", "") in json.dumps(state)
        answers = {}
        for qid, q in questions.items():
            if q["type"] == "choice":
                labels = list(q["criteria"])
                # Put most of the mass on the first option when the keyword appears.
                first = 0.9 if hit else 1.0 / len(labels)
                rest = (1 - first) / max(len(labels) - 1, 1)
                answers[qid] = {"type": "choice", "probabilities": {k: first if i == 0 else rest for i, k in enumerate(labels)}}
            elif q["type"] == "score":
                n = len(q["criteria"])
                answers[qid] = {"type": "score", "probabilities": {str(i): 1 / n for i in range(n)}}
            else:
                answers[qid] = {"type": "noul", "noul": 0.9 if hit else 0.5}
        return answers
