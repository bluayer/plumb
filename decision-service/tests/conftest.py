import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from plumb_decision.plugins import ModelPlugin, NotReady  # noqa: E402


class FakePlugin(ModelPlugin):
    """Puts 0.8 on the first option of every choice; options["raw"] overrides the output."""

    def ready(self):
        return self.options.get("ready", True)

    def predict(self, state, questions):
        if not self.ready():
            raise NotReady("loading")
        self.last_state = state
        if "raw" in self.options:
            return self.options["raw"]
        out = {}
        for qid, q in questions.items():
            if q["type"] == "choice":
                keys = list(q["criteria"])
                out[qid] = {"type": "choice", "probabilities": {k: 0.8 if i == 0 else 0.2 / (len(keys) - 1) for i, k in enumerate(keys)}}
            elif q["type"] == "score":
                out[qid] = {"type": "score", "probabilities": {str(i): 1 / len(q["criteria"]) for i in range(len(q["criteria"]))}}
            else:
                out[qid] = {"type": "noul", "noul": 0.9}
        return out
