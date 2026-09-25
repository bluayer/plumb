from plumb_decision.plugins import ModelPlugin, NotReady


class FakePlugin(ModelPlugin):
    """Puts 0.8 on the first option of every choice question."""

    plugin_name = "fake"

    def __init__(self, instance="fake", options=None):
        super().__init__(instance, options or {})
        self.calls = []

    @property
    def model_id(self):
        return self.options.get("model", "fake-1")

    def ready(self):
        return self.options.get("ready", True)

    def predict(self, state, questions):
        if not self.ready():
            raise NotReady("loading")
        self.calls.append((state, questions))
        if "raw" in self.options:
            return self.options["raw"]
        out = {}
        for qid, q in questions.items():
            if q["type"] == "choice":
                keys = list(q["criteria"])
                out[qid] = {"type": "choice", "probabilities": {k: (0.8 if i == 0 else 0.2 / (len(keys) - 1)) for i, k in enumerate(keys)}}
            elif q["type"] == "score":
                n = len(q["criteria"])
                out[qid] = {"type": "score", "probabilities": {str(i): 1.0 / n for i in range(n)}}
            else:
                out[qid] = {"type": "noul", "noul": 0.9}
        return out
