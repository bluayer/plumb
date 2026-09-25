"""gRPC server for plumb.decision.v1.DecisionService.

Models config (JSON, --models-config):

    {"default": "laya",
     "models": {"laya": {"plugin": "laya", "options": {"model": "convaiinnovations/laya"},
                         "temperatures": {"choice": 1.0, "action": 1.3}},
                "jev":  {"plugin": "jev", "options": {"model": "jev-latest"}}}}

"plugin" is a built-in, an entry point or "pkg.module:Class"; "enabled": false skips an
instance. Temperatures are per instance because each model is calibrated separately.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import time
from concurrent import futures
from dataclasses import dataclass
from typing import Dict, Sequence

import grpc
from grpc_health.v1 import health, health_pb2_grpc

from . import answers, decision_pb2, decision_pb2_grpc, plugins

log = logging.getLogger("plumb_decision")

QUESTIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "questions")


@dataclass
class Instance:
    plugin_name: str
    plugin: plugins.ModelPlugin
    temperatures: Dict[str, float]


def load_models(cfg: dict) -> tuple[Dict[str, Instance], str]:
    instances = {}
    for name, spec in (cfg.get("models") or {}).items():
        if not isinstance(spec, dict) or not spec.get("plugin"):
            raise ValueError(f"model {name!r} needs a 'plugin'")
        if spec.get("enabled", True):
            instances[name] = Instance(spec["plugin"], plugins.create(name, spec["plugin"], spec.get("options", {})),
                                       answers.check_temperatures(spec.get("temperatures", {})))
    default = cfg.get("default") or next(iter(instances), "")
    if default not in instances:
        raise ValueError(f"default model {default!r} is not an enabled instance")
    return instances, default


def build_questions(name: str, region_choices: Sequence[str], max_choices: int) -> Dict[str, dict]:
    """Loads questions/<name>.json and adds target_region from the candidate regions.
    Larger option sets must be split hierarchically (region → zone → instance type)."""
    if not name.isidentifier() or not os.path.exists(path := os.path.join(QUESTIONS_DIR, f"{name}.json")):
        raise ValueError(f"unknown question set {name!r}")
    with open(path, encoding="utf-8") as f:
        qset = json.load(f)
    qs = copy.deepcopy(qset["questions"])
    choices = list(dict.fromkeys(region_choices))
    if choices and "target_region" in qset:
        # A single-option choice carries no information; keep the question well-formed.
        qs["target_region"] = {"type": "choice", "instructions": qset["target_region"]["instructions"],
                               "criteria": {c: f"region {c}" for c in choices + ["none"] * (len(choices) == 1)}}
    for qid, q in qs.items():
        if q["type"] == "choice" and len(q["criteria"]) > max_choices:
            raise ValueError(f"{qid} has {len(q['criteria'])} options, limit is {max_choices}")
    return qs


class DecisionServicer(decision_pb2_grpc.DecisionServiceServicer):
    def __init__(self, instances: Dict[str, Instance], default: str):
        self.instances, self.default = instances, default

    def Decide(self, request, context):
        start = time.monotonic()
        name = request.model or self.default
        if (inst := self.instances.get(name)) is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"model instance {name!r} is not configured")
        p = inst.plugin
        try:
            if len(request.state_json) > p.max_state_bytes:
                raise ValueError(f"state is {len(request.state_json)} bytes, {name} accepts {p.max_state_bytes}")
            state = json.loads(request.state_json)
            qs = build_questions(request.question_set, request.region_choices, p.max_choices)
        except ValueError as e:  # includes JSONDecodeError
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        try:
            raw = p.predict(state, qs)
            out = {qid: answers.answer(qid, q, raw.get(qid), inst.temperatures) for qid, q in qs.items()}
        except (plugins.NotReady, plugins.Unavailable) as e:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        except plugins.Rejected as e:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(e))
        except Exception:  # a plugin bug or malformed answer must not reach the agent
            log.exception("%s: predict failed", name)
            context.abort(grpc.StatusCode.INTERNAL, f"{name}: predict failed or returned malformed answers")
        return decision_pb2.DecideResponse(
            request_id=request.request_id, model=p.model_id, instance=name, plugin=inst.plugin_name,
            latency_ms=(time.monotonic() - start) * 1000,
            answers=[decision_pb2.Answer(question_id=qid, type=a["type"], choice=a.get("choice", ""), score=a.get("score", 0),
                                         noul=a.get("noul", 0), confidence=a["confidence"], probabilities=a["probabilities"])
                     for qid, a in out.items()])


def serve(instances: Dict[str, Instance], default: str, address: str, workers: int = 4) -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    decision_pb2_grpc.add_DecisionServiceServicer_to_server(DecisionServicer(instances, default), server)
    health_pb2_grpc.add_HealthServicer_to_server(health.HealthServicer(), server)  # liveness only
    server.add_insecure_port(address)
    server.start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default="127.0.0.1:50051")
    parser.add_argument("--models-config", required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    with open(args.models_config, encoding="utf-8") as f:
        instances, default = load_models(json.load(f))
    for name, inst in instances.items():
        log.info("model %s: plugin %s, model %s, remote=%s", name, inst.plugin_name, inst.plugin.model_id, inst.plugin.remote)
        inst.plugin.start()
    serve(instances, default, args.address, args.workers).wait_for_termination()


if __name__ == "__main__":
    main()
