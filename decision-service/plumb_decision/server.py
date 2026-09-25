"""gRPC server for plumb.decision.v1.DecisionService."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from . import answers, calibration, models, questions  # noqa: F401  (package init sets sys.path)
from .plugins import NotReady, Rejected, Unavailable
from plumb.decision.v1 import decision_pb2, decision_pb2_grpc  # type: ignore  # generated

log = logging.getLogger("plumb_decision")


class DecisionServicer(decision_pb2_grpc.DecisionServiceServicer):
    def __init__(self, model_set: models.ModelSet, questions_dir: str = questions.QUESTIONS_DIR):
        self.models = model_set
        self.questions_dir = questions_dir

    def Decide(self, request, context):
        start = time.monotonic()
        inst = self.models.get(request.model)
        if inst is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"model instance {request.model!r} is not configured")
        caps = inst.plugin.capabilities
        if len(request.state_json) > caps.max_state_bytes:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f"state is {len(request.state_json)} bytes, {inst.name} accepts {caps.max_state_bytes}")
        try:
            state = json.loads(request.state_json)
        except json.JSONDecodeError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"state_json: {e}")
        try:
            qset = questions.load(request.question_set, self.questions_dir)
            qs = questions.build(qset, list(request.region_choices), max_choices=caps.max_choices)
        except questions.QuestionSetError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

        try:
            raw = inst.plugin.predict(state, qs)
        except (NotReady, Unavailable) as e:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        except Rejected as e:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(e))
        except Exception:  # noqa: BLE001 - a plugin bug must not take the server down
            log.exception("%s: predict failed", inst.name)
            context.abort(grpc.StatusCode.INTERNAL, f"{inst.name}: predict failed")

        resp = decision_pb2.DecideResponse(request_id=request.request_id, model=inst.plugin.model_id,
                                           instance=inst.name, plugin=inst.plugin_name)
        for qid, q in qs.items():
            try:
                norm = answers.normalize(qid, q, raw.get(qid) if isinstance(raw, dict) else None)
            except answers.InvalidAnswer as e:
                context.abort(grpc.StatusCode.INTERNAL, f"{inst.name}: {e}")
            a = calibration.calibrate(qid, norm, inst.temperatures)
            resp.answers.append(decision_pb2.Answer(
                question_id=qid, type=a["type"], choice=a.get("choice", ""), score=a.get("score", 0.0),
                noul=a.get("noul", 0.0), confidence=a["confidence"], probabilities=a["probabilities"]))
        resp.latency_ms = (time.monotonic() - start) * 1000.0
        return resp

    def ListModels(self, request, context):
        out = decision_pb2.ListModelsResponse()
        for inst in self.models:
            caps = inst.plugin.capabilities
            out.models.append(decision_pb2.ModelInfo(
                instance=inst.name, plugin=inst.plugin_name, model=inst.plugin.model_id, ready=inst.plugin.ready(),
                default=inst.name == self.models.default, max_choices=caps.max_choices,
                max_state_bytes=caps.max_state_bytes, remote=caps.remote))
        return out


def serve(model_set: models.ModelSet, address: str, workers: int = 4) -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    decision_pb2_grpc.add_DecisionServiceServicer_to_server(DecisionServicer(model_set), server)
    # "" reports the process; each instance name reports that model's readiness.
    hs = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(hs, server)
    hs.set("", health_pb2.HealthCheckResponse.SERVING)
    server.add_insecure_port(address)
    server.start()

    def watch_ready():
        pending = {i.name: i for i in model_set}
        for name in pending:
            hs.set(name, health_pb2.HealthCheckResponse.NOT_SERVING)
        while pending:
            for name, inst in list(pending.items()):
                if inst.plugin.ready():
                    hs.set(name, health_pb2.HealthCheckResponse.SERVING)
                    log.info("model %s ready", name)
                    del pending[name]
            time.sleep(1)

    futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="ready").submit(watch_ready)
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default=os.environ.get("DECISION_ADDRESS", "127.0.0.1:50051"))
    parser.add_argument("--models-config", default=os.environ.get("PLUMB_MODELS_CONFIG"),
                        help="JSON models config; without it a single Laya instance is built from LAYA_* env")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    model_set = models.load(args.models_config)
    model_set.start()
    server = serve(model_set, args.address, args.workers)
    log.info("serving on %s (default model %s)", args.address, model_set.default)
    try:
        server.wait_for_termination()
    finally:
        model_set.close()


if __name__ == "__main__":
    main()
