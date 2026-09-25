import json
from concurrent import futures

import grpc
import pytest

from plumb_decision import calibration, models, server
from plumb_decision.plugins import Rejected, Unavailable
from plumb.decision.v1 import decision_pb2, decision_pb2_grpc
from fakes import FakePlugin

STATE = json.dumps({"v": 1, "t": "ice", "home": "a", "rgs": [{"id": "a"}, {"id": "b"}]})


def model_set(*plugins, default=None):
    insts = [models.Instance(p.instance, "fake", p, calibration.Temperatures()) for p in plugins]
    return models.ModelSet(insts, default or insts[0].name)


@pytest.fixture
def stub():
    servers = []

    def make(ms):
        srv = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        decision_pb2_grpc.add_DecisionServiceServicer_to_server(server.DecisionServicer(ms), srv)
        port = srv.add_insecure_port("127.0.0.1:0")
        srv.start()
        servers.append(srv)
        return decision_pb2_grpc.DecisionServiceStub(grpc.insecure_channel(f"127.0.0.1:{port}"))

    yield make
    for s in servers:
        s.stop(None)


def decide(stub, **kw):
    kw.setdefault("question_set", "ice_event")
    kw.setdefault("state_json", STATE)
    return stub.Decide(decision_pb2.DecideRequest(**kw))


def test_decide_returns_all_answers(stub):
    plugin = FakePlugin()
    resp = decide(stub(model_set(plugin)), request_id="r1", region_choices=["b", "c"])
    by_id = {a.question_id: a for a in resp.answers}
    assert (resp.request_id, resp.instance, resp.plugin, resp.model) == ("r1", "fake", "fake", "fake-1")
    assert set(by_id) == {"ice_kind", "transient", "action", "urgency", "target_region"}
    assert by_id["ice_kind"].choice == "capacity"
    assert by_id["action"].choice == "wait_and_retry"
    assert by_id["target_region"].choice == "b"
    assert abs(by_id["transient"].noul - 0.9) < 1e-6
    assert abs(by_id["urgency"].score - 1.5) < 1e-6
    assert plugin.calls[0][0]["home"] == "a"


def test_routes_to_named_instance(stub):
    a, b = FakePlugin("laya", {"model": "m-a"}), FakePlugin("jev", {"model": "m-b"})
    s = stub(model_set(a, b))
    assert decide(s).instance == "laya"
    assert decide(s, model="jev").model == "m-b"
    with pytest.raises(grpc.RpcError) as e:
        decide(s, model="nope")
    assert e.value.code() == grpc.StatusCode.NOT_FOUND


def test_list_models(stub):
    s = stub(model_set(FakePlugin("laya"), FakePlugin("jev", {"ready": False})))
    got = {m.instance: m for m in s.ListModels(decision_pb2.ListModelsRequest()).models}
    assert got["laya"].ready and got["laya"].default
    assert not got["jev"].ready and not got["jev"].default
    assert got["laya"].max_choices == 20


class Raising(FakePlugin):
    def __init__(self, exc):
        super().__init__()
        self.exc = exc

    def predict(self, state, questions):
        raise self.exc


@pytest.mark.parametrize("exc,code", [
    (Unavailable("429"), grpc.StatusCode.UNAVAILABLE),
    (Rejected("401"), grpc.StatusCode.FAILED_PRECONDITION),
    (KeyError("bug"), grpc.StatusCode.INTERNAL),
])
def test_plugin_errors_map_to_status(stub, exc, code):
    with pytest.raises(grpc.RpcError) as e:
        decide(stub(model_set(Raising(exc))))
    assert e.value.code() == code


def test_not_ready_is_unavailable(stub):
    with pytest.raises(grpc.RpcError) as e:
        decide(stub(model_set(FakePlugin(options={"ready": False}))))
    assert e.value.code() == grpc.StatusCode.UNAVAILABLE


def test_malformed_plugin_output_is_rejected(stub):
    bad = FakePlugin(options={"raw": {"ice_kind": {"type": "choice", "probabilities": {"meteor": 1.0}}}})
    with pytest.raises(grpc.RpcError) as e:
        decide(stub(model_set(bad)))
    assert e.value.code() == grpc.StatusCode.INTERNAL


@pytest.mark.parametrize("kw", [
    {"question_set": "nope"},
    {"state_json": "{not json"},
    {"state_json": "x" * 5000},
])
def test_invalid_requests(stub, kw):
    with pytest.raises(grpc.RpcError) as e:
        decide(stub(model_set(FakePlugin())), **kw)
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
