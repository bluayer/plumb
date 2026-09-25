import json
from concurrent import futures

import grpc
import pytest
from conftest import FakePlugin

from plumb_decision import decision_pb2, decision_pb2_grpc, plugins, server

STATE = json.dumps({"v": 1, "t": "ice", "home": "a", "rgs": [{"id": "a"}, {"id": "b"}]})


def instances(**plugins_by_name):
    return {n: server.Instance("fake", p, {}) for n, p in plugins_by_name.items()}


@pytest.fixture
def stub():
    servers = []

    def make(insts, default=None):
        srv = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        decision_pb2_grpc.add_DecisionServiceServicer_to_server(server.DecisionServicer(insts, default or next(iter(insts))), srv)
        port = srv.add_insecure_port("127.0.0.1:0")
        srv.start()
        servers.append(srv)
        return decision_pb2_grpc.DecisionServiceStub(grpc.insecure_channel(f"127.0.0.1:{port}"))

    yield make
    for s in servers:
        s.stop(None)


def decide(stub, **kw):
    return stub.Decide(decision_pb2.DecideRequest(**{"question_set": "ice_event", "state_json": STATE, **kw}))


def test_decide(stub):
    p = FakePlugin("fake", {"model": "fake-1"})
    resp = decide(stub(instances(fake=p)), request_id="r1", region_choices=["b", "c"])
    got = {a.question_id: a for a in resp.answers}
    assert (resp.request_id, resp.instance, resp.plugin, resp.model) == ("r1", "fake", "fake", "fake-1")
    assert set(got) == {"ice_kind", "transient", "action", "urgency", "target_region"}
    assert (got["ice_kind"].choice, got["action"].choice, got["target_region"].choice) == ("capacity", "wait_and_retry", "b")
    assert got["transient"].noul == pytest.approx(0.9) and got["urgency"].score == pytest.approx(1.5)
    assert p.last_state["home"] == "a"


def test_routes_to_named_instance(stub):
    s = stub(instances(laya=FakePlugin("laya", {}), jev=FakePlugin("jev", {"model": "m-b"})))
    assert decide(s).instance == "laya" and decide(s, model="jev").model == "m-b"
    with pytest.raises(grpc.RpcError) as e:
        decide(s, model="nope")
    assert e.value.code() == grpc.StatusCode.NOT_FOUND


class Raising(FakePlugin):
    def predict(self, state, questions):
        raise self.options["exc"]


@pytest.mark.parametrize("p,code", [
    (Raising("x", {"exc": plugins.Unavailable("429")}), grpc.StatusCode.UNAVAILABLE),
    (Raising("x", {"exc": plugins.Rejected("401")}), grpc.StatusCode.FAILED_PRECONDITION),
    (Raising("x", {"exc": KeyError("bug")}), grpc.StatusCode.INTERNAL),
    (FakePlugin("x", {"ready": False}), grpc.StatusCode.UNAVAILABLE),
    (FakePlugin("x", {"raw": {"ice_kind": {"type": "choice", "probabilities": {"meteor": 1}}}}), grpc.StatusCode.INTERNAL),
])
def test_errors_map_to_status(stub, p, code):
    with pytest.raises(grpc.RpcError) as e:
        decide(stub(instances(x=p)))
    assert e.value.code() == code


@pytest.mark.parametrize("kw", [{"question_set": "nope"}, {"question_set": "../x"}, {"state_json": "{not json"}, {"state_json": "x" * 5000},
                                {"region_choices": [f"r{i}" for i in range(21)]}])
def test_invalid_requests(stub, kw):
    with pytest.raises(grpc.RpcError) as e:
        decide(stub(instances(x=FakePlugin("x", {}))), **kw)
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_single_region_gets_a_second_option():
    assert len(server.build_questions("ice_event", ["b"], 20)["target_region"]["criteria"]) == 2
    assert "target_region" not in server.build_questions("ice_event", [], 20)


def test_load_models():
    insts, default = server.load_models({"default": "b", "models": {
        "a": {"plugin": "uniform", "temperatures": {"choice": 2}},
        "b": {"plugin": "conftest:FakePlugin", "options": {"model": "custom"}},
        "off": {"plugin": "uniform", "enabled": False}}})
    assert default == "b" and insts["b"].plugin.model_id == "custom" and insts["a"].temperatures == {"choice": 2.0} and "off" not in insts
    for bad in [{}, {"models": {"x": {}}}, {"models": {"x": {"plugin": "uniform"}}, "default": "y"},
                {"models": {"x": {"plugin": "uniform", "temperatures": {"choice": 999}}}}]:
        with pytest.raises(ValueError):
            server.load_models(bad)
