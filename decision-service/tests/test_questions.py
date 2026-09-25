import pytest

from plumb_decision import questions


def test_ice_event_loads_and_adds_regions():
    qs = questions.build(questions.load("ice_event"), ["us-west-2", "eu-west-1"])
    assert set(qs) == {"ice_kind", "transient", "action", "urgency", "target_region"}
    assert set(qs["target_region"]["criteria"]) == {"us-west-2", "eu-west-1"}
    assert qs["transient"]["type"] == "noul"


def test_no_regions_means_no_target_question():
    qs = questions.build(questions.load("ice_event"), [])
    assert "target_region" not in qs


def test_single_region_gets_a_second_option():
    qs = questions.build(questions.load("ice_event"), ["us-west-2"])
    assert len(qs["target_region"]["criteria"]) == 2


def test_choice_limit():
    with pytest.raises(questions.QuestionSetError):
        questions.build(questions.load("ice_event"), [f"r{i}" for i in range(21)])


@pytest.mark.parametrize("name", ["", "../etc/passwd", "nope"])
def test_bad_names(name):
    with pytest.raises(questions.QuestionSetError):
        questions.load(name)
