import json

import pytest

import store
import sync_schedules
from tests.lambdas import fakes
from tests.lambdas.test_pipeline_api import HLTV, TRAIN


@pytest.fixture
def scheduler(monkeypatch):
    s = fakes.Scheduler()
    monkeypatch.setattr(sync_schedules, "scheduler", s)
    monkeypatch.setattr(store, "ssm", fakes.Ssm([HLTV, TRAIN]))
    return s


def test_new_schedules_start_disabled_and_carry_their_parameters(scheduler):
    result = sync_schedules.handler({}, None)

    schedule = scheduler.schedules["hltv_cs2.weekly"]
    assert result == {"created": ["hltv_cs2.weekly"], "updated": [], "deleted": []}
    assert schedule["State"] == "DISABLED" and schedule["GroupName"] == "gnome-pipelines"
    assert json.loads(schedule["Target"]["Input"]) == {
        "pipeline_name": "hltv_cs2", "trigger": "scheduled", "schedule": "weekly", "parameters": {"days_back": 8}}


def test_a_changed_schedule_keeps_its_state_and_an_unchanged_one_is_left_alone(scheduler, monkeypatch):
    sync_schedules.handler({}, None)
    scheduler.schedules["hltv_cs2.weekly"]["State"] = "ENABLED"
    assert sync_schedules.handler({}, None)["updated"] == []

    changed = {**HLTV, "schedules": [{**HLTV["schedules"][0], "expression": "cron(0 5 ? * MON *)"}]}
    monkeypatch.setattr(store, "ssm", fakes.Ssm([changed, TRAIN]))

    assert sync_schedules.handler({}, None)["updated"] == ["hltv_cs2.weekly"]
    assert scheduler.schedules["hltv_cs2.weekly"]["State"] == "ENABLED"
    assert scheduler.schedules["hltv_cs2.weekly"]["ScheduleExpression"] == "cron(0 5 ? * MON *)"


def test_a_schedule_no_longer_declared_is_deleted(scheduler, monkeypatch):
    sync_schedules.handler({}, None)
    monkeypatch.setattr(store, "ssm", fakes.Ssm([{**HLTV, "schedules": []}, TRAIN]))

    assert sync_schedules.handler({}, None)["deleted"] == ["hltv_cs2.weekly"]
    assert scheduler.schedules == {}
