import os

import pytest

from gnome_pipelines import PipelineParams, Schedule, manifest
from gnome_pipelines.polling.source import PollingSource


class FakeEcs:
    def __init__(self, running):
        self.running, self.listed, self.stopped = running, [], []

    def get_paginator(self, name):
        assert name == "list_tasks"
        ecs = self

        class Pages:
            def paginate(self, cluster, startedBy, desiredStatus):
                ecs.listed.append((cluster, startedBy, desiredStatus))
                return [{"taskArns": ecs.running.get(startedBy, [])}]
        return Pages()

    def stop_task(self, **kw):
        self.stopped.append(kw)


def test_restart_stops_each_running_source_task_of_the_family_for_the_status_lambda_to_start_again():
    ecs = FakeEcs({"source:cfb_qb_status": ["arn:task/1"]})
    assert manifest.restart_sources("cfb", ecs) == ["arn:task/1"]
    assert ecs.listed == [("gnome-pipelines", "source:cfb_qb_status", "RUNNING")]
    assert ecs.stopped[0]["task"] == "arn:task/1" and ecs.stopped[0]["cluster"] == "gnome-pipelines"


def test_a_stopped_source_and_a_family_without_sources_are_left_alone():
    ecs = FakeEcs({})
    assert manifest.restart_sources("cfb", ecs) == [] and ecs.stopped == []
    assert manifest.restart_sources("cs2", ecs) == [] and ecs.listed == [("gnome-pipelines", "source:cfb_qb_status",
                                                                            "RUNNING")]


def test_sources_are_published_with_their_kind_and_secrets_and_no_schedules():
    qb = manifest.manifest("cfb")["cfb_qb_status"]
    assert qb["kind"] == "source" and qb["secrets"] == ["x", "anthropic"] and qb["schedules"] == []
    update = manifest.manifest("cfb")["cfb_update"]
    assert update["kind"] == "pipeline" and "secrets" not in update



def test_each_ci_command_runs_its_step_in_the_pipelines_region(monkeypatch):
    calls = []
    monkeypatch.setenv("PIPELINE_FAMILY", "cfb")
    # Set, then removed, so teardown restores whatever was there: main() writes AWS_REGION into os.environ.
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.delenv("AWS_REGION")
    monkeypatch.setattr(manifest, "publish", lambda family: calls.append(("publish", family)))
    monkeypatch.setattr(manifest, "restart_sources", lambda family: calls.append(("restart", family)))
    manifest.main(["publish"])
    manifest.main(["restart-sources"])
    manifest.main(["check"])
    assert calls == [("publish", "cfb"), ("restart", "cfb")]
    assert os.environ["AWS_REGION"] == "us-east-1"


def test_a_polling_source_declaring_schedules_is_refused_not_silently_dropped():
    class P(PipelineParams):
        pass

    class Scheduled(PollingSource[P]):
        name, description = "scheduled", "x"
        schedules = (Schedule("daily", "rate(1 day)"),)
    with pytest.raises(ValueError, match="polling sources have no schedules"):
        manifest.entry("cs2", Scheduled)
