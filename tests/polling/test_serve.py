import os
import signal
import threading

import pytest
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines import PipelineParams, families
from gnome_pipelines.datasets.polling import POLL_HEALTH
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.serve import serve
from gnome_pipelines.polling.service import Service, Task, utcnow
from gnome_pipelines.polling.source import PollingSource


class FakeParams(PipelineParams):
    greeting: str = "hi"


class FakeSource(PollingSource[FakeParams]):
    name = "fake_source"
    description = "test source"
    polled: list = []
    stopped_with: list = []

    def build(self, params: FakeParams, health: PollHealth, writer, alerts) -> Service:
        def poll(now):
            FakeSource.polled.append(params.greeting)
            os.kill(os.getpid(), signal.SIGTERM)
            return []
        return Service([Task("poll", lambda now: 60.0, poll)], health, sleep=lambda s: None,
                       on_stop=lambda: FakeSource.stopped_with.append(len(FakeSource.polled)))


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("GNOME_LOCAL_WAREHOUSE", str(tmp_path / "wh"))
    monkeypatch.setenv("GNOME_STAGE", "local")
    monkeypatch.setattr(families, "available_sources", lambda: {"fake_source": FakeSource})
    monkeypatch.setattr(FakeSource, "polled", [])
    monkeypatch.setattr(FakeSource, "stopped_with", [])
    previous = signal.getsignal(signal.SIGTERM)
    yield tmp_path
    signal.signal(signal.SIGTERM, previous)


def health_rows(env):
    return DatasetStore(CatalogConfig(local_warehouse=env / "wh")).load(POLL_HEALTH.name).sort_values("at")


def test_serve_runs_until_sigterm_lets_the_source_finish_and_records_start_and_stop(env):
    serve("fake_source", {"greeting": "hello"})
    assert FakeSource.polled == ["hello"] and FakeSource.stopped_with == [1]
    rows = health_rows(env)
    assert set(rows["source"]) == {"fake_source"} and {"started", "heartbeat", "stopped"} <= set(rows["kind"])
    assert rows[rows["kind"] == "stopped"]["detail"].tolist() == ["SIGTERM"]
    assert "'greeting': 'hello'" in rows[rows["kind"] == "started"]["detail"].iloc[0]


def test_unknown_sources_and_bad_parameters_fail_before_anything_runs(env):
    with pytest.raises(ValueError, match="Unknown polling source 'nope'"):
        serve("nope", {})
    with pytest.raises(Exception, match="greting"):
        serve("fake_source", {"greting": "typo"})
    assert FakeSource.polled == []


def test_a_sigterm_during_startup_ends_serve_without_polling(env, monkeypatch):
    class Slow(FakeSource):
        def build(self, params, health, writer, alerts):
            os.kill(os.getpid(), signal.SIGTERM)
            return super().build(params, health, writer, alerts)
    monkeypatch.setattr(families, "available_sources", lambda: {"fake_source": Slow})
    serve("fake_source", {})
    assert FakeSource.polled == []


def test_a_hung_task_s_row_is_committed_before_the_process_exits(env, monkeypatch):
    committed_at_exit = []
    release = threading.Event()

    class Hanging(FakeSource):
        def build(self, params, health, writer, alerts):
            def stuck(now):
                release.wait(10)
                return []

            def exit_hard():
                # Stands in for os._exit: what's in the table now is all a real exit would have kept.
                committed_at_exit.extend(health_rows(env)["kind"])
                release.set()
                os.kill(os.getpid(), signal.SIGTERM)
            return Service([Task("stuck", lambda now: 60.0, stuck, timeout=0.2)], health, on_hang=exit_hard)
    monkeypatch.setattr(families, "available_sources", lambda: {"fake_source": Hanging})
    serve("fake_source", {})
    assert "hung" in committed_at_exit


def test_what_a_failed_build_recorded_is_still_committed(env, monkeypatch):
    class Broken(FakeSource):
        def build(self, params, health, writer, alerts):
            health.record("note", utcnow(), "the key is unset")
            raise RuntimeError("can't build")
    monkeypatch.setattr(families, "available_sources", lambda: {"fake_source": Broken})
    with pytest.raises(RuntimeError, match="can't build"):
        serve("fake_source", {})
    assert health_rows(env)["detail"].tolist() == ["the key is unset"]
