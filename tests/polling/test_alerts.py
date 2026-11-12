import json
from datetime import datetime, timedelta, timezone

import pytest

from gnome_pipelines.polling.alerts import REFIRE, TYPE, Alerts, BusSink, LocalOpenNames
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.service import Service, Task, TaskAlert

T0 = datetime(2026, 11, 12, 12, tzinfo=timezone.utc)


class Sink:
    def __init__(self):
        self.put_alerts, self.fail = [], False

    def put(self, alert):
        if self.fail:
            raise ConnectionError("bus down")
        self.put_alerts.append(alert)

    def actions(self):
        return [(a["action"], a["key"].split(":")[-1], a.get("severity")) for a in self.put_alerts]


class Names:
    def __init__(self, names=()):
        self.names = set(names)

    def load(self):
        return set(self.names)

    def save(self, names):
        self.names = set(names)


class Monotonic:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def make(names=None):
    sink, names, clock = Sink(), names or Names(), Monotonic()
    return Alerts("rt_reviews", sink, names, clock=lambda: T0, monotonic=clock), sink, names, clock


def test_an_alert_is_put_once_while_it_lasts_then_again_every_refire_and_on_a_new_severity():
    alerts, sink, names, clock = make()
    alerts.fire("variety.fast", "feed failing", "HTTP 503", labels={"task": "variety.fast"})
    (first,) = sink.put_alerts
    assert first["key"] == f"{TYPE}:rt_reviews:variety.fast" and first["type"] == TYPE and first["source"] == "ingest"
    assert first["labels"] == {"pipeline": "rt_reviews", "kind": "source", "check": "variety.fast",
                               "task": "variety.fast"}
    assert (first["severity"], first["title"], first["body"], first["stateAt"]) == ("WARNING", "feed failing",
                                                                                    "HTTP 503", T0.isoformat())
    alerts.fire("variety.fast", "feed failing", "HTTP 503")
    assert len(sink.put_alerts) == 1
    clock.t = REFIRE
    alerts.fire("variety.fast", "feed failing", "HTTP 503")
    alerts.fire("variety.fast", "feed failing", "spent", "ERROR")
    assert sink.actions() == [("FIRING", "variety.fast", "WARNING")] * 2 + [("FIRING", "variety.fast", "ERROR")]
    assert names.names == {"variety.fast"}


def test_resolve_closes_an_open_alert_once_and_ignores_one_never_fired():
    alerts, sink, names, _ = make()
    alerts.resolve("films")
    alerts.fire("films", "down")
    alerts.resolve("films")
    alerts.resolve("films")
    assert sink.actions() == [("FIRING", "films", "WARNING"), ("RESOLVED", "films", None)]
    assert names.names == set() and not alerts.is_open("films")


def test_an_alert_left_open_by_an_earlier_run_is_closed_by_the_next_once_its_cause_is_gone():
    left = Names({"tomatometer"})
    alerts, sink, names, _ = make(left)
    assert alerts.is_open("tomatometer")
    alerts.resolve("tomatometer")
    assert sink.actions() == [("RESOLVED", "tomatometer", None)] and names.names == set()


def test_an_inherited_alert_still_failing_is_put_again_and_kept():
    alerts, sink, names, _ = make(Names({"tomatometer"}))
    alerts.fire("tomatometer", "still down")
    assert sink.actions() == [("FIRING", "tomatometer", "WARNING")] and names.names == {"tomatometer"}


def test_a_failed_put_never_raises_and_is_tried_again():
    alerts, sink, _, _ = make()
    sink.fail = True
    alerts.fire("films", "down")
    sink.fail = False
    alerts.fire("films", "down")
    assert sink.actions() == [("FIRING", "films", "WARNING")]
    sink.fail = True
    alerts.resolve("films")
    assert alerts.is_open("films"), "a resolve that didn't go out leaves it open to resolve again"
    sink.fail = False
    alerts.resolve("films")
    assert sink.actions()[-1] == ("RESOLVED", "films", None)


def test_a_source_never_pages():
    alerts, _, _, _ = make()
    with pytest.raises(ValueError):
        alerts.fire("x", "x", severity="CRITICAL")


def test_open_names_survive_in_a_local_file(tmp_path):
    names = LocalOpenNames(tmp_path / "a" / "rt_reviews.json")
    assert names.load() == set()
    Alerts("rt_reviews", Sink(), names, clock=lambda: T0).fire("films", "down")
    assert names.load() == {"films"}


def test_the_bus_sink_puts_an_alert_event_and_raises_on_a_refused_entry():
    class Client:
        def __init__(self, failed=0):
            self.failed, self.entries = failed, []

        def put_events(self, Entries):
            self.entries += Entries
            return {"FailedEntryCount": self.failed, "Entries": [{"ErrorCode": "x"}] if self.failed else [{}]}
    client = Client()
    BusSink("gnome-alerts", client).put({"key": "k", "action": "FIRING"})
    (entry,) = client.entries
    assert (entry["EventBusName"], entry["Source"], entry["DetailType"]) == ("gnome-alerts", "gnome.alerts", "Alert")
    assert json.loads(entry["Detail"]) == {"key": "k", "action": "FIRING"}
    with pytest.raises(RuntimeError):
        BusSink("gnome-alerts", Client(failed=1)).put({"key": "k", "action": "FIRING"})


# --- a task's alert, through the Service ---------------------------------------------------------------------------

def service_with(task, alerts):
    return Service([task], PollHealth("rt_reviews", publish=lambda df: None), sleep=lambda s: None, alerts=alerts)


def test_a_task_alerts_once_it_has_gone_its_limit_without_a_success_and_resolves_when_it_works_again():
    alerts, sink, _, _ = make()
    failing = [True]

    def run(now):
        if failing[0]:
            raise ConnectionError("HTTP 503")
        return []
    svc = service_with(Task("variety.fast", lambda now: 30, run, alert=TaskAlert(900, "Variety's news feed")), alerts)
    svc.step(T0)
    svc.step(T0 + timedelta(minutes=10))
    assert sink.put_alerts == [], "not yet: 10 min without a success"
    svc.step(T0 + timedelta(minutes=15))
    (fired,) = sink.put_alerts
    assert fired["title"] == "rt_reviews: Variety's news feed has failed for 15 min"
    assert "3 failures in a row" in fired["body"] and "HTTP 503" in fired["body"]
    assert fired["labels"]["task"] == "variety.fast"
    failing[0] = False
    svc.step(T0 + timedelta(minutes=16))
    assert sink.actions()[-1] == ("RESOLVED", "variety.fast", None)


def test_the_limit_counts_from_the_last_success_not_the_first_failure():
    alerts, sink, _, _ = make()
    outcomes = iter([None, "fail", "fail", "fail"])

    def run(now):
        if next(outcomes) == "fail":
            raise ConnectionError("down")
        return []
    svc = service_with(Task("films", lambda now: 600, run, alert=TaskAlert(1800, "film discovery")), alerts)
    svc.step(T0)
    for minutes in (10, 20):
        svc.step(T0 + timedelta(minutes=minutes))
    assert sink.put_alerts == []
    svc.step(T0 + timedelta(minutes=30))
    assert sink.actions() == [("FIRING", "films", "WARNING")]


def test_a_task_without_an_alert_never_alerts():
    alerts, sink, _, _ = make()

    def run(now):
        raise ConnectionError("down")
    svc = service_with(Task("quiet", lambda now: 30, run), alerts)
    for minutes in range(0, 120, 10):
        svc.step(T0 + timedelta(minutes=minutes))
    assert sink.put_alerts == []
