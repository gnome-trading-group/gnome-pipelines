from datetime import datetime, timedelta, timezone

import pytest
import requests

from gnome_pipelines.cfb import cfbd_client, ingest
from gnome_pipelines.cfb.cfbd_client import CfbdClient


class Resp:
    def __init__(self, status=200, body=None):
        self.status_code, self._body = status, body if body is not None else [{"ok": True}]

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


class Session:
    """Plays back a script of responses and exceptions, one per get()."""

    def __init__(self, script):
        self.script, self.calls, self.headers = list(script), 0, {}

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


@pytest.fixture
def client(tmp_path, monkeypatch):
    waits = []
    monkeypatch.setattr(cfbd_client.time, "sleep", waits.append)
    c = CfbdClient(api_key="k", cache_dir=tmp_path)
    c.waits = waits
    return c


def test_a_connection_reset_mid_response_is_retried(client):
    reset = requests.exceptions.ChunkedEncodingError("Connection broken: ConnectionResetError(104)")
    client._session = Session([reset, requests.exceptions.ConnectionError("dropped"), Resp(body=[{"id": 1}])])
    assert client.get("/games", {"year": 2026}) == [{"id": 1}]
    assert client._session.calls == 3 and client.calls_made == 1 and len(client.waits) == 2


def test_a_cfbd_outage_is_waited_out_for_minutes_not_seconds(client):
    client._session = Session([Resp(502)] * 6 + [Resp(body=[{"id": 2}])])
    assert client.get("/games", {"year": 2026}) == [{"id": 2}]
    assert sum(client.waits) >= 240


def test_the_quota_check_is_retried_too(client):
    client._session = Session([Resp(502), Resp(body={"remainingCalls": 900})])
    assert client.remaining_calls() == 900


def test_it_gives_up_after_the_last_attempt(client):
    client._session = Session([requests.exceptions.ConnectionError("down")] * cfbd_client.ATTEMPTS)
    with pytest.raises(requests.exceptions.ConnectionError):
        client.get("/games", {"year": 2026})
    assert client._session.calls == cfbd_client.ATTEMPTS


def test_a_client_error_is_not_retried(client):
    client._session = Session([Resp(404)])
    with pytest.raises(requests.HTTPError):
        client.get("/games", {"year": 2026})
    assert client._session.calls == 1


class FakeClient:
    """Records which requests were refreshed; /games returns the season's games."""

    def __init__(self, games):
        self.games, self.refreshed, self.calls_made = games, [], 0

    def get(self, endpoint, params, *, refresh=False):
        if refresh:
            self.refreshed.append((endpoint, params.get("week")))
        return self.games if endpoint == "/games" and params.get("seasonType") == "regular" else []

    def is_cached(self, endpoint, params):
        return True

    def ensure_budget(self, needed):
        pass


def game(week, kickoff, completed=True):
    return {"week": week, "completed": completed, "startDate": kickoff.isoformat()}


def test_a_week_with_a_recent_final_has_its_plays_refetched():
    now = datetime(2026, 10, 26, 20, tzinfo=timezone.utc)
    fc = FakeClient([game(7, now - timedelta(days=9)), game(8, now - timedelta(days=6)),
                     game(8, now - timedelta(days=2)), game(9, now + timedelta(days=1), completed=False)])
    ingest.fetch(fc, [2026], refresh_current=2026, now=now)
    plays = {w for ep, w in fc.refreshed if ep == "/plays"}
    assert plays == {8}


def test_past_seasons_never_refetch_plays():
    now = datetime(2026, 10, 26, 20, tzinfo=timezone.utc)
    fc = FakeClient([game(8, now - timedelta(days=2))])
    ingest.fetch(fc, [2025], refresh_current=2026, now=now)
    assert fc.refreshed == []
