"""Starting Chrome: a start that times out is cleaned up and retried, never left holding the profile."""
import asyncio

import pytest
from nodriver.core import util as nodriver_util

from gnome_pipelines.hltv_cs2 import chrome


class _Browser:
    def __init__(self):
        self.targets, self._process, self.stopped = [], None, False

    async def aclose(self):
        pass

    def stop(self):
        self.stopped = True


@pytest.fixture
def starts(monkeypatch):
    """uc.start that times out the given number of times, leaving its half-started browser registered."""
    def make(failures):
        calls = {"n": 0, "stale": []}

        async def start(**kwargs):
            calls["n"] += 1
            browser = _Browser()
            nodriver_util.get_registered_instances().add(browser)
            if calls["n"] <= failures:
                calls["stale"].append(browser)
                raise Exception("Failed to connect to browser")
            return browser
        monkeypatch.setattr(chrome.uc, "start", start)
        return calls
    yield make
    nodriver_util.get_registered_instances().clear()


def test_a_slow_start_is_stopped_and_retried(starts):
    calls = starts(failures=1)
    browser = asyncio.run(chrome._start_browser())
    assert calls["n"] == 2 and not browser.stopped
    assert all(b.stopped for b in calls["stale"]), "the timed-out Chrome must release the profile"
    assert browser in nodriver_util.get_registered_instances()
    assert not any(b in nodriver_util.get_registered_instances() for b in calls["stale"])


def test_start_gives_up_after_the_last_attempt(starts):
    calls = starts(failures=chrome._START_ATTEMPTS)
    with pytest.raises(Exception, match="Failed to connect"):
        asyncio.run(chrome._start_browser())
    assert calls["n"] == chrome._START_ATTEMPTS
