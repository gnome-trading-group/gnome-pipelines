"""Page sources for collect.py tests: fixed pages instead of the network."""
from __future__ import annotations

import re


def listing_html(match_ids: list[int], stars: int = 0) -> str:
    star = '<i class="star"></i>' * stars
    links = "".join(f'<a class="a-reset" href="/matches/{mid}/x"><div class="stars">{star}</div></a>' for mid in match_ids)
    return f'<html><body><div class="results-all">{links}</div><footer></footer></body></html>'


class FixtureSource:
    """Serves a results listing of the given match pages, and the pages themselves; None for anything else."""

    concurrency = 2
    can_be_blocked = True
    billed = 0

    def __init__(self, pages: dict[int, str | None], listed: list[int] | None = None) -> None:
        self.pages = pages
        self.listed = list(pages) if listed is None else listed
        self.calls: list[str] = []

    async def html(self, url: str, *, wait_selector: str | None = None, wait_count: int = 1) -> str | None:
        self.calls.append(url)
        if "/results?" in url:
            return listing_html(self.listed) if "offset=0" in url else None
        match = re.search(r"/matches/(\d+)/", url)
        return self.pages.get(int(match.group(1))) if match else None

    async def close(self) -> None:
        pass
