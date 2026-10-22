"""
Raw-HTML cache for scraped HLTV pages, keyed on URL.

HLTV-specific by design: the slug patterns, the /YYYY/month/DD date parsing, the
page kinds and the immutability rules all encode how HLTV structures its site.
It lives beside the scraper that uses it rather than pretending to be a general
URL cache.

This exists for two reasons, and the second is the important one.

The obvious reason: re-scraping HLTV costs 6-10 hours behind a Cloudflare
challenge that needs a visible browser. Any parser change used to mean paying
that again. With a cache, it is paid once and every later change is a local
re-parse.

The load-bearing reason: **an HLTV match page is rendered point-in-time.** A page
for a January match fetched in September still shows the world ranks, head-to-head
record and recent form as they stood in January — verified against our own
rankings dataset, four teams, exact matches. So a cached page is not a stale copy
of a live document; it is a frozen observation. Features parsed out of it are
lookahead-free by construction, with none of the merge_asof discipline that
point-in-time joins normally demand.

That makes the cache a feature store, not an optimisation, and it is why the
`fetched_at` timestamp is mandatory rather than nice to have: some page content
("16 weeks ago" in the recent-form tables) is relative to when it was fetched,
and cannot be resolved months later without it.
"""
from __future__ import annotations

import gzip
import hashlib
import atexit
import io
import logging
import os
import re
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit, parse_qsl, urlencode

from gnomepy._fs import fs_exists, fs_list_files, fs_mkdir, fs_read_bytes, fs_write_bytes, resolve_fs
from gnomepy.data import CatalogConfig

logger = logging.getLogger(__name__)

DEFAULT_PREFIX = "htmlcache/hltv"

# Paths whose trailing slug is cosmetic. HLTV embeds team and event names there,
# so a rebrand would otherwise orphan every page we hold for that team.
_SLUGGED = (
    re.compile(r"^(/matches/\d+)(/.*)?$"),
    re.compile(r"^(/stats/matches/mapstatsid/\d+)(/.*)?$"),
    re.compile(r"^(/stats/teams/maps/\d+)(/.*)?$"),
    re.compile(r"^(/stats/teams/\d+)(/.*)?$"),
    re.compile(r"^(/stats/players/\d+)(/.*)?$"),
    re.compile(r"^(/team/\d+)(/.*)?$"),
    re.compile(r"^(/player/\d+)(/.*)?$"),
)

_MATCH_ID = re.compile(r"/matches/(\d+)")
_TEAM_ID = re.compile(r"/(?:team|stats/teams(?:/maps)?)/(\d+)")
_DATE_IN_PATH = re.compile(r"/(\d{4})/(\w+)/(\d+)")
_ISO_DATE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def normalize_url(url: str) -> str:
    """Canonical form: slug stripped, host lowercased, fragment dropped, query sorted."""
    parts = urlsplit(url.strip())
    path = parts.path.rstrip("/") or "/"
    for pattern in _SLUGGED:
        m = pattern.match(path)
        if m:
            path = m.group(1)
            break
    query = urlencode(sorted((k, v) for k, v in parse_qsl(parts.query, keep_blank_values=False)))
    host = (parts.netloc or "").lower()
    return f"{parts.scheme.lower()}://{host}{path}" + (f"?{query}" if query else "")


def _shard(kind: str, normalized: str) -> str:
    if kind == "match":
        m = _MATCH_ID.search(normalized)
        return str(int(m.group(1)) // 1000) if m else "0"
    if kind in {"team_map_stats", "team", "player"}:
        m = _TEAM_ID.search(normalized)
        return str(int(m.group(1)) // 1000) if m else "0"
    subject = subject_date(normalized)
    return subject.strftime("%Y-%m") if subject else "undated"


def subject_date(normalized: str) -> date | None:
    """
    The date a page is *about*, which drives immutability.

    For dated stats URLs that is the window end; for results and ranking pages it
    is in the path.
    """
    iso = _ISO_DATE.findall(normalized)
    if iso:
        try:
            return date.fromisoformat(max(iso))
        except ValueError:
            pass
    m = _DATE_IN_PATH.search(normalized)
    if m:
        for fmt in ("%Y/%B/%d", "%Y/%b/%d"):
            try:
                return datetime.strptime(f"{m.group(1)}/{m.group(2)}/{m.group(3)}", fmt).date()
            except ValueError:
                continue
    return None


@dataclass(frozen=True)
class CacheEntry:
    html: str
    fetched_at: datetime
    key: str


@dataclass(frozen=True)
class CachePolicy:
    """
    When a stored page may still be served.

    `closed_book_after` is what makes results and ranking pages effectively
    immutable: HLTV can revise a recent listing, but not one from a fortnight ago.
    """
    refresh_all: bool = False
    refresh_kinds: frozenset[str] = frozenset()
    refresh_since: date | None = None
    write: bool = True
    ttl: timedelta = timedelta(hours=24)
    closed_book_after: timedelta = timedelta(days=14)

    def wants_refresh(self, kind: str, subject: date | None) -> bool:
        if self.refresh_all or kind in self.refresh_kinds:
            return True
        if self.refresh_since is not None and subject is not None:
            return subject >= self.refresh_since
        return False


_IMMUTABLE_KINDS = frozenset({"match"})
# Pages that show how things stand now (a team's roster, the transfers feed) rather than as of a match. Each
# day's copy gets its own key instead of overwriting the last: what such a page said on a day is history the
# roster datasets are rebuilt from, and the newest copy is the one served.
_SNAPSHOT_KINDS = frozenset({"team", "transfers"})


def _research_bucket() -> str:
    # Pages are cached in the research bucket, beside the artifact files.
    return CatalogConfig.from_env().artifact_root.removeprefix("s3://").split("/", 1)[0]


def _cache_base() -> Path:
    return Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "gnomepy"


class HtmlCache:
    """URL-keyed page store over a local mirror backed by S3."""

    def __init__(self, prefix: str = DEFAULT_PREFIX, bucket: str | None = None,
                 policy: CachePolicy | None = None, local_root: Path | None = None,
                 remote_root: str | None = None):
        self.prefix = prefix.strip("/")
        self.policy = policy or CachePolicy()
        self.local_root = Path(local_root) if local_root else _cache_base() / self.prefix
        self._remote_root = remote_root or f"s3://{bucket or _research_bucket()}/{self.prefix}"
        self._index: set[str] | None = None
        self._pending: set[str] = set()
        # The local write stays inline — it is what makes a stalled run resumable.
        # Only the S3 push moves off the loop; at ~100-300ms each, 7k serialised
        # PUTs would block the event loop for 12-35 minutes of an unattended run.
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="html-cache-s3")
        self._inflight: dict[str, Future] = {}
        atexit.register(self.drain)

    # ---- keys ----

    def key_for(self, url: str, *, kind: str, day: date | None = None) -> str:
        """The page's key; a snapshot kind's key also names the day it was fetched (default: today, UTC)."""
        base = self._base_key(url, kind)
        if kind in _SNAPSHOT_KINDS:
            return f"{base}/{(day or datetime.now(timezone.utc).date()).isoformat()}.html.gz"
        return f"{base}.html.gz"

    @staticmethod
    def _base_key(url: str, kind: str) -> str:
        normalized = normalize_url(url)
        digest = hashlib.sha256(normalized.encode()).hexdigest()[:10]
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", normalized.split("://", 1)[-1]).strip("_")[:90]
        return f"{kind}/{_shard(kind, normalized)}/{slug}.{digest}"

    def snapshot_keys(self, url: str, *, kind: str) -> list[str]:
        """Every stored day's copy of a snapshot page, oldest first."""
        prefix = self._base_key(url, kind) + "/"
        found = {str(p.relative_to(self.local_root)) for p in (self.local_root / prefix).glob("*.html.gz")}
        if self._index is not None:
            found |= {k for k in self._index if k.startswith(prefix)}
        else:
            try:
                fs, root = resolve_fs(f"{self._remote_root}/{prefix}")
                found |= {f"{prefix}{p.rsplit('/', 1)[-1]}" for p in fs_list_files(fs, root)}
            except Exception as exc:
                logger.warning("cache: could not list %s (%s) — using local copies only", prefix, exc)
        return sorted(found)

    # ---- index ----

    def warm_index(self) -> int:
        """
        One recursive LIST instead of a HEAD per page.

        At ~10k objects a per-page existence check would dominate the run; this
        turns the remote lookup into a set membership test.
        """
        fs, root = resolve_fs(self._remote_root)
        paths = fs_list_files(fs, root)
        self._index = {p[len(root):].lstrip("/") for p in paths}
        logger.info("warmed cache index: %d remote objects under %s", len(self._index), self._remote_root)
        return len(self._index)

    # ---- freshness ----

    def _is_fresh(self, kind: str, subject: date | None, fetched_at: datetime) -> bool:
        if kind in _IMMUTABLE_KINDS:
            return True
        if subject is not None and date.today() - subject > self.policy.closed_book_after:
            return True
        return datetime.now(timezone.utc) - fetched_at < self.policy.ttl

    # ---- read / write ----

    def get(self, url: str, *, kind: str) -> CacheEntry | None:
        normalized = normalize_url(url)
        subject = subject_date(normalized)
        if self.policy.wants_refresh(kind, subject):
            return None

        if kind in _SNAPSHOT_KINDS:
            days = self.snapshot_keys(url, kind=kind)
            if not days:
                return None
            key = days[-1]
        else:
            key = self.key_for(url, kind=kind)
        local = self.local_root / key
        blob: bytes | None = None
        if local.exists():
            blob = local.read_bytes()
        elif self._index is None or key in self._index:
            try:
                fs, path = resolve_fs(f"{self._remote_root}/{key}")
                if self._index is not None or fs_exists(fs, path):
                    blob = fs_read_bytes(fs, path)
                    local.parent.mkdir(parents=True, exist_ok=True)
                    local.write_bytes(blob)
            except Exception as exc:
                logger.warning("cache: remote read failed for %s (%s) — treating as a miss", key, exc)
                blob = None
        if blob is None:
            return None

        html, fetched_at = _gunzip(blob)
        if not self._is_fresh(kind, subject, fetched_at):
            return None
        return CacheEntry(html=html, fetched_at=fetched_at, key=key)

    def put(self, url: str, html: str, *, kind: str, fetched_at: datetime | None = None) -> str | None:
        if not self.policy.write:
            return None
        fetched_at = fetched_at or datetime.now(timezone.utc)
        key = self.key_for(url, kind=kind, day=fetched_at.astimezone(timezone.utc).date())
        blob = _gzip(html, fetched_at)

        local = self.local_root / key
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(blob)

        # The local write above is authoritative. A remote failure must never abort
        # an unattended multi-hour scrape over a page we have already captured, so
        # it is recorded and retried rather than raised; drain()/sync_pending()
        # push the stragglers later.
        self._inflight[key] = self._pool.submit(self._push_remote, key, blob)
        return key

    def drain(self) -> int:
        """Wait for in-flight remote pushes. Returns the number that failed."""
        failed = 0
        for key, fut in list(self._inflight.items()):
            try:
                ok = fut.result()
            except Exception as exc:
                logger.warning("cache: remote write raised for %s (%s) — kept locally", key, exc)
                ok = False
            if ok:
                if self._index is not None:
                    self._index.add(key)
                self._pending.discard(key)
            else:
                self._pending.add(key)
                failed += 1
            self._inflight.pop(key, None)
        return failed

    def _push_remote(self, key: str, blob: bytes) -> bool:
        try:
            fs, path = resolve_fs(f"{self._remote_root}/{key}")
            fs_mkdir(fs, path.rsplit("/", 1)[0])  # no-op on S3; local roots need it
            fs_write_bytes(fs, path, blob)
            return True
        except Exception as exc:
            logger.warning("cache: remote write failed for %s (%s) — kept locally", key, exc)
            return False

    def sync_pending(self) -> int:
        """Drain in-flight pushes, then retry anything that failed. Returns the number still pending."""
        self.drain()
        for key in sorted(self._pending):
            local = self.local_root / key
            if not local.exists():
                self._pending.discard(key)
                continue
            if self._push_remote(key, local.read_bytes()):
                self._pending.discard(key)
                if self._index is not None:
                    self._index.add(key)
        if self._pending:
            logger.warning("cache: %d page(s) still only local", len(self._pending))
        return len(self._pending)

    def local_keys(self, kind: str | None = None) -> list[str]:
        """Cached keys on this machine — the enumeration a local re-parse walks."""
        root = self.local_root / kind if kind else self.local_root
        if not root.exists():
            return []
        base = self.local_root
        return sorted(str(p.relative_to(base)) for p in root.rglob("*.html.gz"))

    def read_key(self, key: str) -> CacheEntry | None:
        local = self.local_root / key
        if not local.exists():
            return None
        html, fetched_at = _gunzip(local.read_bytes())
        return CacheEntry(html=html, fetched_at=fetched_at, key=key)


def _gzip(html: str, fetched_at: datetime) -> bytes:
    """Fetch time rides in the gzip MTIME field — no sidecar, and `zcat` still works."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=int(fetched_at.timestamp())) as gz:
        gz.write(html.encode("utf-8", errors="replace"))
    return buf.getvalue()


def _gunzip(blob: bytes) -> tuple[str, datetime]:
    gz = gzip.GzipFile(fileobj=io.BytesIO(blob))
    html = gz.read().decode("utf-8", errors="replace")
    mtime = gz.mtime or 0
    return html, datetime.fromtimestamp(mtime, tz=timezone.utc)


def is_complete(html: str) -> bool:
    """
    Whether a captured page reached its footer.

    A DOM serialised mid-load still ends in </html> - the browser closes open tags
    on capture - so the file looks whole. The footer is the reliable tell.
    """
    return "<footer" in html


def is_cacheable(kind: str, html: str) -> bool:
    """
    Guard the immutable tier.

    Match pages are cached forever on the premise that a finished match never
    changes. A live or postponed page would be frozen wrong, so require evidence
    the match actually completed before storing one. Nothing is cached without
    its footer: a truncated listing hides matches, and a truncated match page
    loses the stats, veto, head-to-head and form sections at the bottom.
    """
    if not is_complete(html):
        logger.warning("not caching an incomplete page (no footer)")
        return False
    if kind != "match":
        return True
    if "results-team-score" not in html:
        logger.info("not caching a match page with no final scores (live or postponed?)")
        return False
    return True
