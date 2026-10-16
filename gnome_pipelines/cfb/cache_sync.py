"""S3 mirror of the CFBD response cache, beside the HLTV page cache in the research bucket.

A Fargate task starts with an empty disk, so without the mirror every run would re-buy responses that never change
(completed weeks of plays, past recruiting classes) against the monthly CFBD quota. A run pulls the mirror first,
fetches only what is missing or refreshed, then pushes back what it wrote.
"""
from __future__ import annotations

import logging
from pathlib import Path

from gnomepy._fs import fs_list_files, fs_mkdir, fs_read_bytes, resolve_fs
from gnomepy.data import CatalogConfig

logger = logging.getLogger(__name__)

REMOTE_PREFIX = "apicache/cfbd"
SUFFIX = ".json.gz"


def default_remote_root() -> str:
    bucket = CatalogConfig.from_env().artifact_root.removeprefix("s3://").split("/", 1)[0]
    return f"s3://{bucket}/{REMOTE_PREFIX}"


def _local_files(local_dir: Path) -> list[Path]:
    return sorted(local_dir.rglob(f"*{SUFFIX}")) if local_dir.exists() else []


def pull(local_dir: Path, remote_root: str) -> int:
    """Copy every mirrored response missing locally; returns how many were copied."""
    fs, root = resolve_fs(remote_root)
    remote = {p[len(root):].lstrip("/") for p in fs_list_files(fs, root) if p.endswith(SUFFIX)}
    have = {p.relative_to(local_dir).as_posix() for p in _local_files(local_dir)}
    for rel in sorted(remote - have):
        dest = local_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(fs_read_bytes(fs, f"{root}/{rel}"))
    logger.info("pulled %d of %d mirrored CFBD responses from %s", len(remote - have), len(remote), remote_root)
    return len(remote - have)


def push(local_dir: Path, remote_root: str, since: float) -> int:
    """Upload local responses written at or after `since` (epoch seconds); returns how many were uploaded."""
    fs, root = resolve_fs(remote_root)
    fresh = [p for p in _local_files(local_dir) if p.stat().st_mtime >= since]
    for p in fresh:
        rel = p.relative_to(local_dir).as_posix()
        fs_mkdir(fs, f"{root}/{Path(rel).parent.as_posix()}")
        # compression=None: pyarrow would otherwise gzip the already-gzipped response again on a .gz path.
        with fs.open_output_stream(f"{root}/{rel}", compression=None) as f:
            f.write(p.read_bytes())
    logger.info("pushed %d CFBD responses to %s", len(fresh), remote_root)
    return len(fresh)
