"""The service's small state files mirrored to S3, beside the CFBD response mirror in the research bucket.

A Fargate task starts with an empty disk. Without the mirror each daily run would forget the month's X spend (so the
monthly cap would reset), lose every query's since_id (so it would pay again for the last day of posts) and have Claude
read every headline again. A run pulls the files first and pushes whichever changed, every minute and at the end.
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path

import pyarrow.fs as pafs
from gnomepy._fs import fs_mkdir, fs_read_bytes, resolve_fs
from gnomepy.data import CatalogConfig

logger = logging.getLogger(__name__)

REMOTE_PREFIX = "qb_status/state"


def default_remote_root() -> str:
    bucket = CatalogConfig.from_env().artifact_root.removeprefix("s3://").split("/", 1)[0]
    return f"s3://{bucket}/{REMOTE_PREFIX}"


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class StateMirror:
    def __init__(self, local_dir: Path, remote_root: str, names: tuple[str, ...]) -> None:
        self.local_dir, self.remote_root, self.names = Path(local_dir), remote_root, names
        self._pushed: dict[str, str] = {}

    def pull(self) -> int:
        """Copy every mirrored file down, replacing the local one; returns how many were copied."""
        fs, root = resolve_fs(self.remote_root)
        copied = 0
        for name in self.names:
            path = f"{root}/{name}"
            if fs.get_file_info(path).type == pafs.FileType.NotFound:
                continue
            data = fs_read_bytes(fs, path)
            self.local_dir.mkdir(parents=True, exist_ok=True)
            (self.local_dir / name).write_bytes(data)
            self._pushed[name] = _digest(data)
            copied += 1
        logger.info("pulled %d state files from %s", copied, self.remote_root)
        return copied

    def push(self) -> int:
        """Upload the files whose contents changed since the last pull or push; returns how many were uploaded."""
        fs, root = resolve_fs(self.remote_root)
        pushed = 0
        for name in self.names:
            p = self.local_dir / name
            if not p.exists():
                continue
            data = p.read_bytes()
            digest = _digest(data)
            if self._pushed.get(name) == digest:
                continue
            fs_mkdir(fs, root)
            with fs.open_output_stream(f"{root}/{name}", compression=None) as f:
                f.write(data)
            self._pushed[name] = digest
            pushed += 1
        return pushed
