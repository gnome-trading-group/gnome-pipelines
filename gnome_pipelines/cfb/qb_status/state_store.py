"""The QB Status Service's durable state: small JSON objects under s3://<research bucket>/qb_status/state/ (a local
folder off AWS), written with conditions so a second writer is noticed instead of overwritten.

Two things live here: the X budget (monthly and daily spend, each query's since_id; x_scan.XBudget) and Claude's
reads (one object per text, by the extractor's hash; ClaudeReadCache).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Protocol

import boto3
from botocore.exceptions import ClientError
from gnomepy.data import CatalogConfig

from gnome_pipelines.environment import Stage, environment

PREFIX = "qb_status/state"
LOCAL_ROOT = Path.home() / ".cache" / "gnome-pipelines" / "qb_status_state"
CLAUDE_PREFIX = "claude"


class Objects(Protocol):
    def get(self, key: str) -> tuple[bytes, str] | None:
        """The object's bytes and version (its ETag), or None if there is none."""

    def put(self, key: str, data: bytes, if_match: str | None = None, if_absent: bool = False) -> str | None:
        """Writes the object if it still has version if_match (or, with if_absent, doesn't exist yet) and returns its
        new version; None when that condition fails: someone else wrote it first."""


class S3Objects:
    def __init__(self, bucket: str, prefix: str = PREFIX, client=None) -> None:
        self.bucket, self.prefix = bucket, prefix.strip("/")
        self.client = client or boto3.client("s3")

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}"

    def get(self, key: str) -> tuple[bytes, str] | None:
        try:
            r = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
        return r["Body"].read(), r["ETag"]

    def put(self, key: str, data: bytes, if_match: str | None = None, if_absent: bool = False) -> str | None:
        condition = {"IfMatch": if_match} if if_match else {"IfNoneMatch": "*"} if if_absent else {}
        try:
            r = self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data, **condition)
        except ClientError as e:
            # 412: the version changed; 409: a concurrent conditional write to the same key won.
            if e.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict"):
                return None
            raise
        return r["ETag"]


class LocalObjects:
    """The same over a folder, for laptop and local-stage runs; the conditions hold within one process only."""

    def __init__(self, root: Path = LOCAL_ROOT) -> None:
        self.root = Path(root)

    def get(self, key: str) -> tuple[bytes, str] | None:
        path = self.root / key
        if not path.exists():
            return None
        data = path.read_bytes()
        return data, hashlib.md5(data).hexdigest()

    def put(self, key: str, data: bytes, if_match: str | None = None, if_absent: bool = False) -> str | None:
        current = self.get(key)
        if (if_absent and current is not None) or (if_match and (current is None or current[1] != if_match)):
            return None
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(path.name + ".partial")
        partial.write_bytes(data)
        partial.replace(path)
        return hashlib.md5(data).hexdigest()


def state_objects() -> Objects:
    """S3 for a dev or prod run, a local folder for a local one."""
    if environment().stage is Stage.LOCAL:
        return LocalObjects()
    bucket = CatalogConfig.from_env().artifact_root.removeprefix("s3://").split("/", 1)[0]
    return S3Objects(bucket)


class ClaudeReadCache:
    """polling.claude's cache for the extractor: one object per read under claude/, kept in memory once seen."""

    def __init__(self, objects: Objects) -> None:
        self.objects = objects
        self.memory: dict[str, dict | None] = {}

    def lookup(self, key: str) -> tuple[bool, dict | None]:
        if key in self.memory:
            return True, self.memory[key]
        found = self.objects.get(f"{CLAUDE_PREFIX}/{key}.json")
        if found is None:
            return False, None
        output = json.loads(found[0])["output"]
        self.memory[key] = output
        return True, output

    def store(self, key: str, output: dict | None) -> None:
        self.memory[key] = output
        # Two reads of the same text racing both pay; whichever lands first is kept.
        self.objects.put(f"{CLAUDE_PREFIX}/{key}.json", json.dumps({"output": output}).encode(), if_absent=True)
