import json

import pytest
from botocore.exceptions import ClientError

from gnome_pipelines.cfb.qb_status import state_store
from gnome_pipelines.cfb.qb_status.state_store import ClaudeReadCache, LocalObjects, S3Objects


class Body:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data


class FakeS3:
    """Conditional writes as S3 applies them: IfMatch against the current ETag, IfNoneMatch "*" against existence."""

    def __init__(self):
        self.objects, self.puts = {}, []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        data, etag = self.objects[Key]
        return {"Body": Body(data), "ETag": etag}

    def put_object(self, Bucket, Key, Body, IfMatch=None, IfNoneMatch=None):
        self.puts.append((Bucket, Key, IfMatch, IfNoneMatch))
        current = self.objects.get(Key)
        if (IfNoneMatch == "*" and current is not None) or (IfMatch and (current is None or current[1] != IfMatch)):
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        etag = f'"v{len(self.puts)}"'
        self.objects[Key] = (Body, etag)
        return {"ETag": etag}


@pytest.mark.parametrize("make", [lambda tmp: S3Objects("bucket", client=FakeS3()), lambda tmp: LocalObjects(tmp)])
def test_conditional_writes_refuse_a_stale_version_and_an_existing_object(tmp_path, make):
    objects = make(tmp_path)
    assert objects.get("a.json") is None
    v1 = objects.put("a.json", b"1", if_absent=True)
    assert v1 and objects.put("a.json", b"x", if_absent=True) is None
    v2 = objects.put("a.json", b"2", if_match=v1)
    assert v2 and v2 != v1 and objects.put("a.json", b"3", if_match=v1) is None
    assert objects.get("a.json") == (b"2", v2)


def test_s3_objects_live_under_the_qb_status_state_prefix():
    s3 = FakeS3()
    S3Objects("gnome-research-prod", client=s3).put("qb_status_x_state.json", b"{}")
    assert s3.puts == [("gnome-research-prod", "qb_status/state/qb_status_x_state.json", None, None)]


def test_other_s3_errors_are_raised_not_taken_for_a_conflict():
    class Denied(FakeS3):
        def put_object(self, **kw):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
    with pytest.raises(ClientError):
        S3Objects("b", client=Denied()).put("k", b"x", if_match='"v1"')


def test_claude_reads_are_kept_one_object_each_and_found_again_by_a_new_cache(tmp_path):
    objects = LocalObjects(tmp_path)
    cache = ClaudeReadCache(objects)
    assert cache.lookup("k1") == (False, None)
    cache.store("k1", {"claims": []})
    cache.store("k2", None)
    fresh = ClaudeReadCache(objects)
    assert fresh.lookup("k1") == (True, {"claims": []}) and fresh.lookup("k2") == (True, None)
    assert json.loads((tmp_path / "claude" / "k1.json").read_text()) == {"output": {"claims": []}}


def test_a_local_run_keeps_state_in_a_folder_and_dev_or_prod_on_s3(monkeypatch):
    monkeypatch.setenv("GNOME_STAGE", "local")
    assert isinstance(state_store.state_objects(), LocalObjects)
    monkeypatch.setenv("GNOME_STAGE", "prod")
    monkeypatch.setattr(state_store.CatalogConfig, "from_env",
                        classmethod(lambda cls: type("C", (), {"artifact_root": "s3://gnome-research-prod/artifacts"})()))
    monkeypatch.setattr(state_store.boto3, "client", lambda name: FakeS3())
    objects = state_store.state_objects()
    assert isinstance(objects, S3Objects) and objects.bucket == "gnome-research-prod"
