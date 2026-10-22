from gnome_pipelines.cfb.qb_status.state_sync import StateMirror


def test_state_files_round_trip_through_the_mirror(tmp_path):
    local, remote = tmp_path / "a", str(tmp_path / "remote")
    local.mkdir()
    (local / "x.json").write_text('{"posts_month": 12}')
    (local / "c.jsonl").write_text('{"key": "k"}\n')
    (local / "other.txt").write_text("not state")
    m = StateMirror(local, remote, ("x.json", "c.jsonl"))
    assert m.push() == 2 and m.push() == 0
    (local / "x.json").write_text('{"posts_month": 13}')
    assert m.push() == 1
    fresh = tmp_path / "b"
    restored = StateMirror(fresh, remote, ("x.json", "c.jsonl"))
    assert restored.pull() == 2
    assert (fresh / "x.json").read_text() == '{"posts_month": 13}' and not (fresh / "other.txt").exists()
    assert restored.push() == 0


def test_an_empty_mirror_pulls_nothing_and_missing_files_are_not_pushed(tmp_path):
    m = StateMirror(tmp_path / "a", str(tmp_path / "remote"), ("x.json",))
    assert m.pull() == 0 and m.push() == 0
