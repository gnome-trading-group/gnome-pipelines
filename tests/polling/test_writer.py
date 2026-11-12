import pyarrow as pa
import pytest
from gnomepy.data import TableSpec

from gnome_pipelines.polling.writer import DEFAULT_EVERY, TableWriter

FAST = TableSpec("fast", pa.schema([pa.field("k", pa.string(), nullable=False)]), key=("k",))
SLOW = TableSpec("slow", pa.schema([pa.field("k", pa.string(), nullable=False)]), key=("k",))
VALUED = TableSpec("valued", pa.schema([pa.field("k", pa.string(), nullable=False), pa.field("v", pa.int64())]),
                   key=("k",))


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_rows_for_one_table_are_committed_together_once_its_interval_is_up():
    commits, clock = [], Clock()
    w = TableWriter(lambda spec, df: commits.append((spec.name, sorted(df["k"]))), every={"fast": 2.0}, clock=clock)
    w.add(FAST, [{"k": "a"}])
    w.add(FAST, [{"k": "b"}])
    w.add(SLOW, [{"k": "x"}])
    assert w.flush(only_due=True) == 0
    clock.t = 2.0
    assert w.flush(only_due=True) == 2 and commits == [("fast", ["a", "b"])]
    clock.t = DEFAULT_EVERY
    w.flush(only_due=True)
    assert commits[-1] == ("slow", ["x"]) and w.pending() == {}


def test_a_failed_commit_keeps_its_rows_for_the_next_round():
    fail, commits = [True], []

    def publish(spec, df):
        if fail[0]:
            raise ConnectionError("catalog down")
        commits.append(sorted(df["k"]))
    w = TableWriter(publish)
    w.add(FAST, [{"k": "a"}])
    assert w.flush() == 0 and w.pending() == {"fast": 1}
    fail[0] = False
    w.add(FAST, [{"k": "b"}])
    assert w.flush() == 2 and commits == [["a", "b"]]


def test_close_commits_whatever_is_left():
    commits = []
    w = TableWriter(lambda spec, df: commits.append(len(df)))
    w.start()
    w.add(SLOW, [{"k": "a"}, {"k": "b"}])
    w.close()
    assert commits == [2]


def test_several_rows_for_one_key_are_committed_in_successive_layers_oldest_first():
    commits = []
    w = TableWriter(lambda spec, df: commits.append(list(zip(df["k"], df["v"]))))
    w.add(VALUED, [{"k": "a", "v": 1}, {"k": "b", "v": 1}])
    w.add(VALUED, [{"k": "a", "v": 2}])
    w.add(VALUED, [{"k": "a", "v": 3}, {"k": "c", "v": 1}])
    assert w.flush() == 5
    assert commits == [[("a", 1), ("b", 1), ("c", 1)], [("a", 2)], [("a", 3)]]


def test_a_failed_layer_keeps_it_and_every_later_one_and_they_go_next_round():
    calls, commits = [0], []

    def publish(spec, df):
        calls[0] += 1
        if calls[0] == 2:
            raise ConnectionError("catalog down")
        commits.append(list(zip(df["k"], df["v"])))
    w = TableWriter(publish)
    w.add(VALUED, [{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "a", "v": 3}])
    assert w.flush() == 1 and w.pending() == {"valued": 2}
    assert w.flush() == 2
    assert commits == [[("a", 1)], [("a", 2)], [("a", 3)]]


def test_rows_queued_behind_a_long_outage_are_capped_at_the_newest(monkeypatch):
    monkeypatch.setattr("gnome_pipelines.polling.writer.MAX_QUEUED", 5)

    def publish(spec, df):
        raise ConnectionError("catalog down")
    w = TableWriter(publish)
    for i in range(4):
        w.add(VALUED, [{"k": f"r{i}a", "v": i}, {"k": f"r{i}b", "v": i}])
        w.flush()
    assert w.pending() == {"valued": 5}
    kept = w._queued["valued"][1][0]
    assert list(kept["k"]) == ["r1b", "r2a", "r2b", "r3a", "r3b"]
