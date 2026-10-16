from gnome_pipelines.manifest import manifest
from params import problems


def test_real_schemas_accept_what_the_pipelines_accept():
    schema = manifest("cs2")["hltv_cs2"]["params_schema"]

    assert problems(schema, {"start_date": "2026-10-11", "end_date": "2026-10-13", "min_stars": 0,
                             "fetcher": "cache", "match_ids": [1, 2], "refresh": False}) == []
    assert problems(schema, {}) == []


def test_real_schemas_refuse_wrong_types_and_bounds():
    schema = manifest("cs2")["hltv_cs2"]["params_schema"]

    found = problems(schema, {"min_stars": 9, "match_ids": [1, "x"], "refresh": "no", "days_back": True})

    assert found == ["min_stars: must be <= 5", "match_ids: item 1: expected integer, got str",
                     "refresh: expected boolean, got str", "days_back: expected integer, got bool"]


def test_required_fields_are_reported():
    assert problems({"properties": {"a": {"type": "string"}}, "required": ["a"]}, {}) == ["a: required"]
    assert problems({"properties": {}}, []) == ["parameters must be a JSON object"]
