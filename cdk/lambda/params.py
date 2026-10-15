"""Checks run parameters against a pipeline's published JSON schema before a task is started.

Covers what Pydantic's schemas for PipelineParams use: required and unknown keys, types, enums, dates,
numeric bounds, arrays and optional (anyOf with null) fields. Cross-field rules a schema can't express
are left to the runner, which validates with the Pydantic model itself before the pipeline starts.
"""
from __future__ import annotations

import re

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TYPES = {
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
    "null": lambda v: v is None,
}


def problems(schema: dict, params: object) -> list[str]:
    if not isinstance(params, dict):
        return ["parameters must be a JSON object"]
    properties = schema.get("properties", {})
    found = [f"{key}: required" for key in schema.get("required", []) if key not in params]
    found += [f"{key}: not a parameter of this pipeline" for key in params if key not in properties]
    for key, value in params.items():
        if key in properties:
            found += [f"{key}: {p}" for p in _check(properties[key], value)]
    return found


def _check(schema: dict, value: object) -> list[str]:
    if "anyOf" in schema:
        options = [_check(option, value) for option in schema["anyOf"]]
        return [] if any(not o for o in options) else min(options, key=len)
    kind = schema.get("type")
    if kind and not _TYPES[kind](value):
        return [f"expected {kind}, got {type(value).__name__}"]
    if "enum" in schema and value not in schema["enum"]:
        return [f"must be one of {', '.join(map(str, schema['enum']))}"]
    if schema.get("format") == "date" and not _DATE.match(value):
        return ["expected a date like 2026-10-11"]
    for bound, test, word in (("minimum", lambda v, b: v >= b, ">="), ("maximum", lambda v, b: v <= b, "<="),
                              ("exclusiveMinimum", lambda v, b: v > b, ">"),
                              ("exclusiveMaximum", lambda v, b: v < b, "<")):
        if bound in schema and not test(value, schema[bound]):
            return [f"must be {word} {schema[bound]}"]
    if kind == "array" and "items" in schema:
        return [f"item {i}: {p}" for i, item in enumerate(value) for p in _check(schema["items"], item)]
    return []
