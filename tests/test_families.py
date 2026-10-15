"""
Families: every pipeline is in exactly one, and each family's code reaches only what its image installs.

A family image installs the base plus the family's extra, so an import outside it would only fail when
CI builds that image (or, worse, in a code path the build's import check doesn't reach). This follows a
family's imports through gnome_pipelines; a gnomepy_research module it reaches must declare no extras
beyond those the family installs gnomepy-research with (gnomepy_research.dependencies), and research's
own test keeps each research module inside its declared extras.
"""
from __future__ import annotations

import ast
import importlib
import inspect
import pkgutil
import sys
import tomllib
from importlib.metadata import PackageNotFoundError, distribution, packages_distributions
from pathlib import Path

import pytest
from gnomepy_research.dependencies import module_extras
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

import gnome_pipelines
from gnome_pipelines import Pipeline, families

ROOT = Path(gnome_pipelines.__file__).parent
PROJECT = tomllib.loads((ROOT.parent / "pyproject.toml").read_text())["project"]
RESEARCH = "gnomepy-research"


def _closure(requirements: list[str]) -> frozenset[str]:
    seen: set[tuple[str, frozenset[str]]] = set()
    stack = [Requirement(r) for r in requirements]
    while stack:
        req = stack.pop()
        if req.marker is not None and not req.marker.evaluate({"extra": ""}):
            continue
        key = (canonicalize_name(req.name), frozenset(req.extras))
        if key in seen:
            continue
        seen.add(key)
        try:
            requires = distribution(req.name).requires or []
        except PackageNotFoundError:
            continue
        for spec in map(Requirement, requires):
            if spec.marker is None or any(spec.marker.evaluate({"extra": e}) for e in ("", *req.extras)):
                stack.append(Requirement(f"{spec.name}{'[' + ','.join(spec.extras) + ']' if spec.extras else ''}"))
    return frozenset(name for name, _ in seen)


def _requirements(family: str) -> list[str]:
    return [*PROJECT["dependencies"], *PROJECT["optional-dependencies"][family]]


def _research_extras(family: str) -> set[str]:
    return {e for r in map(Requirement, _requirements(family)) if canonicalize_name(r.name) == RESEARCH for e in r.extras}


def _source(module: str) -> Path | None:
    path = ROOT.parent.joinpath(*module.split("."))
    return path / "__init__.py" if path.is_dir() else path.with_suffix(".py") if path.with_suffix(".py").exists() else None


def _imports(module: str) -> list[str]:
    path = _source(module)
    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    names = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            names += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                parts = package.split(".")[: len(package.split(".")) - node.level + 1]
                base = ".".join(parts + ([base] if base else []))
            names += [base] + [f"{base}.{a.name}" for a in node.names]
    return names


def reached(family: str) -> tuple[dict[str, str], dict[str, str]]:
    """(third-party top-level name → importing module, gnomepy_research module → importing module)."""
    external, research = {}, {}
    seen, stack = set(), [f"gnome_pipelines.families.{family}"]
    while stack:
        module = stack.pop()
        if module in seen or _source(module) is None:
            continue
        seen.add(module)
        parts = module.split(".")
        stack += [".".join(parts[:i]) for i in range(1, len(parts))]
        for name in _imports(module):
            top = name.split(".")[0]
            if top == "gnome_pipelines":
                stack.append(name)
            elif top == "gnomepy_research":
                research.setdefault(name, module)
            elif top and top not in sys.stdlib_module_names and top != "__future__":
                external.setdefault(top, module)
    return external, research


@pytest.mark.parametrize("family", families.names())
def test_a_family_reaches_only_what_its_image_installs(family):
    allowed = _closure(_requirements(family))
    dists = packages_distributions()
    research_extras = _research_extras(family)
    external, research = reached(family)

    outside = [f"{name} (imported by {via})" for name, via in sorted(external.items())
               if not any(canonicalize_name(d) in allowed for d in dists.get(name, [name]))]
    needs_more = [f"{name} needs gnomepy-research{list(module_extras(name))} (imported by {via})"
                  for name, via in sorted(research.items()) if not set(module_extras(name)) <= research_extras]
    assert not outside + needs_more, f"the {family} image doesn't install:\n" + "\n".join(outside + needs_more)


def test_every_family_has_its_extra():
    assert set(families.names()) <= set(PROJECT["optional-dependencies"])


def test_every_pipeline_is_in_exactly_one_family():
    for module in pkgutil.walk_packages(gnome_pipelines.__path__, "gnome_pipelines."):
        importlib.import_module(module.name)
    defined = {cls for cls in Pipeline.__subclasses__() if cls.__module__.startswith("gnome_pipelines.")}
    listed = [cls for family in families.names() for cls in importlib.import_module(
        f"gnome_pipelines.families.{family}").PIPELINES]

    assert sorted(c.__name__ for c in listed) == sorted(c.__name__ for c in defined)
    assert all(inspect.isclass(c) and issubclass(c, Pipeline) for c in listed)


def test_an_image_sees_only_its_own_family(monkeypatch):
    monkeypatch.setenv(families.FAMILY_VARIABLE, "cs2")
    assert families.available() == families.pipelines("cs2")

    monkeypatch.setenv(families.FAMILY_VARIABLE, "nope")
    with pytest.raises(ValueError, match="no pipeline family 'nope'"):
        families.available()
