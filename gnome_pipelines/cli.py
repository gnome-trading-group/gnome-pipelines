"""`gnome-pipelines`: run a registered pipeline from a laptop, through the same execute() the Fargate task uses.

    gnome-pipelines list
    gnome-pipelines describe hltv_cs2
    gnome-pipelines run hltv_cs2 --stage dev -p start_date=2026-10-11 -p end_date=2026-10-13 -p min_stars=0
    gnome-pipelines run cs2_prematch_predict --stage prod --params '{"dry_run": true}'

--stage picks the data and the credentials together: dev and prod use the AWS profile of that name, and local
writes to a SQLite warehouse on disk. A local run records no run item; its outputs are printed instead.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import click
from pydantic import ValidationError
from pydantic_core import PydanticUndefined

from gnome_pipelines import PIPELINE_REGISTRY
from gnome_pipelines.environment import Stage
from gnome_pipelines.runner import execute

DEFAULT_WAREHOUSE = Path.home() / ".cache" / "gnome-pipelines" / "warehouse"
REPO = Path(__file__).resolve().parents[1]


@click.group()
def main() -> None:
    """Run gnome pipelines on this machine."""


@main.command("list")
def list_pipelines() -> None:
    """The registered pipelines."""
    for name in sorted(PIPELINE_REGISTRY):
        click.echo(name)


@main.command()
@click.argument("pipeline")
def describe(pipeline: str) -> None:
    """PIPELINE's parameters: type, default and what each is for."""
    fields = _pipeline(pipeline).params_type.model_fields
    if not fields:
        click.echo("no parameters")
    for name, info in fields.items():
        default = "required" if info.default is PydanticUndefined else f"default {info.default!r}"
        click.echo(f"{name}: {_type_name(info.annotation)} ({default})")
        if info.description:
            click.echo(f"    {info.description}")


@main.command()
@click.argument("pipeline")
@click.option("--stage", required=True, type=click.Choice([s.value for s in Stage]),
              help="whose data, and with it the AWS profile: local, dev or prod")
@click.option("-p", "--param", "param_pairs", multiple=True, metavar="KEY=VALUE",
              help="a parameter, converted to its declared type (`describe` lists them)")
@click.option("--params", "params_json", default="{}", help="parameters as one JSON object; -p values override it")
@click.option("--warehouse", type=click.Path(path_type=Path), default=DEFAULT_WAREHOUSE, show_default=True,
              help="the warehouse for --stage local")
@click.option("--yes", is_flag=True, help="don't ask before writing to prod")
def run(pipeline: str, stage: str, param_pairs: tuple[str, ...], params_json: str, warehouse: Path, yes: bool) -> None:
    """Run PIPELINE here, through the same execute() as the Fargate task. `gnome-pipelines list` names them."""
    params = {**json.loads(params_json), **dict(_param(p) for p in param_pairs)}
    try:
        # Bad parameters fail here, before the prompt; execute() validates the same way in the task.
        _pipeline(pipeline).params_type.model_validate(params)
    except ValidationError as e:
        raise click.UsageError(f"{pipeline} parameters:\n{e}") from None
    stage = Stage(stage)
    if stage is Stage.PROD and not yes:
        click.confirm(f"Run {pipeline} against prod data?", abort=True)

    os.environ.update(_run_environment(stage, warehouse))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("Running %s against %s as %s (%s): %s", pipeline, stage.value, os.environ["RUN_ID"],
                 os.environ["GNOME_CODE_VERSION"], params)
    result = execute(pipeline, params)
    click.echo(json.dumps(result.outputs, indent=2, default=str))


def _pipeline(name: str):
    if name not in PIPELINE_REGISTRY:
        raise click.BadParameter(f"not one of {', '.join(sorted(PIPELINE_REGISTRY))}", param_hint="PIPELINE")
    return PIPELINE_REGISTRY[name]


def _type_name(annotation: object) -> str:
    return getattr(annotation, "__name__", str(annotation)).replace("typing.", "")


def _run_environment(stage: Stage, warehouse: Path) -> dict[str, str]:
    env = {
        "GNOME_STAGE": stage.value,
        "RUN_ID": f"local-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}",
        "GNOME_CODE_VERSION": f"gnome-pipelines@{_checkout_version()}",
    }
    if stage is Stage.LOCAL:
        env["GNOME_LOCAL_WAREHOUSE"] = str(warehouse)
    else:
        env["AWS_PROFILE"] = stage.value
    return env


def _checkout_version() -> str:
    # Rows a laptop writes say which commit wrote them, and whether it had uncommitted changes.
    def git(*args: str) -> str:
        return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, check=True).stdout.strip()

    try:
        sha = git("rev-parse", "--short", "HEAD")
        return f"{sha}-dirty" if git("status", "--porcelain") else sha
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "local"


def _param(text: str) -> tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep:
        raise click.BadParameter(f"takes KEY=VALUE, got {text!r}", param_hint="-p")
    return key, value


if __name__ == "__main__":
    main()
