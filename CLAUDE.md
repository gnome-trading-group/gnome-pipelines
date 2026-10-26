# CLAUDE.md

gnome-pipelines holds every writer of the research datasets and models: HLTV and CFBD scrapers, derived feature
tables, model fits, and the AWS infrastructure they run on. Datasets are append-only Iceberg tables on S3 Tables and
artifacts are files indexed in the same bucket, both written through `gnomepy.data`. Consumers: gnomepy-research
(reads only), gnome-launcher's CS2 scanner (reads `cs2_upcoming_matches` after each `cs2_upcoming` run), the CS2
strategies (`cs2_upcoming_features`, the `cs2_bayes_fit` posterior), and gnome-controller's Research pages.

## Commands

Poetry project, Python 3.13.

```
poetry install --all-extras        # every family's extra; an image installs only its own
poetry run pytest                  # also covers tests/lambdas (cdk/lambda) and tests/data_api (cdk/data-api)
poetry run gnome-pipelines list
poetry run gnome-pipelines describe <pipeline>                       # its parameters
poetry run gnome-pipelines run <pipeline> --stage local|dev|prod -p key=value [--params '{...}'] [--yes]
poetry run gnome-pipelines sync-schema <tables> --stage dev [--drop-removed] [--recreate T] [--apply]
PIPELINE_FAMILY=cs2 poetry run python -m gnome_pipelines.manifest check   # what CI's image build runs
cd cdk && npm ci && npx cdk synth
```

- `--stage dev|prod` uses the AWS profile of that name; `local` writes a SQLite warehouse at
  `~/.cache/gnome-pipelines/warehouse`. Prod asks before writing unless `--yes`.
- `sync-schema` prints a plan only; nothing changes without `--apply`.

## Layout

- `gnome_pipelines/__init__.py`: `Pipeline[Params]`, `PipelineParams`, `Resources`, `Schedule`.
- `families/{cs2,cfb}.py`: `PIPELINES` per family; one image and one task definition each.
- `hltv_cs2/`: `hltv_cs2`, daily 06:00 UTC. HLTV matches, stats, veto, h2h, rankings, then rebuilds `cs2_match_priors`.
- `hltv_rosters/`: `hltv_rosters`, daily 05:30 UTC (`mode=daily`). Roster moves and current rosters.
- `cs2_upcoming/`: `cs2_upcoming`, every 15 min. Upcoming BO3s from HLTV, venue listings and books (Polymarket
  Intl, Kalshi, Polymarket US), writes `cs2_upcoming_matches` and `cs2_upcoming_features`.
- `cs2_prematch/train.py`: `cs2_prematch_train`, monthly on the 1st at 07:00 UTC. Pre-veto model refit.
- `cs2_bayes/`: `cs2_bayes_fit`, Mondays 06:00 UTC, 8 vCPU/16 GB. `cs2_bayes_blend` has no schedule; run by hand
  with weights from a research walk-forward.
- `cs2_round_model/train.py`: not a pipeline; `poetry run python -m gnome_pipelines.cs2_round_model.train`.
- `cfb/pipeline.py`: `cfb_update`, 08:00 and 20:00 UTC in Jan and Aug-Dec. CFBD refresh, quality-gated publish,
  feature tables; CFBD responses mirrored to `s3://<research bucket>/apicache/cfbd`.
- `cfb/qb_status/`: `cfb_qb_status`, daily 09:00 UTC in Jan and Aug-Dec. One long task (default 23.75 h) of
  15-60 s pollers; state (X spend, Claude cache) mirrored to `s3://<research bucket>/qb_status/state`.
- `datasets/{cs2,cfb,qb_status}.py`: `TableSpec` schemas. `schema_sync.py`: the `sync-schema` logic.
- `stores.py`, `credentials.py`, `environment.py`, `manifest.py`, `runner.py`, `cli.py`: framework.
- `cdk/lib/stacks/data-stack.ts` (`PipelinesDataStack`): S3 Tables bucket `gnome-research-tables` (namespaces
  `research`, `artifacts`), SSM `/gnome/research/table-bucket-arn` and `/gnome/research/artifact-root`, the
  `gnome/pipelines/*` secrets, and `gnome-pipelines-data-api` (container Lambda, `cdk/data-api/app.py`).
- `cdk/lib/stacks/run-stack.ts` (`PipelinesRunStack`): ECR `gnome-pipelines`, Fargate cluster, `gnome-pipelines`
  DynamoDB runs table, schedule group, the Lambdas in `cdk/lambda/`, alarms.
- `cdk/lambda/`: `api.py` (`gnome-pipelines-api`), `trigger_pipeline.py` (`-trigger`), `sync_schedules.py`
  (`-sync`), `pipeline_status.py` (`-status`).

## Conventions and invariants

- A pipeline is a `Pipeline[XParams]` subclass with `name`, `description`, `resources`, `schedules`, listed in
  exactly one family. A new pipeline joins the family whose dependencies it shares; a new family needs a module in
  `families/`, a same-named extra in `pyproject.toml`, and its name in `FAMILIES` in `run-stack.ts`.
  `tests/test_families.py` catches a pipeline that won't import with only its family's extra.
- `families.pipelines()` imports a family's module by name, not at the top of the file, on purpose: an image has
  only its own family's extra, so importing every family would fail there.
- `PipelineParams` forbids extra fields, so a misspelled parameter fails the run instead of taking the default.
- Schedules are declared in code. Sync creates new ones switched off and keeps the on/off state of changed ones;
  toggling on the Pipelines page is the only runtime edit.
- Always publish through `stores.dataset_store()` / `artifact_store()`: rows are stamped with
  `gnome-pipelines@<sha>` and `<pipeline>/<run id>`, which the controller's Datasets page links back to the run.
- Tables are append-only; readers read as of a time. For `cs2_upcoming_matches` the newest row per match is its state.
- A publish refuses a table whose columns differ from its spec. Dropping or renaming columns needs `sync-schema` first.
- Never add Iceberg tags, branches or `history.expire.*` properties: they break S3 Tables snapshot management for
  the whole table.
- Secrets: `credentials.secret(name, ENV_VAR)` uses the env var locally, else `gnome/pipelines/<name>` (us-east-1).
  Names: `zenrows`, `cfbd`, `x`, `anthropic` (`SECRETS` in data-stack.ts). A placeholder value `unset` fails
  loudly. Set real values only with `aws secretsmanager put-secret-value --profile dev|prod`, never in code.
- `cfb_qb_status` runs with X and Claude off when `gnome/pipelines/x` or `gnome/pipelines/anthropic` is unset, and
  says so in its output rather than failing.
- ZenRows credits are reserved for prod pipelines. Ad-hoc and local HLTV work uses `-p fetcher=chrome` (visible
  local Chrome, free) or `-p fetcher=cache` (no network). `fetcher=chrome` refuses to run in a Fargate task.
  `cs2_upcoming` caps spend with `daily_credit_budget` (default 15000/UTC day) and slows refetches past it.
- HLTV fetching aborts after 15 consecutive failures (`RateLimited`); retrying immediately escalates the block.
- Dagster-ready: each family is meant to become a Dagster code location. No Dagster code exists yet.

## Operations

- Infra deploys: CodePipeline `ETLPipelinesPipeline` (stack `ETLPipelinesPipelineStack`) synths `cdk/` from main,
  deploys Dev (`PipelinesDataStack`, `PipelinesRunStack`), then `ApproveProd`, then Prod.
- Code deploys are separate: `.github/workflows/build-image.yml` builds `gnome-pipelines:<family>-<sha>` on pushes to
  main touching `gnome_pipelines/`, `Dockerfile`, `pyproject.toml` or `poetry.lock`, pushes to dev ECR, publishes the
  manifest (SSM `/gnome/pipelines/types/<pipeline>`) and invokes `gnome-pipelines-sync`. The prod job waits for a
  reviewer on the `prod` GitHub environment and promotes the same image. Re-run the workflow by hand to republish.
- Runs: EventBridge Scheduler (group `gnome-pipelines`, schedules named `<pipeline>.<schedule>`, UTC) or "Run" on
  the controller's Pipelines page invokes `gnome-pipelines-trigger`, which validates parameters, records a PENDING
  run in the `gnome-pipelines` table and starts one Fargate task sized by the pipeline's `Resources`. Which
  schedules are switched on is runtime state: `aws scheduler list-schedules --profile prod` (as of 2026-10-26 every
  prod schedule is ENABLED except `cs2_upcoming.quarter_hourly`).
- `gnome-pipelines-status` handles ECS task-stopped events: a task that failed to start is relaunched up to 3 times;
  a task that stopped with a non-zero exit (including OOM) is marked FAILED.
- Events: each SUCCEEDED run puts source `gnome.pipelines`, detail-type `Pipeline Run Succeeded`, detail
  `{pipeline, family, run_id, code_version}` on the default bus. gnome-launcher's Cs2Scanner listens for
  `cs2_upcoming`. A failure to put it is only logged; the launcher misses that one run.
- Failed runs put alert `PIPELINE_FAILED` (key `PIPELINE_FAILED:<pipeline>:<run_id>`, labels `pipeline`, `runId`,
  `trigger`) on the `gnome-alerts` bus. Per the runbook, scheduled runs are ERROR and manual runs INFO (the payload
  carries only the `trigger` label; the registry's `alert-types.ts` maps `trigger: scheduled` to ERROR, anything else to INFO).
- Logs: runs in `/gnome-pipelines/runs` (stream `pipeline/pipeline/<task id>`, 3-month retention, also on the
  Pipelines page); the run item's `error_message` holds the traceback tail. Lambdas log to `/aws/lambda/<name>`.
- Alarms (no actions; the registry's alerting stack picks them up):
  - `Pipelines--Trigger-Fault-Count-Error`: a run couldn't start. A scheduled run that never starts shows only here,
    never as `PIPELINE_FAILED`.
  - `Pipelines--Status-Fault-Count-Error`: a stopped task wasn't recorded, so a failure may be unmarked and unalerted.
  - `Pipelines--Sync-Fault-Count-Error`: schedules still match the previous manifest. Re-run the image workflow.
  - `Pipelines--Api-Fault-Count-Warning`: the Pipelines page API errored.
- No alarm watches dataset freshness. A stale dataset means a schedule is off, its runs fail, or the trigger alarm
  fired; check the pipeline's recent runs on the Pipelines page. `cs2_series` sessions stop trading on a
  `cs2_bayes_fit` posterior older than two weeks; a fit that didn't converge is not published.
- Runbooks (gnome-registry): `docs/runbooks/cloudwatch/Pipelines.md` (the alarms) and
  `docs/runbooks/alerts/PIPELINE_FAILED.md`. Fix, then re-run from the Pipelines page.
