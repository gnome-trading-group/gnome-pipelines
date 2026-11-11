# CLAUDE.md

gnome-pipelines holds every writer of the research datasets and models: HLTV and CFBD scrapers, derived feature
tables, model fits, long-running polling sources (alternative data on 10 s to few-minute cadences), and the AWS
infrastructure they run on. Datasets are append-only Iceberg tables on S3 Tables and
artifacts are files indexed in the same bucket, both written through `gnomepy.data`. Consumers: gnomepy-research
(reads only), gnome-launcher's CS2 scanner (reads `cs2_upcoming_matches` after each `cs2_upcoming` run), the CS2
strategies (`cs2_upcoming_features`, the `cs2_e12_fit` model and its blend), and gnome-controller's Research pages.

## Commands

Poetry project, Python 3.13.

```
poetry install --all-extras        # every family's extra; an image installs only its own
poetry run pytest                  # also covers tests/lambdas (cdk/lambda) and tests/data_api (cdk/data-api)
poetry run gnome-pipelines list
poetry run gnome-pipelines describe <pipeline>                       # its parameters
poetry run gnome-pipelines run <pipeline> --stage local|dev|prod -p key=value [--params '{...}'] [--yes]
poetry run gnome-pipelines sync-schema <tables> --stage dev [--drop-removed] [--recreate T] [--apply]
poetry run gnome-pipelines serve <source> --stage local|dev|prod [-p key=value] [--state-root DIR]   # until Ctrl-C
PIPELINE_FAMILY=cs2 poetry run python -m gnome_pipelines.manifest check   # what CI's image build runs
cd cdk && npm ci && npx cdk synth
```

- `--stage dev|prod` uses the AWS profile of that name; `local` writes a SQLite warehouse at
  `~/.cache/gnome-pipelines/warehouse`. Prod asks before writing unless `--yes`.
- `sync-schema` prints a plan only; nothing changes without `--apply`.

## Layout

- `gnome_pipelines/__init__.py`: `Pipeline[Params]`, `PipelineParams`, `Resources`, `Schedule`.
- `families/{cs2,cfb}.py`: `PIPELINES` and polling `SOURCES` per family; one image and one task definition each.
- `polling/`: the polling-source framework. `service.py` (`Service`, `Task`, `Meter`: one thread per lane, a task's
  lane being its name unless it names a shared one; first-seen stamping after the task returns, records handed to the
  store one lane at a time, gap/stall catch-up, retry backoff, heartbeats and hourly spend rows),
  `health.py` (`PollHealth` → the shared `poll_health` table), `source.py` (`PollingSource[Params]`: name,
  resources, secrets, `build()`), `serve.py` (build, run until SIGTERM, then the Service's `on_stop`; runner.py serves
  a source's run with it), `claude.py` (optional `ClaudeReader`: structured output, a pluggable read cache, in
  memory by default; per-process daily cap; a spend meter). The framework keeps no state on disk: a source keeps what
  must outlive a restart in its own tables or objects, written as it goes.
- `hltv_cs2/`: `hltv_cs2`, daily 06:00 UTC. HLTV matches, stats, veto, h2h, rankings, then rebuilds `cs2_match_priors`.
- `hltv_rosters/`: `hltv_rosters`, daily 05:30 UTC (`mode=daily`). Roster moves and current rosters.
- `faceit/`: `faceit_daily`, 07:30 UTC daily (after `hltv_cs2`). FACEIT history of the players in
  `cs2_hltv_faceit_map`, organised series, the FACEIT-to-HLTV series link and the e12 model's exclusions, into the
  `faceit_*` tables. `faceit_backfill` (no schedule) loads a research FACEIT build once.
- `cs2_upcoming/`: `cs2_upcoming`, every 15 min. BO3s from 2.5h before kickoff (match pages every 15 min in the last
  75), venue listings and books (Polymarket Intl, Kalshi, Polymarket US), writes `cs2_upcoming_matches` and
  `cs2_upcoming_features`. Kalshi's kickoff is its esports milestone's start (tickers keep the original schedule),
  else the ticker's time.
- `cs2_e12/`: `cs2_e12_fit`, daily 08:30 UTC (after `hltv_cs2` and `faceit_daily`), 4 vCPU/30 GB. Fits the
  e12-wide-mm series model at its start time and publishes `e12_model/cs2_series`; refuses on inputs more than 2 days
  stale, an implausibly small fit, or a skill fit whose gradient shows it stopped short of the optimum. `cs2_e12_blend` has no schedule; run by hand with blend weights fitted in research.
- `cs2_round_model/train.py`: not a pipeline; `poetry run python -m gnome_pipelines.cs2_round_model.train`.
- `cfb/pipeline.py`: `cfb_update`, 08:00 and 20:00 UTC in Jan and Aug-Dec. CFBD refresh, quality-gated publish,
  feature tables; CFBD responses mirrored to `s3://<research bucket>/apicache/cfbd`.
- `cfb/qb_status/`: the `cfb_qb_status` polling source (`source.py`), started for the season (Aug) and stopped
  after it (Feb) on the Pipelines page; a run still going on July 1 ends itself and restarts on the new season.
  15-60 s pollers in `service.py` (`Context`, `build_service`), all in
  one lane because `Context` keeps unlocked in-memory state. Its state lives in `s3://<research bucket>/qb_status/state`
  (`state_store.py`, a local folder for local runs): the X budget object `qb_status_x_state.json` (spend and
  since_ids, merged in every 5 s and on stop with conditional writes) and one object per Claude read under `claude/`. `python -m gnome_pipelines.cfb.qb_status.service` is the laptop tool
  (`--once`, `--health`, `--load-registry`).
- `datasets/{cs2,faceit,cfb,qb_status,polling}.py`: `TableSpec` schemas. `schema_sync.py`: the `sync-schema` logic.
- `stores.py`, `credentials.py`, `environment.py`, `manifest.py`, `runner.py`, `cli.py`: framework.
- `cdk/lib/stacks/data-stack.ts` (`PipelinesDataStack`): S3 Tables bucket `gnome-research-tables` (namespaces
  `research`, `artifacts`), SSM `/gnome/research/table-bucket-arn` and `/gnome/research/artifact-root`, the
  `gnome/pipelines/*` secrets, and `gnome-pipelines-data-api` (container Lambda, `cdk/data-api/app.py`).
- `cdk/lib/stacks/run-stack.ts` (`PipelinesRunStack`): ECR `gnome-pipelines`, Fargate cluster, `gnome-pipelines`
  DynamoDB runs table, schedule group, the Lambdas in `cdk/lambda/`, alarms, and per entry of `POLLING_SOURCES` a
  `SourceTask` (`cdk/lib/source-task.ts`): the source's own task definition and a role with only its secrets.
- `cdk/lambda/`: `api.py` (`gnome-pipelines-api`), `trigger_pipeline.py` (`-trigger`), `sync_schedules.py`
  (`-sync`), `pipeline_status.py` (`-status`), `sources.py` (a source's one live run and stopping it).

## Conventions and invariants

- A pipeline is a `Pipeline[XParams]` subclass with `name`, `description`, `resources`, `schedules`, listed in
  exactly one family. A new pipeline joins the family whose dependencies it shares; a new family needs a module in
  `families/`, a same-named extra in `pyproject.toml`, and its name in `FAMILIES` in `run-stack.ts`.
  `tests/test_families.py` catches a pipeline that won't import with only its family's extra.
- A polling source is a `PollingSource[XParams]` subclass in exactly one family's `SOURCES`, with a matching entry
  (family, secrets) in `POLLING_SOURCES` in `run-stack.ts`; `tests/test_families.py` checks both. It is published in
  the manifest with `kind: "source"` and started, given parameters and stopped on the Pipelines page like a pipeline,
  but its run never ends on its own: at most one is live (a conditional `SOURCE#<name>`/`ACTIVE` item in the runs
  table), and the status Lambda starts its task again whenever it stops unasked. Sources have no schedules. An
  orchestrator observes sources through their tables, never runs them.
- Polling-source records must have a `first_seen_at` attribute (stamped by `Service` after the task returns; a task
  returning a record without one fails) beside the source's own `published_at`; readers decide on `first_seen_at`. Framework health goes to `poll_health`, never only to logs.
- `polling/claude.py` is the only polling module that imports `anthropic`: sources without Claude run in any family
  image (`tests/test_families.py`).
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
  Names: `zenrows`, `cfbd`, `x`, `anthropic`, `faceit` (`SECRETS` in data-stack.ts). A placeholder value `unset` fails
  loudly. Set real values only with `aws secretsmanager put-secret-value --profile dev|prod`, never in code.
- `cfb_qb_status` runs with X and Claude off when `gnome/pipelines/x` or `gnome/pipelines/anthropic` is unset, and
  says so in a `poll_health` `note` row rather than failing. It caps X spend with `x_monthly_usd` (default 60; sweeps stop at 80%)
  and `x_daily_usd` (default 3), counted at $0.005 per post returned and kept in its state store object.
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
- Polling sources: Start/Stop on the Pipelines page (or `POST /research/pipelines/<name>/stop`). Run states PENDING →
  RUNNING → STOPPING → STOPPED, or FAILED. When a source's task stops without a stop being asked for, the status
  Lambda starts it again on the same run (status back to PENDING, `restarts` +1; `ecs_task_arn` is `RESTARTING#...`
  while it does, so a duplicate stopped event can't launch twice). A non-zero exit or failed start, unless the stop
  came from `stop_task` (`UserInitiated`), is a crash: each puts a WARNING alert (`SOURCE_RESTARTED:<name>:<run>`), and
  the 3rd in 30 min or the 10th in a day fails the run with an ERROR `PIPELINE_FAILED` (label `kind: source`). A run
  whose task is gone without a stopped event having ended it (a lost event, a failed Lambda) is ended by the next
  Stop or Start (`sources.dead`). A task call running 3x `task_timeout` makes the process exit 1 (`hung` row) so it
  restarts; past `task_timeout` it records a `stall`. CI's `restart-sources` step stops each running source task of the family after pushing
  `<family>-latest`, which restarts it on the new image (dev, then prod after approval); a stopped source stays
  stopped. Every restart leaves ~1 min with nothing polling, so avoid pushing a source's family during a window it
  must not miss. Health: `poll_health` (`heartbeat` every 10 min, `spend` hourly per meter, `gap`, `stall`,
  `hung`, `task_error`, `note`, `started`/`stopped`).
- Runs: EventBridge Scheduler (group `gnome-pipelines`, schedules named `<pipeline>.<schedule>`, UTC) or "Run" on
  the controller's Pipelines page invokes `gnome-pipelines-trigger`, which validates parameters, records a PENDING
  run in the `gnome-pipelines` table and starts one Fargate task sized by the pipeline's `Resources`. Which
  schedules are switched on is runtime state: `aws scheduler list-schedules --profile prod` (as of 2026-10-26 every
  prod schedule is ENABLED except `cs2_upcoming.quarter_hourly`).
- `gnome-pipelines-status` handles ECS task-stopped events: a pipeline task that failed to start is relaunched up to 3
  times; one that stopped with a non-zero exit (including OOM) is marked FAILED. Sources: see above.
- Events: each SUCCEEDED run puts source `gnome.pipelines`, detail-type `Pipeline Run Succeeded`, detail
  `{pipeline, family, run_id, code_version}` on the default bus. gnome-launcher's Cs2Scanner listens for
  `cs2_upcoming`. A failure to put it is only logged; the launcher misses that one run.
- Failed runs put alert `PIPELINE_FAILED` (key `PIPELINE_FAILED:<pipeline>:<run_id>`, labels `pipeline`, `runId`,
  `trigger`, `kind`) on the `gnome-alerts` bus. The registry's `alert-types.ts` maps `trigger: scheduled` or
  `kind: source` to ERROR, anything else to INFO.
- Logs: runs in `/gnome-pipelines/runs` (stream `pipeline/pipeline/<task id>`, 3-month retention, also on the
  Pipelines page); the run item's `error_message` holds the traceback tail. Lambdas log to `/aws/lambda/<name>`.
- Alarms (no actions; the registry's alerting stack picks them up):
  - `Pipelines--Trigger-Fault-Count-Error`: a run couldn't start. A scheduled run that never starts shows only here,
    never as `PIPELINE_FAILED`.
  - `Pipelines--Status-Fault-Count-Error`: a stopped task wasn't recorded, so a failure may be unmarked and unalerted.
  - `Pipelines--Sync-Fault-Count-Error`: schedules still match the previous manifest. Re-run the image workflow.
  - `Pipelines--Api-Fault-Count-Warning`: the Pipelines page API errored.
- No alarm watches dataset freshness. A stale dataset means a schedule is off, its runs fail, or the trigger alarm
  fired; check the pipeline's recent runs on the Pipelines page. `cs2_e12_fit` does not publish on stale HLTV or
  FACEIT inputs, so a gap in `hltv_cs2` or `faceit_daily` leaves the previous day's `e12_model` in place (the
  `cs2_series` sessions stop trading once the newest is more than 2 days old).
- Runbooks (gnome-registry): `docs/runbooks/cloudwatch/Pipelines.md` (the alarms) and
  `docs/runbooks/alerts/PIPELINE_FAILED.md`. Fix, then re-run from the Pipelines page.
