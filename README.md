# gnome-pipelines

Every writer of the research datasets and models: scrapers, ingestion, derived tables, training, and
the infrastructure they run on. Datasets are append-only Iceberg tables on S3 Tables, written and read
through `gnomepy.data`; gnomepy-research holds the feature and model code these call, and only reads.

| Path | Contents |
|---|---|
| `gnome_pipelines/datasets/` | Table schemas (`TableSpec`), one module per domain. Readers don't need them: `load(name)` takes the key from the table. |
| `gnome_pipelines/stores.py` | Stores stamped with the build's code version and the run id; every writer publishes through them. |
| `gnome_pipelines/credentials.py` | `secret(name, env_var)`: the environment variable locally, else the `gnome/pipelines/<name>` secret |
| `gnome_pipelines/hltv_cs2/` | The `hltv_cs2` pipeline: parsers (`scraper.py`), page sources (`fetch.py`: ZenRows, cache; `chrome.py`: local Chrome + demos), the collection loops (`collect.py`), page cache, demo parser, publishing |
| `gnome_pipelines/cs2_prematch/` | `cs2_prematch_train` and `cs2_prematch_predict` pipelines |
| `gnome_pipelines/cs2_round_model/` | Round win probability model training |
| `gnome_pipelines/cfb/` | CFBD ingestion, quality checks, feature snapshots, team-week posteriors |
| `gnome_pipelines/__init__.py` | `Pipeline[Params]`: what a pipeline declares (parameters, description, `Resources`, `Schedule`s) |
| `gnome_pipelines/manifest.py` | Those declarations as JSON, published to SSM by CI for the API and the scheduler |
| `gnome_pipelines/cli.py` | `gnome-pipelines run <pipeline> --stage local\|dev\|prod`: a registered pipeline on your machine |
| `gnome_pipelines/environment.py` | `environment()`: the run's stage (`local`/`dev`/`prod`) and whether it's in a Fargate task |
| `gnome_pipelines/runner.py` | Fargate entry point the trigger Lambda starts (`PIPELINE_NAME`, `RUN_ID`, `PIPELINE_PARAMS`); records the run's status |
| `Dockerfile`, `.github/workflows/build-image.yml` | The `gnome-pipelines` image: every push to main goes to dev ECR, then to prod once approved on the `prod` GitHub environment |
| `cdk/lib/stacks/data-stack.ts` | The S3 Tables bucket, its namespaces, the SSM parameters `gnomepy.data` finds it by, the pipeline secrets, and the data API |
| `cdk/lib/stacks/run-stack.ts` | Running pipelines: ECR repo, Fargate cluster and task, the `gnome-pipelines` runs table, schedule group, Lambdas, alarms |
| `cdk/lambda/` | The pipelines API (`api.py`), run trigger, schedule sync and the task-stopped status handler |
| `cdk/data-api/` | The datasets and artifacts API: a container Lambda reading the Iceberg tables |

```
poetry install
poetry run pytest     # includes tests/lambdas (cdk/lambda) and tests/data_api (cdk/data-api)
```

## How pipelines run

A pipeline is declared entirely in code, the way an orchestrator like Dagster would have it:

```python
@register_pipeline
class HltvCs2Pipeline(Pipeline[HltvCs2Params]):
    name = "hltv_cs2"
    description = "HLTV matches, ... then the match priors rebuilt"
    resources = Resources(cpu=2048, memory=8192)
    schedules = (Schedule("weekly", "cron(0 6 ? * MON *)", {"days_back": 8, "min_stars": 0}),)
```

- **Publishing.** When CI pushes the image to an account, it publishes the declarations from that image as
  one SSM parameter per pipeline under `/gnome/pipelines/types/` and invokes `gnome-pipelines-sync`, which
  makes the EventBridge schedules (group `gnome-pipelines`, named `<pipeline>.<schedule>`) match: new ones
  start **off**, changed ones keep their on/off state, removed ones are deleted. Re-run the workflow by hand
  to republish.
- **Running.** A schedule, or "Run" on the Pipelines page, invokes `gnome-pipelines-trigger`. It checks the
  parameters against the published schema, records the run in the `gnome-pipelines` table and starts one
  Fargate task with the pipeline's declared size; the runner then validates with the Pydantic model itself.
  A failed run is put on the `gnome-alerts` bus as `PIPELINE_FAILED`; logs go to `/gnome-pipelines/runs`.
- **The UI.** gnome-controller's Research pages call `/research/pipelines/*`, `/research/datasets/*` and
  `/research/artifacts/*`, which it forwards to `gnome-pipelines-api` and `gnome-pipelines-data-api`. A
  new endpoint here needs no controller change. Switching a schedule on or off is the only runtime edit.

## Running a pipeline locally

```
poetry run gnome-pipelines list
poetry run gnome-pipelines describe hltv_cs2                              # its parameters
poetry run gnome-pipelines run hltv_cs2 --stage dev -p days_back=3 -p min_stars=0
poetry run gnome-pipelines run cs2_prematch_predict --stage prod --params '{"dry_run": true}'
poetry run gnome-pipelines run cs2_prematch_train --stage local          # ~/.cache/gnome-pipelines/warehouse

# HLTV through a visible Chrome window instead of ZenRows, with demos for the round features
poetry run gnome-pipelines run hltv_cs2 --stage dev -p fetcher=chrome -p demos=true -p start_date=… -p end_date=…
# Re-parse already-cached pages after a parser change: no network
poetry run gnome-pipelines run hltv_cs2 --stage dev -p fetcher=cache -p start_date=… -p end_date=…
```

It runs the same `execute()` as the Fargate task, which first validates the parameters against the
pipeline's `PipelineParams` model (`class HltvCs2Pipeline(Pipeline[HltvCs2Params])`): `-p` values are
converted to the declared types, and a missing, misspelled or mistyped parameter stops the run before
it starts. `--stage` picks the data and the credentials together:
`dev` and `prod` use the AWS profile of that name, and `local` a SQLite warehouse on disk (`--warehouse`
to move it). Prod asks first (`--yes` skips). A local run records no run item and prints its outputs;
the rows it writes carry `gnome-pipelines@<commit>[-dirty]` and a `local-<timestamp>` run id. Inside a
pipeline, `environment()` says which stage it's writing to and whether it's in a task.

Outside the command, the AWS profile picks the account: `gnomepy.data` reads `/gnome/research/table-bucket-arn` and
`/gnome/research/artifact-root` from SSM. Set `GNOME_LOCAL_WAREHOUSE=/some/dir` to write to a local
SQLite catalog instead.

Secrets (the ZenRows key for HLTV, the CFBD key for college football) come from `ZENROWS_API_KEY` /
`CFBD_API_KEY` locally. In the pipeline task they come from Secrets Manager: `cdk/` creates `gnome/pipelines/<name>`
with a placeholder, and the real value is set once per account, never in code:

```
aws secretsmanager put-secret-value --profile dev --secret-id gnome/pipelines/zenrows --secret-string "$ZENROWS_API_KEY"
```

A new secret: add its name to `SECRETS` in `cdk/lib/stacks/data-stack.ts` and call `secret("<name>", "<ENV_VAR>")`.
