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
| `gnome_pipelines/cli.py` | `gnome-pipelines run <pipeline> --stage local\|dev\|prod`: a registered pipeline on your machine |
| `gnome_pipelines/environment.py` | `environment()`: the run's stage (`local`/`dev`/`prod`) and whether it's in a Fargate task |
| `gnome_pipelines/runner.py` | Fargate entry point the trigger Lambda starts (`PIPELINE_NAME`, `RUN_ID`, `PIPELINE_PARAMS`); records the run's status |
| `Dockerfile`, `.github/workflows/build-image.yml` | The `gnome-pipelines` image: every push to main goes to dev ECR, then to prod once approved on the `prod` GitHub environment |
| `cdk/lib/stacks/data-stack.ts` | The S3 Tables bucket, its namespaces, the SSM parameters `gnomepy.data` finds it by, and the pipeline secrets |
| `cdk/lib/stacks/run-stack.ts` | Running pipelines: ECR repo, Fargate cluster and task, the `gnome-pipelines` runs table, schedules, alarms |
| `cdk/lambda/` | The pipelines API (list, create, get, update, trigger) and the task-stopped status handler |

```
poetry install
poetry run pytest     # includes tests/lambdas for cdk/lambda
```

gnome-controller's Research → Pipelines pages call `/research/pipelines` on the controller API, which
routes to the `gnome-pipelines-*` Lambdas by name. Everything else about running a pipeline lives here:
a definition (parameters, cpu/memory, an optional EventBridge schedule in the `gnome-pipelines` schedule
group) and its runs are items in the `gnome-pipelines` DynamoDB table; a run is one Fargate task, and a
failed run is put on the `gnome-alerts` bus as `PIPELINE_FAILED`.

## Running a pipeline locally

```
poetry run gnome-pipelines list
poetry run gnome-pipelines describe hltv_cs2                              # its parameters
poetry run gnome-pipelines run hltv_cs2 --stage dev -p start_date=2026-10-11 -p end_date=2026-10-13 -p min_stars=0
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
