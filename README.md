# gnome-pipelines

Every writer of the research datasets and models: scrapers, ingestion, derived tables, training, and
the infrastructure they run on. Datasets are append-only Iceberg tables on S3 Tables, written and read
through `gnomepy.data`; gnomepy-research holds the feature and model code these call, and only reads.

| Path | Contents |
|---|---|
| `gnome_pipelines/datasets/` | Table schemas (`TableSpec`), one module per domain. Readers don't need them: `load(name)` takes the key from the table. |
| `gnome_pipelines/stores.py` | Stores stamped with the build's code version and the run id; every writer publishes through them. |
| `gnome_pipelines/credentials.py` | `secret(name, env_var)`: the environment variable locally, else the `gnome/pipelines/<name>` secret |
| `gnome_pipelines/hltv_cs2/` | HLTV scraper, local backfill, page cache, demo parser, publishing, the weekly `hltv_cs2` pipeline |
| `gnome_pipelines/cs2_prematch/` | `cs2_prematch_train` and `cs2_prematch_predict` pipelines |
| `gnome_pipelines/cs2_round_model/` | Round win probability model training |
| `gnome_pipelines/cfb/` | CFBD ingestion, quality checks, feature snapshots, team-week posteriors |
| `gnome_pipelines/runner.py` | Fargate entry point the controller's scheduler starts (`PIPELINE_NAME`, `RUN_ID`, `PIPELINE_PARAMS`) |
| `Dockerfile`, `.github/workflows/build-image.yml` | The `gnomepy-pipelines` image, pushed to ECR in dev and prod on every push to main |
| `cdk/` | The S3 Tables bucket, its namespaces, the SSM parameters `gnomepy.data` finds it by, and the pipeline secrets |

```
poetry install
poetry run pytest
```

Locally, the AWS profile picks the account: `gnomepy.data` reads `/gnome/research/table-bucket-arn` and
`/gnome/research/artifact-root` from SSM. Set `GNOME_LOCAL_WAREHOUSE=/some/dir` to write to a local
SQLite catalog instead.

Secrets (the ZenRows key for HLTV, the CFBD key for college football) come from `ZENROWS_API_KEY` /
`CFBD_API_KEY` locally. In the pipeline task they come from Secrets Manager: `cdk/` creates `gnome/pipelines/<name>`
with a placeholder, and the real value is set once per account, never in code:

```
aws secretsmanager put-secret-value --profile dev --secret-id gnome/pipelines/zenrows --secret-string "$ZENROWS_API_KEY"
```

A new secret: add its name to `SECRETS` in `cdk/lib/stacks/data-stack.ts` and call `secret("<name>", "<ENV_VAR>")`.
