# gnome-pipelines

Writers for the research datasets: ingestion, scheduled pipelines and their infrastructure.
Datasets are append-only Iceberg tables on S3 Tables, written and read through `gnomepy.data`.

Table schemas live in `gnome_pipelines/datasets/`, next to the code that writes them.
Readers don't need them: `DatasetStore.load(name)` takes the key from the table itself.

```
poetry install
poetry run pytest
```
