FROM python:3.13-slim

ARG GIT_SHA=unknown

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    GNOME_CODE_VERSION=gnome-pipelines@${GIT_SHA}

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry

WORKDIR /app
COPY pyproject.toml poetry.lock README.md ./
RUN poetry install --only main --no-root

COPY gnome_pipelines gnome_pipelines/

ENTRYPOINT ["python", "-m", "gnome_pipelines.runner"]
