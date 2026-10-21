# One image per pipeline family (gnome_pipelines/families/): the base dependencies plus that family's extra only.
#   docker build --build-arg FAMILY=cs2 --build-arg GIT_SHA=$(git rev-parse --short HEAD) .
FROM python:3.13-slim AS build

ARG FAMILY

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    VIRTUAL_ENV=/venv \
    PATH=/venv/bin:$PATH

# git: Poetry clones gnomepy-research at its tag.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*
RUN pip install poetry && python -m venv /venv

WORKDIR /app
COPY pyproject.toml poetry.lock README.md ./
RUN test -n "$FAMILY" && poetry install --only main --no-root --extras "$FAMILY"


FROM python:3.13-slim

ARG FAMILY
ARG GIT_SHA=unknown

ENV PYTHONUNBUFFERED=1 \
    PATH=/venv/bin:$PATH \
    PIPELINE_FAMILY=${FAMILY} \
    GNOME_CODE_VERSION=gnome-pipelines@${GIT_SHA}

# g++: pytensor (pymc) compiles its graph to C and falls back to slow pure-Python ops without a compiler. Only the
# families whose extra brings pymc need it.
RUN case "$FAMILY" in cs2|cfb) apt-get update && apt-get install -y --no-install-recommends g++ \
        && rm -rf /var/lib/apt/lists/* ;; esac

COPY --from=build /venv /venv
WORKDIR /app
COPY gnome_pipelines gnome_pipelines/

# Fails the build if the family's pipelines don't import with only its extra installed.
RUN python -m gnome_pipelines.manifest check

ENTRYPOINT ["python", "-m", "gnome_pipelines.runner"]
