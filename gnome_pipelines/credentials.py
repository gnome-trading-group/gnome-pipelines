"""Secrets for pipelines: an environment variable locally, else gnome/pipelines/<name> in Secrets Manager.

DataStack creates each secret with a placeholder value; the real one is set by hand with
`aws secretsmanager put-secret-value`, so it never appears in code or a CloudFormation template.
"""
from __future__ import annotations

import os

import boto3

PLACEHOLDER = "unset"
# The secrets live beside the research tables, whatever region a run is in.
REGION = "us-east-1"


def secret(name: str, env_var: str) -> str:
    value = os.environ.get(env_var)
    if value:
        return value
    secret_id = f"gnome/pipelines/{name}"
    value = _stored(secret_id)
    if value == PLACEHOLDER:
        raise RuntimeError(
            f"{secret_id} has no value in this account; set it with "
            f"`aws secretsmanager put-secret-value --secret-id {secret_id} --secret-string ...` or set {env_var}"
        )
    return value


def _stored(secret_id: str) -> str:
    return boto3.client("secretsmanager", region_name=REGION).get_secret_value(SecretId=secret_id)["SecretString"]
