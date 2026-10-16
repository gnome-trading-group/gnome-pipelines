import pytest

from gnome_pipelines import credentials


def test_environment_wins_without_touching_secrets_manager(monkeypatch):
    monkeypatch.setenv("SOME_TOKEN", "from-env")
    monkeypatch.setattr(credentials, "_stored", lambda secret_id: pytest.fail("read Secrets Manager"))

    assert credentials.secret("zenrows", "SOME_TOKEN") == "from-env"


def test_falls_back_to_the_named_secret(monkeypatch):
    monkeypatch.delenv("SOME_TOKEN", raising=False)
    monkeypatch.setattr(credentials, "_stored", lambda secret_id: f"value-of-{secret_id}")

    assert credentials.secret("cfbd", "SOME_TOKEN") == "value-of-gnome/pipelines/cfbd"


def test_placeholder_secret_is_rejected(monkeypatch):
    monkeypatch.delenv("SOME_TOKEN", raising=False)
    monkeypatch.setattr(credentials, "_stored", lambda secret_id: credentials.PLACEHOLDER)

    with pytest.raises(RuntimeError, match="gnome/pipelines/zenrows has no value"):
        credentials.secret("zenrows", "SOME_TOKEN")
