"""Tests for `AnakinResource` configuration and client construction."""

from __future__ import annotations

import re
from pathlib import Path

import dagster as dg
import pytest
from anakin import Anakin

import dagster_anakin
from dagster_anakin import AnakinResource

KEY = "ask_" + "0" * 64


def test_get_client_returns_sdk_client_with_key() -> None:
    client = AnakinResource(api_key=KEY).get_client()

    assert isinstance(client, Anakin)
    assert client.api_key_configured


def test_get_client_without_key_is_keyless(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANAKIN_API_KEY", raising=False)

    client = AnakinResource().get_client()

    assert not client.api_key_configured


def test_get_client_falls_back_to_env_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANAKIN_API_KEY", KEY)

    assert AnakinResource().get_client().api_key_configured


def test_http_client_defaults() -> None:
    with AnakinResource(api_key=KEY).get_http_client() as http:
        assert str(http.base_url).rstrip("/") == "https://api.anakin.io/v1"
        assert http.headers["x-api-key"] == KEY


def test_http_client_base_url_override() -> None:
    resource = AnakinResource(api_key=KEY, base_url="https://staging.example/v1")

    with resource.get_http_client() as http:
        assert str(http.base_url).rstrip("/") == "https://staging.example/v1"


def test_http_client_falls_back_to_env_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANAKIN_API_KEY", KEY)

    with AnakinResource().get_http_client() as http:
        assert http.headers["x-api-key"] == KEY


def test_http_client_requires_a_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANAKIN_API_KEY", raising=False)

    with pytest.raises(ValueError, match="needs an Anakin API key"):
        AnakinResource().get_http_client()


def test_env_var_api_key_resolves_at_run_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_ANAKIN_KEY", KEY)
    seen: dict[str, bool] = {}

    @dg.asset
    def probe(anakin: AnakinResource) -> None:
        seen["configured"] = anakin.get_client().api_key_configured
        seen["key"] = anakin.api_key == KEY

    result = dg.materialize(
        [probe], resources={"anakin": AnakinResource(api_key=dg.EnvVar("MY_ANAKIN_KEY"))}
    )

    assert result.success
    assert seen == {"configured": True, "key": True}


def test_version_matches_pyproject() -> None:
    # tomllib is 3.11+, and the package supports 3.10.
    pyproject = (Path(__file__).parents[1] / "pyproject.toml").read_text()
    match = re.search(r'^version = "([^"]+)"', pyproject, re.MULTILINE)

    assert match and match.group(1) == dagster_anakin.__version__
