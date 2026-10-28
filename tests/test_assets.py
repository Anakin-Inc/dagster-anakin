"""Run `AnakinResource` and `build_anakin_scrape_asset` inside real Dagster runs.

The SDK client is mocked; everything else (resource wiring, IO manager,
metadata) is real Dagster.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import dagster as dg
import pytest
from anakin.models import Document, WireResult

from dagster_anakin import AnakinResource, build_anakin_scrape_asset


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock()
    mock.scrape.return_value = Document.model_validate(
        {
            "id": "job_1",
            "url": "https://example.com",
            "status": "completed",
            "markdown": "# Example Domain",
            "cached": True,
        }
    )
    monkeypatch.setattr("dagster_anakin.resource.Anakin", lambda **_kwargs: mock)
    return mock


def test_scrape_asset_materializes_with_metadata(client: MagicMock) -> None:
    page = build_anakin_scrape_asset(
        "example_page", "https://example.com", group_name="web", use_browser=True
    )

    result = dg.materialize([page], resources={"anakin": AnakinResource()})

    assert result.success
    value: dict[str, Any] = result.output_for_node("example_page")
    assert value["markdown"] == "# Example Domain"
    metadata = result.asset_materializations_for_node("example_page")[0].metadata
    assert metadata["url"] == dg.MetadataValue.url("https://example.com")
    assert metadata["anakin_job_id"].value == "job_1"
    assert metadata["cached"].value is True
    assert metadata["markdown_chars"].value == len("# Example Domain")
    assert metadata["preview"] == dg.MetadataValue.md("# Example Domain")
    assert client.scrape.call_args.kwargs["use_browser"] is True


def test_scrape_asset_custom_resource_key_and_group(client: MagicMock) -> None:
    page = build_anakin_scrape_asset(
        "docs_page", "https://example.com/docs", resource_key="web", group_name="docs"
    )

    assert page.group_names_by_key[dg.AssetKey("docs_page")] == "docs"
    assert "web" in page.required_resource_keys
    result = dg.materialize([page], resources={"web": AnakinResource()})
    assert result.success


def test_resource_in_asset_and_downstream(client: MagicMock) -> None:
    client.wire.run.return_value = WireResult(
        job_id="j1", status="completed", data={"items": [1, 2]}
    )

    @dg.asset
    def products(anakin: AnakinResource) -> dict[str, Any]:
        return anakin.wire_read_action("shop.list_products", params={"q": "tv"})

    @dg.asset
    def product_count(products: dict[str, Any]) -> int:
        return len(products["data"]["items"])

    result = dg.materialize([products, product_count], resources={"anakin": AnakinResource()})

    assert result.success
    assert result.output_for_node("product_count") == 2


def test_definitions_validate() -> None:
    defs = dg.Definitions(
        assets=[build_anakin_scrape_asset("a", "https://example.com")],
        resources={"anakin": AnakinResource(api_key=dg.EnvVar("ANAKIN_API_KEY"))},
    )

    assert defs.get_assets_def("a").key == dg.AssetKey("a")
