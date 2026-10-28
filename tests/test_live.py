"""Live calls to api.anakin.io. Skipped unless ANAKIN_LIVE_TESTS=1.

Uses the keyless endpoints (scrape, Wire discovery and catalog), so no API
key is needed.
"""

from __future__ import annotations

import os
from typing import Any

import dagster as dg
import pytest

from dagster_anakin import AnakinResource, build_anakin_scrape_asset

pytestmark = pytest.mark.skipif(
    os.environ.get("ANAKIN_LIVE_TESTS") != "1",
    reason="set ANAKIN_LIVE_TESTS=1 to call the live API",
)


def test_keyless_assets_against_live_api() -> None:
    page = build_anakin_scrape_asset("example_page", "https://example.com")

    @dg.asset
    def amazon_actions(anakin: AnakinResource) -> list[dict[str, Any]]:
        return anakin.wire_discover("search products on amazon", limit=3)

    @dg.asset
    def wire_sites(anakin: AnakinResource) -> int:
        catalogs = anakin.wire_catalog()
        assert isinstance(catalogs, list)
        return len(catalogs)

    result = dg.materialize(
        [page, amazon_actions, wire_sites], resources={"anakin": AnakinResource()}
    )

    assert result.success
    assert "documentation examples" in (result.output_for_node("example_page")["markdown"] or "")
    assert result.output_for_node("amazon_actions")[0]["action_id"]
    assert result.output_for_node("wire_sites") > 0
