"""Asset factory: one Dagster asset per scraped web page."""

# No `from __future__ import annotations` here: Dagster inspects the asset
# function's `context` annotation at definition time and rejects strings.

from typing import Any

from dagster import AssetExecutionContext, AssetsDefinition, MetadataValue, Output, asset

from dagster_anakin.resource import AnakinResource

_PREVIEW_CHARS = 2000


def build_anakin_scrape_asset(
    name: str,
    url: str,
    *,
    resource_key: str = "anakin",
    group_name: str | None = None,
    description: str | None = None,
    use_browser: bool = False,
    country: str = "us",
    generate_json: bool = False,
    output_schema: dict[str, Any] | None = None,
    force_fresh: bool = False,
    session_id: str | None = None,
) -> AssetsDefinition:
    """Build an asset that scrapes `url` with Anakin each time it materializes.

    The asset's value is the scraped document as a dict (`markdown`,
    `generated_json`, `url`, `status`, ...). Each materialization records the
    URL, job id, cache hit and markdown length as metadata, plus a markdown
    preview you can read in the Dagster UI.

    Args:
        name: Asset name (also its key).
        url: The page to scrape.
        resource_key: Key of the `AnakinResource` in your `Definitions`.
        group_name: Dagster asset group.
        description: Asset description. Defaults to "Scrape of <url>".
        use_browser: Render with a headless browser. Use for JavaScript-heavy pages.
        country: Proxy country code.
        generate_json: Extract structured JSON from the page with AI.
        output_schema: JSON Schema for the extracted JSON (implies AI extraction).
        force_fresh: Skip Anakin's cache on every materialization.
        session_id: Saved browser session to scrape as a logged-in user.

    Examples:
        ```python
        import dagster as dg
        from dagster_anakin import AnakinResource, build_anakin_scrape_asset

        hn_front_page = build_anakin_scrape_asset("hn_front_page", "https://news.ycombinator.com")

        defs = dg.Definitions(
            assets=[hn_front_page],
            resources={"anakin": AnakinResource()},  # reads ANAKIN_API_KEY
        )
        ```
    """

    @asset(
        name=name,
        group_name=group_name,
        description=description or f"Scrape of {url}",
        required_resource_keys={resource_key},
        kinds={"anakin"},
    )
    def _scrape_asset(context: AssetExecutionContext) -> Output[dict[str, Any]]:
        anakin: AnakinResource = getattr(context.resources, resource_key)
        document = anakin.scrape_url(
            url,
            country=country,
            use_browser=use_browser,
            generate_json=generate_json,
            output_schema=output_schema,
            force_fresh=force_fresh,
            session_id=session_id,
        )
        markdown = document.get("markdown") or ""
        metadata: dict[str, Any] = {
            "url": MetadataValue.url(url),
            "anakin_job_id": document.get("id") or "",
            "cached": bool(document.get("cached")),
            "markdown_chars": len(markdown),
        }
        if markdown:
            metadata["preview"] = MetadataValue.md(markdown[:_PREVIEW_CHARS])
        return Output(document, metadata=metadata)

    return _scrape_asset
