"""Example Dagster definitions using dagster-anakin.

Run it with the Dagster UI:

    pip install dagster-anakin dagster-webserver
    export ANAKIN_API_KEY=ask_...   # optional: example_page works without a key
    dagster dev -f examples/definitions.py

Or materialize the keyless asset from the command line:

    dagster asset materialize -f examples/definitions.py --select example_page
"""

from typing import Any

import dagster as dg

from dagster_anakin import AnakinResource, build_anakin_scrape_asset

# One asset per page: re-scraped on every materialization, with a markdown
# preview and the Anakin job id in the asset's metadata.
example_page = build_anakin_scrape_asset("example_page", "https://example.com", group_name="web")


@dg.asset(group_name="web", kinds={"anakin"})
def competitor_pricing(anakin: AnakinResource) -> dict[str, Any]:
    """Structured pricing data extracted from a page with AI (needs an API key)."""
    document = anakin.scrape_url(
        "https://example.com/pricing",
        output_schema={
            "type": "object",
            "properties": {
                "plans": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "monthly_price": {"type": "number"},
                        },
                    },
                }
            },
        },
    )
    return document.get("generated_json") or {}


@dg.asset(group_name="web", kinds={"anakin"})
def blog_pages(anakin: AnakinResource) -> list[dict[str, Any]]:
    """Markdown for up to 20 blog posts, for a RAG index or a weekly digest (needs an API key)."""
    crawl = anakin.crawl("https://example.com/blog", max_pages=20, include_patterns=["/blog/*"])
    return [p for p in crawl["pages"] if p.get("markdown")]


@dg.asset(group_name="web", kinds={"anakin"})
def blog_page_count(blog_pages: list[dict[str, Any]]) -> dg.MaterializeResult:
    return dg.MaterializeResult(metadata={"pages": len(blog_pages)})


defs = dg.Definitions(
    assets=[example_page, competitor_pricing, blog_pages, blog_page_count],
    # No api_key: the SDK reads ANAKIN_API_KEY at run time, and without it runs
    # keyless (scrape and Wire discovery only).
    resources={"anakin": AnakinResource()},
    schedules=[
        dg.ScheduleDefinition(
            name="daily_web_refresh",
            target=dg.AssetSelection.groups("web"),
            cron_schedule="0 6 * * *",
        )
    ],
)
