# dagster-anakin

A [Dagster](https://dagster.io) resource and asset factory for [Anakin](https://anakin.io), a web data API. Turn web pages into Dagster assets: scrape pages to markdown or structured JSON, crawl sites, run AI web search, run [Wire](https://anakin.io/docs) actions on supported sites, and manage website monitors from your assets and ops.

```bash
pip install dagster-anakin
```

Requires Dagster 1.8+ and Python 3.10 to 3.13.

## Authentication

Get an API key at [anakin.io/dashboard](https://anakin.io/dashboard) (the free tier needs no card) and set it in the environment where your code location runs:

```bash
export ANAKIN_API_KEY=ask_...
```

`AnakinResource()` with no arguments reads `ANAKIN_API_KEY` at run time. Without a key it runs in keyless mode, where `scrape_url`, `wire_discover` and `wire_catalog` work on a small free per-IP allowance. To use a differently named variable, pass `AnakinResource(api_key=dg.EnvVar("MY_VAR"))`. Don't pass the key as a string literal: Dagster shows resource config in the UI.

## Quickstart

```python
import dagster as dg
from dagster_anakin import AnakinResource, build_anakin_scrape_asset

# One asset per page, re-scraped on every materialization.
pricing_page = build_anakin_scrape_asset("pricing_page", "https://example.com/pricing")


@dg.asset
def pricing_plans(anakin: AnakinResource) -> dict:
    """Structured data extracted from the page with AI."""
    doc = anakin.scrape_url(
        "https://example.com/pricing",
        output_schema={
            "type": "object",
            "properties": {"plans": {"type": "array", "items": {"type": "object"}}},
        },
    )
    return doc["generated_json"]


defs = dg.Definitions(
    assets=[pricing_page, pricing_plans],
    resources={"anakin": AnakinResource()},
)
```

`build_anakin_scrape_asset` records the URL, the Anakin job id, whether the result came from cache, the markdown length and a markdown preview as materialization metadata, so you can read the page in the Dagster UI.

A fuller example with a crawl and a daily schedule is in [`examples/definitions.py`](examples/definitions.py):

```bash
dagster dev -f examples/definitions.py
```

## Methods

Every method returns JSON-serializable dicts and lists, so the results work with any IO manager.

| Area | Methods |
|---|---|
| Scrape and search | `scrape_url`, `map`, `crawl`, `search`, `agentic_search` |
| Wire site actions | `wire_discover`, `wire_catalog`, `wire_read_action`, `wire_write_action`, `wire_identities`, `wire_login`, `wire_build`, `wire_build_status` |
| Website monitoring | `monitor_create`, `monitor_list`, `monitor_changes`, `monitor_control` |
| AI visibility | `ai_visibility_search`, `ai_visibility_sources` |
| Browser | `session_list`, `session_delete`, `browser_task` |

For anything else, `anakin.get_client()` returns the underlying [`anakin-sdk`](https://pypi.org/project/anakin-sdk/) client.

### Wire actions

[Wire](https://anakin.io/docs) runs prebuilt actions on supported sites and returns structured JSON. Find an action and its parameters with `wire_discover` (works without a key), then run it:

```python
anakin.wire_discover("search products on walmart")
# -> [..., {"action_id": "walmart_search", "params": {"required": [{"name": "query", ...}], ...}, "credits": 2}]


@dg.asset
def walmart_tvs(anakin: AnakinResource) -> dict:
    return anakin.wire_read_action("walmart_search", params={"query": "tv"})
```

Read actions only extract data. `wire_write_action` changes state on the site (submits a form, adds to cart), so don't call it from an asset or op with a `RetryPolicy`. Write methods refuse payment and fund-transfer requests; set `ANAKIN_ALLOW_FINANCIAL=true` to turn that check off.

### Website monitors

`monitor_create` registers a monitor that Anakin runs on its own schedule (every 15 minutes or more) and alerts a webhook or email on changes. To pull changes into Dagster instead, poll `monitor_changes` from a scheduled asset. Monitor results redact the webhook signing secret; read it in the Anakin dashboard.

## Pricing

Calls use Anakin credits; see [anakin.io/pricing](https://anakin.io/pricing) for costs and the free tier.

## Development

```bash
uv venv && uv pip install -e . --group test --group lint
uv run --no-sync pytest
uv run --no-sync ruff check . && uv run --no-sync ruff format --check . && uv run --no-sync mypy dagster_anakin
ANAKIN_LIVE_TESTS=1 uv run --no-sync pytest tests/test_live.py   # keyless calls to the live API
```

## License

Apache-2.0
