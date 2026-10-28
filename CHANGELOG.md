# Changelog

## 0.1.0 (unreleased)

First release.

- `AnakinResource`, a `ConfigurableResource` on `anakin-sdk` 0.2 with one method per
  Anakin capability (22 in all): scrape, map, crawl, search, agentic search, Wire
  discovery, catalog, read/write actions, identities, login and builds, website
  monitors, AI visibility, browser sessions and AI browser tasks.
- `build_anakin_scrape_asset`, an asset factory that scrapes one page per asset and
  records the URL, job id, cache hit and a markdown preview as materialization metadata.
- Keyless mode: without an API key, `scrape_url`, `wire_discover` and `wire_catalog`
  work on a small free per-IP allowance.
- Write methods (`wire_write_action`, `wire_build`, `browser_task`) refuse payment and
  fund-transfer requests. Monitor results redact the webhook signing secret.
- Supports Dagster 1.8 and later on Python 3.10 to 3.13.
