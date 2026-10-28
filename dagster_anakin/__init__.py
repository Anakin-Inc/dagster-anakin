"""Dagster integration for Anakin (https://anakin.io), a web data API.

- `AnakinResource`: a `ConfigurableResource` with methods for every Anakin
  capability (scrape, map, crawl, search, agentic search, Wire site actions,
  website monitors, AI visibility, browser sessions and AI browser tasks).
- `build_anakin_scrape_asset`: one asset per scraped page, with a markdown
  preview in the Dagster UI.
"""

from dagster_anakin._version import __version__
from dagster_anakin.assets import build_anakin_scrape_asset
from dagster_anakin.resource import AnakinResource

__all__ = ["AnakinResource", "__version__", "build_anakin_scrape_asset"]
