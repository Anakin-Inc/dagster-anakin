"""Unit tests for `AnakinResource` methods.

SDK-backed methods get a mocked `anakin.Anakin` client that returns real
`anakin.models` objects, so the tests cover argument pass-through and the
JSON-serializable shape each method returns. `browser_task` gets a fake
`httpx.Client`. `test_assets.py` runs the resource inside real Dagster runs.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from anakin.models import (
    AgenticSearchData,
    AgenticSearchResult,
    AIVisibilitySearch,
    AIVisibilitySource,
    BrowserSession,
    CrawlResult,
    Document,
    MapResult,
    Monitor,
    SearchResult,
    SearchResultItem,
    WireActionMatch,
    WireBuildRequest,
    WireCatalog,
    WireLoginResult,
    WireResult,
)

from dagster_anakin import AnakinResource


@pytest.fixture
def anakin() -> AnakinResource:
    return AnakinResource(api_key="ask_" + "0" * 64)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock()
    monkeypatch.setattr("dagster_anakin.resource.Anakin", lambda **_kwargs: mock)
    return mock


def _assert_json(value: Any) -> None:
    json.dumps(value)  # raises if a datetime or model slipped through


# ─── Scrape and search ──────────────────────────────────────────────────────


def test_scrape_url(client: MagicMock, anakin: AnakinResource) -> None:
    client.scrape.return_value = Document.model_validate(
        {
            "id": "job_1",
            "url": "https://example.com",
            "status": "completed",
            "markdown": "# Hello",
            "createdAt": "2026-10-16T10:00:00Z",
        }
    )

    result = anakin.scrape_url("https://example.com", country="gb", use_browser=True)

    assert result["markdown"] == "# Hello"
    assert result["created_at"] == "2026-10-16T10:00:00Z"
    _assert_json(result)
    client.scrape.assert_called_once_with(
        "https://example.com",
        formats=("markdown",),
        country="gb",
        use_browser=True,
        generate_json=False,
        output_schema=None,
        force_fresh=False,
        session_id=None,
        session_name=None,
    )


def test_map(client: MagicMock, anakin: AnakinResource) -> None:
    client.map.return_value = MapResult(id="m1", links=["https://a.com/x"], total_links=1)

    result = anakin.map("https://a.com", depth=1, search="x")

    assert result["links"] == ["https://a.com/x"]
    assert client.map.call_args.kwargs["depth"] == 1
    assert client.map.call_args.kwargs["search"] == "x"


def test_crawl(client: MagicMock, anakin: AnakinResource) -> None:
    client.crawl.return_value = CrawlResult.model_validate(
        {
            "id": "c1",
            "totalPages": 1,
            "completedPages": 1,
            "results": [{"url": "u", "status": "completed", "markdown": "m"}],
        }
    )

    result = anakin.crawl("https://a.com", include_patterns=["/blog/*"])

    assert result["pages"][0]["markdown"] == "m"
    assert client.crawl.call_args.kwargs["include_patterns"] == ["/blog/*"]
    assert client.crawl.call_args.kwargs["exclude_patterns"] == ()


def test_search(client: MagicMock, anakin: AnakinResource) -> None:
    client.search.return_value = SearchResult(
        id="s1", results=[SearchResultItem(url="https://example.com", title="Example")]
    )

    result = anakin.search("what is anakin", limit=3)

    assert result["results"][0]["url"] == "https://example.com"
    client.search.assert_called_once_with("what is anakin", limit=3)


def test_agentic_search(client: MagicMock, anakin: AnakinResource) -> None:
    client.agentic_search.return_value = AgenticSearchResult(
        id="a1", status="completed", generated_json=AgenticSearchData(summary="done")
    )

    result = anakin.agentic_search("research x", schema={"type": "object"})

    assert result["generated_json"]["summary"] == "done"
    client.agentic_search.assert_called_once_with(
        "research x", use_browser=True, schema={"type": "object"}
    )


# ─── Wire ───────────────────────────────────────────────────────────────────


def test_wire_discover(client: MagicMock, anakin: AnakinResource) -> None:
    client.wire.discover.return_value = [WireActionMatch(action_id="amazon.search", mode="read")]

    result = anakin.wire_discover("search amazon", limit=3)

    assert result[0]["action_id"] == "amazon.search"
    client.wire.discover.assert_called_once_with("search amazon", catalog=None, limit=3)


def test_wire_catalog_lists_all_without_slug(client: MagicMock, anakin: AnakinResource) -> None:
    client.wire.catalogs.return_value = [WireCatalog(id="c1", slug="amazon", name="Amazon")]

    result = anakin.wire_catalog()

    assert isinstance(result, list)
    assert result[0]["slug"] == "amazon"
    client.wire.catalog.assert_not_called()


def test_wire_catalog_with_slug(client: MagicMock, anakin: AnakinResource) -> None:
    detail = MagicMock()
    detail.model_dump.return_value = {"slug": "amazon", "actions": []}
    client.wire.catalog.return_value = detail

    result = anakin.wire_catalog(slug="amazon")

    assert result == {"slug": "amazon", "actions": []}
    client.wire.catalog.assert_called_once_with("amazon")


def test_wire_read_action_passes_credential(client: MagicMock, anakin: AnakinResource) -> None:
    client.wire.run.return_value = WireResult(job_id="j1", status="completed", data={"price": 9})

    result = anakin.wire_read_action(
        "amazon.get_product", params={"asin": "B0"}, credential_id="cred_1"
    )

    assert result["data"] == {"price": 9}
    client.wire.run.assert_called_once_with(
        "amazon.get_product", {"asin": "B0"}, credential_id="cred_1", identity_id=None
    )


def test_wire_read_action_defaults_params(client: MagicMock, anakin: AnakinResource) -> None:
    client.wire.run.return_value = WireResult(status="completed")

    anakin.wire_read_action("x.list")

    assert client.wire.run.call_args.args == ("x.list", {})


def test_wire_write_action_runs_benign_action(client: MagicMock, anakin: AnakinResource) -> None:
    client.wire.run.return_value = WireResult(status="completed")

    anakin.wire_write_action("shop.add_to_cart", params={"sku": "1"})

    client.wire.run.assert_called_once()


def test_wire_write_action_refuses_payment(client: MagicMock, anakin: AnakinResource) -> None:
    with pytest.raises(ValueError, match="payments or fund transfers"):
        anakin.wire_write_action("shop.checkout")
    client.wire.run.assert_not_called()


def test_wire_identities(client: MagicMock, anakin: AnakinResource) -> None:
    client.wire.identities.return_value = []

    assert anakin.wire_identities(catalog_id="cat_1") == []
    client.wire.identities.assert_called_once_with(catalog_id="cat_1")


def test_wire_login(client: MagicMock, anakin: AnakinResource) -> None:
    client.wire.login.return_value = WireLoginResult(
        status="ok", identity_id="i1", credential_id="cred_1"
    )

    result = anakin.wire_login("neb", params={"email": "a@b.c", "password": "x"})

    assert result["credential_id"] == "cred_1"
    client.wire.login.assert_called_once_with(
        "neb",
        {"email": "a@b.c", "password": "x"},
        identity_name=None,
        source_id=None,
        source_ref=None,
    )


def test_wire_build(client: MagicMock, anakin: AnakinResource) -> None:
    client.wire.build.return_value = WireBuildRequest(id="b1", status="pending")

    result = anakin.wire_build("https://example.com", "list products")

    assert result["status"] == "pending"
    client.wire.build.assert_called_once_with(
        "https://example.com", "list products", catalog_id=None, visibility="private", force=False
    )


def test_wire_build_refuses_payment(client: MagicMock, anakin: AnakinResource) -> None:
    with pytest.raises(ValueError):
        anakin.wire_build("https://shop.com", "pay now for the order")
    client.wire.build.assert_not_called()


def test_wire_build_status(client: MagicMock, anakin: AnakinResource) -> None:
    client.wire.get_build.return_value = WireBuildRequest(id="b1", status="success")

    assert anakin.wire_build_status("b1")["status"] == "success"


# ─── Monitors ───────────────────────────────────────────────────────────────


def _monitor(**extra: Any) -> Monitor:
    return Monitor.model_validate({"id": "mon_1", "url": "https://a.com", **extra})


def test_monitor_create_maps_options_and_redacts_secret(
    client: MagicMock, anakin: AnakinResource
) -> None:
    client.monitors.create.return_value = _monitor(alertWebhookSecret="whsec_live")

    result = anakin.monitor_create(
        "https://a.com", 60, ai_mode=True, alert_webhook_url="https://hook"
    )

    assert result["id"] == "mon_1"
    assert result["alert_webhook_secret"].startswith("[redacted")
    assert "whsec_live" not in json.dumps(result)
    args, kwargs = client.monitors.create.call_args
    assert args == ("https://a.com", 60)
    assert kwargs["ai_mode"] is True
    assert kwargs["alert_webhook_url"] == "https://hook"
    assert "output_schema" not in kwargs


def test_monitor_list_one_and_all(client: MagicMock, anakin: AnakinResource) -> None:
    client.monitors.get.return_value = _monitor(alertWebhookSecret="s")
    client.monitors.list.return_value = [_monitor(alertWebhookSecret="s")]

    one = anakin.monitor_list(monitor_id="mon_1")
    many = anakin.monitor_list()

    assert isinstance(one, dict) and one["alert_webhook_secret"].startswith("[redacted")
    assert isinstance(many, list) and many[0]["alert_webhook_secret"].startswith("[redacted")


def test_monitor_changes(client: MagicMock, anakin: AnakinResource) -> None:
    client.monitors.changes.return_value = []

    assert anakin.monitor_changes("mon_1") == []
    client.monitors.changes.assert_called_once_with("mon_1")


@pytest.mark.parametrize(
    ("action", "method"),
    [("pause", "pause"), ("resume", "resume"), ("run_now", "run_now")],
)
def test_monitor_control(
    client: MagicMock, anakin: AnakinResource, action: str, method: str
) -> None:
    getattr(client.monitors, method).return_value = _monitor()

    result = anakin.monitor_control("mon_1", action)

    assert result is not None and result["id"] == "mon_1"
    getattr(client.monitors, method).assert_called_once_with("mon_1")


def test_monitor_control_delete(client: MagicMock, anakin: AnakinResource) -> None:
    assert anakin.monitor_control("mon_1", "delete") is None
    client.monitors.delete.assert_called_once_with("mon_1")


def test_monitor_control_rejects_unknown_action(anakin: AnakinResource) -> None:
    with pytest.raises(ValueError, match="Unknown monitor action"):
        anakin.monitor_control("mon_1", "explode")


# ─── AI visibility ──────────────────────────────────────────────────────────


def test_ai_visibility_sources(client: MagicMock, anakin: AnakinResource) -> None:
    client.ai_visibility.sources.return_value = [
        AIVisibilitySource.model_validate({"slug": "chatgpt", "label": "ChatGPT"})
    ]

    assert anakin.ai_visibility_sources()[0]["slug"] == "chatgpt"


def _visibility_result() -> AIVisibilitySearch:
    return AIVisibilitySearch.model_validate(
        {
            "search_id": "v1",
            "status": "completed",
            "results": [{"source": "chatgpt", "status": "completed", "full_content": "long"}],
        }
    )


def test_ai_visibility_search_strips_full_content(
    client: MagicMock, anakin: AnakinResource
) -> None:
    client.ai_visibility.search.return_value = _visibility_result()

    result = anakin.ai_visibility_search("best scraping api", sources=["chatgpt"])

    assert "full_content" not in result["results"][0]
    client.ai_visibility.search.assert_called_once_with(
        "best scraping api", sources=["chatgpt"], country="us"
    )


def test_ai_visibility_search_keeps_full_content(client: MagicMock, anakin: AnakinResource) -> None:
    client.ai_visibility.search.return_value = _visibility_result()

    result = anakin.ai_visibility_search("q", include_full_content=True)

    assert result["results"][0]["full_content"] == "long"


# ─── Sessions and browser tasks ─────────────────────────────────────────────


def test_session_list(client: MagicMock, anakin: AnakinResource) -> None:
    client.sessions.list.return_value = [BrowserSession.model_validate({"id": "s1", "name": "amz"})]

    result = anakin.session_list(domain="amazon.com")

    assert result[0]["id"] == "s1"
    client.sessions.list.assert_called_once_with(domain="amazon.com")


def test_session_delete(client: MagicMock, anakin: AnakinResource) -> None:
    anakin.session_delete("s1")
    client.sessions.delete.assert_called_once_with("s1")


def _fake_http(*bodies: Any) -> MagicMock:
    http = MagicMock()
    http.__enter__.return_value = http
    http.__exit__.return_value = False
    responses = []
    for body in bodies:
        response = MagicMock(is_error=False, content=b"x")
        response.json.return_value = body
        responses.append(response)
    http.request.side_effect = responses
    return http


def test_browser_task_polls_until_complete(
    monkeypatch: pytest.MonkeyPatch, anakin: AnakinResource
) -> None:
    http = _fake_http(
        {"workflow_id": "wf_1"},
        {"status": "running"},
        {"status": "completed", "result": {"success": True, "result": {"x": 1}, "run_id": "r1"}},
    )
    monkeypatch.setattr(AnakinResource, "get_http_client", lambda _self: http)
    monkeypatch.setattr("dagster_anakin._http.time.sleep", lambda _s: None)

    result = anakin.browser_task("find the cheapest TV", url="https://shop.com", max_steps=5)

    assert result["success"] is True
    assert result["run_id"] == "r1"
    method, path = http.request.call_args_list[0].args
    assert (method, path) == ("POST", "/ai/evaluate")
    assert http.request.call_args_list[0].kwargs["json"] == {
        "prompt": "find the cheapest TV",
        "async": True,
        "url": "https://shop.com",
        "max_steps": 5,
    }
    assert http.request.call_args_list[1].args == ("GET", "/ai/jobs/wf_1")


def test_browser_task_raises_on_failure(
    monkeypatch: pytest.MonkeyPatch, anakin: AnakinResource
) -> None:
    http = _fake_http({"workflow_id": "wf_1"}, {"status": "failed", "error": "site unreachable"})
    monkeypatch.setattr(AnakinResource, "get_http_client", lambda _self: http)

    with pytest.raises(RuntimeError, match="site unreachable"):
        anakin.browser_task("do something")


def test_browser_task_refuses_payment(anakin: AnakinResource) -> None:
    with pytest.raises(ValueError):
        anakin.browser_task("checkout and buy now")
