"""Tests for SERP adapters."""

import pytest
from serp_adapter.adapters.apify import ApifyGoogleSearchAdapter
from serp_adapter.adapters.base import BaseSerpAdapter
from serp_adapter.models import NormalizedSerpResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

APIFY_RAW_ITEM = {
    "searchQuery": {
        "term": "plumber san jose",
        "countryCode": "US",
        "city": "San Jose",
        "languageCode": "en",
    },
    "device": "MOBILE",
    "crawledAt": "2025-04-24T09:00:00.000Z",
    "#runId": "run-abc123",
    "organicResults": [
        {
            "position": 1,
            "title": "Best Plumbers in San Jose",
            "url": "https://www.example.com/plumber-san-jose",
            "domain": "example.com",
            "description": "Top-rated local plumbers available 24/7.",
        },
        {
            "position": 2,
            "title": "San Jose Emergency Plumbing",
            "url": "https://plumbing.sj.com/emergency",
            "domain": "plumbing.sj.com",
            "description": "Fast, reliable emergency plumbing services.",
        },
    ],
}


# ---------------------------------------------------------------------------
# Base adapter
# ---------------------------------------------------------------------------

class TestBaseSerpAdapter:
    def test_is_abstract(self):
        """BaseSerpAdapter cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseSerpAdapter()  # type: ignore[abstract]

    def test_concrete_subclass_must_implement_normalize(self):
        class Incomplete(BaseSerpAdapter):
            pass  # missing normalize()

        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]

    def test_concrete_subclass_works(self):
        class Stub(BaseSerpAdapter):
            def normalize(self, raw):
                return raw  # trivial

        assert Stub().normalize("x") == "x"


# ---------------------------------------------------------------------------
# Apify adapter – happy path
# ---------------------------------------------------------------------------

class TestApifyGoogleSearchAdapter:
    @pytest.fixture()
    def adapter(self):
        return ApifyGoogleSearchAdapter()

    @pytest.fixture()
    def result(self, adapter):
        return adapter.normalize(APIFY_RAW_ITEM)

    def test_returns_normalized_type(self, result):
        assert isinstance(result, NormalizedSerpResult)

    def test_query(self, result):
        assert result.query == "plumber san jose"

    def test_location(self, result):
        assert result.location.country == "US"
        assert result.location.city == "San Jose"

    def test_device_normalized_to_lowercase(self, result):
        assert result.device == "mobile"

    def test_engine_is_google(self, result):
        assert result.engine == "google"

    def test_timestamp_parsed(self, result):
        # 2025-04-24T09:00:00Z → 1745485200
        assert result.ts == 1745485200

    def test_result_count(self, result):
        assert len(result.results) == 2

    def test_first_result_rank(self, result):
        assert result.results[0].rank == 1

    def test_first_result_title(self, result):
        assert result.results[0].title == "Best Plumbers in San Jose"

    def test_first_result_url(self, result):
        assert result.results[0].url == "https://www.example.com/plumber-san-jose"

    def test_first_result_domain(self, result):
        assert result.results[0].domain == "example.com"

    def test_first_result_snippet(self, result):
        assert result.results[0].snippet == "Top-rated local plumbers available 24/7."

    def test_second_result(self, result):
        r = result.results[1]
        assert r.rank == 2
        assert r.title == "San Jose Emergency Plumbing"
        assert r.domain == "plumbing.sj.com"

    def test_source_provider(self, result):
        assert result.source.provider == "apify"

    def test_source_actor(self, result):
        assert result.source.actor == "apify/google-search-scraper"

    def test_source_run_id(self, result):
        assert result.source.run_id == "run-abc123"

    # -----------------------------------------------------------------------
    # Edge cases
    # -----------------------------------------------------------------------

    def test_raises_on_non_dict(self, adapter):
        with pytest.raises(TypeError):
            adapter.normalize(["not", "a", "dict"])

    def test_desktop_device_default(self, adapter):
        raw = {**APIFY_RAW_ITEM, "device": "DESKTOP"}
        result = adapter.normalize(raw)
        assert result.device == "desktop"

    def test_missing_device_defaults_to_desktop(self, adapter):
        raw = {k: v for k, v in APIFY_RAW_ITEM.items() if k != "device"}
        result = adapter.normalize(raw)
        assert result.device == "desktop"

    def test_domain_inferred_from_url_when_missing(self, adapter):
        raw = {
            **APIFY_RAW_ITEM,
            "organicResults": [
                {
                    "position": 1,
                    "title": "A",
                    "url": "https://www.inferred.com/path",
                    "description": "...",
                    # no "domain" key
                }
            ],
        }
        result = adapter.normalize(raw)
        assert result.results[0].domain == "inferred.com"

    def test_empty_organic_results(self, adapter):
        raw = {**APIFY_RAW_ITEM, "organicResults": []}
        result = adapter.normalize(raw)
        assert result.results == []

    def test_missing_crawled_at_falls_back_to_now(self, adapter, monkeypatch):
        import time

        fixed_ts = 1_000_000
        monkeypatch.setattr(time, "time", lambda: fixed_ts)

        raw = {k: v for k, v in APIFY_RAW_ITEM.items() if k != "crawledAt"}
        result = adapter.normalize(raw)
        assert result.ts == fixed_ts

    def test_run_id_via_alternate_key(self, adapter):
        raw = {
            **{k: v for k, v in APIFY_RAW_ITEM.items() if k != "#runId"},
            "runId": "alt-run-99",
        }
        result = adapter.normalize(raw)
        assert result.source.run_id == "alt-run-99"
