"""Tests for serp_adapter.models."""

import pytest
from serp_adapter.models import (
    Location,
    NormalizedSerpResult,
    SerpResultItem,
    SerpSource,
)


class TestLocation:
    def test_required_field(self):
        loc = Location(country="US")
        assert loc.country == "US"
        assert loc.region is None
        assert loc.city is None

    def test_all_fields(self):
        loc = Location(country="US", region="CA", city="San Jose")
        assert loc.country == "US"
        assert loc.region == "CA"
        assert loc.city == "San Jose"


class TestSerpResultItem:
    def test_fields(self):
        item = SerpResultItem(
            rank=1,
            title="Best Plumbers",
            url="https://example.com/plumber",
            domain="example.com",
            snippet="Top-rated local plumbers.",
        )
        assert item.rank == 1
        assert item.title == "Best Plumbers"
        assert item.url == "https://example.com/plumber"
        assert item.domain == "example.com"
        assert item.snippet == "Top-rated local plumbers."


class TestSerpSource:
    def test_defaults(self):
        source = SerpSource(provider="apify")
        assert source.provider == "apify"
        assert source.actor is None
        assert source.run_id is None

    def test_all_fields(self):
        source = SerpSource(
            provider="apify",
            actor="apify/google-search-scraper",
            run_id="run-xyz",
        )
        assert source.actor == "apify/google-search-scraper"
        assert source.run_id == "run-xyz"


class TestNormalizedSerpResult:
    def _make_result(self, **kwargs):
        defaults = dict(
            query="plumber san jose",
            location=Location(country="US", region="CA", city="San Jose"),
            device="mobile",
            engine="google",
            ts=1761330000,
        )
        defaults.update(kwargs)
        return NormalizedSerpResult(**defaults)

    def test_minimal_construction(self):
        result = self._make_result()
        assert result.query == "plumber san jose"
        assert result.device == "mobile"
        assert result.engine == "google"
        assert result.ts == 1761330000
        assert result.results == []
        assert result.source is None

    def test_with_results(self):
        items = [
            SerpResultItem(
                rank=1,
                title="Plumber A",
                url="https://a.com",
                domain="a.com",
                snippet="...",
            ),
            SerpResultItem(
                rank=2,
                title="Plumber B",
                url="https://b.com",
                domain="b.com",
                snippet="...",
            ),
        ]
        result = self._make_result(results=items)
        assert len(result.results) == 2
        assert result.results[0].rank == 1
        assert result.results[1].rank == 2

    def test_with_source(self):
        source = SerpSource(
            provider="apify",
            actor="apify/google-search-scraper",
            run_id="abc123",
        )
        result = self._make_result(source=source)
        assert result.source.provider == "apify"
        assert result.source.run_id == "abc123"

    def test_location_embedded(self):
        result = self._make_result()
        assert result.location.country == "US"
        assert result.location.region == "CA"
        assert result.location.city == "San Jose"
