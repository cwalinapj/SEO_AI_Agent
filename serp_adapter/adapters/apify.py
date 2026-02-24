"""Adapter for the Apify *Google Search Scraper* actor.

Actor page: https://apify.com/apify/google-search-scraper

The actor returns a list of result objects.  Each object contains metadata
about the search (query, country, …) and an ``organicResults`` array.

Example raw item (abbreviated)::

    {
        "searchQuery": {
            "term": "plumber san jose",
            "countryCode": "US",
            "languageCode": "en"
        },
        "device": "MOBILE",
        "crawledAt": "2025-04-24T09:00:00.000Z",
        "#runId": "abc123",
        "organicResults": [
            {
                "position": 1,
                "title": "Best Plumbers in San Jose",
                "url": "https://example.com/plumber-san-jose",
                "domain": "example.com",
                "description": "Top-rated local plumbers …"
            }
        ]
    }
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from serp_adapter.adapters.base import BaseSerpAdapter
from serp_adapter.models import (
    Location,
    NormalizedSerpResult,
    SerpResultItem,
    SerpSource,
)

# Apify device strings → canonical device strings
_DEVICE_MAP: Dict[str, str] = {
    "DESKTOP": "desktop",
    "MOBILE": "mobile",
    "TABLET": "tablet",
}

_PROVIDER = "apify"
_ACTOR = "apify/google-search-scraper"


def _parse_ts(crawled_at: Optional[str]) -> int:
    """Return a Unix timestamp from an ISO-8601 string, falling back to now."""
    if crawled_at:
        try:
            dt = datetime.fromisoformat(crawled_at.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except ValueError:
            pass
    return int(time.time())


def _extract_domain(url: str) -> str:
    """Return the hostname from *url*, stripping any leading ``www.``."""
    try:
        host = urlparse(url).hostname or ""
        return host.removeprefix("www.")
    except Exception:
        return ""


class ApifyGoogleSearchAdapter(BaseSerpAdapter):
    """Normalize a single Apify Google Search Scraper result object.

    A single *raw* value corresponds to one item from the actor's dataset
    (i.e. one search query / SERP page).
    """

    def normalize(self, raw: Any) -> NormalizedSerpResult:
        """Convert an Apify actor dataset item to a :class:`NormalizedSerpResult`.

        Parameters
        ----------
        raw:
            A ``dict`` representing one item from the Apify dataset.

        Returns
        -------
        NormalizedSerpResult
        """
        if not isinstance(raw, dict):
            raise TypeError(
                f"Expected a dict, got {type(raw).__name__}"
            )

        search_query: Dict[str, Any] = raw.get("searchQuery") or {}

        query: str = (
            search_query.get("term")
            or raw.get("query")
            or ""
        )

        location = Location(
            country=search_query.get("countryCode") or raw.get("country") or "",
            region=raw.get("region") or None,
            city=search_query.get("city") or raw.get("city") or None,
        )

        raw_device: str = (raw.get("device") or "DESKTOP").upper()
        device: str = _DEVICE_MAP.get(raw_device, "desktop")

        ts: int = _parse_ts(raw.get("crawledAt") or raw.get("scrapedAt"))

        organic_results = raw.get("organicResults") or []
        results = []
        for item in organic_results:
            url: str = item.get("url") or ""
            domain: str = item.get("domain") or _extract_domain(url)
            results.append(
                SerpResultItem(
                    rank=item.get("position") or len(results) + 1,
                    title=item.get("title") or "",
                    url=url,
                    domain=domain,
                    snippet=item.get("description") or item.get("snippet") or "",
                )
            )

        source = SerpSource(
            provider=_PROVIDER,
            actor=_ACTOR,
            run_id=raw.get("#runId") or raw.get("runId") or None,
        )

        return NormalizedSerpResult(
            query=query,
            location=location,
            device=device,
            engine="google",
            ts=ts,
            results=results,
            source=source,
        )
