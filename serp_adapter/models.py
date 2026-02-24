"""Normalized SERP result models.

Every SERP provider adapter converts its raw response into a
:class:`NormalizedSerpResult`.  All downstream consumers (taste alignment,
inspiration selection, keyword scoring, …) read *only* this format.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Location:
    """Geographic targeting for the SERP query."""

    country: str  # ISO 3166-1 alpha-2, e.g. "US"
    region: Optional[str] = None  # State / province code, e.g. "CA"
    city: Optional[str] = None  # City name, e.g. "San Jose"


@dataclass
class SerpResultItem:
    """A single organic result inside a SERP."""

    rank: int
    title: str
    url: str
    domain: str
    snippet: str


@dataclass
class SerpSource:
    """Provenance metadata – which provider/tool produced the raw data."""

    provider: str  # e.g. "apify"
    actor: Optional[str] = None  # e.g. "apify/google-search-scraper"
    run_id: Optional[str] = None  # Provider-specific run / job identifier


@dataclass
class NormalizedSerpResult:
    """Canonical, provider-agnostic representation of a SERP response.

    All SERP adapters must return an instance of this class.  Downstream
    modules must consume *only* this class.

    Example::

        NormalizedSerpResult(
            query="plumber san jose",
            location=Location(country="US", region="CA", city="San Jose"),
            device="mobile",
            engine="google",
            ts=1761330000,
            results=[
                SerpResultItem(rank=1, title="...", url="...",
                               domain="...", snippet="..."),
            ],
            source=SerpSource(
                provider="apify",
                actor="apify/google-search-scraper",
                run_id="...",
            ),
        )
    """

    query: str
    location: Location
    device: str  # "desktop" | "mobile" | "tablet"
    engine: str  # "google" | "bing" | …
    ts: int  # Unix timestamp (seconds) of when the SERP was fetched
    results: List[SerpResultItem] = field(default_factory=list)
    source: Optional[SerpSource] = None
