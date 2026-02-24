"""serp_adapter – SERP provider adapters and normalized result models."""

from serp_adapter.models import (
    Location,
    NormalizedSerpResult,
    SerpResultItem,
    SerpSource,
)
from serp_adapter.adapters.base import BaseSerpAdapter
from serp_adapter.adapters.apify import ApifyGoogleSearchAdapter

__all__ = [
    "Location",
    "NormalizedSerpResult",
    "SerpResultItem",
    "SerpSource",
    "BaseSerpAdapter",
    "ApifyGoogleSearchAdapter",
]
