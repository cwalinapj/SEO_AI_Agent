"""serp_adapter – SERP provider adapters and normalized result models."""

from serp_adapter.models import (
    KeywordIntent,
    KeywordUniverseRow,
    Location,
    NormalizedSerpResult,
    SerpResultItem,
    SerpSource,
)
from serp_adapter.adapters.base import BaseSerpAdapter
from serp_adapter.adapters.apify import ApifyGoogleSearchAdapter
from serp_adapter.infer_intent import infer_intent
from serp_adapter.serp_archetype import classify_domain, count_serp_archetypes

__all__ = [
    "Location",
    "NormalizedSerpResult",
    "KeywordUniverseRow",
    "KeywordIntent",
    "SerpResultItem",
    "SerpSource",
    "BaseSerpAdapter",
    "ApifyGoogleSearchAdapter",
    "classify_domain",
    "count_serp_archetypes",
    "infer_intent",
]
