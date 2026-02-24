"""serp_adapter.adapters package."""

from serp_adapter.adapters.base import BaseSerpAdapter
from serp_adapter.adapters.apify import ApifyGoogleSearchAdapter

__all__ = ["BaseSerpAdapter", "ApifyGoogleSearchAdapter"]
