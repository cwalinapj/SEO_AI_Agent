"""SERP domain archetype tagging helpers."""

from __future__ import annotations

from typing import Dict, Iterable

_DIRECTORY_DOMAINS = {
    "yelp.com",
    "angi.com",
    "homeadvisor.com",
    "thumbtack.com",
    "yellowpages.com",
}
_PUBLISHER_DOMAINS = {
    "reddit.com",
    "quora.com",
    "wikipedia.org",
    "wikihow.com",
    "medium.com",
}
_ECOMMERCE_DOMAINS = {
    "amazon.com",
    "ebay.com",
    "walmart.com",
    "homedepot.com",
    "lowes.com",
}

_ARCHETYPES = ("directory", "local_service", "publisher", "ecommerce")


def _normalize_domain(domain: str) -> str:
    normalized = (domain or "").strip().lower()
    return normalized.removeprefix("www.")


def _is_match(domain: str, roots: set[str]) -> bool:
    return any(domain == root or domain.endswith(f".{root}") for root in roots)


def classify_domain(domain: str) -> str:
    """Classify a SERP domain into the expected archetype buckets."""
    normalized = _normalize_domain(domain)
    if not normalized:
        return "publisher"

    if _is_match(normalized, _DIRECTORY_DOMAINS):
        return "directory"
    if _is_match(normalized, _PUBLISHER_DOMAINS) or "forum" in normalized or "blog" in normalized:
        return "publisher"
    if _is_match(normalized, _ECOMMERCE_DOMAINS) or "shop" in normalized or "store" in normalized:
        return "ecommerce"
    return "local_service"


def count_serp_archetypes(domains: Iterable[str]) -> Dict[str, int]:
    """Count archetypes present in top SERP domains."""
    counts = {k: 0 for k in _ARCHETYPES}
    for domain in domains:
        counts[classify_domain(domain)] += 1
    return counts
