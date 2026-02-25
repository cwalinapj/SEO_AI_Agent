"""Deterministic keyword intent inference from keyword + SERP signals."""

from __future__ import annotations

from typing import Iterable, Mapping

from serp_adapter.models import KeywordIntent, KeywordUniverseRow
from serp_adapter.serp_archetype import count_serp_archetypes

DIY_MODIFIERS = (
    "how to",
    "diy",
    "fix",
    "repair yourself",
    "what is",
    "cost to do myself",
)
HIRE_MODIFIERS = (
    "near me",
    "service",
    "company",
    "contractor",
    "quote",
    "estimate",
    "installation",
    "licensed",
    "24/7",
)
LOCAL_MODIFIERS = ("near me", "emergency", "24/7", "open now")
COMPARISON_MODIFIERS = ("best", "top", "reviews", "vs", "compare")

HIRE_WEIGHTS = {"cpc": 0.45, "modifier": 0.3, "local_service": 0.15, "directory": 0.1}
DIY_WEIGHTS = {"inverse_cpc": 0.45, "modifier": 0.35, "publisher": 0.2}
LOCAL_WEIGHTS = {"modifier": 0.55, "local_service": 0.25, "directory": 0.2}
COMPARISON_WEIGHTS = {"modifier": 0.65, "serp_review_like": 0.35}
BRAND_WEIGHTS = {"modifier": 0.8, "directory": 0.2}


def _contains_any(text: str, terms: Iterable[str]) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in terms)


def _normalize_cpc(cpc: float | None, max_cpc: float = 50.0) -> float:
    if cpc is None or cpc <= 0:
        return 0.0
    return min(cpc / max_cpc, 1.0)


def _ratio(counts: Mapping[str, int], key: str) -> float:
    total = sum(counts.values())
    if total <= 0:
        return 0.0
    return counts.get(key, 0) / total


def infer_intent(
    row: KeywordUniverseRow,
    archetype_counts: Mapping[str, int] | None = None,
    brand_terms: Iterable[str] = (),
) -> KeywordIntent:
    """Infer keyword intent bucket with explainable deterministic scoring."""
    archetypes = archetype_counts or count_serp_archetypes(row.serp_top_domains)
    kw = row.kw.lower()

    has_diy = _contains_any(kw, DIY_MODIFIERS)
    has_hire = _contains_any(kw, HIRE_MODIFIERS)
    has_local = _contains_any(kw, LOCAL_MODIFIERS)
    has_comparison = _contains_any(kw, COMPARISON_MODIFIERS)
    has_brand = _contains_any(kw, brand_terms)

    cpc_norm = _normalize_cpc(row.cpc)
    directory_ratio = _ratio(archetypes, "directory")
    local_service_ratio = _ratio(archetypes, "local_service")
    publisher_ratio = _ratio(archetypes, "publisher")

    scores = {
        "commercial_hire": HIRE_WEIGHTS["cpc"] * cpc_norm
        + HIRE_WEIGHTS["modifier"] * float(has_hire)
        + HIRE_WEIGHTS["local_service"] * local_service_ratio
        + HIRE_WEIGHTS["directory"] * directory_ratio,
        "DIY_research": DIY_WEIGHTS["inverse_cpc"] * (1.0 - cpc_norm)
        + DIY_WEIGHTS["modifier"] * float(has_diy)
        + DIY_WEIGHTS["publisher"] * publisher_ratio,
        "local_immediate": LOCAL_WEIGHTS["modifier"] * float(has_local)
        + LOCAL_WEIGHTS["local_service"] * local_service_ratio
        + LOCAL_WEIGHTS["directory"] * directory_ratio,
        "comparison": COMPARISON_WEIGHTS["modifier"] * float(has_comparison)
        + COMPARISON_WEIGHTS["serp_review_like"] * (directory_ratio + publisher_ratio),
        "brand_navigational": BRAND_WEIGHTS["modifier"] * float(has_brand)
        + BRAND_WEIGHTS["directory"] * directory_ratio,
    }

    ranking = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    intent_bucket, top_score = ranking[0]
    second_score = ranking[1][1] if len(ranking) > 1 else 0.0
    confidence = max(0.0, min(1.0, top_score - second_score + 0.5))

    explanation = (
        f"cpc={row.cpc or 0}, modifiers="
        f"{{hire:{has_hire}, diy:{has_diy}, local:{has_local}, comparison:{has_comparison}, brand:{has_brand}}}, "
        f"serp={dict(archetypes)}, top={intent_bucket}"
    )

    return KeywordIntent(
        intent_bucket=intent_bucket,
        confidence=confidence,
        scores=scores,
        explanation=explanation,
    )
