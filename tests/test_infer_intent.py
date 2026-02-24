"""Tests for deterministic keyword intent inference."""

from serp_adapter.infer_intent import infer_intent
from serp_adapter.models import KeywordUniverseRow


def test_hire_intent_from_modifier_and_high_cpc():
    row = KeywordUniverseRow(
        kw="licensed plumber near me",
        geo_bucket="US-CA-San Jose",
        cpc=30.0,
        serp_top_domains=["yelp.com", "localplumber.com"],
    )
    result = infer_intent(row)
    assert result.intent_bucket in {"commercial_hire", "local_immediate"}
    assert result.scores["commercial_hire"] >= result.scores["DIY_research"]


def test_diy_intent_from_modifier_and_low_cpc():
    row = KeywordUniverseRow(
        kw="how to fix clogged drain diy",
        geo_bucket="US-CA-San Jose",
        cpc=0.5,
        serp_top_domains=["reddit.com", "wikihow.com"],
    )
    result = infer_intent(row)
    assert result.intent_bucket == "DIY_research"
    assert result.scores["DIY_research"] > result.scores["commercial_hire"]


def test_brand_navigational_detected():
    row = KeywordUniverseRow(
        kw="acme plumbing san jose",
        geo_bucket="US-CA-San Jose",
        cpc=4.0,
        serp_top_domains=["acmeplumbing.com"],
    )
    result = infer_intent(row, brand_terms=["acme"])
    assert result.intent_bucket == "brand_navigational"
