"""Tests for SERP archetype tagging."""

from serp_adapter.serp_archetype import classify_domain, count_serp_archetypes


def test_classify_directory_domain():
    assert classify_domain("www.yelp.com") == "directory"


def test_classify_publisher_domain():
    assert classify_domain("reddit.com") == "publisher"


def test_classify_ecommerce_domain():
    assert classify_domain("shop.example.com") == "ecommerce"


def test_count_serp_archetypes():
    counts = count_serp_archetypes(
        ["www.yelp.com", "reddit.com", "localplumber.com", "amazon.com"]
    )
    assert counts == {
        "directory": 1,
        "local_service": 1,
        "publisher": 1,
        "ecommerce": 1,
    }


def test_substring_false_positives_are_not_misclassified():
    assert classify_domain("forumshopping.com") == "local_service"
    assert classify_domain("restore.com") == "local_service"
