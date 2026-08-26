#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["pytest"]
# ///

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from atlas_labels import excluded_dids, filter_labeled_rows


def test_atlas_excludes_active_labels_but_keeps_policy_exceptions():
    summary = {
        "counts": {"labeled": 2},
        "authors": [
            {"did": "did:spam", "state": "labeled", "kept": False},
            {"did": "did:delightful", "state": "labeled", "kept": True},
            {"did": "did:pending", "state": "pending", "kept": False},
            {"did": "did:rejected", "state": "rejected", "kept": False},
        ]
    }
    excluded = excluded_dids(summary)
    rows = [
        {"id": "1", "did": "did:spam"},
        {"id": "2", "did": "did:delightful"},
        {"id": "3", "did": "did:pending"},
        {"id": "4", "did": "did:human"},
    ]

    assert excluded == {"did:spam"}
    assert [row["id"] for row in filter_labeled_rows(rows, excluded)] == ["2", "3", "4"]


def test_atlas_refuses_a_truncated_labeler_summary():
    summary = {
        "counts": {"labeled": 2},
        "authors": [{"did": "did:only-one", "state": "labeled", "kept": False}],
    }

    import pytest

    with pytest.raises(ValueError, match="every labeled author"):
        excluded_dids(summary)
