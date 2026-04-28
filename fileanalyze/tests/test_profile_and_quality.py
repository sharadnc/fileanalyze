"""Tests for profiling and quality services."""

from __future__ import annotations

from pathlib import Path

from fileanalyze.services.ingest import load_dataframe, parse_metadata, resolve_meta_path
from fileanalyze.services.profile import generate_quick_stats
from fileanalyze.services.quality import build_quality_summary


def test_generate_quick_stats_and_key_profile() -> None:
    """
    Purpose:
        Validate quick stats generation for numeric, char, date, and key fields.

    Internal Logic:
        1. Loads fixture data and metadata.
        2. Runs quick profiling with small worker count.
        3. Asserts stat presence and duplicate key detection.

    Example invocation:
        pytest fileanalyze/tests/test_profile_and_quality.py
    """

    data_path = Path("fileanalyze/tests/fixtures/sample_data.csv")
    metadata = parse_metadata(resolve_meta_path(data_path))
    dataframe = load_dataframe(data_path, ",")
    stats = generate_quick_stats(dataframe, metadata, top_n=5, worker_count=2)
    assert "sales" in stats
    assert stats["sales"].stat_type == "Num"
    assert "__FILEKEY__" in stats
    assert stats["__FILEKEY__"].metrics["duplicate_key_count"] >= 1


def test_quality_summary_outputs_findings() -> None:
    """
    Purpose:
        Verify quality summary includes scorecard and finding entries.

    Internal Logic:
        1. Profiles fixture dataset.
        2. Builds quality summary with an injected schema warning.
        3. Checks required summary keys and finding content.

    Example invocation:
        pytest fileanalyze/tests/test_profile_and_quality.py
    """

    data_path = Path("fileanalyze/tests/fixtures/sample_data.csv")
    metadata = parse_metadata(resolve_meta_path(data_path))
    dataframe = load_dataframe(data_path, ",")
    stats = generate_quick_stats(dataframe, metadata, top_n=5, worker_count=2)
    summary = build_quality_summary(metadata, stats, ["Metadata field missing in data: fake_col"])
    assert "scorecard" in summary
    assert "top_findings" in summary
    assert len(summary["top_findings"]) >= 1

