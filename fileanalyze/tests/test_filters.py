"""Tests for Tab 2 filtering expressions."""

from __future__ import annotations

from pathlib import Path

from fileanalyze.services.filters import apply_filters
from fileanalyze.services.ingest import load_dataframe, parse_metadata, resolve_meta_path


def test_dimension_contains_filter() -> None:
    """
    Purpose:
        Validate text filtering for dimension fields.

    Internal Logic:
        1. Loads fixture dataset and metadata.
        2. Applies dimension contains filter.
        3. Asserts resulting rows match expected region.

    Example invocation:
        pytest fileanalyze/tests/test_filters.py
    """

    data_path = Path("fileanalyze/tests/fixtures/sample_data.csv")
    metadata = parse_metadata(resolve_meta_path(data_path))
    dataframe = load_dataframe(data_path, ",")
    filtered = apply_filters(dataframe, metadata, {"region": "north"}, {})
    assert len(filtered) == 3


def test_measure_between_filter() -> None:
    """
    Purpose:
        Validate numeric expression parsing for measure fields.

    Internal Logic:
        1. Loads fixture dataset and metadata.
        2. Applies `between` expression to numeric measure.
        3. Verifies resulting row count.

    Example invocation:
        pytest fileanalyze/tests/test_filters.py
    """

    data_path = Path("fileanalyze/tests/fixtures/sample_data.csv")
    metadata = parse_metadata(resolve_meta_path(data_path))
    dataframe = load_dataframe(data_path, ",")
    filtered = apply_filters(dataframe, metadata, {}, {"sales": "between 120 and 220"})
    assert len(filtered) == 3

