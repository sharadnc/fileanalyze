"""Tests for ingestion and metadata parsing."""

from __future__ import annotations

from pathlib import Path

from fileanalyze.services.ingest import load_dataframe, parse_metadata, resolve_meta_path


def test_parse_metadata_key_value() -> None:
    """
    Purpose:
        Verify key-value metadata format parsing works as expected.

    Internal Logic:
        1. Loads fixture metadata file.
        2. Parses bundle and checks field/key values.
        3. Asserts expected number of fields and primary keys.

    Example invocation:
        pytest fileanalyze/tests/test_ingest.py
    """

    data_path = Path("fileanalyze/tests/fixtures/sample_data.csv")
    metadata = parse_metadata(resolve_meta_path(data_path))
    assert metadata.file_keys == ["order_id", "order_date"]
    assert len(metadata.fields) == 4
    assert metadata.fields[2].field_name == "sales"
    assert metadata.fields[2].field_dtype == "M"


def test_load_dataframe_with_delimiter() -> None:
    """
    Purpose:
        Verify CSV loading honors delimiter and row count.

    Internal Logic:
        1. Reads sample CSV with comma delimiter.
        2. Validates expected columns and row count.
        3. Ensures fixture is ingested without parser errors.

    Example invocation:
        pytest fileanalyze/tests/test_ingest.py
    """

    dataframe = load_dataframe(Path("fileanalyze/tests/fixtures/sample_data.csv"), ",")
    assert list(dataframe.columns) == ["order_id", "region", "sales", "order_date"]
    assert len(dataframe) == 4


def test_parse_compact_single_line_metadata() -> None:
    """
    Purpose:
        Ensure compact one-line metadata format is parsed correctly.

    Internal Logic:
        1. Loads real-world compact metadata sample fixture.
        2. Parses metadata and validates file keys and field extraction.
        3. Confirms `CharNN` type token normalization behavior.

    Example invocation:
        pytest fileanalyze/tests/test_ingest.py
    """

    meta_path = Path("fileanalyze/sample/NST-EST2025-ALLDATA.csv_Meta")
    metadata = parse_metadata(meta_path)
    assert metadata.file_keys[:2] == ["SUMLEV", "REGION"]
    assert "DIVISION" in metadata.file_keys
    assert len(metadata.fields) >= 2
    assert metadata.fields[0].field_name == "SUMLEV"
    assert metadata.fields[0].field_type == "Char"
    assert metadata.fields[0].field_length == "3"
    assert metadata.fields[0].field_desc == "Level"
    assert metadata.fields[0].field_dtype == "D"

