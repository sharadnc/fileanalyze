"""Parallel quick profiling service."""

from __future__ import annotations

from functools import partial
from typing import Any

import duckdb
import pandas as pd

from fileanalyze.models.schemas import FieldMetadata, MetadataBundle, QuickStats
from fileanalyze.utils.concurrency import run_parallel


def generate_quick_stats(
    dataframe: pd.DataFrame,
    metadata: MetadataBundle,
    top_n: int,
    worker_count: int,
) -> dict[str, QuickStats]:
    """
    Purpose:
        Build quick descriptive statistics for all metadata fields in parallel.

    Internal Logic:
        1. Creates one profiling task per metadata field.
        2. Routes each field to char/num/date profiler by declared type.
        3. Executes tasks concurrently and returns a field-indexed result map.

    Example invocation:
        stats = generate_quick_stats(df, metadata, top_n=25, worker_count=4)
    """

    tasks = [
        partial(_profile_field, dataframe=dataframe, field=field, top_n=top_n)
        for field in metadata.fields
        if field.field_name in dataframe.columns
    ]
    result_items = run_parallel(tasks, max_workers=worker_count)
    result_map = {item.field_name: item for item in result_items}
    key_stats = _profile_keys(dataframe, metadata.file_keys)
    result_map.update(key_stats)
    return result_map


def _profile_field(dataframe: pd.DataFrame, field: FieldMetadata, top_n: int) -> QuickStats:
    """
    Purpose:
        Profile one field using a type-specific strategy.

    Internal Logic:
        1. Reads declared metadata field type.
        2. Delegates to the matching profiler.
        3. Falls back to character profiling for unknown types.

    Example invocation:
        stat = _profile_field(df, field, top_n=20)
    """

    kind = field.field_type.strip().lower()
    if kind == "num":
        return _profile_numeric(dataframe, field.field_name)
    if kind in {"date", "datetime"}:
        return _profile_datetime(dataframe, field.field_name, kind.title())
    return _profile_character(dataframe, field.field_name, top_n)


def _profile_character(dataframe: pd.DataFrame, column: str, top_n: int) -> QuickStats:
    """
    Purpose:
        Generate categorical frequency statistics.

    Internal Logic:
        1. Computes null and distinct counts.
        2. Retrieves top-N frequent values using DuckDB aggregation.
        3. Returns metrics and frequency rows.

    Example invocation:
        stat = _profile_character(df, "region", 25)
    """

    relation = dataframe[[column]].copy()
    relation.columns = ["col"]
    relation["col"] = relation["col"].astype("string")
    conn = duckdb.connect()
    conn.register("tbl", relation)
    top_rows = conn.execute(
        """
        SELECT CAST(col AS VARCHAR) AS value, COUNT(*) AS freq
        FROM tbl
        GROUP BY 1
        ORDER BY freq DESC, value
        LIMIT ?
        """,
        [top_n],
    ).fetchall()
    metrics = {
        "row_count": int(len(relation)),
        "null_count": int(relation["col"].isna().sum()),
        "distinct_count": int(relation["col"].nunique(dropna=True)),
    }
    return QuickStats(
        field_name=column,
        stat_type="Char",
        metrics=metrics,
        top_values=[{"value": row[0], "freq": int(row[1])} for row in top_rows],
    )


def _profile_numeric(dataframe: pd.DataFrame, column: str) -> QuickStats:
    """
    Purpose:
        Generate numeric descriptive statistics.

    Internal Logic:
        1. Converts values to numeric with coercion for invalid tokens.
        2. Computes min/max/sum/mean/median and null metrics.
        3. Includes parse success ratio for data quality visibility.

    Example invocation:
        stat = _profile_numeric(df, "sales")
    """

    series = pd.to_numeric(dataframe[column], errors="coerce")
    valid = series.dropna()
    metrics: dict[str, Any] = {
        "row_count": int(len(series)),
        "null_count": int(series.isna().sum()),
        "valid_count": int(valid.size),
        "parse_success_rate": float(valid.size / len(series)) if len(series) else 0.0,
    }
    if valid.size:
        metrics.update(
            {
                "min": float(valid.min()),
                "max": float(valid.max()),
                "sum": float(valid.sum()),
                "mean": float(valid.mean()),
                "median": float(valid.median()),
            }
        )
    return QuickStats(field_name=column, stat_type="Num", metrics=metrics)


def _profile_datetime(dataframe: pd.DataFrame, column: str, stat_type: str) -> QuickStats:
    """
    Purpose:
        Generate date/datetime range statistics.

    Internal Logic:
        1. Parses values to UTC-aware datetimes.
        2. Calculates min/max and parse success ratio.
        3. Emits null/valid counts for quality context.

    Example invocation:
        stat = _profile_datetime(df, "order_date", "Date")
    """

    parsed = pd.to_datetime(dataframe[column], errors="coerce", utc=True)
    valid = parsed.dropna()
    metrics: dict[str, Any] = {
        "row_count": int(len(parsed)),
        "null_count": int(parsed.isna().sum()),
        "valid_count": int(valid.size),
        "parse_success_rate": float(valid.size / len(parsed)) if len(parsed) else 0.0,
    }
    if valid.size:
        metrics["min"] = valid.min().isoformat()
        metrics["max"] = valid.max().isoformat()
    return QuickStats(field_name=column, stat_type=stat_type, metrics=metrics)


def _profile_keys(dataframe: pd.DataFrame, file_keys: list[str]) -> dict[str, QuickStats]:
    """
    Purpose:
        Profile composite primary key behavior from metadata `FileKey`.

    Internal Logic:
        1. Skips processing when key columns are unavailable.
        2. Calculates unique ratio and duplicate-key counts.
        3. Captures the top duplicate key signatures for diagnostics.

    Example invocation:
        key_stats = _profile_keys(df, ["id", "date"])
    """

    if not file_keys or not set(file_keys).issubset(set(dataframe.columns)):
        return {}
    key_df = dataframe[file_keys].astype("string")
    dup_counts = key_df.value_counts(dropna=False).reset_index(name="freq")
    duplicates = dup_counts[dup_counts["freq"] > 1].head(10)
    row_count = len(key_df)
    unique_count = int(dup_counts.shape[0])
    metrics = {
        "row_count": int(row_count),
        "unique_key_count": unique_count,
        "duplicate_key_count": int((dup_counts["freq"] > 1).sum()),
        "uniqueness_ratio": float(unique_count / row_count) if row_count else 0.0,
    }
    top_values = []
    for _, row in duplicates.iterrows():
        key_sig = "|".join(str(row[key]) for key in file_keys)
        top_values.append({"value": key_sig, "freq": int(row["freq"])})
    return {
        "__FILEKEY__": QuickStats(
            field_name="__FILEKEY__",
            stat_type="Key",
            metrics=metrics,
            top_values=top_values,
        )
    }

