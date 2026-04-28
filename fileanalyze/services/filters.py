"""Filtering helpers for Tab 2 data exploration."""

from __future__ import annotations

import re

import pandas as pd

from fileanalyze.models.schemas import MetadataBundle

BETWEEN_PATTERN = re.compile(r"^\s*between\s+(-?\d+(?:\.\d+)?)\s+and\s+(-?\d+(?:\.\d+)?)\s*$", re.I)
SIMPLE_PATTERN = re.compile(r"^\s*(<=|>=|=|<|>)\s*(-?\d+(?:\.\d+)?)\s*$", re.I)


def apply_filters(
    dataframe: pd.DataFrame,
    metadata: MetadataBundle,
    dimension_filters: dict[str, str],
    measure_filters: dict[str, str],
) -> pd.DataFrame:
    """
    Purpose:
        Apply dimension and measure filters to a DataFrame.

    Internal Logic:
        1. Applies text contains matching for declared dimensions.
        2. Applies numeric expression filters for declared measures.
        3. Returns a filtered copy for safe downstream rendering/export.

    Example invocation:
        subset = apply_filters(df, meta, {"region": "north"}, {"sales": ">100"})
    """

    result = dataframe.copy()
    field_map = metadata.field_map()

    for field_name, expr in dimension_filters.items():
        if not expr.strip():
            continue
        field = field_map.get(field_name)
        if not field or field.field_dtype != "D" or field_name not in result.columns:
            continue
        result = result[result[field_name].astype("string").str.contains(expr, case=False, na=False)]

    for field_name, expr in measure_filters.items():
        if not expr.strip():
            continue
        field = field_map.get(field_name)
        if not field or field.field_dtype != "M" or field_name not in result.columns:
            continue
        result = _apply_measure_expression(result, field_name, expr)
    return result


def _apply_measure_range_mask(dataframe: pd.DataFrame, field_name: str, expression: str) -> pd.Series:
    """
    Purpose:
        Return a boolean mask for a single measure expression (without filtering DataFrame).

    Internal Logic:
        1. Converts measure column to numeric and parses expression.
        2. Builds a boolean series aligned to rows.
        3. Reuses the same `between` and simple-comparison rules as `apply_filters`.

    Example invocation:
        mask = _apply_measure_range_mask(df, "sales", "between 1 and 10")
    """

    if field_name not in dataframe.columns:
        return pd.Series(False, index=dataframe.index)
    numeric = pd.to_numeric(dataframe[field_name], errors="coerce")
    between_match = BETWEEN_PATTERN.match(expression)
    if between_match:
        low = float(between_match.group(1))
        high = float(between_match.group(2))
        return ((numeric >= low) & (numeric <= high)).fillna(False)

    op_match = SIMPLE_PATTERN.match(expression)
    if not op_match:
        return pd.Series(True, index=dataframe.index)
    operator = op_match.group(1)
    value = float(op_match.group(2))
    if operator == "<":
        mask = numeric < value
    elif operator == ">":
        mask = numeric > value
    elif operator == "<=":
        mask = numeric <= value
    elif operator == ">=":
        mask = numeric >= value
    else:
        mask = numeric == value
    return mask.fillna(False)


def _apply_measure_expression(dataframe: pd.DataFrame, field_name: str, expression: str) -> pd.DataFrame:
    """
    Purpose:
        Apply one numeric expression filter to a measure column.

    Internal Logic:
        1. Parses expression tokens (`between`, `<`, `>`, `=`, `<=`, `>=`).
        2. Converts column values to numeric.
        3. Applies boolean mask and returns filtered rows.

    Example invocation:
        subset = _apply_measure_expression(df, "sales", "between 10 and 20")
    """

    mask = _apply_measure_range_mask(dataframe, field_name, expression)
    return dataframe[mask]


def apply_data_grid_filters(
    dataframe: pd.DataFrame,
    metadata: MetadataBundle,
    dimension_rows: list[tuple[str | None, list[str] | None]],
    measure_rows: list[tuple[str | None, list[str] | None]],
) -> pd.DataFrame:
    """
    Purpose:
        Apply multi-select dimension and measure range filters to a DataFrame.

    Internal Logic:
        1. For each non-empty dimension row, filters rows with field value in selected set (IN).
        2. For each non-empty measure row, ORs masks from multiple "between" expressions, then ANDs with prior rows.
        3. Respects `FieldDType` metadata and skips invalid fields.

    Example invocation:
        subset = apply_data_grid_filters(df, meta, [("region", ["A", "B"])], [("sales", ["between 0 and 10"]])
    """

    result = dataframe.copy()
    field_map = metadata.field_map()

    for field_name, selected in dimension_rows:
        if not field_name or not selected:
            continue
        field = field_map.get(field_name)
        if not field or field.field_dtype != "D" or field_name not in result.columns:
            continue
        as_strings = {str(s) for s in selected if s is not None and str(s) != ""}
        if not as_strings:
            continue
        col = result[field_name].astype("string")
        result = result[col.isin(as_strings)]

    for field_name, expressions in measure_rows:
        if not field_name or not expressions:
            continue
        field = field_map.get(field_name)
        if not field or field.field_dtype != "M" or field_name not in result.columns:
            continue
        clean_exprs = [ex.strip() for ex in expressions if ex and str(ex).strip()]
        if not clean_exprs:
            continue
        combined: pd.Series | None = None
        for expression in clean_exprs:
            m = _apply_measure_range_mask(result, field_name, expression)
            combined = m if combined is None else (combined | m)
        if combined is not None:
            result = result[combined]

    return result

