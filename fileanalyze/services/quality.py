"""Quality and insight generation services."""

from __future__ import annotations

from typing import Any

from fileanalyze.models.schemas import MetadataBundle, QuickStats


def build_quality_summary(
    metadata: MetadataBundle,
    quick_stats: dict[str, QuickStats],
    schema_warnings: list[str],
) -> dict[str, Any]:
    """
    Purpose:
        Build a data-quality scorecard and top insight summary.

    Internal Logic:
        1. Aggregates parse/null/duplicate signals from quick stats.
        2. Converts schema warnings and metadata gaps into alerts.
        3. Produces concise findings for dashboard display.

    Example invocation:
        quality = build_quality_summary(metadata, quick_stats, [])
    """

    findings: list[str] = []
    parse_rates: list[float] = []
    null_rates: list[float] = []
    skew_candidates: list[tuple[str, float, float]] = []
    outlier_hints: list[tuple[str, float, float]] = []
    for stat in quick_stats.values():
        if stat.field_name == "__FILEKEY__":
            continue
        row_count = float(stat.metrics.get("row_count", 0))
        null_count = float(stat.metrics.get("null_count", 0))
        parse_rate = float(stat.metrics.get("parse_success_rate", 1.0))
        parse_rates.append(parse_rate)
        if row_count > 0:
            null_rates.append(null_count / row_count)
        if stat.stat_type.lower().startswith("num"):
            mean_value = float(stat.metrics.get("mean", 0.0)) if stat.metrics.get("mean") is not None else 0.0
            median_value = float(stat.metrics.get("median", 0.0)) if stat.metrics.get("median") is not None else 0.0
            min_value = float(stat.metrics.get("min", 0.0)) if stat.metrics.get("min") is not None else 0.0
            max_value = float(stat.metrics.get("max", 0.0)) if stat.metrics.get("max") is not None else 0.0
            if median_value != 0:
                skew_ratio = abs(mean_value - median_value) / abs(median_value)
                skew_candidates.append((stat.field_name, skew_ratio, mean_value - median_value))
            if min_value >= 0 and min_value > 0:
                spread_ratio = max_value / min_value if min_value else 0.0
                outlier_hints.append((stat.field_name, spread_ratio, max_value))
            elif min_value < 0 < max_value:
                outlier_hints.append((stat.field_name, abs(max_value - min_value), max_value))
    avg_parse_rate = sum(parse_rates) / len(parse_rates) if parse_rates else 1.0
    avg_null_rate = sum(null_rates) / len(null_rates) if null_rates else 0.0

    key_stats = quick_stats.get("__FILEKEY__")
    duplicate_key_count = int(key_stats.metrics.get("duplicate_key_count", 0)) if key_stats else 0
    key_row_count = int(key_stats.metrics.get("row_count", 0)) if key_stats else 0
    duplicate_pk_pct = (duplicate_key_count / key_row_count * 100.0) if key_row_count else 0.0
    invalid_parse_pct = (1.0 - avg_parse_rate) * 100.0
    avg_null_pct = avg_null_rate * 100.0

    if duplicate_key_count > 0:
        findings.append(f"Detected {duplicate_key_count} duplicate key combinations in FileKey columns.")
    if avg_null_rate > 0.1:
        findings.append("Average null ratio exceeds 10%; review missingness patterns.")
    if avg_parse_rate < 0.95:
        findings.append("Type parsing success below 95%; metadata type mismatch likely.")
    skew_candidates.sort(key=lambda item: item[1], reverse=True)
    if skew_candidates and skew_candidates[0][1] >= 0.5:
        field_name, skew_ratio, direction = skew_candidates[0]
        direction_text = "right-skewed" if direction > 0 else "left-skewed"
        findings.append(
            f"Distribution hint: {field_name} appears {direction_text} (mean/median gap ratio {skew_ratio:.2f})."
        )
    outlier_hints.sort(key=lambda item: item[1], reverse=True)
    if outlier_hints and outlier_hints[0][1] >= 20.0:
        field_name, spread_ratio, _ = outlier_hints[0]
        findings.append(
            f"Outlier hint: {field_name} shows wide spread (max/min-style ratio indicator {spread_ratio:,.2f})."
        )

    missing_descriptions = [field.field_name for field in metadata.fields if not field.field_desc.strip()]
    if missing_descriptions:
        findings.append(f"{len(missing_descriptions)} fields are missing FieldDesc metadata.")
    findings.extend(schema_warnings[:3])

    if not findings:
        findings.append("No major data quality risks detected from quick profile.")

    return {
        "scorecard": {
            "avg_parse_success_pct": round(avg_parse_rate * 100.0, 2),
            "invalid_parse_pct": round(invalid_parse_pct, 2),
            "avg_null_pct": round(avg_null_pct, 2),
            "duplicate_pk_pct": round(duplicate_pk_pct, 2),
            "duplicate_key_count": duplicate_key_count,
            "schema_warning_count": len(schema_warnings),
        },
        "top_findings": findings[:25],
    }


def recommend_chart(field_types: dict[str, str], dimension: str, measure: str) -> str:
    """
    Purpose:
        Recommend an optimal chart based on selected field types.

    Internal Logic:
        1. Uses metadata types for dimension/measure.
        2. Prefers line for datetime dimensions and numeric measures.
        3. Defaults to bar for categorical analyses.

    Example invocation:
        chart = recommend_chart({"order_date": "Datetime"}, "order_date", "sales")
    """

    dim_type = field_types.get(dimension, "").lower()
    mea_type = field_types.get(measure, "").lower()
    if dim_type in {"date", "datetime"} and mea_type == "num":
        return "Line"
    if mea_type == "num":
        return "Bar"
    return "Pie"

