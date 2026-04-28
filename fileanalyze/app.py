"""File Analyze Dash application entrypoint."""

from __future__ import annotations

import json
from pathlib import Path
import threading
import uuid

from dash import ALL, MATCH, Dash, Input, Output, State, ctx, dcc, html, no_update
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import pandas as pd

from fileanalyze.config import AppConfig, load_config
from fileanalyze.layouts.main_layout import build_layout
from fileanalyze.models.schemas import MetadataBundle, QuickStats, RuntimeContext
from fileanalyze.services.charts import build_chart
from fileanalyze.services.filters import apply_data_grid_filters
from fileanalyze.services.ingest import (
    create_run_paths,
    load_dataframe,
    parse_metadata,
    resolve_meta_path,
    validate_metadata_against_data,
)
from fileanalyze.services.profile import generate_quick_stats
from fileanalyze.services.quality import build_quality_summary, recommend_chart


class RuntimeRegistry:
    """
    Purpose:
        Hold run-scoped runtime contexts for isolated multi-user execution.

    Internal Logic:
        1. Stores contexts keyed by session and run IDs.
        2. Protects updates with a lock for thread-safe callbacks.
        3. Exposes retrieval helpers used by chart and filter callbacks.

    Example invocation:
        registry.save(context)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: dict[tuple[str, str], RuntimeContext] = {}

    def save(self, context: RuntimeContext) -> None:
        """
        Purpose:
            Save one runtime context into memory.

        Internal Logic:
            1. Creates key from session/run pair.
            2. Acquires lock for thread-safe mutation.
            3. Writes context to registry map.

        Example invocation:
            registry.save(context)
        """

        key = (context.session_id, context.run_id)
        with self._lock:
            self._items[key] = context

    def get(self, session_id: str, run_id: str) -> RuntimeContext | None:
        """
        Purpose:
            Retrieve a previously saved runtime context.

        Internal Logic:
            1. Builds key from session/run IDs.
            2. Uses lock for consistent concurrent reads.
            3. Returns context or `None` if not found.

        Example invocation:
            context = registry.get("session-1", "run-1")
        """

        key = (session_id, run_id)
        with self._lock:
            return self._items.get(key)


def _stats_to_json(stats: dict[str, QuickStats]) -> dict[str, dict[str, object]]:
    """
    Purpose:
        Convert typed quick stats into JSON-safe payloads for Dash stores.

    Internal Logic:
        1. Iterates all quick stats objects.
        2. Serializes metrics and frequency rows as dictionaries.
        3. Returns a field-keyed payload suitable for dcc.Store.

    Example invocation:
        payload = _stats_to_json(stats)
    """

    payload: dict[str, dict[str, object]] = {}
    for key, value in stats.items():
        payload[key] = {
            "field_name": value.field_name,
            "stat_type": value.stat_type,
            "metrics": value.metrics,
            "top_values": value.top_values,
        }
    return payload


def _metadata_to_json(metadata: MetadataBundle) -> dict[str, object]:
    """
    Purpose:
        Convert metadata bundle into JSON payload for UI callbacks.

    Internal Logic:
        1. Serializes file key list.
        2. Converts each `FieldMetadata` object to dictionary form.
        3. Returns a compact schema for client state stores.

    Example invocation:
        payload = _metadata_to_json(metadata)
    """

    return {
        "file_keys": metadata.file_keys,
        "fields": [
            {
                "field_name": field.field_name,
                "field_type": field.field_type,
                "field_length": field.field_length,
                "field_desc": field.field_desc,
                "field_dtype": field.field_dtype,
            }
            for field in metadata.fields
        ],
    }


def _metadata_from_json(payload: dict[str, object]) -> MetadataBundle:
    """
    Purpose:
        Rebuild typed metadata bundle from JSON store payload.

    Internal Logic:
        1. Reads raw dictionary values.
        2. Reconstructs `FieldMetadata` records via dataclass unpacking.
        3. Returns a typed bundle for service APIs.

    Example invocation:
        metadata = _metadata_from_json(payload)
    """

    from fileanalyze.models.schemas import FieldMetadata

    fields = [FieldMetadata(**item) for item in payload.get("fields", [])]  # type: ignore[arg-type]
    return MetadataBundle(file_keys=list(payload.get("file_keys", [])), fields=fields)


def _safe_html_id(prefix: str, field_name: str) -> str:
    """
    Purpose:
        Build deterministic safe HTML element IDs for tooltip targets.

    Internal Logic:
        1. Lowercases field names for stable IDs.
        2. Replaces unsupported characters with underscores.
        3. Prefixes IDs by section to avoid collisions.

    Example invocation:
        target_id = _safe_html_id("dim", "order date")
    """

    cleaned = "".join(char if char.isalnum() else "_" for char in field_name.lower())
    return f"{prefix}_{cleaned}"


def _format_metric_label(metric_key: str) -> str:
    """
    Purpose:
        Convert internal metric keys into user-friendly title-cased labels.

    Internal Logic:
        1. Replaces underscores with spaces for readability.
        2. Collapses extra spaces from irregular keys.
        3. Applies title case for consistent hover table presentation.

    Example invocation:
        label = _format_metric_label("null_count")
    """

    normalized = " ".join(metric_key.replace("_", " ").split())
    return normalized.title() if normalized else metric_key


def _format_field_name_display(field_name: str) -> str:
    """
    Purpose:
        Standardize all report/grid field-name labels in uppercase display form.

    Internal Logic:
        1. Trims whitespace from incoming field names.
        2. Converts resulting label to uppercase.
        3. Returns empty-safe uppercase string for consistent UI rendering.

    Example invocation:
        label = _format_field_name_display("order_id")
    """

    return field_name.strip().upper()


def _hover_table(field_name: str, stats_payload: dict[str, dict[str, object]], display_name: str | None = None) -> html.Div:
    """
    Purpose:
        Render hover content as a table instead of JSON text.

    Internal Logic:
        1. Reads metrics from quick-stats payload by field name.
        2. Converts metrics to metric/value row pairs.
        3. Builds compact table markup shown in tooltips.

    Example invocation:
        hover = _hover_table("sales", quick_stats_payload)
    """

    stats = stats_payload.get(field_name, {})
    metrics = stats.get("metrics", {})
    metric_rows: list[html.Tr] = []
    if isinstance(metrics, dict):
        for key, value in metrics.items():
            metric_rows.append(
                html.Tr([html.Td(_format_metric_label(str(key))), html.Td(_format_numeric_for_display(value))])
            )
    if not metric_rows:
        metric_rows.append(html.Tr([html.Td("status"), html.Td("No quick stats available")]))
    return html.Div(
        [
            html.Div(display_name or _format_field_name_display(field_name), style={"fontWeight": "700", "marginBottom": "6px"}),
            html.Table(
                [
                    html.Thead(html.Tr([html.Th("Metric"), html.Th("Value")])),
                    html.Tbody(metric_rows),
                ],
                className="fa-hover-table",
                style={"minWidth": "260px"},
            ),
        ]
    )


def _format_nested_values(payload: object) -> object:
    """
    Purpose:
        Recursively format number-like values for display outputs.

    Internal Logic:
        1. Walks dictionaries and lists recursively.
        2. Formats scalar numeric values using `_format_numeric_for_display`.
        3. Returns transformed payload preserving original structure.

    Example invocation:
        pretty = _format_nested_values({"a": 12000.123456})
    """

    if isinstance(payload, dict):
        return {key: _format_nested_values(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_format_nested_values(item) for item in payload]
    if isinstance(payload, tuple):
        return tuple(_format_nested_values(item) for item in payload)
    return _format_numeric_for_display(payload)


def _format_numeric_for_display(value: object) -> str:
    """
    Purpose:
        Format numeric values with comma separators and 4 decimal precision.

    Internal Logic:
        1. Attempts conversion to float for number-like values.
        2. Uses thousand separators and up to 4 decimal places.
        3. Returns original string when conversion is not numeric.

    Example invocation:
        pretty = _format_numeric_for_display(12345.67891)
    """

    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
        return f"{value:,.4f}"
    try:
        parsed = float(str(value))
        if parsed.is_integer():
            return f"{int(parsed):,}"
        return f"{parsed:,.4f}"
    except (TypeError, ValueError):
        return str(value)


def _build_grid_column_defs(preview: pd.DataFrame, metadata: MetadataBundle | None = None) -> list[dict[str, object]]:
    """
    Purpose:
        Build AG Grid column definitions with alignment and numeric formatting.

    Internal Logic:
        1. Uses metadata field dtype when available to identify measure/dimension fields.
        2. Applies thousand-separator formatter only to measure (`M`) columns.
        3. Keeps dimension (`D`) columns unformatted even when values look numeric.
        4. Skips numeric fallback so non-measure columns never receive comma formatting.

    Example invocation:
        defs = _build_grid_column_defs(preview_df, metadata)
    """

    defs: list[dict[str, object]] = []
    field_map = metadata.field_map() if metadata else {}
    normalized_field_map = (
        {str(name).strip().casefold(): field for name, field in field_map.items()} if field_map else {}
    )
    for col in preview.columns:
        col_key = str(col)
        normalized_col_key = col_key.strip().casefold()
        meta_field = field_map.get(col_key) or normalized_field_map.get(normalized_col_key)
        field_dtype = str(meta_field.field_dtype).strip().upper() if meta_field else ""
        field_type = str(meta_field.field_type).strip().upper() if meta_field else ""
        is_measure = field_dtype == "M" or field_type in {
            "NUM",
            "NUMBER",
            "DECIMAL",
            "FLOAT",
            "DOUBLE",
            "INT",
            "INTEGER",
        }
        should_format_numeric = is_measure
        if should_format_numeric:
            defs.append(
                {
                    "field": col,
                    "headerName": _format_field_name_display(str(col)),
                    "sortable": True,
                    "filter": True,
                    "resizable": True,
                    "type": "rightAligned",
                    "valueFormatter": {
                        "function": "if (params.value == null || params.value === '') { return ''; } const n = Number(params.value); if (!Number.isFinite(n)) { return params.value; } return Number.isInteger(n) ? n.toLocaleString() : n.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 4});"
                    },
                }
            )
        else:
            defs.append(
                {
                    "field": col,
                    "headerName": _format_field_name_display(str(col)),
                    "sortable": True,
                    "filter": True,
                    "resizable": True,
                    "type": "leftAligned",
                }
            )
    return defs


def _normalize_multi_filter_values(raw: list[str] | str | None) -> list[str] | None:
    """
    Purpose:
        Coerce Data Grid filter dropdown state to a list of non-empty string tokens.

    Internal Logic:
        1. Returns None when input is None or all-empty.
        2. Unwraps single string into a one-element list when needed.
        3. Drops null-like entries after trimming for stable filtering.

    Example invocation:
        tokens = _normalize_multi_filter_values(["A", "B"])
    """

    if raw is None:
        return None
    if isinstance(raw, list):
        out = [str(x) for x in raw if x is not None and str(x).strip() != ""]
        return out or None
    s = str(raw).strip()
    return [s] if s else None


def _pair_field_and_token_rows(
    field_list: list[str | None] | None,
    token_list: list[object] | None,
) -> list[tuple[str | None, list[str] | None]]:
    """
    Purpose:
        Zip parallel ALL-state lists into `(field, tokens)` rows for the filter service.

    Internal Logic:
        1. Pads to the shared minimum length to ignore Dash timing mismatches.
        2. Skips rows missing a field name or with no selected tokens.
        3. Returns tuples suitable for `apply_data_grid_filters`.

    Example invocation:
        rows = _pair_field_and_token_rows(["A"], [["x", "y"]])
    """

    fields = field_list or []
    tokens = token_list or []
    n = min(len(fields), len(tokens))
    out: list[tuple[str | None, list[str] | None]] = []
    for index in range(n):
        name = fields[index] if index < len(fields) else None
        toks = _normalize_multi_filter_values(tokens[index] if index < len(tokens) else None)
        if name and toks:
            out.append((name, toks))
    return out


def _build_cascading_field_options(
    all_field_names: list[str],
    selected_fields: list[str | None] | None,
) -> list[list[dict[str, str]]]:
    """
    Purpose:
        Build per-row dropdown options that enforce unique field picks across rows.

    Internal Logic:
        1. Collects all currently selected non-empty field names.
        2. For each row, excludes fields selected by other rows.
        3. Keeps the row's own selected field available so existing choice remains visible.

    Example invocation:
        options = _build_cascading_field_options(["A", "B"], ["A", None])
    """

    rows = selected_fields or []
    used = {name for name in rows if name}
    per_row: list[list[dict[str, str]]] = []
    for field_name in rows:
        blocked = used.copy()
        if field_name:
            blocked.discard(field_name)
        allowed = [name for name in all_field_names if name not in blocked]
        per_row.append(
            [{"label": _format_field_name_display(name), "value": name} for name in allowed]
        )
    return per_row


def _reorder_dataframe_columns(dataframe: pd.DataFrame, sort_alphabetically: bool) -> pd.DataFrame:
    """
    Purpose:
        Reorder DataFrame columns for Data Grid when user opts into sorted names.

    Internal Logic:
        1. When `sort_alphabetically` is true, reorders to lexicographic column order.
        2. Otherwise returns the DataFrame with original column order.
        3. Does not add or remove columns, only reorders.

    Example invocation:
        reordered = _reorder_dataframe_columns(df, True)
    """

    if not sort_alphabetically:
        return dataframe
    return dataframe[sorted(dataframe.columns.astype(str))].copy()


def _format_measure_columns_for_display(dataframe: pd.DataFrame, metadata: MetadataBundle) -> pd.DataFrame:
    """
    Purpose:
        Apply display-only thousand separators to measure columns for grid rendering.

    Internal Logic:
        1. Copies incoming DataFrame to avoid mutating filter/export source frames.
        2. Detects measure fields from metadata (`FieldDType=M` or numeric `FieldType`).
        3. Formats only numeric-like values in those columns via `_format_numeric_for_display`.

    Example invocation:
        display_df = _format_measure_columns_for_display(preview_df, metadata)
    """

    display_df = dataframe.copy()
    numeric_types = {"NUM", "NUMBER", "DECIMAL", "FLOAT", "DOUBLE", "INT", "INTEGER"}
    field_map = metadata.field_map()
    for column in display_df.columns:
        field = field_map.get(str(column))
        if not field:
            continue
        field_dtype = str(field.field_dtype).strip().upper()
        field_type = str(field.field_type).strip().upper()
        is_measure = field_dtype == "M" or field_type in numeric_types
        if not is_measure:
            continue

        def _format_if_number(value: object) -> object:
            if value is None:
                return value
            text = str(value).strip()
            if text == "":
                return value
            try:
                float(text.replace(",", ""))
            except ValueError:
                return value
            return _format_numeric_for_display(text.replace(",", ""))

        display_df[column] = display_df[column].map(_format_if_number)
    return display_df


def _build_clipboard_tsv(dataframe: pd.DataFrame, visible_columns: list[str] | None) -> str:
    """
    Purpose:
        Build an Excel-friendly tab-separated text block from current grid rows.

    Internal Logic:
        1. Applies visible-column filtering when a column selection exists.
        2. Preserves row ordering exactly as shown in the current grid dataset.
        3. Emits UTF-8 text with header row and tab separators for clipboard paste.

    Example invocation:
        tsv = _build_clipboard_tsv(preview_df, ["COL_A", "COL_B"])
    """

    frame = dataframe.copy()
    if visible_columns:
        visible = [col for col in visible_columns if col in frame.columns]
        if visible:
            frame = frame[visible]
    header_map = {col: _format_field_name_display(str(col)) for col in frame.columns}
    return frame.rename(columns=header_map).to_csv(sep="\t", index=False)


def _build_clipboard_tsv_from_grid_rows(
    rows: list[dict[str, object]] | None,
    column_defs: list[dict[str, object]] | None,
) -> str:
    """
    Purpose:
        Build clipboard TSV from AG Grid's current filtered/sorted client-side rows.

    Internal Logic:
        1. Reads visible data columns from `columnDefs` in displayed order.
        2. Excludes hidden columns and helper record-number column.
        3. Caps clipboard payload rows for browser responsiveness.
        4. Converts current `virtualRowData` rows into TSV with header row.

    Example invocation:
        tsv = _build_clipboard_tsv_from_grid_rows(row_data, defs)
    """

    defs = column_defs or []
    current_rows = rows or []
    if len(current_rows) > GRID_CLIPBOARD_ROW_LIMIT:
        current_rows = current_rows[:GRID_CLIPBOARD_ROW_LIMIT]
    ordered_visible_cols: list[tuple[str, str]] = []
    for definition in defs:
        field = str(definition.get("field", "")).strip()
        if not field or field == "__line_number__":
            continue
        if bool(definition.get("hide", False)):
            continue
        header = str(definition.get("headerName", field))
        ordered_visible_cols.append((field, header))
    if not ordered_visible_cols:
        return ""

    headers = [header for _, header in ordered_visible_cols]
    lines = ["\t".join(headers)]
    for row in current_rows:
        values: list[str] = []
        for field, _ in ordered_visible_cols:
            raw = row.get(field, "") if isinstance(row, dict) else ""
            text = "" if raw is None else str(raw)
            cleaned = text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
            values.append(cleaned)
        lines.append("\t".join(values))
    return "\n".join(lines)


GRID_FILTER_ROW_MAX = 25
GRID_FILTER_ROW_MIN = 5
GRID_DIMENSION_OPTION_LIMIT = 2500
GRID_CLIPBOARD_ROW_LIMIT = 100000
GRID_RENDER_BLOCK_ROW_THRESHOLD = 200000
GRID_RENDER_BLOCK_CELL_THRESHOLD = 12000000


def _read_parquet_columns(data_path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    """
    Purpose:
        Read only required parquet columns to reduce memory/CPU for large datasets.

    Internal Logic:
        1. Uses pandas parquet reader with optional projection list.
        2. Falls back to full read when projection is empty/None.
        3. Returns DataFrame scoped to requested columns.

    Example invocation:
        frame = _read_parquet_columns(path, ["REGION"])
    """

    if columns:
        return pd.read_parquet(data_path, columns=columns)
    return pd.read_parquet(data_path)


def _build_large_data_guard_message(total_rows: int, total_cols: int, sample_rows: int) -> html.Div:
    """
    Purpose:
        Build a warning banner when full grid rendering is blocked for very large result sets.

    Internal Logic:
        1. Shows filtered row/column counts for transparency.
        2. Explains that only a sampled preview is rendered in grid.
        3. Directs users to CSV export for complete filtered output.

    Example invocation:
        banner = _build_large_data_guard_message(500000, 80, 10000)
    """

    return html.Div(
        (
            f"Large result guard active: {total_rows:,} rows x {total_cols:,} columns. "
            f"Grid rendering is limited to first {sample_rows:,} rows. "
            "Use Export Filtered CSV to download the full filtered dataset."
        ),
        className="fa-card fa-muted",
        style={"marginBottom": "8px", "border": "1px solid #f59e0b", "background": "#fff7ed"},
    )


def _apply_grid_filter_model(dataframe: pd.DataFrame, filter_model: dict[str, object] | None) -> pd.DataFrame:
    """
    Purpose:
        Apply AG Grid filter model server-side for infinite row pagination mode.

    Internal Logic:
        1. Iterates filter model entries by field.
        2. Supports core text and number operators.
        3. Applies each field filter cumulatively (AND semantics).

    Example invocation:
        subset = _apply_grid_filter_model(df, {"REGION": {"filterType": "text", "type": "contains", "filter": "1"}})
    """

    if not filter_model:
        return dataframe
    result = dataframe
    for field, raw_model in filter_model.items():
        if field not in result.columns or not isinstance(raw_model, dict):
            continue
        filter_type = str(raw_model.get("filterType", "")).strip().lower()
        op = str(raw_model.get("type", "")).strip().lower()
        if filter_type == "text":
            target = str(raw_model.get("filter", "")).strip()
            series = result[field].astype("string").fillna("")
            if op == "contains":
                mask = series.str.contains(target, case=False, na=False)
            elif op == "notcontains":
                mask = ~series.str.contains(target, case=False, na=False)
            elif op == "equals":
                mask = series.str.lower() == target.lower()
            elif op == "notequal":
                mask = series.str.lower() != target.lower()
            elif op == "startswith":
                mask = series.str.lower().str.startswith(target.lower())
            elif op == "endswith":
                mask = series.str.lower().str.endswith(target.lower())
            else:
                mask = pd.Series(True, index=result.index)
            result = result[mask]
        elif filter_type == "number":
            series_num = pd.to_numeric(result[field], errors="coerce")
            try:
                val = float(str(raw_model.get("filter", "")).replace(",", ""))
            except ValueError:
                val = None
            try:
                val_to = float(str(raw_model.get("filterTo", "")).replace(",", ""))
            except ValueError:
                val_to = None
            if op == "equals" and val is not None:
                mask = series_num == val
            elif op == "notequal" and val is not None:
                mask = series_num != val
            elif op == "lessthan" and val is not None:
                mask = series_num < val
            elif op == "lessthanorequal" and val is not None:
                mask = series_num <= val
            elif op == "greaterthan" and val is not None:
                mask = series_num > val
            elif op == "greaterthanorequal" and val is not None:
                mask = series_num >= val
            elif op == "inrange" and val is not None and val_to is not None:
                mask = (series_num >= val) & (series_num <= val_to)
            else:
                mask = pd.Series(True, index=result.index)
            result = result[mask.fillna(False)]
    return result


def _apply_grid_sort_model(dataframe: pd.DataFrame, sort_model: list[dict[str, object]] | None) -> pd.DataFrame:
    """
    Purpose:
        Apply AG Grid sort model server-side for infinite row pagination mode.

    Internal Logic:
        1. Reads ordered sort instructions from grid request.
        2. Keeps only sortable fields available in the DataFrame.
        3. Applies stable mergesort with ascending/descending flags.

    Example invocation:
        sorted_df = _apply_grid_sort_model(df, [{"colId": "POPESTIMATE2025", "sort": "desc"}])
    """

    if not sort_model:
        return dataframe
    sort_cols: list[str] = []
    ascending: list[bool] = []
    for item in sort_model:
        if not isinstance(item, dict):
            continue
        field = str(item.get("colId", "")).strip()
        direction = str(item.get("sort", "asc")).strip().lower()
        if not field or field not in dataframe.columns:
            continue
        sort_cols.append(field)
        ascending.append(direction != "desc")
    if not sort_cols:
        return dataframe
    return dataframe.sort_values(by=sort_cols, ascending=ascending, kind="mergesort")


def _build_dim_filter_row(
    index: int,
    dim_options: list[dict[str, str]],
    selected_field: str | None = None,
    selected_values: list[str] | None = None,
) -> html.Div:
    """
    Purpose:
        Build one Dimension field + multi-select value row in Data Grid.

    Internal Logic:
        1. Renders a field `dcc.Dropdown` and a multi-select value `dcc.Dropdown`.
        2. Assigns pattern-matched IDs for callback wiring.
        3. Applies small-font styling class for compact layout.

    Example invocation:
        row = _build_dim_filter_row(0, [{"label": "x", "value": "x"}])
    """

    return html.Div(
        [
            dcc.Dropdown(
                id={"type": "grid-dim-field", "index": index},
                options=dim_options,
                value=selected_field,
                className="fa-filter-dropdown",
            ),
            dcc.Dropdown(
                id={"type": "grid-dim-value", "index": index},
                multi=True,
                options=[],
                value=selected_values,
                placeholder="Select values for the dimension",
                className="fa-filter-dropdown",
            ),
        ],
        className="fa-filter-pair-row",
    )


def _build_mea_filter_row(
    index: int,
    mea_options: list[dict[str, str]],
    selected_field: str | None = None,
    selected_ranges: list[str] | None = None,
) -> html.Div:
    """
    Purpose:
        Build one Measure field + multi-select range row in Data Grid.

    Internal Logic:
        1. Renders measure `dcc.Dropdown` and range multi-select.
        2. Uses pattern IDs for per-row value population and placeholders.
        3. Pairs with measure range builder from dataset min/max.

    Example invocation:
        row = _build_mea_filter_row(0, [{"label": "sales", "value": "sales"}])
    """

    return html.Div(
        [
            dcc.Dropdown(
                id={"type": "grid-mea-field", "index": index},
                options=mea_options,
                value=selected_field,
                className="fa-filter-dropdown",
            ),
            dcc.Dropdown(
                id={"type": "grid-mea-range", "index": index},
                multi=True,
                options=[],
                value=selected_ranges,
                placeholder="Select Range for the measure",
                className="fa-filter-dropdown",
            ),
        ],
        className="fa-filter-pair-row",
    )


def _sort_by_primary_keys(dataframe: pd.DataFrame, metadata: MetadataBundle) -> tuple[pd.DataFrame, list[str]]:
    """
    Purpose:
        Sort dataset deterministically by configured metadata primary keys.

    Internal Logic:
        1. Reads `FileKey` columns from metadata.
        2. Keeps only key columns that exist in the DataFrame.
        3. Applies stable sort over available key columns.
        4. Returns sorted DataFrame and keys actually used.

    Example invocation:
        sorted_df, used_keys = _sort_by_primary_keys(df, metadata)
    """

    candidate_keys = [key for key in metadata.file_keys if key in dataframe.columns]
    if not candidate_keys:
        return dataframe, []
    sorted_df = dataframe.sort_values(by=candidate_keys, kind="mergesort").reset_index(drop=True)
    return sorted_df, candidate_keys


def _apply_chart_click_filter(
    dataframe: pd.DataFrame,
    click_data: dict[str, object] | None,
    chart_dimension: str | None,
) -> tuple[pd.DataFrame, str]:
    """
    Purpose:
        Filter grid data to underlying rows for a clicked chart data point.

    Internal Logic:
        1. Reads first clicked point from Plotly click payload.
        2. Extracts dimension key from `x` or `label` depending on chart type.
        3. Filters the DataFrame by selected chart dimension equality.
        4. Returns filtered frame and a user-facing filter status message.

    Example invocation:
        subset, status = _apply_chart_click_filter(df, click_data, "region")
    """

    if not click_data or not chart_dimension or chart_dimension not in dataframe.columns:
        return dataframe, ""
    points = click_data.get("points", [])
    if not isinstance(points, list) or not points:
        return dataframe, ""
    first_point = points[0] if isinstance(points[0], dict) else {}
    raw_value = first_point.get("x")
    if raw_value is None:
        raw_value = first_point.get("label")
    if raw_value is None:
        return dataframe, ""
    filter_value = str(raw_value)
    filtered = dataframe[dataframe[chart_dimension].astype("string") == filter_value]
    status = f"Chart click filter applied: {chart_dimension} = {filter_value}"
    return filtered, status


def _build_dimension_value_options(dataframe: pd.DataFrame, dimension_field: str) -> list[dict[str, str]]:
    """
    Purpose:
        Build sorted unique dimension values for live-search filter options.

    Internal Logic:
        1. Reads selected dimension column as string.
        2. Drops null/blank entries and computes unique values.
        3. Caps very high-cardinality results for responsive UI.
        4. Sorts values alphabetically and maps to dropdown options.

    Example invocation:
        options = _build_dimension_value_options(df, "region")
    """

    if dimension_field not in dataframe.columns:
        return []
    series = dataframe[dimension_field].astype("string").dropna()
    cleaned = [value.strip() for value in series.tolist() if value and value.strip()]
    unique_sorted = sorted(set(cleaned))
    if len(unique_sorted) > GRID_DIMENSION_OPTION_LIMIT:
        unique_sorted = unique_sorted[:GRID_DIMENSION_OPTION_LIMIT]
    return [{"label": value, "value": value} for value in unique_sorted]


def _build_measure_range_options(dataframe: pd.DataFrame, measure_field: str) -> list[dict[str, str]]:
    """
    Purpose:
        Build 10 equal-width min/max ranges for measure live-search options.

    Internal Logic:
        1. Converts selected measure column to numeric and drops invalid values.
        2. Computes min, max, and equal step size across 10 bins.
        3. Emits dropdown options with readable labels and `between` expressions.

    Example invocation:
        options = _build_measure_range_options(df, "sales")
    """

    if measure_field not in dataframe.columns:
        return []
    numeric = pd.to_numeric(dataframe[measure_field], errors="coerce").dropna()
    if numeric.empty:
        return []
    min_value = float(numeric.min())
    max_value = float(numeric.max())
    if min_value == max_value:
        expression = f"between {min_value} and {max_value}"
        label = f"{_format_numeric_for_display(min_value)} to {_format_numeric_for_display(max_value)}"
        return [{"label": label, "value": expression}]

    step = (max_value - min_value) / 10.0
    options: list[dict[str, str]] = []
    for index in range(10):
        low = min_value + (index * step)
        high = max_value if index == 9 else (min_value + ((index + 1) * step))
        expression = f"between {low} and {high}"
        label = f"{_format_numeric_for_display(low)} to {_format_numeric_for_display(high)}"
        options.append({"label": label, "value": expression})
    return options


def _render_selected_stats_card(selected_payload: dict[str, object]) -> html.Div:
    """
    Purpose:
        Render selected field statistics as polished cards and tables.

    Internal Logic:
        1. Iterates selected fields and reads quick-stat payload sections.
        2. Builds metric table rows from `metrics`.
        3. Adds top-values table for frequency-style fields when available.

    Example invocation:
        card = _render_selected_stats_card({"sales": stats_payload})
    """

    if not selected_payload:
        return html.Div("No selected field stats available.", className="fa-card fa-muted")

    field_cards: list[html.Div] = []
    for field_name, payload in selected_payload.items():
        item = payload if isinstance(payload, dict) else {}
        stat_type = str(item.get("stat_type", "")).strip().lower()
        is_measure_stat = stat_type.startswith("num")
        metrics = item.get("metrics", {})
        top_values = item.get("top_values", [])

        metric_rows: list[html.Tr] = []
        if isinstance(metrics, dict):
            for key, value in metrics.items():
                metric_rows.append(
                    html.Tr(
                        [
                            html.Td(_format_metric_label(str(key)), className="fa-left"),
                            html.Td(
                                _format_numeric_for_display(value) if is_measure_stat else str(value),
                                className="fa-right",
                            ),
                        ]
                    )
                )
        if not metric_rows:
            metric_rows.append(html.Tr([html.Td("status"), html.Td("No metrics", className="fa-right")]))

        top_rows: list[html.Tr] = []
        if isinstance(top_values, list):
            for row in top_values[:10]:
                if isinstance(row, dict):
                    top_rows.append(
                        html.Tr(
                            [
                                html.Td(str(row.get("value", "")), className="fa-left"),
                                html.Td(_format_numeric_for_display(row.get("freq", "")), className="fa-right"),
                            ]
                        )
                    )
        top_values_table = (
            html.Table(
                [html.Thead(html.Tr([html.Th("Value"), html.Th("Freq")])), html.Tbody(top_rows)],
                className="fa-data-table",
            )
            if top_rows
            else html.Div("No top values for this field.", className="fa-muted")
        )

        field_cards.append(
            html.Div(
                [
                    html.H5(_format_field_name_display(str(field_name)), className="fa-table-title"),
                    html.Table(
                        [html.Thead(html.Tr([html.Th("Metric"), html.Th("Value")])), html.Tbody(metric_rows)],
                        className="fa-data-table",
                    ),
                    html.Div(top_values_table, style={"marginTop": "8px"}),
                ],
                className="fa-card",
            )
        )
    return html.Div(field_cards, className="fa-grid-2")


def _field_label(field_name: str, metadata: MetadataBundle | None) -> str:
    """
    Purpose:
        Build display label as `FieldName [FieldDesc]` when description exists.

    Internal Logic:
        1. Looks up field metadata by field name.
        2. Reads and trims `FieldDesc`.
        3. Returns enriched label or fallback field name.

    Example invocation:
        label = _field_label("sales", metadata)
    """

    display_name = _format_field_name_display(field_name)
    if not metadata:
        return display_name
    field_item = metadata.field_map().get(field_name)
    if not field_item:
        return display_name
    desc = field_item.field_desc.strip()
    return f"{display_name} [{desc}]" if desc else display_name


def _render_quality_summary_card(quality_payload: dict[str, object]) -> html.Div:
    """
    Purpose:
        Render quality scorecard and findings in polished table/card format.

    Internal Logic:
        1. Builds scorecard table from quality metrics.
        2. Lists top findings as readable bullets.
        3. Returns combined dashboard card block.

    Example invocation:
        quality_card = _render_quality_summary_card(payload)
    """

    scorecard = quality_payload.get("scorecard", {}) if isinstance(quality_payload, dict) else {}
    findings = quality_payload.get("top_findings", []) if isinstance(quality_payload, dict) else []

    score_rows: list[html.Tr] = []
    if isinstance(scorecard, dict):
        for key, value in scorecard.items():
            display_value = _format_numeric_for_display(value)
            normalized_key = str(key).strip().lower()
            if normalized_key.endswith("_pct") and value is not None:
                display_value = f"{_format_numeric_for_display(value)}%"
            score_rows.append(
                html.Tr(
                    [
                        html.Td(_format_metric_label(str(key)), className="fa-left"),
                        html.Td(display_value, className="fa-right"),
                    ]
                )
            )
    if not score_rows:
        score_rows.append(html.Tr([html.Td("status"), html.Td("No quality metrics", className="fa-right")]))

    finding_items = []
    if isinstance(findings, list) and findings:
        finding_items = [html.Li(str(item)) for item in findings]
    else:
        finding_items = [html.Li("No quality findings available.")]

    return html.Div(
        [
            html.Div(
                [
                    html.H5("Quality Scorecard", className="fa-table-title"),
                    html.Table(
                        [html.Thead(html.Tr([html.Th("Metric"), html.Th("Value")])), html.Tbody(score_rows)],
                        className="fa-data-table",
                    ),
                ],
                className="fa-card",
            ),
            html.Div(
                [
                    html.H5("Top Findings", className="fa-table-title"),
                    html.Ul(finding_items, className="fa-muted"),
                ],
                className="fa-card",
                style={"marginTop": "10px"},
            ),
        ]
    )


def create_app() -> Dash:
    """
    Purpose:
        Build and configure the Dash app instance.

    Internal Logic:
        1. Loads validated application configuration.
        2. Initializes app layout and callback registry.
        3. Returns a runnable Dash application object.

    Example invocation:
        app = create_app()
    """

    config = load_config()
    app = Dash(
        __name__,
        suppress_callback_exceptions=True,
        external_stylesheets=[dbc.themes.FLATLY],
    )
    app.title = "File Analyze"
    load_figure_template("flatly")
    app.layout = build_layout()
    _register_callbacks(app, config)
    return app


def _register_callbacks(app: Dash, config: AppConfig) -> None:
    """
    Purpose:
        Register all Dash callbacks for loading, charting, and filtering.

    Internal Logic:
        1. Initializes a process-local runtime registry.
        2. Binds callback handlers for each interactive feature.
        3. Connects stores/tables/graphs to the backing services.

    Example invocation:
        _register_callbacks(app, config)
    """

    registry = RuntimeRegistry()

    @app.callback(Output("session-id", "data"), Input("submit-load", "n_clicks"), State("session-id", "data"), prevent_initial_call=False)
    def initialize_session(_: int, existing_session: str | None) -> str:
        """
        Purpose:
            Ensure every browser session has a stable session identifier.

        Internal Logic:
            1. Reuses existing session ID when available.
            2. Generates UUID when session is first initialized.
            3. Returns session ID for all downstream callbacks.

        Example invocation:
            session_id = initialize_session(0, None)
        """

        return existing_session or str(uuid.uuid4())

    @app.callback(
        Output("run-state", "data"),
        Output("metadata-store", "data"),
        Output("quick-stats-store", "data"),
        Output("schema-warnings-store", "data"),
        Output("quality-store", "data"),
        Output("load-status", "children"),
        Output("sort-status", "children"),
        Output("grid-dim-row-count", "data"),
        Output("grid-mea-row-count", "data"),
        Input("submit-load", "n_clicks"),
        State("session-id", "data"),
        State("input-file-path", "value"),
        State("input-delimiter", "value"),
        prevent_initial_call=True,
    )
    def load_and_profile(
        n_clicks: int,
        session_id: str | None,
        file_path_raw: str | None,
        delimiter: str | None,
    ):
        """
        Purpose:
            Load source files and generate parallel quick stats on submit.

        Internal Logic:
            1. Resolves delimiter and validates required input.
            2. Loads data + metadata and computes quick stats in parallel.
            3. Creates run-scoped parquet artifact and in-memory context.
            4. Returns stores and status for both analysis tabs.

        Example invocation:
            state, meta, stats, warnings, quality, message = load_and_profile(...)
        """

        del n_clicks
        if not session_id:
            return (
                {},
                {},
                {},
                [],
                {},
                html.Div("Session initialization failed.", style={"color": "#000000"}),
                "",
                no_update,
                no_update,
            )
        if not file_path_raw:
            return (
                {},
                {},
                {},
                [],
                {},
                html.Div("Please provide a file path.", style={"color": "#000000"}),
                "",
                no_update,
                no_update,
            )
        chosen_delimiter = delimiter or ","

        data_path = Path(file_path_raw).expanduser().resolve()
        try:
            meta_path = resolve_meta_path(data_path)
            dataframe = load_dataframe(data_path, chosen_delimiter)
            metadata = parse_metadata(meta_path)
            dataframe, used_sort_keys = _sort_by_primary_keys(dataframe, metadata)
            warnings = validate_metadata_against_data(metadata, dataframe)
            quick_stats = generate_quick_stats(
                dataframe=dataframe,
                metadata=metadata,
                top_n=config.profile_topn,
                worker_count=config.worker_count,
            )
            quality = build_quality_summary(metadata, quick_stats, warnings)

            run_id, run_root = create_run_paths(config.output_root, session_id)
            data_file = run_root / "dataset.parquet"
            dataframe.to_parquet(data_file, index=False)

            runtime = RuntimeContext(
                session_id=session_id,
                run_id=run_id,
                run_root=run_root,
                data_path=data_file,
                metadata=metadata,
                quick_stats=quick_stats,
            )
            registry.save(runtime)
            status = html.Div(
                (
                    "Loaded "
                    f"{_format_numeric_for_display(len(dataframe))} rows and "
                    f"{_format_numeric_for_display(len(dataframe.columns))} columns. "
                    f"Run ID: {run_id}"
                ),
                style={"color": "#000000"},
            )
            sort_status = (
                f"Sorted by primary key: {', '.join(used_sort_keys)}"
                if used_sort_keys
                else "Primary key sort skipped: no FileKey columns found in source data."
            )
            return (
                {"session_id": session_id, "run_id": run_id, "delimiter": chosen_delimiter, "data_path": str(data_file)},
                _metadata_to_json(metadata),
                _stats_to_json(quick_stats),
                warnings,
                quality,
                status,
                sort_status,
                GRID_FILTER_ROW_MIN,
                GRID_FILTER_ROW_MIN,
            )
        except Exception as exc:  # noqa: BLE001
            return (
                {},
                {},
                {},
                [],
                {},
                html.Div(f"Load failed: {exc}", style={"color": "#000000"}),
                "",
                no_update,
                no_update,
            )

    @app.callback(
        Output("dimension-field-list", "children"),
        Output("measure-field-list", "children"),
        Output("chart-dimension", "options"),
        Output("chart-measure", "options"),
        Output("chart-color-dimension", "options"),
        Input("metadata-store", "data"),
        Input("quick-stats-store", "data"),
    )
    def load_field_controls(
        metadata_payload: dict[str, object] | None,
        quick_stats_payload: dict[str, dict[str, object]] | None,
    ) -> tuple[list[html.Li], list[html.Li], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
        """
        Purpose:
            Populate field menus and dropdown options for both tabs.

        Internal Logic:
            1. Reads metadata and quick stats from stores.
            2. Groups fields into dimension and measure collections.
            3. Generates hover tooltips containing quick metric summaries.

        Example invocation:
            controls = load_field_controls(meta_payload, stats_payload)
        """

        if not metadata_payload:
            empty: list[dict[str, str]] = []
            return [], [], empty, empty, empty
        metadata = _metadata_from_json(metadata_payload)
        stats_payload = quick_stats_payload or {}

        dim_fields = [field for field in metadata.fields if field.field_dtype == "D"]
        mea_fields = [field for field in metadata.fields if field.field_dtype == "M"]

        def _li(field_name: str, prefix: str) -> html.Li:
            target_id = _safe_html_id(prefix, field_name)
            display_name = _field_label(field_name, metadata)
            return html.Li(
                [
                    html.Span(_format_field_name_display(field_name), id=target_id, style={"cursor": "help"}),
                    dbc.Tooltip(
                        _hover_table(field_name, stats_payload, display_name),
                        target=target_id,
                        placement="right",
                        class_name="fa-tooltip",
                    ),
                ]
            )

        dimension_items = [_li(field.field_name, "dim") for field in dim_fields]
        measure_items = [_li(field.field_name, "mea") for field in mea_fields]
        dimension_options = [
            {"label": _format_field_name_display(field.field_name), "value": field.field_name} for field in dim_fields
        ]
        measure_options = [
            {"label": _format_field_name_display(field.field_name), "value": field.field_name} for field in mea_fields
        ]
        return (
            dimension_items,
            measure_items,
            dimension_options,
            measure_options,
            dimension_options,
        )

    @app.callback(
        Output("grid-dim-row-count", "data", allow_duplicate=True),
        Input("add-dim-filters", "n_clicks"),
        State("grid-dim-row-count", "data"),
        prevent_initial_call=True,
    )
    def add_dimension_filter_row(n_clicks: int, current: int | None) -> int | object:
        """
        Purpose:
            Add one more dimension field/value filter row up to the maximum count.

        Internal Logic:
            1. Triggers on Add More click count.
            2. Cap increments to `GRID_FILTER_ROW_MAX` with minimum baseline.

        Example invocation:
            add_dimension_filter_row(1, 5)
        """

        if not n_clicks:
            return no_update
        return min(int(current or GRID_FILTER_ROW_MIN) + 1, GRID_FILTER_ROW_MAX)

    @app.callback(
        Output("grid-mea-row-count", "data", allow_duplicate=True),
        Input("add-mea-filters", "n_clicks"),
        State("grid-mea-row-count", "data"),
        prevent_initial_call=True,
    )
    def add_measure_filter_row(n_clicks: int, current: int | None) -> int | object:
        """
        Purpose:
            Add one more measure field/range filter row up to the maximum count.

        Internal Logic:
            1. Triggers on Add More click for measure area.
            2. Reuses the same high bound as dimension rows.

        Example invocation:
            add_measure_filter_row(1, 5)
        """

        if not n_clicks:
            return no_update
        return min(int(current or GRID_FILTER_ROW_MIN) + 1, GRID_FILTER_ROW_MAX)

    @app.callback(
        Output("grid-dim-filters-container", "children"),
        Input("metadata-store", "data"),
        Input("grid-dim-row-count", "data"),
        State({"type": "grid-dim-field", "index": ALL}, "value"),
        State({"type": "grid-dim-value", "index": ALL}, "value"),
    )
    def build_dimension_filter_container(
        metadata_payload: dict[str, object] | None,
        row_count: int | None,
        current_fields: list[str | None] | None,
        current_values: list[object] | None,
    ) -> list[object]:
        """
        Purpose:
            Render the Data Grid left-column dimension filter row stack.

        Internal Logic:
            1. Rebuilds rows from metadata and row-count store.
            2. Uses a minimum of five filter rows and caps at the maximum.
            3. Returns a helpful placeholder when no dataset is loaded.

        Example invocation:
            build_dimension_filter_container({"fields": []}, 5)
        """

        count = int(row_count or GRID_FILTER_ROW_MIN)
        count = min(max(count, GRID_FILTER_ROW_MIN), GRID_FILTER_ROW_MAX)
        if not metadata_payload or not metadata_payload.get("fields"):
            return [html.P("Load and profile a file to use dimension filters.", className="fa-muted")]
        metadata = _metadata_from_json(metadata_payload)
        dim_options = [
            {"label": _format_field_name_display(f.field_name), "value": f.field_name}
            for f in metadata.fields
            if f.field_dtype == "D"
        ]
        fields = current_fields or []
        values = current_values or []
        rows: list[html.Div] = []
        for index in range(count):
            selected_field = fields[index] if index < len(fields) else None
            raw_values = values[index] if index < len(values) else None
            selected_values = _normalize_multi_filter_values(raw_values)
            rows.append(_build_dim_filter_row(index, dim_options, selected_field, selected_values))
        return rows

    @app.callback(
        Output("grid-mea-filters-container", "children"),
        Input("metadata-store", "data"),
        Input("grid-mea-row-count", "data"),
        State({"type": "grid-mea-field", "index": ALL}, "value"),
        State({"type": "grid-mea-range", "index": ALL}, "value"),
    )
    def build_measure_filter_container(
        metadata_payload: dict[str, object] | None,
        row_count: int | None,
        current_fields: list[str | None] | None,
        current_ranges: list[object] | None,
    ) -> list[object]:
        """
        Purpose:
            Render the Data Grid right-column measure filter row stack.

        Internal Logic:
            1. Mirrors the dimension filter builder for measure-typed fields.
            2. Pairs with multi-select range options populated in separate callbacks.
            3. Shows a placeholder until metadata exists.

        Example invocation:
            build_measure_filter_container({"fields": []}, 5)
        """

        count = int(row_count or GRID_FILTER_ROW_MIN)
        count = min(max(count, GRID_FILTER_ROW_MIN), GRID_FILTER_ROW_MAX)
        if not metadata_payload or not metadata_payload.get("fields"):
            return [html.P("Load and profile a file to use measure filters.", className="fa-muted")]
        metadata = _metadata_from_json(metadata_payload)
        mea_options = [
            {"label": _format_field_name_display(f.field_name), "value": f.field_name}
            for f in metadata.fields
            if f.field_dtype == "M"
        ]
        fields = current_fields or []
        ranges = current_ranges or []
        rows: list[html.Div] = []
        for index in range(count):
            selected_field = fields[index] if index < len(fields) else None
            raw_ranges = ranges[index] if index < len(ranges) else None
            selected_ranges = _normalize_multi_filter_values(raw_ranges)
            rows.append(_build_mea_filter_row(index, mea_options, selected_field, selected_ranges))
        return rows

    @app.callback(
        Output({"type": "grid-dim-field", "index": ALL}, "options"),
        Input("metadata-store", "data"),
        Input({"type": "grid-dim-field", "index": ALL}, "value"),
    )
    def cascade_dimension_field_options(
        metadata_payload: dict[str, object] | None,
        selected_fields: list[str | None] | None,
    ) -> list[list[dict[str, str]]]:
        """
        Purpose:
            Enforce cascading unique field selection across all dimension dropdown rows.

        Internal Logic:
            1. Reads all available Dimension (`D`) fields from metadata.
            2. Computes per-row options excluding selections made in other rows.
            3. Returns row-index-aligned options list for ALL pattern outputs.

        Example invocation:
            options = cascade_dimension_field_options(meta_payload, ["REGION", None])
        """

        rows = selected_fields or []
        if not metadata_payload:
            return [[] for _ in rows]
        metadata = _metadata_from_json(metadata_payload)
        all_dim_fields = [field.field_name for field in metadata.fields if field.field_dtype == "D"]
        return _build_cascading_field_options(all_dim_fields, rows)

    @app.callback(
        Output({"type": "grid-mea-field", "index": ALL}, "options"),
        Input("metadata-store", "data"),
        Input({"type": "grid-mea-field", "index": ALL}, "value"),
    )
    def cascade_measure_field_options(
        metadata_payload: dict[str, object] | None,
        selected_fields: list[str | None] | None,
    ) -> list[list[dict[str, str]]]:
        """
        Purpose:
            Enforce cascading unique field selection across all measure dropdown rows.

        Internal Logic:
            1. Reads all available Measure (`M`) fields from metadata.
            2. Computes per-row options excluding selections made in other rows.
            3. Returns row-index-aligned options list for ALL pattern outputs.

        Example invocation:
            options = cascade_measure_field_options(meta_payload, ["SALES", None])
        """

        rows = selected_fields or []
        if not metadata_payload:
            return [[] for _ in rows]
        metadata = _metadata_from_json(metadata_payload)
        all_mea_fields = [field.field_name for field in metadata.fields if field.field_dtype == "M"]
        return _build_cascading_field_options(all_mea_fields, rows)

    @app.callback(
        Output({"type": "grid-dim-value", "index": MATCH}, "options"),
        Output({"type": "grid-dim-value", "index": MATCH}, "value"),
        Input({"type": "grid-dim-field", "index": MATCH}, "value"),
        State("run-state", "data"),
        State({"type": "grid-dim-value", "index": MATCH}, "value"),
        prevent_initial_call=False,
    )
    def update_dimension_value_options_matched(
        field_name: str | None,
        run_state: dict[str, object] | None,
        current_value: list[str] | str | None,
    ) -> tuple[list[dict[str, str]], list[str] | None]:
        """
        Purpose:
            Preload sorted unique options for a dimension value multi-select row.

        Internal Logic:
            1. Reuses `_build_dimension_value_options` for option labels.
            2. Trims the selected value list when options change and become invalid.
            3. Resets the selection when the field is cleared or unknown.

        Example invocation:
            update_dimension_value_options_matched("A", run_state, ["1"])
        """

        if not field_name or not run_state:
            return [], None
        data_path = Path(str(run_state.get("data_path", "")))
        if not data_path.exists():
            return [], None
        dataframe = _read_parquet_columns(data_path, [field_name])
        options = _build_dimension_value_options(dataframe, field_name)
        valid = {o["value"] for o in options}
        if isinstance(current_value, list):
            next_vals = [v for v in current_value if v in valid]
        elif current_value in valid:
            next_vals = [str(current_value)]
        else:
            next_vals = []
        return options, (next_vals if next_vals else None)

    @app.callback(
        Output({"type": "grid-dim-value", "index": MATCH}, "placeholder"),
        Input({"type": "grid-dim-field", "index": MATCH}, "value"),
        prevent_initial_call=False,
    )
    def update_dimension_value_placeholder_matched(field_name: str | None) -> str:
        """
        Purpose:
            Set dimension multi-select guidance text to include the chosen field.

        Internal Logic:
            1. Uses `Select values for the` prefix when a field is chosen.
            2. Preserves a generic message when the field is empty.

        Example invocation:
            update_dimension_value_placeholder_matched("Region")
        """

        if not field_name:
            return "Select values for the dimension"
        return f"Select values for the {_format_field_name_display(field_name)}"

    @app.callback(
        Output({"type": "grid-mea-range", "index": MATCH}, "options"),
        Output({"type": "grid-mea-range", "index": MATCH}, "value"),
        Input({"type": "grid-mea-field", "index": MATCH}, "value"),
        State("run-state", "data"),
        State({"type": "grid-mea-range", "index": MATCH}, "value"),
        prevent_initial_call=False,
    )
    def update_measure_range_options_matched(
        field_name: str | None,
        run_state: dict[str, object] | None,
        current_value: list[str] | str | None,
    ) -> tuple[list[dict[str, str]], list[str] | None]:
        """
        Purpose:
            Preload ten min/max range segments for a measure range multi-select row.

        Internal Logic:
            1. Delegates to `_build_measure_range_options` for `between` value strings.
            2. Keeps only still-valid range selections on field change.
            3. Clears selections that no longer map to a generated option.

        Example invocation:
            update_measure_range_options_matched("amount", run_state, None)
        """

        if not field_name or not run_state:
            return [], None
        data_path = Path(str(run_state.get("data_path", "")))
        if not data_path.exists():
            return [], None
        dataframe = _read_parquet_columns(data_path, [field_name])
        options = _build_measure_range_options(dataframe, field_name)
        valid = {o["value"] for o in options}
        if isinstance(current_value, list):
            next_vals = [v for v in current_value if v in valid]
        elif current_value in valid:
            next_vals = [str(current_value)]
        else:
            next_vals = []
        return options, (next_vals if next_vals else None)

    @app.callback(
        Output({"type": "grid-mea-range", "index": MATCH}, "placeholder"),
        Input({"type": "grid-mea-field", "index": MATCH}, "value"),
        prevent_initial_call=False,
    )
    def update_measure_range_placeholder_matched(field_name: str | None) -> str:
        """
        Purpose:
            Set measure range multi-select guidance to include the chosen field.

        Internal Logic:
            1. Shows `Select Range for the` plus field name when a measure is set.
            2. Preserves a generic line when the measure is empty.

        Example invocation:
            update_measure_range_placeholder_matched("Revenue")
        """

        if not field_name:
            return "Select Range for the measure"
        return f"Select Range for the {_format_field_name_display(field_name)}"

    @app.callback(
        Output("grid-column-offcanvas", "is_open"),
        Input("grid-column-menu-toggle", "n_clicks"),
        State("grid-column-offcanvas", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_grid_column_drawer(n_clicks: int, is_open: bool) -> bool:
        """
        Purpose:
            Toggle the Data Grid column hide/unhide drawer from hamburger click.

        Internal Logic:
            1. Listens to hamburger button clicks.
            2. Flips current drawer visibility boolean.
            3. Returns the next drawer state.

        Example invocation:
            state = toggle_grid_column_drawer(1, False)
        """

        del n_clicks
        return not is_open

    @app.callback(
        Output("grid-visible-columns", "options"),
        Output("grid-visible-columns", "value"),
        Output("grid-col-select-all", "value"),
        Output("grid-col-deselect-all", "value"),
        Input("metadata-store", "data"),
        Input("grid-column-search", "value"),
        Input("grid-col-select-all", "value"),
        Input("grid-col-deselect-all", "value"),
        State("grid-visible-columns", "value"),
        prevent_initial_call=False,
    )
    def load_grid_column_visibility_options(
        metadata_payload: dict[str, object] | None,
        search_text: str | None,
        select_all_checked: bool | None,
        deselect_all_checked: bool | None,
        current_visible: list[str] | None,
    ) -> tuple[list[dict[str, str]], list[str], bool, bool]:
        """
        Purpose:
            Populate sorted column hide/unhide options with search and select actions.

        Internal Logic:
            1. Builds sorted column names from metadata.
            2. Applies optional search text to narrow displayed checklist options.
            3. Supports Select All / Deselect All actions and resets action checkboxes.
            4. Defaults to all columns visible and ensures primary keys remain selected.

        Example invocation:
            options, value = load_grid_column_visibility_options(metadata_payload, None)
        """

        if not metadata_payload:
            return [], [], False, False
        metadata = _metadata_from_json(metadata_payload)
        all_columns = sorted({field.field_name for field in metadata.fields})
        valid = set(all_columns)
        current = [column for column in (current_visible or []) if column in valid]
        if not current:
            pk_columns = [key for key in metadata.file_keys if key in valid]
            current = list(dict.fromkeys(all_columns + pk_columns))
        trigger = ctx.triggered_id
        if trigger == "grid-col-select-all" and select_all_checked:
            current = list(all_columns)
        elif trigger == "grid-col-deselect-all" and deselect_all_checked:
            current = []
        search = (search_text or "").strip().lower()
        filtered_columns = [col for col in all_columns if search in col.lower()] if search else all_columns
        options = [{"label": _format_field_name_display(column), "value": column} for column in filtered_columns]
        pk_columns = [key for key in metadata.file_keys if key in valid]
        # Always keep PKs selected while allowing other columns to be deselected.
        current = list(dict.fromkeys(current + pk_columns))
        return options, current, False, False

    @app.callback(
        Output("chart-type", "value"),
        Input("chart-dimension", "value"),
        Input("chart-measure", "value"),
        State("metadata-store", "data"),
        prevent_initial_call=True,
    )
    def auto_recommend_chart(
        dimension: str | None,
        measure: str | None,
        metadata_payload: dict[str, object] | None,
    ) -> str | object:
        """
        Purpose:
            Auto-select a recommended chart type from field metadata.

        Internal Logic:
            1. Reads type map from metadata payload.
            2. Computes recommendation for selected fields.
            3. Leaves chart type unchanged when selections are incomplete.

        Example invocation:
            chart_type = auto_recommend_chart("date", "sales", meta)
        """

        if not metadata_payload or not dimension or not measure:
            return no_update
        metadata = _metadata_from_json(metadata_payload)
        type_map = {field.field_name: field.field_type for field in metadata.fields}
        return recommend_chart(type_map, dimension, measure)

    @app.callback(
        Output("analysis-chart", "figure"),
        Output("selected-stats", "children"),
        Output("quality-summary", "children"),
        Input("submit-chart", "n_clicks"),
        State("run-state", "data"),
        State("metadata-store", "data"),
        State("quick-stats-store", "data"),
        State("quality-store", "data"),
        State("chart-dimension", "value"),
        State("chart-measure", "value"),
        State("chart-color-dimension", "value"),
        State("chart-type", "value"),
        prevent_initial_call=True,
    )
    def render_analysis(
        n_clicks: int,
        run_state: dict[str, object] | None,
        metadata_payload: dict[str, object] | None,
        quick_stats_payload: dict[str, dict[str, object]] | None,
        quality_payload: dict[str, object] | None,
        dimension: str | None,
        measure: str | None,
        color_dimension: str | None,
        chart_type: str | None,
    ) -> tuple[object, html.Div, html.Div]:
        """
        Purpose:
            Build chart, selected-field stats, and quality summary output.

        Internal Logic:
            1. Loads run-scoped parquet dataset from saved state.
            2. Builds the requested Plotly chart from selected fields.
            3. Displays stats and quality JSON snapshots for transparency.

        Example invocation:
            fig, stats_text, quality_text = render_analysis(...)
        """

        del n_clicks
        if not run_state or not metadata_payload:
            return (
                build_chart(pd.DataFrame(), "Bar", "", ""),
                html.Div("No run loaded.", className="fa-card fa-muted"),
                html.Div("No quality summary.", className="fa-card fa-muted"),
            )

        data_path = Path(str(run_state.get("data_path", "")))
        if not data_path.exists():
            return (
                build_chart(pd.DataFrame(), "Bar", "", ""),
                html.Div("Run dataset missing.", className="fa-card fa-muted"),
                html.Div("No quality summary.", className="fa-card fa-muted"),
            )
        dataframe = _read_parquet_columns(data_path)
        figure = build_chart(dataframe, chart_type or "Bar", dimension or "", measure or "", color_dimension)
        quick_stats_payload = quick_stats_payload or {}
        selected = {}
        for key in [dimension, measure, color_dimension]:
            if key and key in quick_stats_payload:
                selected[key] = quick_stats_payload[key]
        formatted_selected = _format_nested_values(selected)
        formatted_quality_payload = _format_nested_values(quality_payload or {})
        labeled_selected: dict[str, object] = {}
        metadata_obj = _metadata_from_json(metadata_payload)
        if isinstance(formatted_selected, dict):
            for raw_key, raw_value in formatted_selected.items():
                labeled_selected[_field_label(str(raw_key), metadata_obj)] = raw_value
        return (
            figure,
            _render_selected_stats_card(labeled_selected),
            _render_quality_summary_card(formatted_quality_payload if isinstance(formatted_quality_payload, dict) else {}),
        )

    @app.callback(
        Output("data-grid-bottom-pane", "children"),
        Output("export-status", "children"),
        Output("download-filtered-csv", "data"),
        Output("grid-server-state", "data"),
        Input("apply-filters", "n_clicks"),
        Input("export-filtered", "n_clicks"),
        Input("analysis-chart", "clickData"),
        Input("grid-sort-column-names", "value"),
        Input("grid-visible-columns", "value"),
        State("run-state", "data"),
        State("metadata-store", "data"),
        State({"type": "grid-dim-field", "index": ALL}, "value"),
        State({"type": "grid-dim-value", "index": ALL}, "value"),
        State({"type": "grid-mea-field", "index": ALL}, "value"),
        State({"type": "grid-mea-range", "index": ALL}, "value"),
        State("chart-dimension", "value"),
        prevent_initial_call=True,
    )
    def render_filtered_grid(
        filter_clicks: int,
        export_clicks: int,
        click_data: dict[str, object] | None,
        sort_column_mode: list[str] | None,
        visible_columns: list[str] | None,
        run_state: dict[str, object] | None,
        metadata_payload: dict[str, object] | None,
        grid_dim_fields: list[str | None] | None,
        grid_dim_values: list[object] | None,
        grid_mea_fields: list[str | None] | None,
        grid_mea_ranges: list[object] | None,
        chart_dimension: str | None,
    ) -> tuple[html.Div, str, object, dict[str, object]]:
        """
        Purpose:
            Apply filters and render Tab 2 table with optional CSV export.

        Internal Logic:
            1. Reads run dataset and metadata from state stores.
            2. Applies multi-row Data Grid dimension and measure filters.
            3. Optionally reorders data columns alphabetically for display.
            4. Renders a virtualized grid with a pinned row number column and exports on demand.

        Example invocation:
            table, status = render_filtered_grid(...)
        """

        del filter_clicks
        if not run_state or not metadata_payload:
            return html.Div("Load a dataset first."), "", no_update, {"enabled": False}

        data_path = Path(str(run_state.get("data_path", "")))
        if not data_path.exists():
            return html.Div("Run dataset no longer exists."), "", no_update, {"enabled": False}

        metadata = _metadata_from_json(metadata_payload)
        dataframe = _read_parquet_columns(data_path)
        dim_rows = _pair_field_and_token_rows(grid_dim_fields, grid_dim_values)
        mea_rows = _pair_field_and_token_rows(grid_mea_fields, grid_mea_ranges)
        filtered = apply_data_grid_filters(dataframe, metadata, dim_rows, mea_rows)
        triggered_id = ctx.triggered_id
        # Apply chart-click filtering only on direct chart click events.
        # This prevents sort/export/filter actions from reusing stale clickData.
        effective_click_data = click_data if triggered_id == "analysis-chart" else None
        filtered, click_status = _apply_chart_click_filter(filtered, effective_click_data, chart_dimension)
        sort_names = bool(sort_column_mode) and "on" in (sort_column_mode or [])
        ordered = _reorder_dataframe_columns(filtered, sort_names)
        total_rows = int(len(ordered))
        total_cols = int(len(ordered.columns))
        total_cells = total_rows * total_cols
        render_blocked = (
            total_rows > GRID_RENDER_BLOCK_ROW_THRESHOLD or total_cells > GRID_RENDER_BLOCK_CELL_THRESHOLD
        )
        preview_limit = config.max_preview_rows
        preview = ordered.head(preview_limit).copy()
        display_preview = _format_measure_columns_for_display(preview, metadata)
        selected_visible = set(visible_columns or [])

        row_number_col = {
            "headerName": "Record",
            "colId": "__line_number__",
            "valueGetter": {"function": "params.node.rowIndex + 1"},
            "sortable": False,
            "filter": False,
            "resizable": True,
            "width": 86,
            "maxWidth": 96,
            "type": "rightAligned",
            "pinned": "left",
        }
        data_column_defs = _build_grid_column_defs(display_preview, metadata)
        for definition in data_column_defs:
            field_name = str(definition.get("field", ""))
            definition["hide"] = bool(selected_visible) and field_name not in selected_visible
        column_defs = [row_number_col] + data_column_defs

        table = dag.AgGrid(
            id="data-grid-table",
            rowData=display_preview.to_dict("records"),
            columnDefs=column_defs,
            defaultColDef={"floatingFilter": True, "minWidth": 92, "maxWidth": 280},
            className="ag-theme-alpine fa-table-wrap",
            columnSize="autoSize",
            columnSizeOptions={"skipHeader": False},
            dashGridOptions={
                "pagination": True,
                "paginationPageSize": 100,
                "animateRows": True,
                "suppressColumnVirtualisation": False,
                "enableRangeSelection": True,
                "copyHeadersToClipboard": True,
                "enableCellTextSelection": True,
                "rowBuffer": 10,
            },
        )

        export_status = click_status
        download_payload: object = no_update
        server_state: dict[str, object] = {"enabled": False}
        content_blocks: list[object] = []
        if render_blocked:
            session_id = str(run_state.get("session_id", ""))
            run_id = str(run_state.get("run_id", ""))
            runtime = registry.get(session_id, run_id)
            server_grid_path = (
                runtime.run_root / "grid_server_filtered.parquet" if runtime else data_path.parent / "grid_server_filtered.parquet"
            )
            ordered.to_parquet(server_grid_path, index=False)
            server_table = dag.AgGrid(
                id="data-grid-table",
                columnDefs=column_defs,
                defaultColDef={"floatingFilter": True, "minWidth": 92, "maxWidth": 280},
                className="ag-theme-alpine fa-table-wrap",
                rowModelType="infinite",
                columnSize="autoSize",
                columnSizeOptions={"skipHeader": False},
                dashGridOptions={
                    "cacheBlockSize": 500,
                    "maxBlocksInCache": 8,
                    "pagination": False,
                    "animateRows": True,
                    "suppressColumnVirtualisation": False,
                    "enableRangeSelection": True,
                    "copyHeadersToClipboard": True,
                    "enableCellTextSelection": True,
                    "rowBuffer": 10,
                },
            )
            content_blocks.append(_build_large_data_guard_message(total_rows, total_cols, preview_limit))
            guard_note = (
                f"Large result guard active ({total_rows:,} rows, {total_cols:,} cols). "
                "Using server-side pagination/filtering mode. Export CSV for full output."
            )
            export_status = f"{click_status} | {guard_note}" if click_status else guard_note
            content_blocks.append(server_table)
            server_state = {"enabled": True, "parquet_path": str(server_grid_path)}
        else:
            content_blocks.append(table)
        if triggered_id == "export-filtered" and export_clicks:
            session_id = str(run_state.get("session_id"))
            run_id = str(run_state.get("run_id"))
            runtime = registry.get(session_id, run_id)
            if runtime:
                export_path = runtime.run_root / "filtered_export.csv"
                # Export file column order (unsorted) unless the user also wants sorted file.
                export_frame = _reorder_dataframe_columns(filtered, sort_names)
                export_frame.to_csv(export_path, index=False, encoding="utf-8")
                download_payload = dcc.send_data_frame(export_frame.to_csv, "filtered_export.csv", index=False)
                export_status = (
                    f"{click_status} | " if click_status else ""
                ) + "Download started: filtered_export.csv"
        return html.Div(content_blocks), export_status, download_payload, server_state

    @app.callback(
        Output("data-grid-table", "getRowsResponse"),
        Input("data-grid-table", "getRowsRequest"),
        State("grid-server-state", "data"),
        prevent_initial_call=True,
    )
    def fetch_server_grid_rows(
        get_rows_request: dict[str, object] | None,
        server_state: dict[str, object] | None,
    ) -> dict[str, object] | object:
        """
        Purpose:
            Serve AG Grid infinite row requests in server-side mode for massive result sets.

        Internal Logic:
            1. Reads filtered parquet snapshot path from `grid-server-state`.
            2. Applies AG Grid filter/sort request models on server-side DataFrame.
            3. Returns requested row window and total row count to grid.

        Example invocation:
            response = fetch_server_grid_rows(request_payload, {"enabled": True, "parquet_path": "..."})
        """

        if not get_rows_request or not server_state or not bool(server_state.get("enabled")):
            return no_update
        parquet_path = Path(str(server_state.get("parquet_path", "")))
        if not parquet_path.exists():
            return {"rowData": [], "rowCount": 0}

        start_row = int(get_rows_request.get("startRow", 0) or 0)
        end_row = int(get_rows_request.get("endRow", start_row + 500) or (start_row + 500))
        filter_model = get_rows_request.get("filterModel", {})
        sort_model = get_rows_request.get("sortModel", [])

        dataframe = _read_parquet_columns(parquet_path)
        filtered = _apply_grid_filter_model(dataframe, filter_model if isinstance(filter_model, dict) else None)
        sorted_frame = _apply_grid_sort_model(filtered, sort_model if isinstance(sort_model, list) else None)
        page = sorted_frame.iloc[start_row:end_row].copy()
        row_count = int(len(sorted_frame))
        return {"rowData": page.to_dict("records"), "rowCount": row_count}

    @app.callback(
        Output("grid-copy-content", "data"),
        Input("copy-grid-button", "n_clicks"),
        State("data-grid-table", "virtualRowData"),
        State("data-grid-table", "columnDefs"),
        prevent_initial_call=True,
    )
    def build_copy_content_from_current_grid(
        n_clicks: int,
        virtual_rows: list[dict[str, object]] | None,
        column_defs: list[dict[str, object]] | None,
    ) -> str | object:
        """
        Purpose:
            Build copy payload from the currently filtered/sorted grid state on demand.

        Internal Logic:
            1. Triggers only when user clicks the copy icon button.
            2. Reads `virtualRowData` so AG Grid column-level filtering is honored.
            3. Converts visible columns and rows into Excel-friendly TSV text.

        Example invocation:
            payload = build_copy_content_from_current_grid(1, rows, defs)
        """

        if not n_clicks:
            return no_update
        content = _build_clipboard_tsv_from_grid_rows(virtual_rows, column_defs)
        return content

    app.clientside_callback(
        """
        function(content) {
            if (!content) {
                return "No grid data to copy.";
            }
            const fallbackCopy = (text) => {
                const area = document.createElement("textarea");
                area.value = text;
                area.setAttribute("readonly", "");
                area.style.position = "absolute";
                area.style.left = "-9999px";
                document.body.appendChild(area);
                area.select();
                document.execCommand("copy");
                document.body.removeChild(area);
            };
            try {
                if (navigator && navigator.clipboard && window.isSecureContext) {
                    navigator.clipboard.writeText(content);
                } else {
                    fallbackCopy(content);
                }
                return "Copied grid to clipboard.";
            } catch (e) {
                fallbackCopy(content);
                return "Copied grid to clipboard.";
            }
        }
        """,
        Output("copy-grid-feedback", "children"),
        Input("grid-copy-content", "data"),
        prevent_initial_call=True,
    )


def main() -> None:
    """
    Purpose:
        Launch the File Analyze Dash server.

    Internal Logic:
        1. Creates the configured Dash app.
        2. Loads host/port/debug values from validated config.
        3. Starts web server for interactive analysis.

    Example invocation:
        python -m fileanalyze.app
    """

    config = load_config()
    app = create_app()
    app.run(host=config.host, port=config.port, debug=config.debug)


if __name__ == "__main__":
    main()

