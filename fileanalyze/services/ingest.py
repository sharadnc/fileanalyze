"""Ingestion and metadata parsing services."""

from __future__ import annotations

from pathlib import Path
import uuid

import pandas as pd

from fileanalyze.models.schemas import FieldMetadata, MetadataBundle

META_KEYS: set[str] = {
    "FileKey",
    "FieldName",
    "FieldType",
    "FieldLength",
    "FieldDesc",
    "FieldDType",
}


def resolve_meta_path(data_path: Path) -> Path:
    """
    Purpose:
        Locate the `${Filename}_Meta` file for a source dataset.

    Internal Logic:
        1. Builds default sidecar path by appending `_Meta`.
        2. Validates that the sidecar exists.
        3. Raises an actionable error when not found.

    Example invocation:
        meta_path = resolve_meta_path(Path("orders.csv"))
    """

    meta_path = data_path.with_name(f"{data_path.name}_Meta")
    if not meta_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {meta_path}")
    return meta_path


def parse_metadata(meta_path: Path) -> MetadataBundle:
    """
    Purpose:
        Parse pipe-delimited sidecar metadata into typed schema objects.

    Internal Logic:
        1. Reads all metadata lines as UTF-8 text.
        2. Supports either key-value style (`Key|Value`) or tabular style rows.
        3. Aggregates global `FileKey` values and per-field attributes.
        4. Validates mandatory attributes before returning `MetadataBundle`.

    Example invocation:
        metadata = parse_metadata(Path("orders.csv_Meta"))
    """

    text = meta_path.read_text(encoding="utf-8-sig")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        raise ValueError(f"Metadata file is empty: {meta_path}")

    if _looks_like_compact_metadata(lines):
        return _parse_compact_metadata(lines[0])

    headers = [part.strip() for part in lines[0].split("|")]
    if set(headers).issuperset(META_KEYS):
        return _parse_tabular_metadata(lines)
    return _parse_key_value_metadata(lines)


def _looks_like_compact_metadata(lines: list[str]) -> bool:
    """
    Purpose:
        Detect compact single-line metadata used by external sample files.

    Internal Logic:
        1. Accepts only one non-empty metadata line.
        2. Requires both comma and pipe delimiters.
        3. Excludes standard key-value and tabular metadata signatures.

    Example invocation:
        is_compact = _looks_like_compact_metadata(lines)
    """

    if len(lines) != 1:
        return False
    line = lines[0]
    if "|" not in line or "," not in line:
        return False
    return not any(token in line for token in ["FieldName|", "FieldType|", "FileKey|"])


def _normalize_compact_type(type_token: str) -> tuple[str, str]:
    """
    Purpose:
        Split compact type tokens (e.g., Char50) into type and length.

    Internal Logic:
        1. Trims and inspects raw type token.
        2. For char declarations, extracts numeric suffix as field length.
        3. Returns normalized `(field_type, field_length)` tuple.

    Example invocation:
        field_type, field_length = _normalize_compact_type("Char50")
    """

    cleaned = type_token.strip()
    lowered = cleaned.lower()
    if lowered.startswith("char"):
        suffix = cleaned[4:].strip()
        return "Char", suffix
    if lowered in {"num", "number", "numeric"}:
        return "Num", ""
    if lowered == "date":
        return "Date", ""
    if lowered == "datetime":
        return "Datetime", ""
    return cleaned.title(), ""


def _to_compact_field(tokens: list[str]) -> FieldMetadata:
    """
    Purpose:
        Convert compact metadata tokens into one canonical `FieldMetadata`.

    Internal Logic:
        1. Supports 4-token layout: `Field|Type|Desc|DType`.
        2. Supports 5-token layout: `Field|Type|Length|Desc|DType`.
        3. Normalizes type/length when length is embedded in type token.
        4. Preserves explicit length when provided as separate token.

    Example invocation:
        field = _to_compact_field(["SUMLEV", "Char", "3", "Level", "D"])
    """

    if len(tokens) >= 5:
        field_name = tokens[0]
        type_token = tokens[1]
        raw_length = tokens[2]
        field_desc = tokens[3]
        field_dtype = tokens[4]
        normalized_type, inferred_length = _normalize_compact_type(type_token)
        field_length = raw_length if raw_length else inferred_length
        return FieldMetadata(
            field_name=field_name,
            field_type=normalized_type,
            field_length=field_length,
            field_desc=field_desc,
            field_dtype=field_dtype.upper(),
        )

    field_name = tokens[0]
    type_token = tokens[1]
    field_desc = tokens[2]
    field_dtype = tokens[3]
    field_type, field_length = _normalize_compact_type(type_token)
    return FieldMetadata(
        field_name=field_name,
        field_type=field_type,
        field_length=field_length,
        field_desc=field_desc,
        field_dtype=field_dtype.upper(),
    )


def _parse_compact_metadata(line: str) -> MetadataBundle:
    """
    Purpose:
        Parse compact single-line metadata format into canonical schema.

    Internal Logic:
        1. Splits metadata into comma-separated segments.
        2. Treats first segment as first FileKey component.
        3. Uses first token of second segment as additional FileKey.
        4. Parses every field descriptor in `Field|Type|Desc|DType` form.
        5. Supports first descriptor carrying an extra leading key token.

    Example invocation:
        bundle = _parse_compact_metadata(raw_line)
    """

    segments = [part.strip() for part in line.split(",") if part.strip()]
    if len(segments) < 2:
        raise ValueError("Compact metadata requires file keys and at least one field descriptor.")

    descriptor_start = next((index for index, segment in enumerate(segments) if "|" in segment), -1)
    if descriptor_start <= 0:
        raise ValueError("Compact metadata requires explicit file keys and field descriptors.")

    # Some files provide multiple comma-separated keys before the first field descriptor.
    file_keys: list[str] = segments[:descriptor_start]
    descriptor_segments = segments[descriptor_start:]
    fields: list[FieldMetadata] = []

    for index, segment in enumerate(descriptor_segments):
        tokens = [token.strip() for token in segment.split("|") if token.strip()]
        if len(tokens) < 4:
            continue
        if index == 0 and len(tokens) >= 6:
            # Some compact files prepend an additional key before first field tokens.
            if tokens[0] not in file_keys:
                file_keys.append(tokens[0])
            field_tokens = tokens[1:]
        elif index == 0 and len(tokens) >= 5 and len(file_keys) <= 1:
            # Legacy compact shape can embed one extra key in the first descriptor.
            if tokens[0] not in file_keys:
                file_keys.append(tokens[0])
            field_tokens = tokens[1:]
        else:
            field_tokens = tokens
        fields.append(_to_compact_field(field_tokens))
    return _validate_metadata_bundle(file_keys, fields)


def _parse_tabular_metadata(lines: list[str]) -> MetadataBundle:
    """
    Purpose:
        Parse metadata represented as table rows with a header line.

    Internal Logic:
        1. Uses the first line as the metadata column header.
        2. Builds one dictionary per row.
        3. Extracts `FileKey` once and maps all rows to `FieldMetadata`.

    Example invocation:
        bundle = _parse_tabular_metadata(lines)
    """

    header = [part.strip() for part in lines[0].split("|")]
    rows: list[dict[str, str]] = []
    for line in lines[1:]:
        values = [part.strip() for part in line.split("|")]
        row: dict[str, str] = {}
        for index, key in enumerate(header):
            row[key] = values[index] if index < len(values) else ""
        rows.append(row)

    file_keys: list[str] = []
    fields: list[FieldMetadata] = []
    for row in rows:
        if not file_keys and row.get("FileKey"):
            file_keys = [key.strip() for key in row["FileKey"].split(",") if key.strip()]
        fields.append(
            FieldMetadata(
                field_name=row.get("FieldName", ""),
                field_type=row.get("FieldType", "Char"),
                field_length=row.get("FieldLength", ""),
                field_desc=row.get("FieldDesc", ""),
                field_dtype=(row.get("FieldDType", "D") or "D").upper(),
            )
        )
    return _validate_metadata_bundle(file_keys, fields)


def _parse_key_value_metadata(lines: list[str]) -> MetadataBundle:
    """
    Purpose:
        Parse metadata stored as key-value pairs grouped by field.

    Internal Logic:
        1. Parses each `Key|Value` row and ignores malformed rows.
        2. Captures `FileKey` globally whenever present.
        3. Starts a new field record on each repeated `FieldName`.
        4. Converts completed dictionaries into `FieldMetadata`.

    Example invocation:
        bundle = _parse_key_value_metadata(lines)
    """

    file_keys: list[str] = []
    field_records: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in lines:
        parts = [part.strip() for part in line.split("|", 1)]
        if len(parts) != 2:
            continue
        key, value = parts
        if key not in META_KEYS:
            continue
        if key == "FileKey":
            file_keys = [item.strip() for item in value.split(",") if item.strip()]
            continue
        if key == "FieldName" and current.get("FieldName"):
            field_records.append(current)
            current = {}
        current[key] = value
    if current.get("FieldName"):
        field_records.append(current)

    fields = [
        FieldMetadata(
            field_name=record.get("FieldName", ""),
            field_type=record.get("FieldType", "Char"),
            field_length=record.get("FieldLength", ""),
            field_desc=record.get("FieldDesc", ""),
            field_dtype=(record.get("FieldDType", "D") or "D").upper(),
        )
        for record in field_records
    ]
    return _validate_metadata_bundle(file_keys, fields)


def _validate_metadata_bundle(file_keys: list[str], fields: list[FieldMetadata]) -> MetadataBundle:
    """
    Purpose:
        Validate metadata integrity before downstream profiling.

    Internal Logic:
        1. Ensures at least one field definition is provided.
        2. Validates every field has a non-empty name.
        3. Normalizes field type and dimension type values.

    Example invocation:
        bundle = _validate_metadata_bundle(["id"], fields)
    """

    if not fields:
        raise ValueError("Metadata does not contain any field definitions.")
    normalized: list[FieldMetadata] = []
    for field in fields:
        if not field.field_name:
            raise ValueError("Metadata contains an empty FieldName.")
        normalized.append(
            FieldMetadata(
                field_name=field.field_name,
                field_type=(field.field_type or "Char").title(),
                field_length=field.field_length,
                field_desc=field.field_desc,
                field_dtype=(field.field_dtype or "D").upper(),
            )
        )
    return MetadataBundle(file_keys=file_keys, fields=normalized)


def load_dataframe(data_path: Path, delimiter: str) -> pd.DataFrame:
    """
    Purpose:
        Load source data into a DataFrame with delimiter control.

    Internal Logic:
        1. Reads file with UTF-8 BOM tolerance.
        2. Uses configurable delimiter from UI.
        3. Preserves raw values as strings for metadata-driven casting.

    Example invocation:
        df = load_dataframe(Path("orders.csv"), ",")
    """

    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")
    return pd.read_csv(
        data_path,
        sep=delimiter,
        dtype="string",
        encoding="utf-8-sig",
        low_memory=False,
    )


def validate_metadata_against_data(metadata: MetadataBundle, dataframe: pd.DataFrame) -> list[str]:
    """
    Purpose:
        Check schema alignment between metadata and data columns.

    Internal Logic:
        1. Compares metadata field names with DataFrame columns.
        2. Emits warnings for missing metadata columns in data.
        3. Emits warnings for data columns lacking metadata entries.

    Example invocation:
        warnings = validate_metadata_against_data(metadata, df)
    """

    warnings: list[str] = []
    data_cols = set(dataframe.columns.astype(str))
    meta_cols = {field.field_name for field in metadata.fields}
    for missing in sorted(meta_cols - data_cols):
        warnings.append(f"Metadata field missing in data: {missing}")
    for extra in sorted(data_cols - meta_cols):
        warnings.append(f"Data column missing in metadata: {extra}")
    return warnings


def create_run_paths(output_root: Path, session_id: str) -> tuple[str, Path]:
    """
    Purpose:
        Create run-scoped output directories for isolated execution.

    Internal Logic:
        1. Generates a UUID run ID.
        2. Creates nested output path using session/run IDs.
        3. Returns both run ID and created path.

    Example invocation:
        run_id, run_root = create_run_paths(Path("fileanalyze/output"), "session-a")
    """

    run_id = str(uuid.uuid4())
    run_root = output_root / session_id / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    return run_id, run_root

