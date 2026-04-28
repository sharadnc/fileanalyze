"""Typed schemas for the file insights framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class FieldMetadata:
    """
    Purpose:
        Represent one field definition loaded from `${Filename}_Meta`.

    Internal Logic:
        1. Stores canonical metadata attributes for one source column.
        2. Provides normalized field/dimension descriptors for profiling/UI.
        3. Acts as the typed contract shared by ingestion, profiling, and charts.

    Example invocation:
        field = FieldMetadata(
            field_name="sales_amount",
            field_type="Num",
            field_length="10,2",
            field_desc="Total sales",
            field_dtype="M",
        )
    """

    field_name: str
    field_type: str
    field_length: str
    field_desc: str
    field_dtype: str


@dataclass(slots=True)
class MetadataBundle:
    """
    Purpose:
        Keep all metadata details parsed from the sidecar meta file.

    Internal Logic:
        1. Stores table-level primary key columns.
        2. Stores per-field descriptors.
        3. Exposes a quick lookup map for field metadata by field name.

    Example invocation:
        meta = MetadataBundle(file_keys=["id"], fields=[field])
    """

    file_keys: list[str]
    fields: list[FieldMetadata]

    def field_map(self) -> dict[str, FieldMetadata]:
        """
        Purpose:
            Build a lookup of metadata keyed by field name.

        Internal Logic:
            1. Iterates over every field definition.
            2. Uses field name as dictionary key.
            3. Returns the map for fast downstream access.

        Example invocation:
            lookup = meta.field_map()
        """

        return {item.field_name: item for item in self.fields}


@dataclass(slots=True)
class QuickStats:
    """
    Purpose:
        Represent the quick profiling result for a single field.

    Internal Logic:
        1. Stores field identity and profile type.
        2. Persists generic metrics for display and export.
        3. Holds optional top frequency values for categorical fields.

    Example invocation:
        stats = QuickStats(field_name="region", stat_type="Char", metrics={"null_count": 0})
    """

    field_name: str
    stat_type: str
    metrics: dict[str, Any] = field(default_factory=dict)
    top_values: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class RuntimeContext:
    """
    Purpose:
        Track one isolated user execution run and all related artifacts.

    Internal Logic:
        1. Captures session/run IDs used for multi-user isolation.
        2. Stores paths to run-scoped assets like parquet and exports.
        3. Persists metadata and stats payloads used by callbacks.

    Example invocation:
        context = RuntimeContext(
            session_id="session-1",
            run_id="run-1",
            run_root=Path("fileanalyze/output/session-1/run-1"),
            data_path=Path("fileanalyze/output/session-1/run-1/data.parquet"),
        )
    """

    session_id: str
    run_id: str
    run_root: Path
    data_path: Path
    metadata: MetadataBundle
    quick_stats: dict[str, QuickStats]

