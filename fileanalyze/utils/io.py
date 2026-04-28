"""Portable file I/O utilities."""

from __future__ import annotations

from pathlib import Path
import tempfile


def atomic_write_text(target: Path, content: str) -> None:
    """
    Purpose:
        Persist text safely without partial-write corruption.

    Internal Logic:
        1. Writes content to a temp file in the same directory.
        2. Flushes data to disk.
        3. Replaces target with the temp file atomically.

    Example invocation:
        atomic_write_text(Path("output.json"), '{"ok": true}')
    """

    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        dir=str(target.parent),
    ) as handle:
        handle.write(content)
        handle.flush()
        temp_path = Path(handle.name)
    temp_path.replace(target)

