"""Concurrency helpers for parallel profiling."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, TypeVar

T = TypeVar("T")


def run_parallel(tasks: list[Callable[[], T]], max_workers: int) -> list[T]:
    """
    Purpose:
        Execute multiple independent functions in parallel.

    Internal Logic:
        1. Schedules every task in a thread pool.
        2. Waits for completion and preserves successful results.
        3. Raises task exceptions directly so callers can surface failures.

    Example invocation:
        results = run_parallel([lambda: 1, lambda: 2], max_workers=2)
    """

    if not tasks:
        return []
    worker_count = max(1, min(max_workers, len(tasks)))
    results: list[T] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(task) for task in tasks]
        for future in as_completed(futures):
            results.append(future.result())
    return results

