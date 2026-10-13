"""HTTP `Range` parsing, kept away from the web framework on purpose.

A range is the difference between a video that can be seeked and one that cannot:
without a 206 the browser either refuses to play at all (Safari) or can only start
from byte zero (Chrome). It is also pure arithmetic, so it lives here where it can
be tested directly instead of only through a running server.
"""

from __future__ import annotations

import re

from .errors import StorageError

_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")


def parse_range(header: str | None, total: int) -> tuple[int, int]:
    """Inclusive `(start, end)` byte bounds for the request.

    Handles the three forms a client actually sends — `bytes=0-9`, `bytes=10-`
    (to the end) and `bytes=-10` (the last ten bytes) — clamps an over-long end,
    and raises `416` when the range starts past the end of the file.
    """
    if not header or total <= 0:
        return 0, max(0, total - 1)
    match = _RANGE_RE.search(header)
    if not match:
        return 0, max(0, total - 1)
    start_raw, end_raw = match.group(1), match.group(2)
    if not start_raw and not end_raw:
        return 0, max(0, total - 1)
    if not start_raw:
        suffix = int(end_raw)
        if suffix <= 0:
            return 0, max(0, total - 1)
        return max(0, total - suffix), max(0, total - 1)
    start = int(start_raw)
    end = int(end_raw) if end_raw else total - 1
    if start >= total:
        raise StorageError(
            f"Range starts at {start} but the file is {total} bytes",
            416,
            "RANGE_NOT_SATISFIABLE",
        )
    return start, min(end, max(0, total - 1))
