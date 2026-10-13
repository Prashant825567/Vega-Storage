"""Query parsing and evaluation — the `where=`/`order_by=` half of the contract.

The clients were written against ZentraGrid's query dialect and cannot change:

- `where=field:operator:value`, repeated parameters are **ANDed**
- operators: `eq`, `ne`, `gt` (and the aliases below, which cost nothing)
- `order_by=field` or `order_by=field:desc`
- `limit` (a page is at most `max_page_limit`, above which it is a **400** — not a
  silent truncation, because a silently trimmed page is what turns a screen into
  a blank tile)
- `cursor` for the next page

Filtering happens against the served document shape (`{"id", "data": {...}}`), so
a query can filter on `data.` fields by their bare name — `where=chat_id:eq:…` —
which is exactly how the panel and the app already ask.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from .errors import validation_error
from .wire import decode_cursor, encode_cursor

SUPPORTED_OPERATORS = {
    "eq": "eq",
    "=": "eq",
    "==": "eq",
    "ne": "ne",
    "!=": "ne",
    "<>": "ne",
    "gt": "gt",
    ">": "gt",
    # The panel only ever needs the three above, but the store accepted these and
    # keeping them means no query that used to work now fails.
    "gte": "gte",
    ">=": "gte",
    "lt": "lt",
    "<": "lt",
    "lte": "lte",
    "<=": "lte",
    "in": "in",
}

_ORDER_SUFFIXES = {":desc": "desc", ":asc": "asc"}


@dataclass(frozen=True)
class Filter:
    field: str
    operator: str
    value: str

    def describe(self) -> str:
        return f"{self.field}:{self.operator}:{self.value}"


@dataclass(frozen=True)
class Ordering:
    field: str
    descending: bool = False


def parse_where(raw_values: Sequence[str]) -> list[Filter]:
    """Turn repeated `where=` parameters into filters.

    The split is `1:1` per side — field, operator and value keep any further
    colons — because a value is very often a timestamp (`2026-10-13T09:00:00Z`)
    or a URL, and splitting on every colon would shred it.
    """
    filters: list[Filter] = []
    for raw in raw_values:
        text = (raw or "").strip()
        if not text:
            continue
        field, sep, rest = text.partition(":")
        if not sep:
            raise validation_error(
                f"where must look like field:operator:value, got {raw!r}"
            )
        operator_raw, sep2, value = rest.partition(":")
        if not sep2:
            raise validation_error(
                f"where must look like field:operator:value, got {raw!r}"
            )
        operator = SUPPORTED_OPERATORS.get(operator_raw.strip().lower())
        if operator is None:
            raise validation_error(
                f"Unsupported operator {operator_raw!r} in {raw!r}. "
                f"Supported: eq, ne, gt, gte, lt, lte, in"
            )
        filters.append(Filter(field=field.strip(), operator=operator, value=value))
    return filters


def parse_order_by(raw: str | None) -> Ordering | None:
    if not raw:
        return None
    text = (raw or "").strip()
    if not text:
        return None
    lowered = text.lower()
    for suffix, direction in _ORDER_SUFFIXES.items():
        if lowered.endswith(suffix):
            return Ordering(field=text[: -len(suffix)].strip(), descending=direction == "desc")
    return Ordering(field=text, descending=False)


def _lookup(document: dict[str, Any], field: str) -> Any:
    """Read a field, checking the document body first and the envelope second.

    `data.chat_id` and `chat_id` mean the same thing, so a client never has to
    know which half of the shape a field lives in.
    """
    if not field:
        return None
    body = document.get("data")
    if field in document and field != "data":
        # `rev`, `id`, `created_at` … live on the envelope.
        if not (isinstance(body, dict) and field in body):
            return document[field]
    node: Any = document
    for part in field.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
            continue
        # Fall through into the body, so `chat_id` reaches `data.chat_id`.
        if isinstance(body, dict) and part in body:
            node = body[part]
            continue
        return None
    return node


def _coerce(left: Any, right: str) -> tuple[Any, Any]:
    """Compare like with like: numbers as numbers, everything else as text.

    ISO-8601 strings compare correctly as text, which is why `gt` on a timestamp
    works without a date parser anywhere in the store.
    """
    if isinstance(left, bool):
        return left, right.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(left, (int, float)):
        try:
            return left, float(right)
        except (TypeError, ValueError):
            return left, None
    if left is None:
        return None, right
    return str(left), right


def _matches(document: dict[str, Any], flt: Filter) -> bool:
    actual = _lookup(document, flt.field)
    left, right = _coerce(actual, flt.value)

    if flt.operator == "eq":
        if isinstance(left, bool):
            return left is right
        if isinstance(left, float):
            return right is not None and left == right
        if left is None:
            return flt.value.strip().lower() in {"", "null", "none"}
        return str(left) == flt.value
    if flt.operator == "ne":
        return not _matches(document, Filter(flt.field, "eq", flt.value))
    if flt.operator == "in":
        wanted = [part.strip() for part in flt.value.split(",") if part.strip()]
        return actual is not None and str(actual) in wanted

    # Ordering comparisons: a missing value never satisfies them.
    if left is None or right is None:
        return False
    if isinstance(left, (int, float)) and not isinstance(left, bool):
        if flt.operator == "gt":
            return left > right
        if flt.operator == "gte":
            return left >= right
        if flt.operator == "lt":
            return left < right
        return left <= right
    if isinstance(left, bool):
        return False
    left_text, right_text = str(left), str(right if right is not None else "")
    if flt.operator == "gt":
        return left_text > right_text
    if flt.operator == "gte":
        return left_text >= right_text
    if flt.operator == "lt":
        return left_text < right_text
    return left_text <= right_text


def apply_filters(
    documents: Iterable[dict[str, Any]], filters: Sequence[Filter]
) -> list[dict[str, Any]]:
    if not filters:
        return list(documents)
    return [doc for doc in documents if all(_matches(doc, flt) for flt in filters)]


def sort_documents(
    documents: list[dict[str, Any]], ordering: Ordering | None
) -> list[dict[str, Any]]:
    """Sort, keeping documents that lack the field **last in both directions**.

    `sorted(..., reverse=True)` would put them first, because reversing the whole
    order moves the missing bucket to the top — and an inbox sorted by
    `sent_at:desc` that opens with a document predating the field is a bug the
    user sees on every poll. So the two groups are sorted separately and joined.
    """
    if ordering is None:
        return documents

    def value_of(document: dict[str, Any]) -> Any:
        return _lookup(document, ordering.field)

    def key(document: dict[str, Any]) -> Any:
        value = value_of(document)
        if isinstance(value, bool):
            return (0, int(value))
        if isinstance(value, (int, float)):
            return (1, float(value))
        return (2, str(value))

    present = [document for document in documents if value_of(document) is not None]
    missing = [document for document in documents if value_of(document) is None]
    present.sort(key=key, reverse=ordering.descending)
    return present + missing


@dataclass(frozen=True)
class Page:
    documents: list[dict[str, Any]]
    next_cursor: str | None


def paginate(
    documents: list[dict[str, Any]],
    *,
    limit: int,
    cursor: str | None,
) -> Page:
    """Slice one page and hand back the cursor for the next one."""
    try:
        offset = decode_cursor(cursor)
    except ValueError as exc:
        raise validation_error(str(exc)) from None

    if offset >= len(documents):
        return Page(documents=[], next_cursor=None)

    window = documents[offset : offset + limit]
    consumed = offset + len(window)
    next_cursor = encode_cursor(consumed) if consumed < len(documents) else None
    return Page(documents=window, next_cursor=next_cursor)


def check_limit(limit: int | None, maximum: int) -> int:
    """A page bigger than the ceiling is refused, never quietly trimmed."""
    if limit is None:
        return maximum
    try:
        value = int(limit)
    except (TypeError, ValueError):
        raise validation_error(f"limit must be an integer, got {limit!r}") from None
    if value <= 0:
        return maximum
    if value > maximum:
        raise validation_error(
            f"query.limit: Input should be less than or equal to {maximum}"
        )
    return value
