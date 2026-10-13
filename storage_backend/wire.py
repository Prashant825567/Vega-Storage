"""The record codec: one document ⇄ one or more channel messages.

A document is stored as **one Telegram message that is edited in place**, so its
`message_id` never changes and no tombstones accumulate. Telegram caps a message
at 4096 characters, so a bigger body is split across consecutive messages and the
part number rides in the header.

Every stored message looks like this::

    ZG1|<collection>|<doc_id>|<rev>|<part>/<total>|<chunk>

- `ZG1` is the format tag. Anything in a channel that does not start with it is
  somebody's human message and is ignored by the index — which matters, because
  these are real channels an operator can also read and type in.
- the header is always six fields, so `split("|", 5)` is enough and a `|` inside
  the JSON chunk is harmless.
- `rev` is the document's revision, kept **in the message itself** rather than in
  a side database. That is what makes a cold rebuild possible at all: scan the
  channel, read every header, and the whole index comes back — no database, no
  export, no reconciliation.

There is deliberately no separate metadata store. If the process dies, the truth
is still sitting in the channel.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

TAG = "ZG1"
SEPARATOR = "|"
HEADER_FIELDS = 6

# `split(sep, maxsplit)` with maxsplit=5 yields exactly 6 fields.
_SPLIT_LIMIT = HEADER_FIELDS - 1

# Document id charset. Chosen by the caller (a heartbeat's fixed id, a uuid, an
# email slug), so it must not contain the separator or a newline, and must not be
# able to blow up a URL path.
DOC_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,127}$")

# Telegram's own limits (Bot API documents 4096/1024 for text/caption). Headers
# are subtracted by the caller through `part_chars`/`caption_part_chars`.
MAX_TEXT_CHARS = 4096
MAX_CAPTION_CHARS = 1024


def now_iso() -> str:
    """An ISO-8601 UTC timestamp with a `Z`, the shape the clients write."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def valid_doc_id(doc_id: str) -> bool:
    return bool(DOC_ID_RE.match(doc_id or ""))


def mark_channel_id(raw: int) -> int:
    """`-100…` → `-100…` (identity, for callers that think in marked ids)."""
    if raw < 0:
        return raw
    return int(f"-100{raw}")


def unmark_channel_id(marked: int) -> int:
    """`-1003597396903` → `3597396903`, which is what `PeerChannel` wants."""
    value = int(marked)
    if value < 0:
        text = str(-value)
        if text.startswith("100") and len(text) > 3:
            return int(text[3:])
        return -value
    return value


@dataclass(frozen=True)
class Record:
    """One stored message, parsed."""

    collection: str
    doc_id: str
    rev: int
    part: int
    total: int
    chunk: str

    @property
    def complete(self) -> bool:
        return self.total == 1


@dataclass(frozen=True)
class Assembled:
    """Every part of one document, joined back into its JSON body."""

    collection: str
    doc_id: str
    rev: int
    total: int
    body: str

    def data(self) -> dict[str, Any]:
        loaded = json.loads(self.body)
        return loaded if isinstance(loaded, dict) else {"value": loaded}


def encode_parts(
    collection: str,
    doc_id: str,
    rev: int,
    body: str,
    part_chars: int,
) -> list[str]:
    """Split a document body into ready-to-send message texts.

    `part_chars` is the caller's budget for the *chunk*, not the whole message;
    `storage_backend.config` picks 3000 for text and 800 for captions so the
    header always fits.
    """
    if not valid_doc_id(doc_id):
        raise ValueError(f"Invalid document id: {doc_id!r}")
    if part_chars < 1:
        raise ValueError("part_chars must be positive")

    chunks = [body[i : i + part_chars] for i in range(0, len(body), part_chars)] or [""]
    total = len(chunks)

    def header(part_no: int) -> str:
        return SEPARATOR.join(
            [TAG, collection, doc_id, str(rev), f"{part_no}/{total}"]
        ) + SEPARATOR

    return [f"{header(index + 1)}{chunk}" for index, chunk in enumerate(chunks)]


def decode(text: str | None) -> Record | None:
    """Parse one message text, or `None` when it is not one of ours."""
    if not text or not text.startswith(TAG + SEPARATOR):
        return None
    fields = text.split(SEPARATOR, _SPLIT_LIMIT)
    if len(fields) != HEADER_FIELDS:
        return None
    tag, collection, doc_id, rev_raw, progress, chunk = fields
    if tag != TAG or not collection or not doc_id:
        return None
    try:
        rev = int(rev_raw)
    except ValueError:
        return None
    part_raw, _, total_raw = progress.partition("/")
    try:
        part = int(part_raw)
        total = int(total_raw)
    except ValueError:
        return None
    if part < 1 or total < 1 or part > total:
        return None
    return Record(
        collection=collection,
        doc_id=doc_id,
        rev=rev,
        part=part,
        total=total,
        chunk=chunk,
    )


def assemble(records: list[Record]) -> Assembled | None:
    """Join a document's parts, or `None` when any part is missing.

    A document with a hole in it is worse than a missing document: it would parse
    into valid-looking JSON with fields silently absent. So an incomplete set is
    dropped, and the caller logs it.
    """
    if not records:
        return None
    ordered = sorted(records, key=lambda record: record.part)
    total = ordered[0].total
    if len(ordered) != total:
        return None
    if [record.part for record in ordered] != list(range(1, total + 1)):
        return None
    if any(record.total != total for record in ordered):
        return None
    return Assembled(
        collection=ordered[0].collection,
        doc_id=ordered[0].doc_id,
        rev=ordered[0].rev,
        total=total,
        body="".join(record.chunk for record in ordered),
    )


# --------------------------------------------------------------------------- #
# Cursor: an opaque, URL-safe token. It is just an offset today, but keeping it
# opaque means paging can become id-based later without breaking a single client.
# --------------------------------------------------------------------------- #


def encode_cursor(offset: int) -> str:
    raw = str(int(offset)).encode("ascii")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("ascii")
        return max(0, int(raw))
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise ValueError(f"Invalid cursor: {cursor!r}") from None


def to_document(id: str, rev: int, body: dict[str, Any], **extra: Any) -> dict[str, Any]:
    """The wire shape of a document, matching what ZentraGrid returns."""
    document: dict[str, Any] = {"id": id, "rev": rev, "data": body.get("data", {})}
    document["created_at"] = body.get("created_at")
    document["updated_at"] = body.get("updated_at")
    document.update(extra)
    return document
