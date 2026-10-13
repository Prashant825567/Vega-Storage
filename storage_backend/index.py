"""Layer 1: every read comes from memory; Telegram is only persistence.

Reads answering in microseconds is the entire reason this store can serve an
inbox at all — the panel polls every 10–15 seconds, and a Telegram round trip per
poll would be both slow and a flood-wait magnet. So the truth is *also* held in
this process:

* the index is rebuilt once at startup by scanning the channels (a **cold
  rebuild**), then kept current by each successful write;
* a read never touches the network;
* a write goes to Telegram **first** and only then into the index, so a failed
  edit leaves the index describing what Telegram actually holds.

Rebuild details that matter:

* Messages are grouped by `(collection, doc_id, rev)` and only the **highest rev
  with a complete set of parts** wins. That is what makes a rebuild safe while an
  edit is in flight: a half-written revision loses to the previous complete one
  instead of producing a document with silently missing fields.
* A message that is not one of ours is ignored, so the channels stay human
  readable and an operator can still type in them.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable

from .config import Config
from .telegram import StoredMessage, TelegramLike
from .wire import assemble, decode

log = logging.getLogger("storage_backend.index")


@dataclass
class IndexedDocument:
    """One document, as the process holds it."""

    collection: str
    doc_id: str
    rev: int
    key: str
    created_at: str | None
    updated_at: str | None
    data: dict[str, Any]
    # (message_id, chunk length) per part, in order — the addresses to edit.
    parts: list[tuple[int, int]] = field(default_factory=list)
    deleted: bool = False

    def envelope(self) -> dict[str, Any]:
        """The shape the clients were written against."""
        return {
            "id": self.doc_id,
            "rev": self.rev,
            "data": self.data,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @property
    def part_count(self) -> int:
        return len(self.parts)


class ChannelIndex:
    """The RAM half of the store."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._docs: dict[str, dict[str, IndexedDocument]] = {}
        # (channel key, message id) → (collection, doc id, part number) — needed
        # to find the message to edit after a rebuild, when only the doc id is
        # known by the caller.
        self._by_message: dict[tuple[str, int], tuple[str, str, int]] = {}
        self.ready = False
        self.rebuilt_at: str | None = None
        self.rebuild_errors: dict[str, str] = {}
        self.scanned_messages = 0
        self.dropped_incomplete = 0

    # ------------------------------------------------------------------ #
    # Lookups
    # ------------------------------------------------------------------ #

    def get(self, collection: str, doc_id: str) -> IndexedDocument | None:
        return self._docs.get(collection, {}).get(doc_id)

    def collection(self, collection: str) -> list[IndexedDocument]:
        return list(self._docs.get(collection, {}).values())

    def collections(self) -> list[str]:
        return sorted(self._docs)

    def documents(self, include_deleted: bool = False) -> Iterable[IndexedDocument]:
        for bucket in self._docs.values():
            for document in bucket.values():
                if include_deleted or not document.deleted:
                    yield document

    def envelope(self, collection: str, doc_id: str) -> dict[str, Any] | None:
        document = self.get(collection, doc_id)
        if document is None or document.deleted:
            return None
        return document.envelope()

    def stats(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "rebuilt_at": self.rebuilt_at,
            "scanned_messages": self.scanned_messages,
            "documents": sum(len(bucket) for bucket in self._docs.values()),
            "incomplete_dropped": self.dropped_incomplete,
            "collections": {name: len(bucket) for name, bucket in sorted(self._docs.items())},
            "rebuild_errors": dict(self.rebuild_errors),
        }

    # ------------------------------------------------------------------ #
    # Mutation (called only after Telegram has accepted the write)
    # ------------------------------------------------------------------ #

    def put(self, document: IndexedDocument) -> None:
        bucket = self._docs.setdefault(document.collection, {})
        previous = bucket.get(document.doc_id)
        if previous is not None:
            for message_id, _ in previous.parts:
                self._by_message.pop((previous.key, message_id), None)
        bucket[document.doc_id] = document
        for part_no, (message_id, _) in enumerate(document.parts, start=1):
            self._by_message[(document.key, message_id)] = (
                document.collection,
                document.doc_id,
                part_no,
            )

    def remove(self, collection: str, doc_id: str) -> None:
        bucket = self._docs.get(collection)
        if not bucket:
            return
        document = bucket.pop(doc_id, None)
        if document is not None:
            for message_id, _ in document.parts:
                self._by_message.pop((document.key, message_id), None)
        if not bucket:
            self._docs.pop(collection, None)

    def message_owner(self, key: str, message_id: int) -> tuple[str, str, int] | None:
        return self._by_message.get((key, message_id))

    # ------------------------------------------------------------------ #
    # Cold rebuild
    # ------------------------------------------------------------------ #

    async def rebuild(self, gateway: TelegramLike, *, keep: bool = False) -> None:
        """Index every channel from scratch.

        Runs once at startup (in the background, so the port opens immediately)
        and can be called again by an operator through `/v1/admin/rebuild`.
        """
        if not keep:
            self._docs = {}
            self._by_message = {}
            self.rebuild_errors = {}
            self.scanned_messages = 0
            self.dropped_incomplete = 0

        for key in self._config.channels:
            # Group parts per (collection, doc, rev) so several revisions can be
            # compared before one is chosen.
            grouped: dict[tuple[str, str, int], list[tuple[int, Any]]] = {}
            try:
                async for message in gateway.iter_history(key, self._config.scan_limit):
                    self.scanned_messages += 1
                    record = decode(message.text)
                    if record is None:
                        continue
                    grouped.setdefault(
                        (record.collection, record.doc_id, record.rev), []
                    ).append((message.id, record))
            except Exception as exc:
                self.rebuild_errors[key] = f"{type(exc).__name__}: {exc}"
                log.warning("rebuild of %s failed: %s", key, exc)
                continue

            for (collection, doc_id, rev), items in grouped.items():
                items.sort(key=lambda pair: pair[1].part)
                records = [record for _, record in items]
                joined = assemble(records)
                if joined is None:
                    # A document whose parts are not all here. Keep whatever is
                    # already indexed at a lower revision rather than a stitched
                    # document with holes in it.
                    self.dropped_incomplete += 1
                    continue
                existing = self.get(collection, doc_id)
                if existing is not None and existing.rev >= rev:
                    continue
                try:
                    body = joined.data()
                except Exception as exc:
                    log.warning("document %s/%s has unreadable JSON: %s", collection, doc_id, exc)
                    continue
                data = body.get("data")
                if not isinstance(data, dict):
                    data = {}
                document = IndexedDocument(
                    collection=collection,
                    doc_id=doc_id,
                    rev=rev,
                    key=key,
                    created_at=body.get("created_at"),
                    updated_at=body.get("updated_at"),
                    data=data,
                    parts=[(message_id, len(record.chunk)) for message_id, record in items],
                    deleted=bool(body.get("deleted")),
                )
                self.put(document)

        self.ready = True
        self.rebuilt_at = _now()
        log.info(
            "cold rebuild finished: %s documents from %s messages",
            sum(len(b) for b in self._docs.values()),
            self.scanned_messages,
        )


def _now() -> str:
    from .wire import now_iso

    return now_iso()
