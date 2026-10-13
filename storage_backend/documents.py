"""Layers 2 and 3: the write path for documents.

**Layer 2 — one lock per document.** Fifty requests aimed at the same document
queue up instead of racing: the whole read-modify-write cycle runs inside the
lock, so nothing can slip in between the revision check and the edit. Different
documents have different locks, so they still go in parallel.

**Layer 3 — `rev` + `expected_rev`.** Every document carries a revision counter.
A caller that says `expected_rev: 3` when the document is already at 4 gets a
`409 REVISION_CONFLICT` and learns its copy was stale, exactly like Firestore's
compare-and-swap. Without it, two overlapping edits silently lose one of the two.

**Order of operations, which is the crash-safety story.** Telegram is written
first, memory second. If Telegram fails, memory was never touched and the store
still describes what is actually stored. If the process dies right after the
edit, the next cold rebuild reads the channel and finds the new revision — the
worst case is a stale cache, never a lost or invented document.

Documents only. Flags whose whole value expires in a minute live in the hot store
(see `hotstore.py`), for the reason in `TELEGRAM_STORAGE_PLAN.md`: one chat
carries about one write per second, and a heartbeat per user per 20 seconds does
not fit in that budget at any scale.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

from .config import Config
from .errors import (
    already_exists,
    deleted as deleted_error,
    not_found,
    validation_error,
)
from .hotstore import HotStore
from .index import ChannelIndex, IndexedDocument
from .query import (
    Filter,
    Ordering,
    Page,
    apply_filters,
    paginate,
    sort_documents,
)
from .telegram import TelegramLike
from .wire import encode_parts, now_iso, valid_doc_id

log = logging.getLogger("storage_backend.documents")

# A document body bigger than this is refused. Telegram itself would hold far
# more, but a 64 KiB document means ~22 message edits per write, and a store that
# hammers a single chat like that is what flood waits are made of.
MAX_BODY_BYTES = 64 * 1024


class DocumentLocks:
    """Per-document `asyncio.Lock`, created on demand and dropped when idle.

    The dictionary is keyed `collection/doc_id`, and a lock is removed once
    nobody holds it — otherwise a long-lived process accumulates a lock object
    for every document it has ever touched.
    """

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._users: dict[str, int] = {}

    def _key(self, collection: str, doc_id: str) -> str:
        return f"{collection}/{doc_id}"

    def lock_for(self, collection: str, doc_id: str) -> asyncio.Lock:
        key = self._key(collection, doc_id)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        self._users[key] = self._users.get(key, 0) + 1
        return lock

    def release(self, collection: str, doc_id: str) -> None:
        key = self._key(collection, doc_id)
        remaining = self._users.get(key, 0) - 1
        if remaining <= 0:
            self._users.pop(key, None)
            lock = self._locks.get(key)
            if lock is not None and not lock.locked():
                self._locks.pop(key, None)
        else:
            self._users[key] = remaining

    @property
    def open_locks(self) -> int:
        return len(self._locks)


class DocumentStore:
    """Documents over Telegram channels, with a hot store for the short-lived ones."""

    def __init__(
        self,
        config: Config,
        gateway: TelegramLike,
        index: ChannelIndex,
        hot: HotStore | None = None,
    ) -> None:
        self._config = config
        self._gateway = gateway
        self._index = index
        self._hot = hot or HotStore(config)
        self._locks = DocumentLocks()

    @property
    def index(self) -> ChannelIndex:
        return self._index

    @property
    def hot(self) -> HotStore:
        return self._hot

    # ------------------------------------------------------------------ #
    # Reads — memory only
    # ------------------------------------------------------------------ #

    def get(self, collection: str, doc_id: str) -> dict[str, Any] | None:
        collection = self._collection(collection)
        doc_id = self._doc_id(doc_id)
        if self._config.is_hot(collection):
            return self._hot.get(collection, doc_id)
        return self._index.envelope(collection, doc_id)

    def query(
        self,
        collection: str,
        *,
        filters: list[Filter],
        ordering: Ordering | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        collection = self._collection(collection)
        if self._config.is_hot(collection):
            return self._hot.query(
                collection, filters=filters, ordering=ordering, limit=limit, cursor=cursor
            )
        envelopes = [
            document.envelope()
            for document in self._index.collection(collection)
            if not document.deleted
        ]
        matched = apply_filters(envelopes, filters)
        ordered = sort_documents(matched, ordering)
        return paginate(ordered, limit=limit, cursor=cursor)

    def counts(self, collection: str) -> int:
        collection = self._collection(collection)
        if self._config.is_hot(collection):
            return self._hot.count(collection)
        return sum(1 for d in self._index.collection(collection) if not d.deleted)

    # ------------------------------------------------------------------ #
    # Writes
    # ------------------------------------------------------------------ #

    async def create(
        self,
        collection: str,
        data: dict[str, Any],
        doc_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a document, optionally under a caller-chosen id.

        The chosen id is the store's own facility and it earns its keep: an id
        like `presence_user_<email>` or `presence_panel_vega-team` makes a
        "create it if it is not there" a single attempt instead of a lookup — and
        a lookup followed by a create is exactly how the same heartbeat ends up
        with four documents in the channel. A duplicate id is refused with
        `409 DOCUMENT_ALREADY_EXISTS`, never silently reused.
        """
        collection = self._collection(collection)
        if not isinstance(data, dict):
            raise validation_error("data must be an object")
        doc_id = self._doc_id(doc_id) if doc_id else uuid.uuid4().hex

        if self._config.is_hot(collection):
            return self._hot.create(collection, doc_id, data)

        lock = self._locks.lock_for(collection, doc_id)
        try:
            async with lock:
                existing = self._index.get(collection, doc_id)
                if existing is not None:
                    raise already_exists(collection, doc_id)
                return await self._write(
                    collection, doc_id, data, rev=1, created_at=now_iso(), parts=[]
                )
        finally:
            self._locks.release(collection, doc_id)

    async def replace(
        self,
        collection: str,
        doc_id: str,
        data: dict[str, Any],
        *,
        expected_rev: int | None = None,
    ) -> dict[str, Any]:
        """Overwrite a whole document body (no merge)."""
        collection = self._collection(collection)
        doc_id = self._doc_id(doc_id)
        if self._config.is_hot(collection):
            return self._hot.replace(collection, doc_id, data, expected_rev=expected_rev)

        lock = self._locks.lock_for(collection, doc_id)
        try:
            async with lock:
                current = self._require(collection, doc_id)
                self._check_rev(collection, doc_id, current, expected_rev)
                return await self._write(
                    collection,
                    doc_id,
                    data,
                    rev=current.rev + 1,
                    created_at=current.created_at or now_iso(),
                    parts=current.parts,
                    key=current.key,
                )
        finally:
            self._locks.release(collection, doc_id)

    async def patch(
        self,
        collection: str,
        doc_id: str,
        patch: dict[str, Any],
        *,
        expected_rev: int | None = None,
    ) -> dict[str, Any]:
        """Merge fields into an existing document (fields left out are kept)."""
        collection = self._collection(collection)
        doc_id = self._doc_id(doc_id)
        if not isinstance(patch, dict):
            raise validation_error("data must be an object")
        if self._config.is_hot(collection):
            return self._hot.patch(collection, doc_id, patch, expected_rev=expected_rev)

        lock = self._locks.lock_for(collection, doc_id)
        try:
            async with lock:
                current = self._require(collection, doc_id)
                self._check_rev(collection, doc_id, current, expected_rev)
                merged = {**current.data, **patch}
                return await self._write(
                    collection,
                    doc_id,
                    merged,
                    rev=current.rev + 1,
                    created_at=current.created_at or now_iso(),
                    parts=current.parts,
                    key=current.key,
                )
        finally:
            self._locks.release(collection, doc_id)

    async def upsert(
        self,
        collection: str,
        doc_id: str,
        data: dict[str, Any],
        *,
        expected_rev: int | None = None,
    ) -> dict[str, Any]:
        """Merge into a document, creating it when it does not exist yet."""
        collection = self._collection(collection)
        doc_id = self._doc_id(doc_id)
        if self._config.is_hot(collection):
            return self._hot.upsert(collection, doc_id, data, expected_rev=expected_rev)

        lock = self._locks.lock_for(collection, doc_id)
        try:
            async with lock:
                current = self._index.get(collection, doc_id)
                if current is None or current.deleted:
                    return await self._write(
                        collection, doc_id, data, rev=1, created_at=now_iso(), parts=[]
                    )
                self._check_rev(collection, doc_id, current, expected_rev)
                return await self._write(
                    collection,
                    doc_id,
                    {**current.data, **data},
                    rev=current.rev + 1,
                    created_at=current.created_at or now_iso(),
                    parts=current.parts,
                    key=current.key,
                )
        finally:
            self._locks.release(collection, doc_id)

    async def delete(
        self,
        collection: str,
        doc_id: str,
        *,
        expected_rev: int | None = None,
    ) -> dict[str, Any]:
        """Delete a document.

        Two flavours, and the difference matters:

        * **default** — the message is *edited into a tombstone* (`deleted: true`
          with an empty body). The message id is kept, so a re-create under the
          same id is a plain edit, the channel does not grow, and the fact that
          something was deleted stays visible to anyone reading the channel.
        * **`ZG_HARD_DELETE=1`** — the messages are actually removed from the
          channel. Use it for data that must not survive (a support thread the
          owner asked to purge), never as the default, because it cannot be
          undone and it loses the id.
        """
        collection = self._collection(collection)
        doc_id = self._doc_id(doc_id)
        if self._config.is_hot(collection):
            return self._hot.delete(collection, doc_id, expected_rev=expected_rev)

        lock = self._locks.lock_for(collection, doc_id)
        try:
            async with lock:
                current = self._require(collection, doc_id)
                self._check_rev(collection, doc_id, current, expected_rev)
                if self._config.hard_delete and current.parts:
                    await self._gateway.delete(
                        current.key, [message_id for message_id, _ in current.parts]
                    )
                    self._index.remove(collection, doc_id)
                    return {"id": doc_id, "deleted": True}

                await self._write(
                    collection,
                    doc_id,
                    {},
                    rev=current.rev + 1,
                    created_at=current.created_at or now_iso(),
                    parts=current.parts,
                    key=current.key,
                    tombstone=True,
                )
                return {"id": doc_id, "deleted": True}
        finally:
            self._locks.release(collection, doc_id)

    # ------------------------------------------------------------------ #
    # The write itself: Telegram first, memory second
    # ------------------------------------------------------------------ #

    async def _write(
        self,
        collection: str,
        doc_id: str,
        data: dict[str, Any],
        *,
        rev: int,
        created_at: str,
        parts: list[tuple[int, int]],
        key: str | None = None,
        tombstone: bool = False,
    ) -> dict[str, Any]:
        key = key or self._config.route_for(collection)
        updated_at = now_iso()
        body = json.dumps(
            {
                "data": data,
                "created_at": created_at,
                "updated_at": updated_at,
                **({"deleted": True} if tombstone else {}),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        encoded = body.encode("utf-8")
        if len(encoded) > MAX_BODY_BYTES:
            raise validation_error(
                f"Document body is {len(encoded)} bytes; the limit is {MAX_BODY_BYTES}. "
                "Store the payload as a file and keep a pointer here."
            )

        texts = encode_parts(collection, doc_id, rev, body, self._config.part_chars)

        # ---- 1. Telegram. Every write that lands here is durable. ---------- #
        #
        # Two write styles, chosen by shape rather than by preference:
        #
        # * **One message in, one message out** — a plain in-place edit. This is
        #   the path almost every document takes, and it keeps the message id, the
        #   channel size and the API call count all at their minimum.
        # * **Anything that spans several messages** — append the whole new
        #   revision, *then* delete the old one. Telegram has no transaction, so an
        #   in-place multipart edit has a torn window: part 1 says revision 9 while
        #   parts 2-5 still say 1. If the process died inside that window, part 1 of
        #   the old revision would already be gone and the document could not be
        #   reconstructed at any revision — real data loss. Append-then-swap costs
        #   a few extra messages and makes the old revision survive until the new
        #   one is whole, which is what lets the rebuild rule ("newest *complete*
        #   revision wins") actually hold.
        message_ids: list[tuple[int, int]] = []
        if len(texts) == 1 and len(parts) <= 1:
            if parts:
                message_id = parts[0][0]
                await self._gateway.edit(key, message_id, texts[0])
            else:
                message_id = await self._gateway.send(key, texts[0])
            message_ids.append((message_id, len(texts[0])))
        else:
            for text in texts:
                message_id = await self._gateway.send(key, text)
                message_ids.append((message_id, len(text)))
            superseded = [
                message_id
                for message_id, _ in parts
                if message_id not in {new_id for new_id, _ in message_ids}
            ]
            if superseded:
                # Only now, with every part of the new revision in the channel.
                await self._gateway.delete(key, superseded)

        # ---- 2. Memory. Only now is the cache allowed to change. ---------- #
        self._index.put(
            IndexedDocument(
                collection=collection,
                doc_id=doc_id,
                rev=rev,
                key=key,
                created_at=created_at,
                updated_at=updated_at,
                data={} if tombstone else data,
                parts=message_ids,
                deleted=tombstone,
            )
        )
        if tombstone:
            return {"id": doc_id, "deleted": True, "rev": rev}
        return {
            "id": doc_id,
            "rev": rev,
            "data": data,
            "created_at": created_at,
            "updated_at": updated_at,
        }

    # ------------------------------------------------------------------ #
    # Guards
    # ------------------------------------------------------------------ #

    def _require(self, collection: str, doc_id: str) -> IndexedDocument:
        current = self._index.get(collection, doc_id)
        if current is None:
            raise not_found(f"No document {doc_id!r} in collection {collection!r}")
        if current.deleted:
            raise deleted_error(collection, doc_id)
        return current

    @staticmethod
    def _check_rev(
        collection: str,
        doc_id: str,
        current: IndexedDocument,
        expected_rev: int | None,
    ) -> None:
        if expected_rev is None:
            return
        from .errors import revision_conflict

        if current.rev != expected_rev:
            raise revision_conflict(collection, doc_id, expected_rev, current.rev)

    @staticmethod
    def _collection(collection: str) -> str:
        name = (collection or "").strip().lower()
        if not name or len(name) > 64:
            raise validation_error(f"Invalid collection name: {collection!r}")
        if "/" in name or "|" in name:
            raise validation_error(
                f"Collection name may not contain '/' or '|': {collection!r}"
            )
        return name

    @staticmethod
    def _doc_id(doc_id: str) -> str:
        candidate = (doc_id or "").strip()
        if not valid_doc_id(candidate):
            raise validation_error(
                f"Invalid document id: {doc_id!r}. Allowed: letters, digits and "
                "._:@+- (max 128 chars, must not start with a dot)."
            )
        return candidate
