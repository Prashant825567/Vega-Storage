"""The hot store: flags whose whole value expires in a minute.

`presence` is the reason this module exists. The app writes a heartbeat per user
every 20 seconds; at 10,000 users that is ~500 writes per second, aimed at one
chat that Telegram asks you to write to about **once per second**. No amount of
caching fixes that ratio — the workload itself does not belong in a channel.

So hot collections are served by the same *API* (same shapes, same `rev`, same
`expected_rev` conflict) but a different *store*: process memory, with a
best-effort disk snapshot so a restart does not blank the roster.

What that costs, stated plainly, because it is a design choice and not an
accident:

* The data is **not durable**. A restart loses what was only in memory — and for
  "who is online right now" that is not a loss at all: a heartbeat is stale within
  60 seconds by definition.
* Render's filesystem is ephemeral too, so the snapshot survives a process
  restart but not a redeploy or a spin-down. It is a convenience, not a backup.
* Anything that must outlive a restart belongs in a channel (a document
  collection), not here. `read` receipts are pinned to their message document
  and therefore stay with it; only free-floating flags live here.

`TTL` is optional per collection: `presence` entries are dropped after they go
stale, so the roster cannot grow forever and "online" needs no sweeper.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .config import Config
from .errors import (
    already_exists,
    not_found,
    revision_conflict,
    validation_error,
)
from .query import Filter, Ordering, Page, apply_filters, paginate, sort_documents
from .wire import now_iso

log = logging.getLogger("storage_backend.hotstore")

# Per-collection TTL, in seconds. A presence beat older than this is dropped on
# the next read: 0 means "never expire" for collections that hold something with
# meaning beyond the current session.
DEFAULT_TTL_SECONDS: dict[str, float] = {
    "presence": 600.0,
    "typing": 30.0,
    "read_cursor": 0.0,
}


@dataclass
class HotDocument:
    collection: str
    doc_id: str
    rev: int
    data: dict[str, Any]
    created_at: str
    updated_at: str
    touches: int = 0
    # monotonic seconds, for TTL only — never exposed.
    seen_at: float = field(default_factory=time.monotonic)


class HotStore:
    """In-memory documents with the same contract as the channel-backed ones."""

    def __init__(self, config: Config, ttl_seconds: dict[str, float] | None = None) -> None:
        self._config = config
        self._docs: dict[str, dict[str, HotDocument]] = {}
        self._ttl = dict(DEFAULT_TTL_SECONDS)
        if ttl_seconds:
            self._ttl.update(ttl_seconds)
        self._lock = threading.RLock()
        self.writes = 0
        self.expired = 0

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #

    def get(self, collection: str, doc_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._expire(collection)
            document = self._docs.get(collection, {}).get(doc_id)
            return None if document is None else self._envelope(document)

    def count(self, collection: str) -> int:
        with self._lock:
            self._expire(collection)
            return len(self._docs.get(collection, {}))

    def query(
        self,
        collection: str,
        *,
        filters: list[Filter],
        ordering: Ordering | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        with self._lock:
            self._expire(collection)
            envelopes = [
                self._envelope(document)
                for document in self._docs.get(collection, {}).values()
            ]
        matched = apply_filters(envelopes, filters)
        ordered = sort_documents(matched, ordering)
        return paginate(ordered, limit=limit, cursor=cursor)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "collections": {
                    name: len(bucket) for name, bucket in sorted(self._docs.items())
                },
                "writes": self.writes,
                "expired": self.expired,
                "ttl_seconds": dict(self._ttl),
            }

    # ------------------------------------------------------------------ #
    # Writes — same rules as the channel-backed store, no network involved
    # ------------------------------------------------------------------ #

    def create(self, collection: str, doc_id: str, data: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(data, dict):
            raise validation_error("data must be an object")
        with self._lock:
            self._expire(collection)
            if doc_id in self._docs.get(collection, {}):
                raise already_exists(collection, doc_id)
            stamp = now_iso()
            document = HotDocument(
                collection=collection,
                doc_id=doc_id,
                rev=1,
                data=dict(data),
                created_at=stamp,
                updated_at=stamp,
            )
            self._docs.setdefault(collection, {})[doc_id] = document
            self.writes += 1
            return self._envelope(document)

    def replace(
        self,
        collection: str,
        doc_id: str,
        data: dict[str, Any],
        *,
        expected_rev: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            current = self._require(collection, doc_id)
            self._check_rev(collection, doc_id, current, expected_rev)
            current.data = dict(data)
            current.rev += 1
            current.updated_at = now_iso()
            current.seen_at = time.monotonic()
            current.touches += 1
            self.writes += 1
            return self._envelope(current)

    def patch(
        self,
        collection: str,
        doc_id: str,
        patch: dict[str, Any],
        *,
        expected_rev: int | None = None,
    ) -> dict[str, Any]:
        if not isinstance(patch, dict):
            raise validation_error("data must be an object")
        with self._lock:
            current = self._require(collection, doc_id)
            self._check_rev(collection, doc_id, current, expected_rev)
            current.data = {**current.data, **patch}
            current.rev += 1
            current.updated_at = now_iso()
            current.seen_at = time.monotonic()
            current.touches += 1
            self.writes += 1
            return self._envelope(current)

    def upsert(
        self,
        collection: str,
        doc_id: str,
        data: dict[str, Any],
        *,
        expected_rev: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            current = self._docs.get(collection, {}).get(doc_id)
            if current is None:
                return self.create(collection, doc_id, data)
            return self.patch(collection, doc_id, data, expected_rev=expected_rev)

    def delete(
        self,
        collection: str,
        doc_id: str,
        *,
        expected_rev: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            current = self._require(collection, doc_id)
            self._check_rev(collection, doc_id, current, expected_rev)
            self._docs.get(collection, {}).pop(doc_id, None)
            self.writes += 1
            return {"id": doc_id, "deleted": True, "rev": current.rev + 1}

    # ------------------------------------------------------------------ #
    # Snapshot (best effort — this store is not a backup)
    # ------------------------------------------------------------------ #

    def load(self) -> None:
        path = self._config.hot_snapshot_path
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except Exception as exc:
            log.warning("hot snapshot unreadable (%s): %s", path, exc)
            return
        loaded = 0
        with self._lock:
            for collection, bucket in (raw or {}).items():
                for doc_id, document in (bucket or {}).items():
                    try:
                        self._docs.setdefault(collection, {})[doc_id] = HotDocument(
                            collection=collection,
                            doc_id=doc_id,
                            rev=int(document["rev"]),
                            data=dict(document.get("data") or {}),
                            created_at=document.get("created_at") or now_iso(),
                            updated_at=document.get("updated_at") or now_iso(),
                        )
                        loaded += 1
                    except Exception:
                        continue
            self._expire_all()
        log.info("hot store restored %s documents from %s", loaded, path)

    def save(self) -> None:
        path = self._config.hot_snapshot_path
        if not path:
            return
        try:
            with self._lock:
                payload = {
                    collection: {
                        doc_id: {
                            "rev": document.rev,
                            "data": document.data,
                            "created_at": document.created_at,
                            "updated_at": document.updated_at,
                        }
                        for doc_id, document in bucket.items()
                    }
                    for collection, bucket in self._docs.items()
                }
            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            temporary = f"{path}.tmp"
            with open(temporary, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False)
            os.replace(temporary, path)
        except Exception as exc:  # pragma: no cover - disk is best effort
            log.debug("hot snapshot save failed: %s", exc)

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _require(self, collection: str, doc_id: str) -> HotDocument:
        self._expire(collection)
        document = self._docs.get(collection, {}).get(doc_id)
        if document is None:
            raise not_found(f"No document {doc_id!r} in collection {collection!r}")
        return document

    @staticmethod
    def _check_rev(
        collection: str,
        doc_id: str,
        current: HotDocument,
        expected_rev: int | None,
    ) -> None:
        if expected_rev is None:
            return
        if current.rev != expected_rev:
            raise revision_conflict(collection, doc_id, expected_rev, current.rev)

    def _envelope(self, document: HotDocument) -> dict[str, Any]:
        return {
            "id": document.doc_id,
            "rev": document.rev,
            "data": document.data,
            "created_at": document.created_at,
            "updated_at": document.updated_at,
        }

    def _ttl_for(self, collection: str) -> float:
        return float(self._ttl.get(collection, 0.0) or 0.0)

    def _expire(self, collection: str) -> None:
        ttl = self._ttl_for(collection)
        if ttl <= 0:
            return
        bucket = self._docs.get(collection)
        if not bucket:
            return
        now = time.monotonic()
        stale = [doc_id for doc_id, doc in bucket.items() if now - doc.seen_at > ttl]
        for doc_id in stale:
            bucket.pop(doc_id, None)
            self.expired += 1

    def _expire_all(self) -> None:
        for collection in list(self._docs):
            self._expire(collection)
