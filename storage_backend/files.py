"""Files: bytes in a channel, a pointer document in the index.

The rule from the plan is kept exactly: **one message = one file (or one part)**,
and the `file_id` handed to clients stays an opaque string. Nothing outside this
module ever sees a `message_id` or a peer, which is what makes the locator free to
change later without breaking a single screen.

How the two halves meet:

* the **bytes** are a real Telegram document message in the Vault channel;
* the **metadata** (name, mime, size, owner, the locator itself) is a normal
  document in the `files` collection, stored as a `ZG1` record like everything
  else. That means a cold rebuild restores file metadata from the very same
  channel scan that restores documents — no second database, no export.

Two things the plan warned about and this code honours:

* **Rename never touches Telegram.** A document's Telegram-side filename cannot be
  changed without re-uploading, so `rename` only edits the metadata document.
* **A `file_reference` is not an id.** It expires. Only `(chat, message_id)` is
  persisted; the media object is re-fetched at download time, which also refreshes
  the reference.
"""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass
from typing import Any, AsyncIterator

from .config import Config
from .documents import DocumentStore
from .errors import not_found, validation_error
from .telegram import TelegramLike
from .wire import now_iso, valid_doc_id

log = logging.getLogger("storage_backend.files")

FILES_COLLECTION = "files"

# Telegram's regular-account ceiling is 2 GB per file (4 GB with Premium). Above
# this a file is split into parts, each its own message, and the locator keeps the
# list. The number is deliberately below 2 GB: the envelope and the multipart
# upload overhead need room too.
MAX_PART_BYTES = int(1.9 * 1024 * 1024 * 1024)
SPOOL_CHUNK = 1024 * 1024


@dataclass
class FileRecord:
    """The `files` document, as this module thinks of it."""

    id: str
    name: str
    size: int
    mime: str
    parts: list[dict[str, Any]]
    owner: str | None = None
    parent_id: str | None = None
    sha256: str | None = None
    created_at: str | None = None
    updated_at: str | None = None

    def to_document(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "size": self.size,
            "mime_type": self.mime,
            "status": "ready",
            "parts": self.parts,
            "owner": self.owner,
            "parent_id": self.parent_id,
            "sha256": self.sha256,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> "FileRecord":
        data = dict(document.get("data") or {})
        return cls(
            id=str(document.get("id")),
            name=str(data.get("name") or ""),
            size=int(data.get("size") or 0),
            mime=str(data.get("mime_type") or "application/octet-stream"),
            parts=list(data.get("parts") or []),
            owner=data.get("owner"),
            parent_id=data.get("parent_id"),
            sha256=data.get("sha256"),
            created_at=document.get("created_at"),
            updated_at=document.get("updated_at"),
        )


class FileStore:
    """Uploads, streams and removes blobs held in a channel."""

    def __init__(
        self,
        config: Config,
        gateway: TelegramLike,
        documents: DocumentStore,
    ) -> None:
        self._config = config
        self._gateway = gateway
        self._documents = documents

    # ------------------------------------------------------------------ #
    # Upload
    # ------------------------------------------------------------------ #

    async def upload(
        self,
        *,
        data: bytes | None = None,
        path: str | None = None,
        name: str = "",
        mime: str = "application/octet-stream",
        owner: str | None = None,
        parent_id: str | None = None,
        file_id: str | None = None,
    ) -> FileRecord:
        """Store bytes and return the file record.

        `parts` always exists, even for the common single-message case, so a
        client never has to know whether the file was split.
        """
        if data is None and path is None:
            raise validation_error("upload needs bytes or a path")
        size = len(data) if data is not None else os.path.getsize(path or "")
        if size <= 0:
            raise validation_error("Refusing to store an empty file")
        if file_id and not valid_doc_id(file_id):
            raise validation_error(f"Invalid file id: {file_id!r}")

        key = self._config.files_key
        chosen_id = file_id or hashlib.sha256(
            f"{name}:{size}:{now_iso()}".encode("utf-8")
        ).hexdigest()[:32]

        parts: list[dict[str, Any]] = []
        if size <= MAX_PART_BYTES:
            message = await self._gateway.send_file(
                key,
                data=data,
                path=path,
                caption="",
                filename=name or chosen_id,
                mime=mime,
            )
            parts.append({"message_id": message.id, "size": size})
        else:
            parts = await self._upload_split(
                key, data=data, path=path, name=name or chosen_id, mime=mime
            )

        record = FileRecord(
            id=chosen_id,
            name=name or chosen_id,
            size=size,
            mime=mime,
            parts=parts,
            owner=owner,
            parent_id=parent_id,
            sha256=None,
            created_at=now_iso(),
            updated_at=now_iso(),
        )
        document = await self._documents.create(
            FILES_COLLECTION, record.to_document(), chosen_id
        )
        record.created_at = document.get("created_at")
        record.updated_at = document.get("updated_at")
        return record

    async def _upload_split(
        self,
        key: str,
        *,
        data: bytes | None,
        path: str | None,
        name: str,
        mime: str,
    ) -> list[dict[str, Any]]:
        """Split a >2 GB blob into channel-sized parts, streamed off disk."""
        parts: list[dict[str, Any]] = []
        if data is not None:
            view = memoryview(data)
            for index, start in enumerate(range(0, len(data), MAX_PART_BYTES)):
                chunk = bytes(view[start : start + MAX_PART_BYTES])
                message = await self._gateway.send_file(
                    key,
                    data=chunk,
                    caption="",
                    filename=f"{name}.part{index + 1:03d}",
                    mime=mime,
                )
                parts.append({"message_id": message.id, "size": len(chunk)})
            return parts

        # Path input: copy each slice to a temp file so Telethon can stream it
        # instead of holding a 2 GB chunk in memory.
        with open(path or "", "rb") as source:
            index = 0
            while True:
                remaining = MAX_PART_BYTES
                temporary = tempfile.NamedTemporaryFile(delete=False)
                written = 0
                try:
                    while remaining > 0:
                        block = source.read(min(SPOOL_CHUNK, remaining))
                        if not block:
                            break
                        temporary.write(block)
                        written += len(block)
                        remaining -= len(block)
                finally:
                    temporary.close()
                if written == 0:
                    os.unlink(temporary.name)
                    break
                try:
                    message = await self._gateway.send_file(
                        key,
                        path=temporary.name,
                        caption="",
                        filename=f"{name}.part{index + 1:03d}",
                        mime=mime,
                    )
                finally:
                    try:
                        os.unlink(temporary.name)
                    except OSError:  # pragma: no cover - Windows lock
                        log.debug("temp part %s survived", temporary.name)
                parts.append({"message_id": message.id, "size": written})
                index += 1
        return parts

    # ------------------------------------------------------------------ #
    # Metadata
    # ------------------------------------------------------------------ #

    def get(self, file_id: str) -> FileRecord:
        document = self._documents.get(FILES_COLLECTION, file_id)
        if document is None:
            raise not_found(f"No file {file_id!r}")
        return FileRecord.from_document(document)

    def list(
        self,
        *,
        owner: str | None = None,
        parent_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        from .query import Filter

        filters: list[Filter] = []
        if owner:
            filters.append(Filter("owner", "eq", owner))
        if parent_id is not None:
            filters.append(Filter("parent_id", "eq", parent_id))
        page = self._documents.query(
            FILES_COLLECTION,
            filters=filters,
            ordering=None,
            limit=limit,
            cursor=None,
        )
        return page.documents

    async def rename(self, file_id: str, name: str) -> FileRecord:
        """Metadata-only rename. The Telegram-side filename is left alone.

        A document's filename on Telegram cannot be changed without uploading the
        bytes again, and re-uploading a 2 GB video to change its label is not a
        trade worth making. The label the user sees is the one in the index.
        """
        cleaned = (name or "").strip()
        if not cleaned:
            raise validation_error("name may not be empty")
        if len(cleaned) > 255:
            raise validation_error("name is longer than 255 characters")
        await self._documents.patch(FILES_COLLECTION, file_id, {"name": cleaned})
        return self.get(file_id)

    async def delete(self, file_id: str) -> dict[str, Any]:
        """Remove the metadata, and the bytes when hard delete is on.

        With the default tombstone behaviour the media message stays in the
        channel: the record is gone from every listing, and the bytes can still be
        recovered by an operator who knows the message id. That is the honest
        trade for a store with no recycle bin — and `ZG_HARD_DELETE=1` is there for
        the case where they must not survive.
        """
        record = self.get(file_id)
        if self._config.hard_delete and record.parts:
            await self._gateway.delete(
                self._config.files_key,
                [int(part["message_id"]) for part in record.parts],
            )
        await self._documents.delete(FILES_COLLECTION, file_id)
        return {"id": file_id, "deleted": True}

    # ------------------------------------------------------------------ #
    # Download
    # ------------------------------------------------------------------ #

    async def stream(
        self,
        file_id: str,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> AsyncIterator[bytes]:
        """Stream a byte range of a stored file.

        Ranges matter for media and not for pictures: without them Safari refuses
        to play a video at all, and Chrome can only start it from the beginning.
        Telegram's own requests are 4 KiB-aligned, so the gateway widens the range
        and trims it — see `telegram.iter_download`.
        """
        record = self.get(file_id)
        if not record.parts:
            raise not_found(f"File {file_id!r} has no stored parts")

        if offset < 0:
            offset = 0
        if length is not None and length < 0:
            length = None

        if len(record.parts) == 1:
            yield_from = record.parts[0]
            async for chunk in self._stream_part(yield_from, offset, length):
                yield chunk
            return

        # Multi-part: walk the parts, skipping the ones before the offset and
        # stopping once the requested length has been served.
        remaining = length
        position = 0
        for part in record.parts:
            part_size = int(part.get("size") or 0)
            part_end = position + part_size
            if offset >= part_end:
                position = part_end
                continue
            local_offset = max(0, offset - position)
            local_length = (
                None if remaining is None else max(0, part_size - local_offset)
            )
            if remaining is not None:
                local_length = min(local_length or 0, remaining)
                if local_length <= 0:
                    return
            async for chunk in self._stream_part(part, local_offset, local_length):
                yield chunk
                if remaining is not None:
                    remaining -= len(chunk)
                    if remaining <= 0:
                        return
            position = part_end

    async def _stream_part(
        self, part: dict[str, Any], offset: int, length: int | None
    ) -> AsyncIterator[bytes]:
        message = await self._gateway.get_message(
            self._config.files_key, int(part["message_id"])
        )
        if message.media is None:
            raise not_found(
                f"Stored bytes for message {part['message_id']} are no longer reachable"
            )
        async for chunk in self._gateway.iter_download(message.media, offset, length):
            yield chunk
