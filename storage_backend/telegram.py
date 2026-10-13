"""The only module that talks MTProto.

Everything above it (`documents.py`, `files.py`) depends on the small
`TelegramLike` protocol below rather than on Telethon, which is what lets the
whole store be tested in-process with a fake channel — the real credentials are
never needed to prove the write path, the revision check or a cold rebuild.

Things this wrapper owns, so no caller has to remember them:

* **Peer resolution.** A `-100…` id is a *marked* id; `PeerChannel` wants the
  unmarked positive part, and an id that the signed-in account has never seen
  cannot form an input peer at all. So each channel is resolved once at startup
  with two strategies, and the result is cached.
* **Flood control.** `flood_sleep_threshold = 0` — this service waits on its own
  terms and answers the caller with a 429 + `Retry-After` instead of holding a
  request open for minutes.
* **`MessageNotModifiedError`.** Editing a message to the text it already has is
  not a failure; it is a no-op. Treating it as one is how a retry storm starts.
* **Per-channel spacing.** Telegram asks for ≤1 message/second per chat. A
  document write is one edit, so documents are naturally slow — but a bulk
  import or a retry sweep can outrun that, so an optional minimum interval
  serialises writes per channel.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Protocol, runtime_checkable

from .config import Channel, Config
from .errors import channel_unavailable, flood_wait, not_found, upstream_failure
from .wire import unmark_channel_id

log = logging.getLogger("storage_backend.telegram")

# Telegram file requests are aligned to 4 KiB. Ranges are widened to this
# boundary and trimmed locally, so a browser's `bytes=100-200` still works.
PART_ALIGNMENT = 4096
DEFAULT_REQUEST_SIZE = 512 * 1024


@dataclass
class StoredMessage:
    """What a caller needs to know about one channel message."""

    id: int
    text: str
    has_media: bool = False
    size: int = 0
    mime: str = ""
    name: str = ""
    media: Any = None
    file_bytes: bytes | None = None


@runtime_checkable
class TelegramLike(Protocol):
    """The channel operations the store needs. Fake implementations are welcome."""

    async def resolve_channels(self) -> dict[str, Any]: ...

    async def send(self, key: str, text: str) -> int: ...

    async def edit(self, key: str, message_id: int, text: str) -> None: ...

    async def delete(self, key: str, message_ids: list[int]) -> None: ...

    async def send_file(
        self,
        key: str,
        *,
        data: bytes | None = None,
        path: str | None = None,
        caption: str = "",
        filename: str = "",
        mime: str = "application/octet-stream",
    ) -> StoredMessage: ...

    async def get_message(self, key: str, message_id: int) -> StoredMessage: ...

    def iter_history(self, key: str, limit: int) -> AsyncIterator[StoredMessage]: ...

    def iter_download(
        self, media: Any, offset: int, length: int | None
    ) -> AsyncIterator[bytes]: ...

    async def close(self) -> None: ...


class _ChannelLimiter:
    """Keeps writes to one chat at or above a minimum spacing."""

    def __init__(self, min_interval_ms: int) -> None:
        self._min_interval = max(0, min_interval_ms) / 1000.0
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self) -> None:
        if self._min_interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            delay = self._min_interval - (now - self._last)
            if delay > 0:
                await asyncio.sleep(delay)
            self._last = time.monotonic()


class TelethonGateway:
    """Telethon behind the `TelegramLike` protocol."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._client: Any = None
        self._peers: dict[str, Any] = {}
        self._unresolved: dict[str, str] = {}
        self._limiters = {
            key: _ChannelLimiter(config.channel_min_interval_ms)
            for key in config.channels
        }

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def connect(self) -> None:
        """Connect (and log in) once, on the server's own event loop."""
        # Imported here so the pure unit tests never need Telethon installed.
        from telethon import TelegramClient
        from telethon.sessions import StringSession

        config = self._config
        client = TelegramClient(
            StringSession(config.session),
            config.api_id,
            config.api_hash,
        )
        # This service handles flood waits itself: a request must never hang for
        # minutes waiting on Telegram. Anything above zero seconds is raised.
        client.flood_sleep_threshold = 0
        await client.start()
        self._client = client
        log.info("MTProto session is up (api_id=%s)", config.api_id)
        await self.resolve_channels()

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:  # pragma: no cover - shutdown best effort
                log.debug("disconnect failed", exc_info=True)
            self._client = None

    # ------------------------------------------------------------------ #
    # Peers
    # ------------------------------------------------------------------ #

    @property
    def resolved(self) -> dict[str, Any]:
        return dict(self._peers)

    @property
    def unresolved(self) -> dict[str, str]:
        return dict(self._unresolved)

    async def resolve_channels(self) -> dict[str, Any]:
        """Resolve every configured channel to an input peer, once.

        Two strategies, in order, because the first one can fail for a channel the
        account has never opened:

        1. `PeerChannel(unmarked_id)` — exact, no session cache needed.
        2. walk the dialogs and match by id — works when the account is a member
           even if the access hash is not cached.

        A failure is recorded and retried on the next call rather than crashing
        startup: one unreachable channel must not take the health endpoint down
        with it.
        """
        assert self._client is not None, "connect() first"
        from telethon.tl.types import PeerChannel

        peers: dict[str, Any] = {}
        unresolved: dict[str, str] = {}
        for key, channel in self._config.channels.items():
            try:
                peers[key] = await self._resolve_one(channel, PeerChannel)
            except Exception as exc:
                unresolved[key] = f"{type(exc).__name__}: {exc}"
                log.warning("channel %s (%s) unresolved: %s", key, channel.title, exc)

        if unresolved and len(unresolved) < len(self._config.channels):
            # Only worth the (expensive) dialog walk when at least one channel is
            # still missing — a full walk on every start is a lot of traffic.
            try:
                by_id = await self._dialogs_by_id()
                for key in list(unresolved):
                    peer = by_id.get(self._config.channel(key).peer_id)
                    if peer is not None:
                        peers[key] = peer
                        unresolved.pop(key)
            except Exception as exc:  # pragma: no cover - best effort
                log.warning("dialog walk failed: %s", exc)

        self._peers = peers
        self._unresolved = unresolved
        return peers

    async def _resolve_one(self, channel: Channel, peer_cls: Any) -> Any:
        assert self._client is not None
        try:
            return await self._client.get_input_entity(
                peer_cls(unmark_channel_id(channel.peer_id))
            )
        except Exception:
            # Some sessions accept the marked id directly; try before giving up.
            entity = await self._client.get_entity(channel.peer_id)
            return await self._client.get_input_entity(entity)

    async def _dialogs_by_id(self) -> dict[int, Any]:
        assert self._client is not None
        found: dict[int, Any] = {}
        wanted = {channel.peer_id for channel in self._config.channels.values()}
        async for dialog in self._client.iter_dialogs():
            try:
                entity = dialog.entity
                marked = int(f"-100{entity.id}")
            except Exception:
                continue
            if marked in wanted:
                found[marked] = await self._client.get_input_entity(entity)
        return found

    def _peer(self, key: str) -> Any:
        peer = self._peers.get(key)
        if peer is None:
            reason = self._unresolved.get(key, "not resolved yet")
            raise channel_unavailable(key, reason)
        return peer

    # ------------------------------------------------------------------ #
    # Small helpers
    # ------------------------------------------------------------------ #

    async def _guarded(self, key: str, operation: str, awaitable: Any) -> Any:
        """Run one API call, translating Telegram's errors into ours."""
        from telethon import errors as tg_errors

        await self._limiters.setdefault(
            key, _ChannelLimiter(self._config.channel_min_interval_ms)
        ).wait()
        try:
            return await awaitable
        except tg_errors.FloodWaitError as exc:
            seconds = int(getattr(exc, "seconds", 0) or 0)
            log.warning("flood wait on %s (%s): %ss", key, operation, seconds)
            raise flood_wait(seconds, key) from exc
        except tg_errors.MessageNotModifiedError:
            # Editing to the current text is a successful no-op.
            return None
        except tg_errors.MessageIdInvalidError as exc:
            raise not_found(f"Message is gone from {key} ({operation})") from exc
        except (
            tg_errors.ChannelPrivateError,
            tg_errors.ChatForbiddenError,
            tg_errors.ChannelInvalidError,
        ) as exc:
            raise channel_unavailable(key, f"{type(exc).__name__}: {exc}") from exc
        except (tg_errors.RPCError, OSError) as exc:
            raise upstream_failure(f"Telegram refused {operation} on {key}: {exc}") from exc

    # ------------------------------------------------------------------ #
    # Writes
    # ------------------------------------------------------------------ #

    async def send(self, key: str, text: str) -> int:
        assert self._client is not None, "connect() first"
        message = await self._guarded(
            key, "send", self._client.send_message(self._peer(key), text)
        )
        return int(message.id)

    async def edit(self, key: str, message_id: int, text: str) -> None:
        assert self._client is not None, "connect() first"
        await self._guarded(
            key,
            "edit",
            self._client.edit_message(self._peer(key), message_id, text=text),
        )

    async def delete(self, key: str, message_ids: list[int]) -> None:
        if not message_ids:
            return
        assert self._client is not None, "connect() first"
        await self._guarded(
            key, "delete", self._client.delete_messages(self._peer(key), message_ids)
        )

    async def send_file(
        self,
        key: str,
        *,
        data: bytes | None = None,
        path: str | None = None,
        caption: str = "",
        filename: str = "",
        mime: str = "application/octet-stream",
    ) -> StoredMessage:
        """Store one blob, always as a document.

        `force_document=True` matters: without it a PNG becomes a photo and a
        short MP3 becomes a voice note, and Telegram then *re-encodes* the bytes.
        A storage layer that silently re-encodes is not a storage layer.
        """
        assert self._client is not None, "connect() first"
        from telethon.tl.types import DocumentAttributeFilename

        source = data if data is not None else path
        if source is None:
            raise ValueError("send_file needs data or path")
        attributes = (
            [DocumentAttributeFilename(filename)] if filename else None
        )
        message = await self._guarded(
            key,
            "send_file",
            self._client.send_file(
                self._peer(key),
                file=source,
                caption=caption or None,
                force_document=True,
                attributes=attributes,
                mime_type=mime,
            ),
        )
        return _stored_from_message(message)

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #

    async def get_message(self, key: str, message_id: int) -> StoredMessage:
        assert self._client is not None, "connect() first"
        message = await self._guarded(
            key, "get_message", self._client.get_messages(self._peer(key), ids=message_id)
        )
        if message is None:
            raise not_found(f"Message {message_id} is not in {key}")
        return _stored_from_message(message)

    def iter_history(self, key: str, limit: int) -> AsyncIterator[StoredMessage]:
        """Oldest → newest, so a cold rebuild indexes in creation order."""

        async def generator() -> AsyncIterator[StoredMessage]:
            assert self._client is not None, "connect() first"
            last_id = 0
            seen = 0
            while seen < limit:
                batch: list[StoredMessage] = []
                try:
                    async for message in self._client.iter_messages(
                        self._peer(key),
                        limit=min(200, limit - seen),
                        min_id=last_id,
                        reverse=True,
                    ):
                        batch.append(_stored_from_message(message))
                except Exception as exc:
                    # A history walk is a best-effort rebuild: log it, keep the
                    # messages already read, and let the service serve what it has.
                    log.warning("history walk on %s stopped early: %s", key, exc)
                    break
                if not batch:
                    break
                for item in batch:
                    yield item
                seen += len(batch)
                last_id = max(item.id for item in batch)
                if len(batch) < min(200, limit - seen + len(batch)):
                    break

        return generator()

    def iter_download(
        self, media: Any, offset: int, length: int | None
    ) -> AsyncIterator[bytes]:
        """Stream a byte range, aligning requests to Telegram's 4 KiB parts."""

        async def generator() -> AsyncIterator[bytes]:
            assert self._client is not None, "connect() first"
            aligned = (offset // PART_ALIGNMENT) * PART_ALIGNMENT
            skip = offset - aligned
            remaining = length
            request_size = DEFAULT_REQUEST_SIZE
            trim_first = skip
            try:
                async for chunk in self._client.iter_download(
                    media,
                    offset=aligned,
                    request_size=request_size,
                ):
                    if trim_first:
                        if trim_first >= len(chunk):
                            trim_first -= len(chunk)
                            continue
                        chunk = chunk[trim_first:]
                        trim_first = 0
                    if remaining is not None:
                        if remaining <= 0:
                            break
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]
                        remaining -= len(chunk)
                    yield chunk
            except Exception as exc:  # pragma: no cover - network path
                log.warning("download failed: %s", exc)
                raise upstream_failure(f"Could not read the file from Telegram: {exc}") from exc

        return generator()


def _stored_from_message(message: Any) -> StoredMessage:
    """Flatten a Telethon `Message` into the few fields the store cares about."""
    document = getattr(message, "document", None)
    text = getattr(message, "raw_text", None) or getattr(message, "text", None) or ""
    mime = ""
    size = 0
    name = ""
    if document is not None:
        mime = getattr(document, "mime_type", "") or ""
        size = int(getattr(document, "size", 0) or 0)
        for attribute in getattr(document, "attributes", []) or []:
            candidate = getattr(attribute, "file_name", None)
            if candidate:
                name = candidate
                break
    return StoredMessage(
        id=int(message.id),
        text=text,
        has_media=document is not None or getattr(message, "media", None) is not None,
        size=size,
        mime=mime,
        name=name,
        media=getattr(message, "media", None),
    )
