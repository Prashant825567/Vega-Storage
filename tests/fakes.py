"""A fake channel that behaves like the real one, minus Telegram.

This is what makes the store testable: `documents.py` and `files.py` only know
the `TelegramLike` protocol, so a fake implementation proves the write path, the
revision check, the multi-part edit and the cold rebuild **without any
credentials and without a network**.
"""

from __future__ import annotations

from typing import Any, AsyncIterator

from storage_backend.config import Channel, Config
from storage_backend.errors import not_found
from storage_backend.telegram import StoredMessage

FAKE_CHANNELS = {
    "tg.vault.1": -1003597396903,
    "tg.ai.1": -1004362809278,
    "tg.support.1": -1004337129320,
    "tg.notify.1": -1003755323456,
    "tg.index.1": -1003960423150,
}


def make_config(**overrides: Any) -> Config:
    """A Config that needs no environment at all."""
    defaults: dict[str, Any] = {
        "api_id": 12345,
        "api_hash": "0123456789abcdef0123456789abcdef",
        "session": "fake-session-string",
        "api_key": "test-key",
        "channels": {
            key: Channel(key=key, peer_id=peer) for key, peer in FAKE_CHANNELS.items()
        },
        "part_chars": 120,  # tiny, so multi-part paths are actually exercised
        "caption_part_chars": 80,
        "hot_snapshot_path": "",
    }
    defaults.update(overrides)
    return Config(**defaults)


class FakeTelegram:
    """An in-memory stand-in for `TelethonGateway`.

    It keeps per-channel message lists exactly like a channel does — ids increase,
    edits change the text in place, deletes leave a hole — so a cold rebuild
    against it is a real test of the rebuild, not a mock of it.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.is_connected = False
        self.messages: dict[str, list[StoredMessage]] = {key: [] for key in config.channels}
        self.files: dict[str, dict[int, bytes]] = {key: {} for key in config.channels}
        self._next_id = 1000
        self.sends = 0
        self.edits = 0
        self.deletes = 0
        self.fail_on_edit: set[int] = set()

    # -- lifecycle ----------------------------------------------------- #

    async def connect(self) -> None:
        """The real gateway signs in here; the fake has no credentials."""
        self.is_connected = True

    async def probe(self) -> None:
        if not self.is_connected:
            raise ConnectionError("fake transport disconnected")

    async def resolve_channels(self) -> dict[str, Any]:
        return self.resolved

    @property
    def resolved(self) -> dict[str, Any]:
        """`/v1/status` reports per-channel resolution, so the fake must have it."""
        return {key: f"peer:{key}" for key in self.config.channels}

    @property
    def unresolved(self) -> dict[str, str]:
        return {}

    async def close(self) -> None:
        self.is_connected = False

    def _new_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def _find(self, key: str, message_id: int) -> StoredMessage:
        for message in self.messages.get(key, []):
            if message.id == message_id:
                return message
        raise not_found(f"Message {message_id} is not in {key}")

    # -- writes -------------------------------------------------------- #

    async def send(self, key: str, text: str) -> int:
        if text == "":
            return self._new_id()
        message_id = self._new_id()
        self.messages.setdefault(key, []).append(StoredMessage(id=message_id, text=text))
        self.sends += 1
        return message_id

    async def edit(self, key: str, message_id: int, text: str) -> None:
        if message_id in self.fail_on_edit:
            raise RuntimeError(f"simulated edit failure on {message_id}")
        message = self._find(key, message_id)
        if message.text == text:
            # The real client raises MessageNotModifiedError and the gateway
            # swallows it; the fake is quieter but keeps the same meaning.
            return
        message.text = text
        self.edits += 1

    async def delete(self, key: str, message_ids: list[int]) -> None:
        wanted = set(message_ids)
        self.messages[key] = [m for m in self.messages.get(key, []) if m.id not in wanted]
        for message_id in wanted:
            self.files.get(key, {}).pop(message_id, None)
        self.deletes += len(wanted)

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
        if data is None and path is not None:
            with open(path, "rb") as handle:
                data = handle.read()
        payload = data or b""
        message_id = self._new_id()
        message = StoredMessage(
            id=message_id,
            text=caption or "",
            has_media=True,
            size=len(payload),
            mime=mime,
            name=filename,
            # The real gateway hands out a Telethon media object here; in the fake
            # the message id *is* the handle to the stored bytes.
            media=message_id,
        )
        self.messages.setdefault(key, []).append(message)
        self.files.setdefault(key, {})[message_id] = payload
        self.sends += 1
        return message

    # -- reads --------------------------------------------------------- #

    async def get_message(self, key: str, message_id: int) -> StoredMessage:
        return self._find(key, message_id)

    def iter_history(self, key: str, limit: int) -> AsyncIterator[StoredMessage]:
        async def generator() -> AsyncIterator[StoredMessage]:
            for message in list(self.messages.get(key, []))[:limit]:
                if message.text:
                    yield message

        return generator()

    def iter_download(
        self, media: Any, offset: int, length: int | None
    ) -> AsyncIterator[bytes]:
        raw = b""
        for bucket in self.files.values():
            if media in bucket:
                raw = bucket[media]
                break

        async def generator() -> AsyncIterator[bytes]:
            window = raw[offset:] if length is None else raw[offset : offset + length]
            # Yield in small pieces so the streaming path is exercised.
            for index in range(0, len(window), 7):
                yield window[index : index + 7]

        return generator()

    # -- assertions helpers -------------------------------------------- #

    def text_of(self, key: str, message_id: int) -> str:
        return self._find(key, message_id).text

    def message_ids(self, key: str) -> list[int]:
        return [message.id for message in self.messages.get(key, [])]
