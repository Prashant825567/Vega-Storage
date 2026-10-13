"""Files: the bytes live in a channel, the metadata in a document, and the
`file_id` stays opaque to every client.

The range tests are the ones with real consequences — a media player that cannot
seek is broken, and Safari refuses to start a video at all without a 206 — so the
byte maths is checked rather than assumed."""

from __future__ import annotations

import unittest

from storage_backend.documents import DocumentStore
from storage_backend.errors import StorageError
from storage_backend.files import FILES_COLLECTION, FileStore
from storage_backend.hotstore import HotStore
from storage_backend.index import ChannelIndex
from storage_backend.ranges import parse_range

from tests.fakes import FakeTelegram, make_config


class FileCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.config = make_config()
        self.gateway = FakeTelegram(self.config)
        self.index = ChannelIndex(self.config)
        self.documents = DocumentStore(
            self.config, self.gateway, self.index, HotStore(self.config)
        )
        self.files = FileStore(self.config, self.gateway, self.documents)

    async def drain(self, file_id: str, *, offset: int = 0, length: int | None = None) -> bytes:
        chunks = []
        async for chunk in self.files.stream(file_id, offset=offset, length=length):
            chunks.append(chunk)
        return b"".join(chunks)


class Upload(FileCase):
    async def test_upload_stores_bytes_in_vault_and_a_record_in_files(self) -> None:
        payload = b"hello vega" * 100
        record = await self.files.upload(data=payload, name="note.bin", mime="application/octet-stream")

        self.assertEqual(record.size, len(payload))
        self.assertEqual(record.name, "note.bin")
        self.assertEqual(len(record.parts), 1)
        # Bytes went to the Vault channel, metadata to the files collection.
        self.assertEqual(len(self.gateway.files["tg.vault.1"]), 1)
        self.assertIsNotNone(self.documents.get(FILES_COLLECTION, record.id))

    async def test_the_download_url_shape_survives(self) -> None:
        """`file_id` stays an opaque string — the locator never leaks to clients."""
        record = await self.files.upload(data=b"x", name="a.bin")
        document = record.to_document()
        self.assertEqual(document["id"], record.id)
        self.assertNotIn("message_id", document)
        self.assertEqual(len(document["parts"]), 1)
        self.assertIn("message_id", document["parts"][0])

    async def test_round_trip_bytes_are_identical(self) -> None:
        payload = bytes(range(256)) * 40
        record = await self.files.upload(data=payload, name="binary.bin")
        self.assertEqual(await self.drain(record.id), payload)

    async def test_a_chosen_file_id_is_honoured(self) -> None:
        record = await self.files.upload(data=b"x", name="a.bin", file_id="files_fixed_1")
        self.assertEqual(record.id, "files_fixed_1")

    async def test_an_empty_upload_is_refused(self) -> None:
        with self.assertRaises(StorageError) as raised:
            await self.files.upload(data=b"", name="empty.bin")
        self.assertEqual(raised.exception.code, "VALIDATION_ERROR")

    async def test_metadata_survives_a_cold_rebuild(self) -> None:
        record = await self.files.upload(data=b"payload", name="keep.bin", mime="text/plain")
        index = ChannelIndex(self.config)
        await index.rebuild(self.gateway)
        restored = index.get(FILES_COLLECTION, record.id)
        assert restored is not None
        self.assertEqual(restored.data["name"], "keep.bin")
        self.assertEqual(restored.data["mime_type"], "text/plain")
        self.assertEqual(restored.data["size"], 7)


class Ranges(FileCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.payload = bytes(range(256)) * 8  # 2048 deterministic bytes
        self.record = await self.files.upload(data=self.payload, name="media.mp4", mime="video/mp4")

    async def test_full_download(self) -> None:
        self.assertEqual(await self.drain(self.record.id), self.payload)

    async def test_offset_and_length(self) -> None:
        self.assertEqual(
            await self.drain(self.record.id, offset=100, length=50),
            self.payload[100:150],
        )

    async def test_offset_to_the_end(self) -> None:
        self.assertEqual(await self.drain(self.record.id, offset=2000), self.payload[2000:])

    async def test_a_browser_style_range(self) -> None:
        """`Range: bytes=0-1023` — how a player asks for the first chunk."""
        self.assertEqual(
            await self.drain(self.record.id, offset=0, length=1024),
            self.payload[:1024],
        )

    async def test_past_the_end_is_empty_not_an_error(self) -> None:
        self.assertEqual(await self.drain(self.record.id, offset=999_999), b"")

    async def test_a_bad_range_header_is_reported_as_416(self) -> None:
        with self.assertRaises(StorageError) as raised:
            parse_range("bytes=5000-", 2048)
        self.assertEqual(raised.exception.status, 416)

    def test_range_parsing(self) -> None:
        self.assertEqual(parse_range(None, 100), (0, 99))
        self.assertEqual(parse_range("bytes=0-9", 100), (0, 9))
        self.assertEqual(parse_range("bytes=10-", 100), (10, 99))
        self.assertEqual(parse_range("bytes=-10", 100), (90, 99))
        self.assertEqual(parse_range("bytes=0-100000", 100), (0, 99))
        self.assertEqual(parse_range("garbage", 100), (0, 99))


class Metadata(FileCase):
    async def test_rename_changes_only_the_record(self) -> None:
        """Telegram's own filename cannot change without a re-upload, so it does not."""
        record = await self.files.upload(data=b"payload", name="original.bin")
        original_telegram_name = self.gateway.messages["tg.vault.1"][0].name

        renamed = await self.files.rename(record.id, "renamed.bin")
        self.assertEqual(renamed.name, "renamed.bin")
        self.assertEqual(self.gateway.messages["tg.vault.1"][0].name, original_telegram_name)
        self.assertEqual(self.files.get(record.id).name, "renamed.bin")

    async def test_rename_needs_a_name(self) -> None:
        record = await self.files.upload(data=b"payload", name="a.bin")
        with self.assertRaises(StorageError) as raised:
            await self.files.rename(record.id, "   ")
        self.assertEqual(raised.exception.code, "VALIDATION_ERROR")

    async def test_list_filters_by_owner(self) -> None:
        await self.files.upload(data=b"a", name="a.bin", owner="u1")
        await self.files.upload(data=b"b", name="b.bin", owner="u2")
        self.assertEqual(len(self.files.list(owner="u1")), 1)

    async def test_delete_hides_the_record_and_keeps_the_bytes_by_default(self) -> None:
        record = await self.files.upload(data=b"bytes", name="a.bin")
        result = await self.files.delete(record.id)
        self.assertTrue(result["deleted"])
        with self.assertRaises(StorageError):
            self.files.get(record.id)
        # Soft delete: an operator can still recover the bytes from the channel.
        self.assertEqual(len(self.gateway.files["tg.vault.1"]), 1)

    async def test_hard_delete_removes_the_bytes(self) -> None:
        config = make_config(hard_delete=True)
        gateway = FakeTelegram(config)
        index = ChannelIndex(config)
        documents = DocumentStore(config, gateway, index, HotStore(config))
        files = FileStore(config, gateway, documents)

        record = await files.upload(data=b"bytes", name="a.bin")
        await files.delete(record.id)
        self.assertEqual(gateway.files["tg.vault.1"], {})

    async def test_a_missing_file_is_a_404(self) -> None:
        with self.assertRaises(StorageError) as raised:
            self.files.get("nope")
        self.assertEqual(raised.exception.status, 404)


if __name__ == "__main__":
    unittest.main()
