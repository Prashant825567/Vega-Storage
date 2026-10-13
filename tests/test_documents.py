"""The write path, proven end to end against a fake channel.

These are the claims the design rests on, and each one has a test:

* a write reaches Telegram **before** the index, so a failed edit cannot leave a
  cache describing a document that does not exist;
* `rev` + `expected_rev` refuses a stale writer;
* an edit happens **in place** — no new message, no tombstone, no growth;
* a cold rebuild restores everything from the channel alone;
* presence writes touch no channel at all.
"""

from __future__ import annotations

import unittest

from storage_backend.documents import DocumentStore
from storage_backend.errors import StorageError
from storage_backend.hotstore import HotStore
from storage_backend.index import ChannelIndex
from storage_backend.wire import decode

from tests.fakes import FakeTelegram, make_config


class StoreCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.config = make_config()
        self.gateway = FakeTelegram(self.config)
        self.index = ChannelIndex(self.config)
        self.hot = HotStore(self.config, ttl_seconds={"presence": 0.0})
        self.store = DocumentStore(self.config, self.gateway, self.index, self.hot)

    def support(self) -> list[int]:
        return self.gateway.message_ids("tg.support.1")

    def total_parts(self, key: str, message_id: int) -> int:
        """How many messages the document says it spans.

        Read from the stored header rather than hard-coded, so these tests keep
        testing the store instead of testing the test's own arithmetic.
        """
        record = decode(self.gateway.text_of(key, message_id))
        assert record is not None
        return record.total


class CreateAndRead(StoreCase):
    async def test_create_writes_one_message_and_returns_rev_one(self) -> None:
        document = await self.store.create("messages", {"chat_id": "a@b.c", "text": "hi"})
        self.assertEqual(document["rev"], 1)
        self.assertEqual(document["data"]["text"], "hi")
        self.assertEqual(len(self.support()), 1)
        self.assertIsNotNone(document["created_at"])

    async def test_a_chosen_id_is_honoured(self) -> None:
        """The chosen id is what makes a heartbeat a single PATCH, not a lookup."""
        document = await self.store.create(
            "messages", {"text": "x"}, doc_id="messages_fixed_1"
        )
        self.assertEqual(document["id"], "messages_fixed_1")

    async def test_a_duplicate_id_is_refused_not_reused(self) -> None:
        await self.store.create("messages", {"text": "x"}, doc_id="same")
        with self.assertRaises(StorageError) as raised:
            await self.store.create("messages", {"text": "y"}, doc_id="same")
        self.assertEqual(raised.exception.status, 409)
        self.assertEqual(raised.exception.code, "DOCUMENT_ALREADY_EXISTS")

    async def test_reads_never_touch_the_network(self) -> None:
        await self.store.create("messages", {"text": "x"})
        sends, edits = self.gateway.sends, self.gateway.edits
        for _ in range(50):
            self.store.get("messages", self.store.index.collection("messages")[0].doc_id)
        self.assertEqual((self.gateway.sends, self.gateway.edits), (sends, edits))

    async def test_a_bad_doc_id_is_refused(self) -> None:
        with self.assertRaises(StorageError) as raised:
            await self.store.create("messages", {"text": "x"}, doc_id="has space")
        self.assertEqual(raised.exception.code, "VALIDATION_ERROR")


class Patch(StoreCase):
    async def test_patch_merges_and_edits_in_place(self) -> None:
        created = await self.store.create("messages", {"text": "hi", "read": False})
        first_id = self.support()[0]

        patched = await self.store.patch("messages", created["id"], {"read": True})
        self.assertEqual(patched["rev"], 2)
        self.assertTrue(patched["data"]["read"])
        # The field that was not mentioned survived.
        self.assertEqual(patched["data"]["text"], "hi")

        # The very same message was edited: no second message appeared.
        self.assertEqual(self.support(), [first_id])
        record = decode(self.gateway.text_of("tg.support.1", first_id))
        assert record is not None
        self.assertEqual(record.rev, 2)

    async def test_a_stale_expected_rev_is_a_409(self) -> None:
        created = await self.store.create("messages", {"text": "hi"})
        await self.store.patch("messages", created["id"], {"text": "second"})

        with self.assertRaises(StorageError) as raised:
            await self.store.patch(
                "messages", created["id"], {"text": "stale"}, expected_rev=1
            )
        self.assertEqual(raised.exception.status, 409)
        self.assertEqual(raised.exception.code, "REVISION_CONFLICT")

    async def test_a_conflict_leaves_the_stored_text_alone(self) -> None:
        created = await self.store.create("messages", {"text": "original"})
        message_id = self.support()[0]
        before = self.gateway.text_of("tg.support.1", message_id)
        with self.assertRaises(StorageError):
            await self.store.patch(
                "messages", created["id"], {"text": "stale"}, expected_rev=7
            )
        self.assertEqual(self.gateway.text_of("tg.support.1", message_id), before)

    async def test_the_right_expected_rev_succeeds(self) -> None:
        created = await self.store.create("messages", {"text": "hi"})
        result = await self.store.patch(
            "messages", created["id"], {"read": True}, expected_rev=1
        )
        self.assertEqual(result["rev"], 2)

    async def test_patch_of_a_missing_document_is_a_404(self) -> None:
        with self.assertRaises(StorageError) as raised:
            await self.store.patch("messages", "nope", {"a": 1})
        self.assertEqual(raised.exception.status, 404)

    async def test_upsert_creates_then_merges(self) -> None:
        first = await self.store.upsert("messages", "fixed", {"a": 1})
        self.assertEqual(first["rev"], 1)
        second = await self.store.upsert("messages", "fixed", {"b": 2})
        self.assertEqual(second["rev"], 2)
        self.assertEqual(second["data"], {"a": 1, "b": 2})


class CrashSafety(StoreCase):
    async def test_a_failed_edit_leaves_the_index_untouched(self) -> None:
        """Telegram first, memory second — the whole point of the order."""
        created = await self.store.create("messages", {"text": "good"})
        message_id = self.support()[0]
        self.gateway.fail_on_edit.add(message_id)

        with self.assertRaises(Exception):
            await self.store.patch("messages", created["id"], {"text": "never stored"})

        # The index still describes what the channel actually holds.
        current = self.store.get("messages", created["id"])
        assert current is not None
        self.assertEqual(current["rev"], 1)
        self.assertEqual(current["data"]["text"], "good")
        self.assertEqual(self.store.index.stats()["documents"], 1)


class Delete(StoreCase):
    async def test_tombstone_keeps_the_message_and_hides_the_document(self) -> None:
        created = await self.store.create("messages", {"text": "x"})
        message_id = self.support()[0]

        result = await self.store.delete("messages", created["id"])
        self.assertTrue(result["deleted"])
        self.assertIsNone(self.store.get("messages", created["id"]))
        # Soft delete: the message stays (id preserved), carrying a tombstone.
        self.assertEqual(self.support(), [message_id])
        record = decode(self.gateway.text_of("tg.support.1", message_id))
        assert record is not None
        self.assertIn('"deleted":true', record.chunk)

    async def test_hard_delete_removes_the_messages(self) -> None:
        config = make_config(hard_delete=True)
        gateway = FakeTelegram(config)
        index = ChannelIndex(config)
        store = DocumentStore(config, gateway, index, HotStore(config))

        created = await store.create("messages", {"text": "x"})
        await store.delete("messages", created["id"])
        self.assertEqual(gateway.message_ids("tg.support.1"), [])

    async def test_deleting_a_missing_document_is_a_404(self) -> None:
        with self.assertRaises(StorageError) as raised:
            await self.store.delete("messages", "missing")
        self.assertEqual(raised.exception.status, 404)


class MultiPart(StoreCase):
    async def test_a_big_document_uses_several_messages(self) -> None:
        # part_chars is 120 in the fake config, so 400 chars cannot fit in one.
        body_text = "z" * 400
        created = await self.store.create("messages", {"text": body_text})
        self.assertGreater(len(self.support()), 1)
        self.assertEqual(len(self.support()), self.total_parts("tg.support.1", self.support()[0]))
        self.assertEqual(created["data"]["text"], body_text)

    async def test_a_shrinking_document_retires_its_extra_parts(self) -> None:
        created = await self.store.create("messages", {"text": "z" * 400})
        self.assertGreater(len(self.support()), 1)

        await self.store.patch("messages", created["id"], {"text": "small"})
        # One message holds it again and the superseded fragments are gone, so a
        # later rebuild cannot resurrect an older revision out of them.
        self.assertEqual(self.store.get("messages", created["id"])["data"]["text"], "small")
        self.assertEqual(len(self.support()), 1)
        record = decode(self.gateway.text_of("tg.support.1", self.support()[0]))
        assert record is not None
        self.assertEqual(record.rev, 2)

    async def test_a_growing_document_adds_parts(self) -> None:
        created = await self.store.create("messages", {"text": "small"})
        self.assertEqual(len(self.support()), 1)
        await self.store.patch("messages", created["id"], {"text": "w" * 400})
        self.assertGreater(len(self.support()), 1)
        self.assertEqual(
            len(self.support()), self.total_parts("tg.support.1", self.support()[0])
        )
        self.assertEqual(
            self.store.get("messages", created["id"])["data"]["text"], "w" * 400
        )

    async def test_oversized_bodies_are_refused_with_a_pointer_hint(self) -> None:
        with self.assertRaises(StorageError) as raised:
            await self.store.create("messages", {"blob": "q" * 70_000})
        self.assertEqual(raised.exception.code, "VALIDATION_ERROR")
        self.assertIn("Store the payload as a file", raised.exception.message)


class ColdRebuild(StoreCase):
    async def test_everything_comes_back_from_the_channel_alone(self) -> None:
        first = await self.store.create("messages", {"chat_id": "a@b.c", "text": "one"})
        second = await self.store.create("messages", {"chat_id": "b@b.c", "text": "two"})
        await self.store.patch("messages", first["id"], {"read": True})
        await self.store.delete("messages", second["id"])

        # A brand-new process: fresh index, fresh store, same channels.
        index = ChannelIndex(self.config)
        store = DocumentStore(self.config, self.gateway, index, HotStore(self.config))
        await index.rebuild(self.gateway)

        restored = store.get("messages", first["id"])
        assert restored is not None
        self.assertEqual(restored["rev"], 2)
        self.assertTrue(restored["data"]["read"])
        self.assertEqual(restored["data"]["text"], "one")
        # The deleted one is still deleted, not resurrected.
        self.assertIsNone(store.get("messages", second["id"]))
        self.assertTrue(index.ready)
        self.assertEqual(index.stats()["documents"], 2)

    async def test_multi_part_documents_rebuild_intact(self) -> None:
        body_text = "m" * 500
        created = await self.store.create("messages", {"text": body_text})
        index = ChannelIndex(self.config)
        await index.rebuild(self.gateway)
        restored = index.get("messages", created["id"])
        assert restored is not None
        self.assertEqual(restored.data["text"], body_text)
        self.assertGreater(restored.part_count, 1)
        self.assertEqual(
            restored.part_count, self.total_parts("tg.support.1", self.support()[0])
        )

    async def test_a_half_written_revision_loses_to_the_complete_one(self) -> None:
        """A crash mid-write must leave an older *complete* document, not a torn one.

        Reproduced the way it actually happens: the process got as far as putting
        the first message of revision 9 into the channel before it died. With
        append-then-swap the old revision is still intact, so the rebuild serves it.
        """
        created = await self.store.create("messages", {"text": "z" * 400, "read": False})
        total = self.total_parts("tg.support.1", self.support()[0])

        await self.gateway.send(
            "tg.support.1",
            f"ZG1|messages|{created['id']}|9|1/{total}|"
            '{"data":{"text":"half written"},"created_at":"2026-10-13T00:00:00Z"}',
        )

        index = ChannelIndex(self.config)
        await index.rebuild(self.gateway)
        restored = index.get("messages", created["id"])
        assert restored is not None, "the previous revision must survive a torn write"
        # Revision 9 is incomplete, so revision 1 stays — complete beats newer.
        self.assertEqual(restored.rev, 1)
        self.assertEqual(restored.data["text"], "z" * 400)
        self.assertFalse(restored.data["read"])
        self.assertGreater(index.dropped_incomplete, 0)

    async def test_foreign_messages_in_a_channel_are_ignored(self) -> None:
        await self.store.create("messages", {"text": "real"})
        await self.gateway.send("tg.support.1", "hello, a human typed this")

        index = ChannelIndex(self.config)
        await index.rebuild(self.gateway)
        self.assertEqual(index.stats()["documents"], 1)

    async def test_the_rebuild_indexes_every_channel(self) -> None:
        await self.store.create("messages", {"text": "support"})
        await self.store.create("chat_broadcast", {"text": "announcement"})
        index = ChannelIndex(self.config)
        await index.rebuild(self.gateway)
        self.assertIn("messages", index.collections())
        self.assertIn("chat_broadcast", index.collections())
        self.assertEqual(index.stats()["scanned_messages"], 2)


class Routing(StoreCase):
    async def test_collections_land_in_their_own_channels(self) -> None:
        await self.store.create("messages", {"text": "a"})
        await self.store.create("chat_broadcast", {"text": "b"})
        await self.store.create("ai_chats", {"text": "c"})
        self.assertEqual(len(self.gateway.message_ids("tg.support.1")), 1)
        self.assertEqual(len(self.gateway.message_ids("tg.notify.1")), 1)
        self.assertEqual(len(self.gateway.message_ids("tg.ai.1")), 1)

    async def test_an_unknown_collection_uses_the_default_channel(self) -> None:
        await self.store.create("something_new", {"text": "x"})
        self.assertEqual(len(self.gateway.message_ids("tg.support.1")), 1)


class HotCollections(StoreCase):
    async def test_presence_never_touches_a_channel(self) -> None:
        """The heartbeat is the workload a ~1 write/second chat cannot take."""
        await self.store.create("presence", {"user": "a@b.c"}, doc_id="presence_user_a")
        for _ in range(100):
            await self.store.patch("presence", "presence_user_a", {"last_seen": "now"})
        self.assertEqual(self.gateway.sends, 0)
        self.assertEqual(self.gateway.edits, 0)
        self.assertEqual(self.store.hot.writes, 101)

    async def test_presence_keeps_the_same_contract(self) -> None:
        created = await self.store.create("presence", {"user": "a"}, doc_id="p1")
        self.assertEqual(created["rev"], 1)
        patched = await self.store.patch("presence", "p1", {"online": True}, expected_rev=1)
        self.assertEqual(patched["rev"], 2)
        with self.assertRaises(StorageError) as raised:
            await self.store.patch("presence", "p1", {"online": False}, expected_rev=1)
        self.assertEqual(raised.exception.code, "REVISION_CONFLICT")

    async def test_a_duplicate_chosen_id_is_still_refused(self) -> None:
        await self.store.create("presence", {"user": "a"}, doc_id="p1")
        with self.assertRaises(StorageError) as raised:
            await self.store.create("presence", {"user": "a"}, doc_id="p1")
        self.assertEqual(raised.exception.code, "DOCUMENT_ALREADY_EXISTS")


class Concurrency(StoreCase):
    async def test_parallel_patches_serialise_on_one_lock(self) -> None:
        """Fifty writers on one document must not lose an update."""
        import asyncio

        created = await self.store.create("messages", {"count": 0})
        doc_id = created["id"]

        async def bump() -> None:
            for _ in range(5):
                current = self.store.get("messages", doc_id)
                assert current is not None
                await self.store.patch(
                    "messages", doc_id, {"count": current["data"]["count"] + 1}
                )

        await asyncio.gather(*(bump() for _ in range(10)))
        final = self.store.get("messages", doc_id)
        assert final is not None
        self.assertEqual(final["data"]["count"], 50)
        self.assertEqual(final["rev"], 51)

    async def test_locks_do_not_accumulate(self) -> None:
        for index in range(30):
            await self.store.create("messages", {"n": index})
        self.assertEqual(self.store._locks.open_locks, 0)


if __name__ == "__main__":
    unittest.main()
