"""The codec is the contract with the channel: get it wrong and every document
becomes unreadable after a restart. So it is tested directly, including the
unhappy paths (a foreign message, a missing part, a bad cursor)."""

from __future__ import annotations

import unittest

from storage_backend.wire import (
    TAG,
    assemble,
    decode,
    decode_cursor,
    encode_cursor,
    encode_parts,
    unmark_channel_id,
    valid_doc_id,
)


class EncodeDecode(unittest.TestCase):
    def test_single_part_round_trip(self) -> None:
        body = '{"data":{"chat_id":"chat_a@example.com"},"created_at":"2026-10-13T00:00:00Z"}'
        texts = encode_parts("messages", "doc-1", 3, body, 3000)
        self.assertEqual(len(texts), 1)
        self.assertTrue(texts[0].startswith(f"{TAG}|messages|doc-1|3|1/1|"))

        record = decode(texts[0])
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.collection, "messages")
        self.assertEqual(record.doc_id, "doc-1")
        self.assertEqual(record.rev, 3)
        self.assertEqual(record.chunk, body)

        joined = assemble([record])
        self.assertIsNotNone(joined)
        assert joined is not None
        self.assertEqual(joined.body, body)
        self.assertEqual(joined.data()["data"]["chat_id"], "chat_a@example.com")

    def test_multi_part_round_trip(self) -> None:
        body = "x" * 25
        texts = encode_parts("messages", "doc", 1, body, 10)
        self.assertEqual(len(texts), 3)
        records = [decode(text) for text in texts]
        self.assertTrue(all(record is not None for record in records))
        joined = assemble([record for record in records if record is not None])
        self.assertIsNotNone(joined)
        assert joined is not None
        self.assertEqual(joined.body, body)
        self.assertEqual(joined.total, 3)

    def test_pipe_inside_the_json_is_harmless(self) -> None:
        """The header is exactly six fields, so a `|` in the payload stays payload."""
        body = '{"data":{"note":"a|b|c"},"created_at":"2026-10-13T00:00:00Z"}'
        record = decode(encode_parts("messages", "doc", 1, body, 3000)[0])
        assert record is not None
        self.assertEqual(record.chunk, body)

    def test_foreign_messages_are_ignored(self) -> None:
        for text in ("", None, "hello there", "ZG2|messages|doc|1|1/1|x", "ZG1|messages|doc"):
            self.assertIsNone(decode(text), msg=f"decoded {text!r}")

    def test_malformed_numbers_are_ignored(self) -> None:
        self.assertIsNone(decode("ZG1|messages|doc|abc|1/1|x"))
        self.assertIsNone(decode("ZG1|messages|doc|1|9/2|x"))
        self.assertIsNone(decode("ZG1|messages|doc|1|0/1|x"))

    def test_empty_body_still_encodes(self) -> None:
        texts = encode_parts("messages", "doc", 1, "", 100)
        self.assertEqual(len(texts), 1)
        joined = assemble([decode(texts[0])])  # type: ignore[list-item]
        self.assertIsNotNone(joined)
        assert joined is not None
        self.assertEqual(joined.body, "")

    def test_incomplete_set_is_dropped(self) -> None:
        """A hole in a multi-part document must not produce a partial document."""
        texts = encode_parts("messages", "doc", 1, "y" * 25, 10)
        records = [decode(text) for text in texts]
        complete = [record for record in records if record is not None]
        self.assertIsNone(assemble(complete[:2]))
        self.assertIsNone(assemble([]))

    def test_bad_doc_id_is_refused(self) -> None:
        for bad in ("", "has space", "with|pipe", "a/b", ".dotstart"):
            with self.assertRaises(ValueError):
                encode_parts("messages", bad, 1, "{}", 100)

    def test_valid_doc_ids(self) -> None:
        for good in ("abc", "chat_user@example.com", "presence_panel_vega-team", "A1._:-"):
            self.assertTrue(valid_doc_id(good), msg=good)


class MarkedIds(unittest.TestCase):
    def test_unmark(self) -> None:
        # What `PeerChannel` needs for the five configured channels.
        self.assertEqual(unmark_channel_id(-1003597396903), 3597396903)
        self.assertEqual(unmark_channel_id(-1004362809278), 4362809278)
        self.assertEqual(unmark_channel_id(-1004337129320), 4337129320)
        self.assertEqual(unmark_channel_id(-1003755323456), 3755323456)
        self.assertEqual(unmark_channel_id(-1003960423150), 3960423150)

    def test_unmark_leaves_plain_ids_alone(self) -> None:
        self.assertEqual(unmark_channel_id(123456), 123456)


class Cursors(unittest.TestCase):
    def test_round_trip(self) -> None:
        self.assertEqual(decode_cursor(encode_cursor(0)), 0)
        self.assertEqual(decode_cursor(encode_cursor(199)), 199)

    def test_none_and_empty_mean_the_first_page(self) -> None:
        self.assertEqual(decode_cursor(None), 0)
        self.assertEqual(decode_cursor(""), 0)

    def test_garbage_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            decode_cursor("!!!not base64!!!")


if __name__ == "__main__":
    unittest.main()
