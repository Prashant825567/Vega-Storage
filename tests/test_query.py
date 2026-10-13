"""The query dialect has to match what the clients already send, because they
cannot be changed. These cases are copied from real call sites in the panel and
the app rather than invented."""

from __future__ import annotations

import unittest

from storage_backend.errors import StorageError
from storage_backend.query import (
    Filter,
    Ordering,
    apply_filters,
    check_limit,
    paginate,
    parse_order_by,
    parse_where,
    sort_documents,
)


def doc(doc_id: str, **data: object) -> dict[str, object]:
    return {"id": doc_id, "rev": 1, "data": data, "created_at": None, "updated_at": None}


class ParseWhere(unittest.TestCase):
    def test_the_panel_shape(self) -> None:
        # `where=chat_id:eq:chat_user@example.com` — as `queryString` builds it.
        filters = parse_where(["chat_id:eq:chat_user@example.com"])
        self.assertEqual(filters, [Filter("chat_id", "eq", "chat_user@example.com")])

    def test_timestamp_values_survive_their_colons(self) -> None:
        """A value is very often an ISO timestamp; splitting on every colon shreds it."""
        filters = parse_where(["sent_at:gt:2026-10-13T00:00:00Z"])
        self.assertEqual(filters[0].value, "2026-10-13T00:00:00Z")

    def test_repeats_are_kept_as_separate_filters(self) -> None:
        filters = parse_where(["chat_id:eq:a", "read:eq:false"])
        self.assertEqual(len(filters), 2)

    def test_aliases(self) -> None:
        self.assertEqual(parse_where(["r:=:1"])[0].operator, "eq")
        self.assertEqual(parse_where(["r:!=:1"])[0].operator, "ne")
        self.assertEqual(parse_where(["r:>=:1"])[0].operator, "gte")

    def test_bad_shapes_are_400s(self) -> None:
        for bad in ("chat_id", "chat_id:eq", "chat_id:like:a"):
            with self.assertRaises(StorageError) as raised:
                parse_where([bad])
            self.assertEqual(raised.exception.status, 400)
            self.assertEqual(raised.exception.code, "VALIDATION_ERROR")

    def test_empty_entries_are_ignored(self) -> None:
        self.assertEqual(parse_where(["", "   "]), [])


class Filters(unittest.TestCase):
    def test_eq_on_a_body_field(self) -> None:
        documents = [doc("1", chat_id="a"), doc("2", chat_id="b")]
        matched = apply_filters(documents, [Filter("chat_id", "eq", "a")])
        self.assertEqual([d["id"] for d in matched], ["1"])

    def test_eq_can_reach_the_envelope(self) -> None:
        matched = apply_filters([doc("abc")], [Filter("id", "eq", "abc")])
        self.assertEqual(len(matched), 1)

    def test_ne(self) -> None:
        documents = [doc("1", read=True), doc("2", read=False)]
        matched = apply_filters(documents, [Filter("read", "ne", "true")])
        self.assertEqual([d["id"] for d in matched], ["2"])

    def test_greater_than_on_a_number_stays_numeric(self) -> None:
        """`rev:gt:9` must not match rev 10 lexicographically *and* rev 2."""
        documents = [{"id": "a", "rev": 2}, {"id": "b", "rev": 10}]
        matched = apply_filters(documents, [Filter("rev", "gt", "9")])
        self.assertEqual([d["id"] for d in matched], ["b"])

    def test_greater_than_on_an_iso_timestamp(self) -> None:
        documents = [
            doc("old", sent_at="2026-01-01T00:00:00Z"),
            doc("new", sent_at="2026-10-13T00:00:00Z"),
        ]
        matched = apply_filters(documents, [Filter("sent_at", "gt", "2026-06-01T00:00:00Z")])
        self.assertEqual([d["id"] for d in matched], ["new"])

    def test_a_missing_field_never_satisfies_an_ordering_filter(self) -> None:
        matched = apply_filters([doc("x")], [Filter("sent_at", "gt", "2000-01-01T00:00:00Z")])
        self.assertEqual(matched, [])

    def test_filters_are_anded(self) -> None:
        documents = [
            doc("1", chat_id="a", read=False),
            doc("2", chat_id="a", read=True),
            doc("3", chat_id="b", read=False),
        ]
        matched = apply_filters(
            documents, [Filter("chat_id", "eq", "a"), Filter("read", "eq", "false")]
        )
        self.assertEqual([d["id"] for d in matched], ["1"])

    def test_booleans_are_compared_as_booleans(self) -> None:
        matched = apply_filters([doc("1", read=False)], [Filter("read", "eq", "false")])
        self.assertEqual(len(matched), 1)

    def test_nested_field_path(self) -> None:
        matched = apply_filters([doc("1", meta={"k": "v"})], [Filter("meta.k", "eq", "v")])
        self.assertEqual(len(matched), 1)


class OrderingCases(unittest.TestCase):
    def test_parse(self) -> None:
        self.assertEqual(parse_order_by("sent_at"), Ordering("sent_at", False))
        self.assertEqual(parse_order_by("sent_at:desc"), Ordering("sent_at", True))
        self.assertEqual(parse_order_by("sent_at:DESC"), Ordering("sent_at", True))
        self.assertIsNone(parse_order_by(None))

    def test_sorts_ascending_and_descending(self) -> None:
        documents = [
            doc("b", sent_at="2026-02-01T00:00:00Z"),
            doc("a", sent_at="2026-01-01T00:00:00Z"),
            doc("c", sent_at="2026-03-01T00:00:00Z"),
        ]
        ascending = sort_documents(list(documents), Ordering("sent_at"))
        self.assertEqual([d["id"] for d in ascending], ["a", "b", "c"])
        descending = sort_documents(list(documents), Ordering("sent_at", True))
        self.assertEqual([d["id"] for d in descending], ["c", "b", "a"])

    def test_missing_values_sort_last_in_both_directions(self) -> None:
        documents = [doc("has", sent_at="2026-01-01T00:00:00Z"), doc("missing")]
        for descending in (False, True):
            ordered = sort_documents(list(documents), Ordering("sent_at", descending))
            self.assertEqual(ordered[-1]["id"], "missing")


class Paging(unittest.TestCase):
    def setUp(self) -> None:
        self.documents = [doc(str(index)) for index in range(5)]

    def test_one_page_when_it_all_fits(self) -> None:
        page = paginate(self.documents, limit=10, cursor=None)
        self.assertEqual(len(page.documents), 5)
        self.assertIsNone(page.next_cursor)

    def test_walks_every_page_exactly_once(self) -> None:
        seen: list[str] = []
        cursor = None
        for _ in range(10):
            page = paginate(self.documents, limit=2, cursor=cursor)
            seen.extend(str(document["id"]) for document in page.documents)
            cursor = page.next_cursor
            if cursor is None:
                break
        self.assertEqual(seen, ["0", "1", "2", "3", "4"])

    def test_an_offset_past_the_end_is_an_empty_page(self) -> None:
        page = paginate(self.documents, limit=2, cursor="OTk5")  # 999
        self.assertEqual(page.documents, [])
        self.assertIsNone(page.next_cursor)


class Limits(unittest.TestCase):
    def test_a_limit_above_the_ceiling_is_a_400_not_a_trim(self) -> None:
        """ZentraGrid refuses; a silently trimmed page is a blank tile in the UI."""
        with self.assertRaises(StorageError) as raised:
            check_limit(500, 200)
        self.assertEqual(raised.exception.status, 400)
        self.assertIn("less than or equal to 200", raised.exception.message)

    def test_the_ceiling_itself_is_allowed(self) -> None:
        self.assertEqual(check_limit(200, 200), 200)

    def test_missing_or_nonsense_limit_falls_back(self) -> None:
        self.assertEqual(check_limit(None, 200), 200)
        self.assertEqual(check_limit(0, 200), 200)

    def test_a_non_integer_limit_is_a_400(self) -> None:
        with self.assertRaises(StorageError):
            check_limit("many", 200)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
