"""The HTTP surface, proven over real requests.

The clients were written against ZentraGrid and are not going to be changed, so
what matters here is the *shape* on the wire: the status codes, the `documents` /
`next_cursor` envelope, the two `error` spellings, the 400 on an oversized page,
the 409 on a stale revision, and a `206` with a correct `Content-Range` for media
seeking.

These run against the app with the fake channel injected, so no account and no
network are involved.
"""

from __future__ import annotations

import asyncio
import json
import unittest

from fastapi.testclient import TestClient

from storage_backend.app import build_app

from tests.fakes import FakeTelegram, make_config

KEY = "test-key"
AUTH = {"Authorization": f"Bearer {KEY}"}


class AppCase(unittest.TestCase):
    def setUp(self) -> None:
        self.config = make_config()
        # One fake channel, shared by the app's lifecycle and by the assertions.
        self.gateway = FakeTelegram(self.config)
        self.app = build_app(self.config, gateway_factory=lambda _config: self.gateway)
        self.client = TestClient(self.app)
        # Entering the context runs the lifespan: connect (no-op) + cold rebuild.
        self.client.__enter__()
        # Startup intentionally runs in the background. Do not depend on the
        # scheduler finishing the handshake/rebuild before TestClient enters.
        async def wait_ready():
            async def poll():
                while not self.app.state.services.index.ready:
                    await asyncio.sleep(0.001)
            await asyncio.wait_for(poll(), timeout=2)
        self.client.portal.call(wait_ready)

    def tearDown(self) -> None:
        self.client.__exit__(None, None, None)


class HealthAndAuth(AppCase):
    def test_healthz_answers_without_a_key(self) -> None:
        response = self.client.get("/healthz")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["connected"])
        self.assertTrue(body["index_ready"])

    def test_status_lists_the_five_channels(self) -> None:
        response = self.client.get("/v1/status", headers=AUTH)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(len(body["channels"]), 5)
        self.assertTrue(all(c["resolved"] for c in body["channels"].values()))
        self.assertTrue(body["index"]["ready"])
        self.assertIn("presence", body["hot_collections"])

    def test_no_key_is_a_401(self) -> None:
        response = self.client.get("/v1/status")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["code"], "INVALID_ID_TOKEN")

    def test_a_wrong_key_is_a_401(self) -> None:
        response = self.client.get("/v1/status", headers={"Authorization": "Bearer nope"})
        self.assertEqual(response.status_code, 401)

    def test_the_api_key_header_works_too(self) -> None:
        response = self.client.get("/v1/status", headers={"X-Api-Key": KEY})
        self.assertEqual(response.status_code, 200)

    def test_an_error_carries_both_spellings(self) -> None:
        """The panel reads `error.message`; a plain reader reads `error`."""
        body = self.client.get("/v1/status").json()
        self.assertIsInstance(body["error"], str)
        self.assertEqual(body["message"], body["error"])
        self.assertEqual(body["error_detail"]["code"], "INVALID_ID_TOKEN")


class DocumentsOverHttp(AppCase):
    def test_create_read_patch_delete(self) -> None:
        created = self.client.post(
            "/v1/data/messages",
            headers=AUTH,
            json={"id": "doc-1", "data": {"chat_id": "a@b.c", "text": "hi", "read": False}},
        )
        self.assertEqual(created.status_code, 201)
        body = created.json()
        self.assertEqual(body["rev"], 1)
        self.assertEqual(body["id"], "doc-1")
        self.assertIsNotNone(body["created_at"])

        fetched = self.client.get("/v1/data/messages/doc-1", headers=AUTH)
        self.assertEqual(fetched.status_code, 200)
        self.assertEqual(fetched.json()["data"]["text"], "hi")

        patched = self.client.patch(
            "/v1/data/messages/doc-1",
            headers=AUTH,
            json={"data": {"read": True}, "expected_rev": 1},
        )
        self.assertEqual(patched.status_code, 200)
        self.assertEqual(patched.json()["rev"], 2)
        self.assertTrue(patched.json()["data"]["read"])

        deleted = self.client.delete("/v1/data/messages/doc-1", headers=AUTH)
        self.assertEqual(deleted.status_code, 200)
        self.assertTrue(deleted.json()["deleted"])

        gone = self.client.get("/v1/data/messages/doc-1", headers=AUTH)
        self.assertEqual(gone.status_code, 404)

    def test_a_stale_expected_rev_is_a_409_with_the_real_revision(self) -> None:
        self.client.post(
            "/v1/data/messages", headers=AUTH, json={"id": "d", "data": {"n": 0}}
        )
        self.client.patch(
            "/v1/data/messages/d", headers=AUTH, json={"data": {"n": 1}, "expected_rev": 1}
        )
        response = self.client.patch(
            "/v1/data/messages/d", headers=AUTH, json={"data": {"n": 2}, "expected_rev": 1}
        )
        self.assertEqual(response.status_code, 409)
        body = response.json()
        self.assertEqual(body["code"], "REVISION_CONFLICT")
        self.assertEqual(body["details"]["rev"], 2)
        self.assertEqual(body["details"]["expected_rev"], 1)

    def test_a_duplicate_chosen_id_is_a_409(self) -> None:
        self.client.post(
            "/v1/data/messages", headers=AUTH, json={"id": "dup", "data": {"n": 1}}
        )
        response = self.client.post(
            "/v1/data/messages", headers=AUTH, json={"id": "dup", "data": {"n": 2}}
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["code"], "DOCUMENT_ALREADY_EXISTS")

    def test_query_envelope_and_cursor(self) -> None:
        for index in range(5):
            self.client.post(
                "/v1/data/messages",
                headers=AUTH,
                json={"id": f"m{index}", "data": {"chat_id": "same", "n": index}},
            )
        response = self.client.get(
            "/v1/data/messages",
            headers=AUTH,
            params={"where": "chat_id:eq:same", "order_by": "n:desc", "limit": 2},
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(len(body["documents"]), 2)
        self.assertEqual([d["data"]["n"] for d in body["documents"]], [4, 3])
        self.assertIsNotNone(body["next_cursor"])

        page_two = self.client.get(
            "/v1/data/messages",
            headers=AUTH,
            params={
                "where": "chat_id:eq:same",
                "order_by": "n:desc",
                "limit": 2,
                "cursor": body["next_cursor"],
            },
        )
        self.assertEqual([d["data"]["n"] for d in page_two.json()["documents"]], [2, 1])

    def test_a_limit_above_the_ceiling_is_a_400(self) -> None:
        response = self.client.get("/v1/data/messages", headers=AUTH, params={"limit": 500})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["code"], "VALIDATION_ERROR")
        self.assertIn("less than or equal to 200", response.json()["error"])

    def test_repeated_where_parameters_are_anded(self) -> None:
        self.client.post(
            "/v1/data/messages",
            headers=AUTH,
            json={"id": "a", "data": {"chat_id": "x", "read": False}},
        )
        self.client.post(
            "/v1/data/messages",
            headers=AUTH,
            json={"id": "b", "data": {"chat_id": "x", "read": True}},
        )
        response = self.client.get(
            "/v1/data/messages",
            headers=AUTH,
            params=[("where", "chat_id:eq:x"), ("where", "read:eq:false")],
        )
        self.assertEqual([d["id"] for d in response.json()["documents"]], ["a"])

    def test_a_bad_where_is_a_400(self) -> None:
        response = self.client.get("/v1/data/messages", headers=AUTH, params={"where": "oops"})
        self.assertEqual(response.status_code, 400)

    def test_a_bad_cursor_is_a_400(self) -> None:
        response = self.client.get(
            "/v1/data/messages", headers=AUTH, params={"cursor": "!!!"}
        )
        self.assertEqual(response.status_code, 400)

    def test_a_non_json_body_is_a_400(self) -> None:
        response = self.client.post(
            "/v1/data/messages",
            headers={**AUTH, "Content-Type": "application/json"},
            content=b"not json",
        )
        self.assertEqual(response.status_code, 400)

    def test_an_unknown_collection_routes_to_the_default_channel(self) -> None:
        created = self.client.post(
            "/v1/data/brand_new", headers=AUTH, json={"data": {"n": 1}}
        )
        self.assertEqual(created.status_code, 201)
        self.assertEqual(len(self.gateway.message_ids("tg.support.1")), 1)

    def test_presence_never_reaches_a_channel(self) -> None:
        self.client.post(
            "/v1/data/presence", headers=AUTH, json={"id": "p1", "data": {"user": "a@b.c"}}
        )
        for _ in range(10):
            response = self.client.patch(
                "/v1/data/presence/p1", headers=AUTH, json={"data": {"last_seen": "now"}}
            )
            self.assertEqual(response.status_code, 200)
        self.assertEqual(self.gateway.messages["tg.vault.1"], [])
        self.assertEqual(len(self.gateway.messages["tg.support.1"]), 0)
        self.assertEqual(self.gateway.sends + self.gateway.edits, 0)


class FilesOverHttp(AppCase):
    PAYLOAD = bytes(range(256)) * 4

    def upload(self, name: str = "media.mp4", mime: str = "video/mp4") -> dict:
        response = self.client.post(
            "/v1/files",
            headers=AUTH,
            files={"file": (name, self.PAYLOAD, mime)},
        )
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()

    def test_upload_returns_the_zentragrid_file_shape(self) -> None:
        record = self.upload()
        self.assertIn("id", record)
        self.assertEqual(record["name"], "media.mp4")
        self.assertEqual(record["size"], len(self.PAYLOAD))
        self.assertEqual(record["mime_type"], "video/mp4")
        self.assertEqual(record["status"], "ready")
        # The locator stays internal.
        self.assertNotIn("message_id", record)

    def test_download_returns_the_exact_bytes(self) -> None:
        record = self.upload()
        response = self.client.get(f"/v1/files/{record['id']}/download", headers=AUTH)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, self.PAYLOAD)
        self.assertEqual(response.headers["accept-ranges"], "bytes")

    def test_a_range_request_is_a_206_with_content_range(self) -> None:
        record = self.upload()
        response = self.client.get(
            f"/v1/files/{record['id']}/download",
            headers={**AUTH, "Range": "bytes=100-199"},
        )
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, self.PAYLOAD[100:200])
        self.assertEqual(
            response.headers["content-range"], f"bytes 100-199/{len(self.PAYLOAD)}"
        )

    def test_a_suffix_range_gives_the_tail(self) -> None:
        record = self.upload()
        response = self.client.get(
            f"/v1/files/{record['id']}/download",
            headers={**AUTH, "Range": "bytes=-50"},
        )
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, self.PAYLOAD[-50:])

    def test_rename_is_metadata_only(self) -> None:
        record = self.upload()
        telegram_name = self.gateway.messages["tg.vault.1"][0].name
        response = self.client.patch(
            f"/v1/files/{record['id']}", headers=AUTH, json={"name": "renamed.mp4"}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["name"], "renamed.mp4")
        self.assertEqual(self.gateway.messages["tg.vault.1"][0].name, telegram_name)

    def test_delete_then_read_is_a_404(self) -> None:
        record = self.upload()
        self.assertEqual(
            self.client.delete(f"/v1/files/{record['id']}", headers=AUTH).status_code, 200
        )
        self.assertEqual(
            self.client.get(f"/v1/files/{record['id']}", headers=AUTH).status_code, 404
        )

    def test_a_missing_file_is_a_404(self) -> None:
        self.assertEqual(self.client.get("/v1/files/nope", headers=AUTH).status_code, 404)


class PersistenceAcrossRestart(AppCase):
    def test_a_document_survives_a_cold_rebuild(self) -> None:
        """The whole point: the channel alone is enough to come back."""
        self.client.post(
            "/v1/data/messages",
            headers=AUTH,
            json={"id": "persisted", "data": {"text": "still here", "read": False}},
        )
        self.client.patch(
            "/v1/data/messages/persisted", headers=AUTH, json={"data": {"read": True}}
        )
        self.client.__exit__(None, None, None)

        # A brand-new app over the same channels — exactly a redeploy.
        app = build_app(self.config, gateway_factory=lambda _config: self.gateway)
        with TestClient(app) as client:
            response = client.get("/v1/data/messages/persisted", headers=AUTH)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["rev"], 2)
        self.assertTrue(body["data"]["read"])
        self.assertEqual(body["data"]["text"], "still here")


class AdminRebuild(AppCase):
    def test_rebuild_is_idempotent(self) -> None:
        self.client.post(
            "/v1/data/messages", headers=AUTH, json={"id": "r1", "data": {"n": 1}}
        )
        first = self.client.post("/v1/admin/rebuild", headers=AUTH)
        second = self.client.post("/v1/admin/rebuild", headers=AUTH)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json()["index"]["documents"], 1)
        # The document is still readable at the same revision after two rebuilds.
        body = self.client.get("/v1/data/messages/r1", headers=AUTH).json()
        self.assertEqual(body["data"]["n"], 1)


if __name__ == "__main__":
    unittest.main()
