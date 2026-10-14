"""Recovery regressions: simulated outages, no live Telegram credentials."""
from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from storage_backend.app import Services, build_app
from storage_backend.telegram import (
    LifecycleFloodWait, SessionUnavailableError, TelethonGateway,
)
from tests.fakes import FakeTelegram, make_config


async def eventually(predicate, timeout=2):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.001)
    await asyncio.wait_for(poll(), timeout=timeout)


class UnstableTelegram(FakeTelegram):
    def __init__(self, config):
        super().__init__(config)
        self.connect_calls = 0
        self.close_calls = 0
        self.history_calls = 0
        self.connect_errors = []
        self.probe_errors = []
        self.hang_connect_once = False
        self.hang_probe_once = False
        self.hang_history_once = False
        self.history_error_once = None
        self.connect_times = []
        self.active_connects = 0
        self.max_active_connects = 0

    async def connect(self):
        self.connect_calls += 1
        self.connect_times.append(time.monotonic())
        self.active_connects += 1
        self.max_active_connects = max(self.max_active_connects, self.active_connects)
        try:
            if self.hang_connect_once:
                self.hang_connect_once = False
                await asyncio.Event().wait()
            if self.connect_errors:
                raise self.connect_errors.pop(0)
            await super().connect()
        finally:
            self.active_connects -= 1

    async def probe(self):
        if self.hang_probe_once:
            self.hang_probe_once = False
            await asyncio.Event().wait()
        if self.probe_errors:
            raise self.probe_errors.pop(0)
        await super().probe()

    async def close(self):
        self.close_calls += 1
        await super().close()

    def iter_history(self, key, limit):
        async def generate():
            self.history_calls += 1
            if self.hang_history_once:
                self.hang_history_once = False
                await asyncio.Event().wait()
            if self.history_error_once:
                exc, self.history_error_once = self.history_error_once, None
                raise exc
            async for message in super(UnstableTelegram, self).iter_history(key, limit):
                yield message
        return generate()


class SupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.config = make_config(
            connect_timeout=0.04, probe_interval=0.01, probe_timeout=0.04,
            reconnect_initial_delay=0.01, reconnect_max_delay=0.04,
            rebuild_timeout=0.05,
        )
        self.gateway = UnstableTelegram(self.config)
        self.services = Services(self.config, self.gateway)

    def start(self):
        self.services.supervisor_task = asyncio.create_task(self.services.bootstrap())

    async def ready(self):
        await eventually(lambda: self.services.connected and self.services.index.ready)

    async def asyncTearDown(self):
        await self.services.shutdown()

    async def test_startup_failure_retries_and_clears_error(self):
        self.gateway.connect_errors = [ConnectionError("temporary outage")]
        self.start()
        await self.ready()
        self.assertEqual(self.gateway.connect_calls, 2)
        self.assertIsNone(self.services.connect_error)
        self.assertEqual(self.gateway.max_active_connects, 1)

    async def test_runtime_disconnect_recovers_without_rebuilding_good_index(self):
        self.start()
        await self.ready()
        document = await self.services.documents.create("messages", {"text": "keep me"})
        scans = self.gateway.history_calls
        self.gateway.is_connected = False
        self.assertFalse(self.services.connected)
        await eventually(lambda: self.gateway.connect_calls == 2 and self.services.connected)
        self.assertEqual(self.gateway.history_calls, scans)
        self.assertEqual(self.services.documents.get("messages", document["id"])["data"],
                         {"text": "keep me"})

    async def test_hung_connect_times_out_and_retries(self):
        self.gateway.hang_connect_once = True
        self.start()
        await self.ready()
        self.assertEqual(self.gateway.connect_calls, 2)
        self.assertEqual(self.gateway.max_active_connects, 1)

    async def test_half_open_transport_probe_timeout_recovers(self):
        self.gateway.hang_probe_once = True
        self.start()
        await self.ready()
        await eventually(lambda: self.gateway.connect_calls == 2 and self.services.connected)
        self.assertGreaterEqual(self.gateway.close_calls, 1)

    async def test_probe_error_recovers_even_when_socket_reports_connected(self):
        self.gateway.probe_errors = [OSError("broken upstream")]
        self.start()
        await eventually(lambda: self.gateway.connect_calls == 2 and self.services.connected)
        await eventually(lambda: self.services.last_successful_probe is not None)
        self.assertIsNone(self.services.connect_error)

    async def test_permanent_session_error_does_not_retry_forever(self):
        self.gateway.connect_errors = [SessionUnavailableError("session revoked")]
        self.start()
        await asyncio.wait_for(self.services.supervisor_task, timeout=1)
        self.assertEqual(self.gateway.connect_calls, 1)
        self.assertFalse(self.services.connected)
        self.assertEqual(self.services.connection_state, "session_error")
        self.assertIn("revoked", self.services.connect_error)

    async def test_revoked_session_during_probe_stops_recovery(self):
        self.gateway.probe_errors = [SessionUnavailableError("session revoked")]
        self.start()
        await asyncio.wait_for(self.services.supervisor_task, timeout=1)
        self.assertFalse(self.services.connected)
        self.assertEqual(self.gateway.connect_calls, 1)

    async def test_flood_wait_is_not_capped_by_short_retry_ceiling(self):
        self.gateway.connect_errors = [LifecycleFloodWait(1)]
        self.start()
        await self.ready()
        self.assertGreaterEqual(self.gateway.connect_times[1] - self.gateway.connect_times[0], 1)

    async def test_failed_rebuild_is_retried(self):
        self.gateway.history_error_once = ConnectionError("history unavailable")
        self.start()
        await self.ready()
        self.assertEqual(self.gateway.connect_calls, 2)
        self.assertEqual(self.services.index.rebuild_errors, {})

    async def test_hung_rebuild_has_timeout_and_recovers(self):
        self.gateway.hang_history_once = True
        self.start()
        await self.ready()
        self.assertEqual(self.gateway.connect_calls, 2)

    async def test_fatal_session_error_in_history_reaches_supervisor(self):
        self.gateway.history_error_once = SessionUnavailableError("revoked during rebuild")
        self.start()
        await asyncio.wait_for(self.services.supervisor_task, timeout=1)
        self.assertFalse(self.services.index.ready)
        self.assertEqual(self.services.connection_state, "session_error")

    async def test_shutdown_cancels_pending_connect(self):
        self.gateway.hang_connect_once = True
        self.start()
        await eventually(lambda: self.gateway.active_connects == 1)
        await self.services.shutdown()
        self.assertTrue(self.services.supervisor_task.done())
        self.assertEqual(self.gateway.active_connects, 0)
        self.assertFalse(self.services.connected)

    async def test_shutdown_interrupts_long_flood_wait(self):
        self.gateway.connect_errors = [LifecycleFloodWait(3600)]
        self.start()
        await eventually(lambda: self.gateway.close_calls >= 1)
        await asyncio.wait_for(self.services.shutdown(), timeout=0.2)
        self.assertTrue(self.services.supervisor_task.done())
        self.assertEqual(self.gateway.connect_calls, 1)

    async def test_backoff_increases_and_is_bounded(self):
        self.gateway.connect_errors = [OSError("offline") for _ in range(4)]
        with patch("storage_backend.app.random.uniform", return_value=1):
            self.start()
            await self.ready()
        gaps = [b - a for a, b in zip(self.gateway.connect_times, self.gateway.connect_times[1:])]
        for gap, lower in zip(gaps, [0.01, 0.02, 0.04, 0.04]):
            self.assertGreaterEqual(gap, lower)
        self.assertLess(sum(gaps), 1)

    async def test_failed_scan_keeps_previously_indexed_documents(self):
        self.start()
        await self.ready()
        document = await self.services.documents.create("messages", {"keep": True})
        # Stop only the supervisor, leaving the fake connected for a manual scan.
        self.services.supervisor_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.services.supervisor_task
        self.gateway.history_error_once = ConnectionError("failed scan")
        await self.services.index.rebuild(self.gateway)
        self.assertFalse(self.services.index.ready)
        self.assertTrue(self.services.index.rebuild_errors)
        self.assertIsNotNone(self.services.index.get("messages", document["id"]))


class HealthTests(unittest.TestCase):
    def test_disconnected_liveness_stays_200_but_readiness_is_503(self):
        config = make_config(probe_interval=60)
        gateway = UnstableTelegram(config)
        app = build_app(config, lambda _: gateway)
        with TestClient(app) as client:
            # Wait on the server's loop, not timing assumptions in this thread.
            client.portal.call(eventually, lambda: app.state.services.index.ready)
            self.assertEqual(client.get("/readyz").status_code, 200)
            gateway.is_connected = False
            response = client.get("/healthz")
            self.assertEqual(response.status_code, 200)
            self.assertFalse(response.json()["connected"])
            self.assertEqual(client.get("/readyz").status_code, 503)
            status = client.get("/v1/status", headers={"Authorization": "Bearer test-key"})
            self.assertFalse(status.json()["connected"])
            write = client.post("/v1/data/messages", json={"data": {}},
                                headers={"Authorization": "Bearer test-key"})
            self.assertEqual(write.status_code, 503)
            self.assertEqual(gateway.sends, 0)

    def test_failed_startup_keeps_http_server_available(self):
        config = make_config(reconnect_initial_delay=60, reconnect_max_delay=60)
        gateway = UnstableTelegram(config)
        gateway.connect_errors = [OSError("offline")]
        app = build_app(config, lambda _: gateway)
        with TestClient(app) as client:
            client.portal.call(eventually, lambda: app.state.services.connect_error is not None)
            self.assertEqual(client.get("/healthz").status_code, 200)
            self.assertEqual(client.get("/readyz").status_code, 503)


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    def client(self):
        client = MagicMock()
        client.connect = AsyncMock()
        client.disconnect = AsyncMock()
        client.is_user_authorized = AsyncMock(return_value=True)
        client.get_me = AsyncMock(return_value=object())
        client.is_connected.return_value = True
        return client

    async def test_non_interactive_connect_and_cleanup_on_failure(self):
        gateway = TelethonGateway(make_config())
        client = self.client()
        client.connect.side_effect = OSError("offline")
        with patch("telethon.sessions.StringSession"), patch("telethon.TelegramClient", return_value=client):
            with self.assertRaises(OSError):
                await gateway.connect()
        client.disconnect.assert_awaited_once()
        client.start.assert_not_called()
        self.assertIsNone(gateway._client)

    async def test_unauthorized_session_never_prompts_for_login(self):
        gateway = TelethonGateway(make_config())
        client = self.client()
        client.get_me.return_value = None
        with patch("telethon.sessions.StringSession"), patch("telethon.TelegramClient", return_value=client):
            with self.assertRaises(SessionUnavailableError):
                await gateway.connect()
        client.start.assert_not_called()
        client.disconnect.assert_awaited_once()

    async def test_cancelled_connection_is_cleaned_up(self):
        gateway = TelethonGateway(make_config())
        client = self.client()
        client.connect.side_effect = asyncio.CancelledError()
        with patch("telethon.sessions.StringSession"), patch("telethon.TelegramClient", return_value=client):
            with self.assertRaises(asyncio.CancelledError):
                await gateway.connect()
        client.disconnect.assert_awaited_once()
        self.assertIsNone(gateway._client)

    async def test_invalid_session_is_fatal_without_leaking_string(self):
        gateway = TelethonGateway(make_config(session="secret-but-not-a-session"))
        with self.assertRaises(SessionUnavailableError) as error:
            await gateway.connect()
        self.assertNotIn("secret-but-not-a-session", str(error.exception))

    async def test_duplicate_auth_key_is_fatal(self):
        from telethon.errors import AuthKeyDuplicatedError
        gateway = TelethonGateway(make_config())
        client = self.client()
        client.connect.side_effect = AuthKeyDuplicatedError(request=None)
        with patch("telethon.sessions.StringSession"), patch("telethon.TelegramClient", return_value=client):
            with self.assertRaises(SessionUnavailableError):
                await gateway.connect()
        client.disconnect.assert_awaited_once()

    async def test_flood_wait_during_initial_auth_is_retryable_not_fatal(self):
        from telethon.errors import FloodWaitError
        gateway = TelethonGateway(make_config())
        client = self.client()
        client.get_me.side_effect = FloodWaitError(request=None, capture=60)
        with patch("telethon.sessions.StringSession"), patch("telethon.TelegramClient", return_value=client):
            with self.assertRaises(LifecycleFloodWait) as error:
                await gateway.connect()
        self.assertEqual(error.exception.seconds, 60)
        client.is_user_authorized.assert_not_awaited()
        client.disconnect.assert_awaited_once()

    async def test_probe_none_means_session_is_no_longer_authorized(self):
        gateway = TelethonGateway(make_config())
        gateway._client = self.client()
        gateway._client.get_me.return_value = None
        with self.assertRaises(SessionUnavailableError):
            await gateway.probe()
        await gateway.close()

    async def test_probe_translates_flood_wait(self):
        from telethon.errors import FloodWaitError
        gateway = TelethonGateway(make_config())
        gateway._client = self.client()
        gateway._client.get_me.side_effect = FloodWaitError(request=None, capture=120)
        with self.assertRaises(LifecycleFloodWait) as error:
            await gateway.probe()
        self.assertEqual(error.exception.seconds, 120)
        await gateway.close()

    async def test_dialog_fallback_runs_when_every_channel_is_unresolved(self):
        gateway = TelethonGateway(make_config())
        gateway._client = self.client()
        gateway._resolve_one = AsyncMock(side_effect=ValueError("missing access hash"))
        expected = {channel.peer_id: object() for channel in gateway._config.channels.values()}
        gateway._dialogs_by_id = AsyncMock(return_value=expected)
        peers = await gateway.resolve_channels()
        self.assertEqual(len(peers), 5)
        self.assertEqual(gateway.unresolved, {})
        gateway._dialogs_by_id.assert_awaited_once()
        await gateway.close()

    async def test_history_error_is_not_silently_treated_as_end_of_history(self):
        gateway = TelethonGateway(make_config())
        gateway._client = self.client()
        gateway._peers = {"tg.support.1": object()}
        async def broken_history(*args, **kwargs):
            raise ConnectionError("history outage")
            yield  # an async generator
        gateway._client.iter_messages = broken_history
        with self.assertRaises(ConnectionError):
            async for _ in gateway.iter_history("tg.support.1", 100):
                pass
        await gateway.close()


if __name__ == "__main__":
    unittest.main()
