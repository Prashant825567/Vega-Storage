"""Configuration: read once, from the environment, and never log the secrets.

Every value here has a working default except the three Telegram credentials and
the API key — those must come from Render's environment. The five channel ids are
**not** secrets (an id alone cannot read anything without the session), so they
ship as defaults to make deployment one step shorter. Override them per
environment with `[TST]` channels.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Final

# --------------------------------------------------------------------------- #
# The five channels. Ids taken from the telegram web links the owner supplied:
#   https://web.telegram.org/a/#-1003597396903   → Vega Vault
#   https://web.telegram.org/a/#-1004362809278   → Vega AI
#   https://web.telegram.org/a/#-1004337129320   → Vega Support
#   https://web.telegram.org/a/#-1003755323456   → Vega Notify
#   https://web.telegram.org/a/#-1003960423150   → Vega Index
# Ids are stored **marked** (-100…) because that is the notation Telegram itself
# prints, and `PeerChannel` needs the unmarked positive part — `wire.unmark()`
# converts between the two in exactly one place instead of everywhere.
# --------------------------------------------------------------------------- #
DEFAULT_CHANNELS: Final[dict[str, int]] = {
    "tg.vault.1": -1003597396903,
    "tg.ai.1": -1004362809278,
    "tg.support.1": -1004337129320,
    "tg.notify.1": -1003755323456,
    "tg.index.1": -1003960423150,
}

CHANNEL_TITLES: Final[dict[str, str]] = {
    "tg.vault.1": "Vega Vault",
    "tg.ai.1": "Vega AI",
    "tg.support.1": "Vega Support",
    "tg.notify.1": "Vega Notify",
    "tg.index.1": "Vega Index",
}

CHANNEL_ENV: Final[dict[str, str]] = {
    "tg.vault.1": "TELEGRAM_VAULT_ID",
    "tg.ai.1": "TELEGRAM_AI_ID",
    "tg.support.1": "TELEGRAM_SUPPORT_ID",
    "tg.notify.1": "TELEGRAM_NOTIFY_ID",
    "tg.index.1": "TELEGRAM_INDEX_ID",
}

# --------------------------------------------------------------------------- #
# Where each collection's documents live.
#
# This is the whole point of the routing table: the app and the panel keep
# shouting the same collection names they always have, and the backend decides
# *which box* the bytes land in. Rule of thumb (see TELEGRAM_STORAGE_PLAN.md):
# durable, low-write-rate content → a Telegram channel; a flag that changes
# every few seconds → the hot store, never a channel.
# --------------------------------------------------------------------------- #
DEFAULT_ROUTES: Final[dict[str, str]] = {
    "messages": "tg.support.1",
    "message_media": "tg.support.1",
    "chat_broadcast": "tg.notify.1",
    "broadcasts": "tg.notify.1",
    "notifications": "tg.notify.1",
    "ai_chats": "tg.ai.1",
    "ai_messages": "tg.ai.1",
    "index_manifests": "tg.index.1",
    "files": "tg.vault.1",
    "vault": "tg.vault.1",
}

# Collections whose value is dead within a minute. They are served by the hot
# store (process memory + best-effort disk snapshot) and never touch Telegram.
DEFAULT_HOT: Final[tuple[str, ...]] = ("presence", "typing", "read_cursor")


def _env(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    return (value if value is not None else default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_routes() -> dict[str, str]:
    """`ZG_ROUTE_MESSAGES=tg.notify.1` overrides one collection at a time."""
    routes = dict(DEFAULT_ROUTES)
    for name, value in os.environ.items():
        if not name.startswith("ZG_ROUTE_") or not value.strip():
            continue
        collection = name[len("ZG_ROUTE_") :].strip().lower()
        if collection and collection != "DEFAULT":
            routes[collection] = value.strip()
    return routes


@dataclass(frozen=True)
class Channel:
    """One configured channel: the key we route by, and its marked id."""

    key: str
    peer_id: int

    @property
    def title(self) -> str:
        return CHANNEL_TITLES.get(self.key, self.key)


@dataclass(frozen=True)
class Config:
    api_id: int
    api_hash: str
    session: str
    api_key: str

    channels: dict[str, Channel] = field(default_factory=dict)
    # Seeded with the built-in table: a Config that forgets to pass routes would
    # otherwise send every collection to one channel, which is the bug this
    # default exists to prevent.
    routes: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_ROUTES))
    hot_collections: tuple[str, ...] = DEFAULT_HOT

    default_collection_key: str = "tg.support.1"
    files_key: str = "tg.vault.1"

    # Telegram message text is 4096 chars; 3000 leaves room for the header, the
    # JSON escaping and any entity the client adds.
    part_chars: int = 3000
    # Media captions are only 1024 chars, so a file record is chunked harder.
    caption_part_chars: int = 800

    # ZentraGrid's own ceiling: `limit` above this is a 400, not a truncation.
    max_page_limit: int = 200

    # How much channel history a cold rebuild is willing to read per channel.
    scan_limit: int = 20_000

    # A per-channel minimum spacing between writes. 0 disables it; set it to
    # ~1000 on a busy account to stay inside Telegram's ~1 write/second per chat
    # guidance instead of learning it from a flood wait.
    channel_min_interval_ms: int = 0

    # DELETE either edits the message into a tombstone (keeps the id, cheap) or
    # deletes it from the channel (removes the bytes for good).
    hard_delete: bool = False

    # Where the hot store snapshots itself. Ephemeral on Render by design.
    hot_snapshot_path: str = "/tmp/vega-hot-store.json"

    def channel(self, key: str) -> Channel:
        try:
            return self.channels[key]
        except KeyError as exc:  # pragma: no cover - config mistake, not a bug
            raise KeyError(
                f"Unknown channel key {key!r}. Known keys: {sorted(self.channels)}"
            ) from exc

    def route_for(self, collection: str) -> str:
        """The channel key a collection writes to.

        An explicit `ZG_ROUTE_<COLLECTION>` always wins, then the built-in table,
        then the default channel — so a brand-new collection works before anyone
        has decided where it belongs.
        """
        name = (collection or "").strip().lower()
        override = _env(f"ZG_ROUTE_{name.upper()}")
        if override:
            return override
        return self.routes.get(name, self.default_collection_key)

    def is_hot(self, collection: str) -> bool:
        return (collection or "").strip().lower() in self.hot_collections


def load_config() -> Config:
    missing: list[str] = []

    api_id_raw = _env("TELEGRAM_API_ID")
    if not api_id_raw:
        missing.append("TELEGRAM_API_ID")
    api_hash = _env("TELEGRAM_API_HASH")
    if not api_hash:
        missing.append("TELEGRAM_API_HASH")
    session = _env("TELEGRAM_SESSION")
    if not session:
        missing.append("TELEGRAM_SESSION")

    # The panel and the app already send `ZENTRAGRID_API_KEY` as a Bearer token,
    # so accept that name first and `STORAGE_API_KEY` as the local alias.
    api_key = _env("ZENTRAGRID_API_KEY") or _env("STORAGE_API_KEY")
    if not api_key:
        missing.append("ZENTRAGRID_API_KEY")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(missing)
            + ". Set them in Render → Environment (see .env.example)."
        )

    channels = {
        key: Channel(key=key, peer_id=_env_int(CHANNEL_ENV[key], default_id))
        for key, default_id in DEFAULT_CHANNELS.items()
    }

    default_hot = _env("ZG_HOT_COLLECTIONS")
    hot = (
        tuple(part.strip().lower() for part in default_hot.split(",") if part.strip())
        if default_hot
        else DEFAULT_HOT
    )

    return Config(
        api_id=int(api_id_raw),
        api_hash=api_hash,
        session=session,
        api_key=api_key,
        channels=channels,
        routes=_env_routes(),
        hot_collections=hot,
        default_collection_key=_env("ZG_DEFAULT_COLLECTION_KEY", "tg.support.1"),
        files_key=_env("ZG_FILES_KEY", "tg.vault.1"),
        part_chars=max(200, _env_int("ZG_PART_CHARS", 3000)),
        caption_part_chars=max(200, _env_int("ZG_CAPTION_PART_CHARS", 800)),
        max_page_limit=max(1, _env_int("ZG_MAX_PAGE_LIMIT", 200)),
        scan_limit=max(100, _env_int("ZG_SCAN_LIMIT", 20_000)),
        channel_min_interval_ms=max(
            0, _env_int("ZG_CHANNEL_MIN_INTERVAL_MS", 0)
        ),
        hard_delete=_env_bool("ZG_HARD_DELETE", False),
        hot_snapshot_path=_env("ZG_HOT_SNAPSHOT", "/tmp/vega-hot-store.json"),
    )


# Loaded lazily so importing a module never explodes in a test that does not need
# credentials (the unit tests import the pure helpers, not the running service).
_config: Config | None = None


def get_config() -> Config:
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """Test helper: forget the cached config so a new env can be read."""
    global _config
    _config = None
