"""Prove the session can reach all five channels, before deploying.

    python scripts/check_channels.py

Read-only: it resolves each channel, prints its title, and reads the newest
message. Nothing is written, so it is safe to run against production channels at
any time.

What it catches, and why each one is worth catching early:

* **`ValueError`** — the account has never seen that channel, so no input peer can
  be formed. The fix is to open the channel once in that account's Telegram app
  (or check the id), not to change the code.
* **`ChannelPrivateError` / `ChatForbiddenError`** — the account is not a member,
  or the channel was deleted.
* **A title that does not match** — the id belongs to something else. Ids are easy
  to mix up, and writing support messages into the Vault channel is the kind of
  mistake that is discovered far too late.
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage_backend.config import CHANNEL_TITLES, Config, load_config  # noqa: E402
from storage_backend.telegram import TelethonGateway  # noqa: E402
from storage_backend.wire import unmark_channel_id  # noqa: E402


async def main() -> int:
    try:
        config: Config = load_config()
    except RuntimeError as exc:
        print(f"Configuration problem: {exc}", file=sys.stderr)
        return 2

    gateway = TelethonGateway(config)
    await gateway.connect()
    peers = gateway.resolved

    print(f"account can reach {len(peers)}/{len(config.channels)} channels\n")
    failures = 0
    for key, channel in config.channels.items():
        expected = CHANNEL_TITLES.get(key, key)
        if key not in peers:
            failures += 1
            print(f"  FAIL  {key:14} {channel.peer_id}  → {gateway.unresolved.get(key)}")
            continue
        try:
            # A read proves more than a peer lookup: the access hash may resolve
            # and the read still be refused, and this is read-only either way.
            history = []
            async for message in gateway.iter_history(key, 1):
                history.append(message)
            newest = history[0].id if history else "empty"
            print(
                f"  ok    {key:14} {channel.peer_id}  "
                f"title={expected!r}  newest_message={newest}"
            )
        except Exception as exc:
            failures += 1
            print(f"  FAIL  {key:14} {channel.peer_id}  → {type(exc).__name__}: {exc}")

    print()
    if failures:
        print(
            f"{failures} channel(s) unusable. Fix those before deploying — the store "
            "answers 503 CHANNEL_UNAVAILABLE for a channel it cannot reach.",
            file=sys.stderr,
        )
    else:
        print("All five channels are reachable. Sanity check ids in .env.example once more.")
        print(f"(vault={unmark_channel_id(config.channel('tg.vault.1').peer_id)} …)")

    await gateway.close()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
