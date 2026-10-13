"""Generate the `TELEGRAM_SESSION` string, once, on your own machine.

Run this locally, never on Render:

    python scripts/make_session.py

It asks for the api_id / api_hash from my.telegram.org, sends a login code to
the phone number of the **storage account** — the account that is a member of the
five channels — and prints a session string. Paste that string into Render's
environment as `TELEGRAM_SESSION`.

Treat the string like the account's password: anybody who has it is signed in as
that account. Do not commit it, do not paste it into a chat, and keep a note of
which account it belongs to — a session string is not self-describing.
"""

from __future__ import annotations

import asyncio
import getpass
import os
import sys

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession


def prompt(label: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    answer = input(f"{label}{suffix}: ").strip()
    return answer or default


async def main() -> int:
    print("Telegram session generator — MTProto (user account)\n")

    api_id_raw = os.environ.get("TELEGRAM_API_ID") or prompt("api_id (my.telegram.org)")
    api_hash = os.environ.get("TELEGRAM_API_HASH") or prompt("api_hash")
    phone = prompt("phone (+91…)", os.environ.get("TELEGRAM_PHONE", ""))

    if not api_id_raw or not api_hash or not phone:
        print("api_id, api_hash and phone are all required.", file=sys.stderr)
        return 1
    try:
        api_id = int(api_id_raw)
    except ValueError:
        print("api_id must be a number.", file=sys.stderr)
        return 1

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()

    sent = await client.send_code_request(phone)
    code = prompt("login code (check the Telegram app)")
    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=sent.phone_code_hash)
    except SessionPasswordNeededError:
        # The account has two-factor enabled; the cloud password is needed once,
        # here, and is never stored.
        password = getpass.getpass("two-factor password: ")
        await client.sign_in(password=password)

    me = await client.get_me()
    session_string = client.session.save()

    print("\n" + "=" * 72)
    print(f"signed in as: {me.first_name or ''} {me.last_name or ''} (@{me.username})".strip())
    print(f"account id:   {me.id}")
    print("=" * 72)
    print("\nTELEGRAM_SESSION (put this in Render → Environment):\n")
    print(session_string)
    print("\n" + "=" * 72)
    print("Next: python scripts/check_channels.py   (proves the account can reach all five)")

    await client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
