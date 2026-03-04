#!/usr/bin/env python3
import asyncio
import json
import sys

from telethon import TelegramClient


async def extract_phone(session_path: str, api_id: int, api_hash: str):
    client = None
    try:
        client = TelegramClient(session_path, api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            return None
        me = await client.get_me()
        return getattr(me, "phone", None)
    finally:
        if client:
            await client.disconnect()


def main():
    if len(sys.argv) != 4:
        print(json.dumps({"ok": False, "error": "usage"}))
        return 2
    session_path = sys.argv[1]
    api_id = int(sys.argv[2])
    api_hash = sys.argv[3]
    try:
        phone = asyncio.run(extract_phone(session_path, api_id, api_hash))
        print(json.dumps({"ok": True, "phone": phone}))
        return 0
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
