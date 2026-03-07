#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import sys

from telethon import TelegramClient


async def _run(session: str, api_id: int, api_hash: str, chat: str):
    client = TelegramClient(session, api_id, api_hash, loop=asyncio.get_running_loop())
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError(f"Service session is not authorized: {os.path.basename(session)}")
        entity = await client.get_entity(chat)
        participants = await client.get_participants(entity, aggressive=False)
        contacts = []
        for p in participants:
            username = getattr(p, "username", None)
            if not username:
                continue
            contacts.append({
                "phone": None,
                "username": username,
                "access_hash": str(getattr(p, "access_hash", "") or "") or None,
                "name": " ".join(
                    [x for x in [getattr(p, "first_name", ""), getattr(p, "last_name", "")] if x]
                ).strip() or None,
                "company": None,
                "position": None,
                "user_id": int(getattr(p, "id", 0) or 0) or None
            })
        return contacts
    finally:
        await client.disconnect()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True)
    parser.add_argument("--api-id", required=True, type=int)
    parser.add_argument("--api-hash", required=True)
    parser.add_argument("--chat", required=True)
    args = parser.parse_args()

    try:
        contacts = asyncio.run(_run(args.session, args.api_id, args.api_hash, args.chat))
        sys.stdout.write(json.dumps(contacts, ensure_ascii=False))
        return 0
    except Exception as e:
        sys.stderr.write(str(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
