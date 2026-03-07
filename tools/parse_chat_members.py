#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import sys

from telethon import TelegramClient
from telethon import functions


def _to_contact(p):
    return {
        "phone": getattr(p, "phone", None) or None,
        "username": getattr(p, "username", None) or None,
        "access_hash": str(getattr(p, "access_hash", "") or "") or None,
        "name": " ".join(
            [x for x in [getattr(p, "first_name", ""), getattr(p, "last_name", "")] if x]
        ).strip() or None,
        "company": None,
        "position": None,
        "user_id": int(getattr(p, "id", 0) or 0) or None
    }


async def _try_join_public_chat(client: TelegramClient, entity):
    try:
        await client(functions.channels.JoinChannelRequest(entity))
    except Exception:
        pass


async def _parse_participants(client: TelegramClient, entity):
    participants = await client.get_participants(entity, aggressive=False)
    contacts = []
    seen = set()
    for p in participants:
        uid = int(getattr(p, "id", 0) or 0)
        if uid <= 0 or uid in seen:
            continue
        seen.add(uid)
        contacts.append(_to_contact(p))
    return contacts


async def _parse_message_authors(client: TelegramClient, entity, messages_limit: int):
    contacts_by_uid = {}
    async for msg in client.iter_messages(entity, limit=messages_limit):
        sender = getattr(msg, "sender", None)
        if sender is None:
            continue
        uid = int(getattr(sender, "id", 0) or 0)
        if uid <= 0:
            continue
        if uid not in contacts_by_uid:
            contacts_by_uid[uid] = _to_contact(sender)
    return list(contacts_by_uid.values())


async def _run(session: str, api_id: int, api_hash: str, chat: str, mode: str, messages_limit: int):
    client = TelegramClient(session, api_id, api_hash, loop=asyncio.get_running_loop())
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError(f"Service session is not authorized: {os.path.basename(session)}")
        entity = await client.get_entity(chat)
        await _try_join_public_chat(client, entity)

        normalized_mode = (mode or "participants").strip().lower()
        if normalized_mode == "authors":
            return await _parse_message_authors(client, entity, messages_limit)
        if normalized_mode == "participants":
            return await _parse_participants(client, entity)
        if normalized_mode == "auto":
            try:
                contacts = await _parse_participants(client, entity)
                if contacts:
                    return contacts
            except Exception:
                pass
            return await _parse_message_authors(client, entity, messages_limit)

        raise RuntimeError(f"Unsupported mode: {mode}")
    finally:
        await client.disconnect()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True)
    parser.add_argument("--api-id", required=True, type=int)
    parser.add_argument("--api-hash", required=True)
    parser.add_argument("--chat", required=True)
    parser.add_argument("--mode", default="auto", choices=["auto", "participants", "authors"])
    parser.add_argument("--messages-limit", type=int, default=3000)
    args = parser.parse_args()

    try:
        messages_limit = int(args.messages_limit or 3000)
        if messages_limit < 100:
            messages_limit = 100
        if messages_limit > 10000:
            messages_limit = 10000
        contacts = asyncio.run(
            _run(args.session, args.api_id, args.api_hash, args.chat, args.mode, messages_limit)
        )
        sys.stdout.write(json.dumps(contacts, ensure_ascii=False))
        return 0
    except Exception as e:
        sys.stderr.write(str(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
