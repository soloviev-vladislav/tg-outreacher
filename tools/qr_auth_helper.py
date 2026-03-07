#!/usr/bin/env python3
import asyncio
import base64
import json
import sys

from telethon import TelegramClient, errors
from telethon.tl.functions.auth import ExportLoginTokenRequest, ImportLoginTokenRequest
from telethon.tl import types as tl_types


def _ok(payload: dict):
    payload = dict(payload or {})
    payload["ok"] = True
    print(json.dumps(payload, ensure_ascii=False))
    raise SystemExit(0)


def _err(message: str):
    print(json.dumps({"ok": False, "error": str(message)}, ensure_ascii=False))
    raise SystemExit(1)


def _enc(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _dec(token: str) -> bytes:
    padding = "=" * ((4 - len(token) % 4) % 4)
    return base64.urlsafe_b64decode(token + padding)


async def do_export(api_id: int, api_hash: str, session_path: str):
    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            return {"already_authorized": True, "phone": str(getattr(me, "phone", "") or "").strip()}

        result = await client(ExportLoginTokenRequest(api_id=api_id, api_hash=api_hash, except_ids=[]))
        if isinstance(result, tl_types.auth.LoginTokenMigrateTo):
            await client._switch_dc(result.dc_id)
            result = await client(ImportLoginTokenRequest(token=result.token))
        if isinstance(result, tl_types.auth.LoginToken):
            return {"token": _enc(result.token)}
        if isinstance(result, tl_types.auth.LoginTokenSuccess):
            me = result.authorization.user
            return {"already_authorized": True, "phone": str(getattr(me, "phone", "") or "").strip()}
        raise RuntimeError("Failed to export login token")
    finally:
        await client.disconnect()


async def do_import(api_id: int, api_hash: str, session_path: str, token: str):
    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    try:
        raw_token = _dec(token)
        result = await client(ImportLoginTokenRequest(token=raw_token))
        if isinstance(result, tl_types.auth.LoginTokenMigrateTo):
            await client._switch_dc(result.dc_id)
            result = await client(ImportLoginTokenRequest(token=result.token))
        if isinstance(result, tl_types.auth.LoginTokenSuccess):
            me = result.authorization.user
            return {"status": "done", "phone": str(getattr(me, "phone", "") or "").strip()}
        return {"status": "pending"}
    except errors.SessionPasswordNeededError:
        return {"status": "2fa_required"}
    finally:
        await client.disconnect()


async def do_2fa(api_id: int, api_hash: str, session_path: str, password: str):
    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    try:
        await client.sign_in(password=password)
        me = await client.get_me()
        return {"phone": str(getattr(me, "phone", "") or "").strip()}
    finally:
        await client.disconnect()


def main():
    if len(sys.argv) < 5:
        _err("Usage: qr_auth_helper.py <mode> <api_id> <api_hash> <session_path> [token_or_password]")

    mode = (sys.argv[1] or "").strip().lower()
    try:
        api_id = int(sys.argv[2])
    except Exception:
        _err("Invalid api_id")
    api_hash = str(sys.argv[3] or "").strip()
    session_path = str(sys.argv[4] or "").strip()
    extra = str(sys.argv[5] or "").strip() if len(sys.argv) > 5 else ""

    if not api_hash or not session_path:
        _err("api_hash and session_path are required")

    try:
        if mode == "export":
            _ok(asyncio.run(do_export(api_id, api_hash, session_path)))
        elif mode == "import":
            if not extra:
                _err("token is required for import")
            _ok(asyncio.run(do_import(api_id, api_hash, session_path, extra)))
        elif mode == "2fa":
            if not extra:
                _err("password is required for 2fa")
            _ok(asyncio.run(do_2fa(api_id, api_hash, session_path, extra)))
        else:
            _err(f"Unsupported mode: {mode}")
    except Exception as e:
        _err(str(e))


if __name__ == "__main__":
    main()
