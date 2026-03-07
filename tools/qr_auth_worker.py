#!/usr/bin/env python3
import asyncio
import json
import os
import sys
import time

from telethon import TelegramClient, errors


def write_state(path: str, payload: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, path)


def print_json(payload: dict, code: int = 0):
    print(json.dumps(payload, ensure_ascii=False))
    raise SystemExit(code)


async def run_start(api_id: int, api_hash: str, session_path: str, state_path: str):
    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            write_state(state_path, {
                'status': 'done',
                'phone': str(getattr(me, 'phone', '') or '').strip(),
                'updated_at': time.time()
            })
            return

        qr_login = await client.qr_login()
        write_state(state_path, {
            'status': 'pending',
            'qr_url': qr_login.url,
            'updated_at': time.time()
        })

        try:
            user = await qr_login.wait(timeout=180)
            me = user or await client.get_me()
            write_state(state_path, {
                'status': 'done',
                'phone': str(getattr(me, 'phone', '') or '').strip(),
                'updated_at': time.time()
            })
        except errors.SessionPasswordNeededError:
            write_state(state_path, {
                'status': '2fa_required',
                'message': 'Нужен пароль двухфакторной аутентификации',
                'updated_at': time.time()
            })
        except asyncio.TimeoutError:
            write_state(state_path, {
                'status': 'expired',
                'message': 'QR-код истек. Нажмите "Обновить QR".',
                'updated_at': time.time()
            })
        except Exception as e:
            write_state(state_path, {
                'status': 'error',
                'message': str(e),
                'updated_at': time.time()
            })
    finally:
        await client.disconnect()


async def run_2fa(api_id: int, api_hash: str, session_path: str, password: str):
    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    try:
        await client.sign_in(password=password)
        me = await client.get_me()
        return str(getattr(me, 'phone', '') or '').strip()
    finally:
        await client.disconnect()


def main():
    if len(sys.argv) < 5:
        print_json({'ok': False, 'error': 'Usage: qr_auth_worker.py <start|2fa> <api_id> <api_hash> <session_path> [state_path_or_password]'}, 1)

    mode = str(sys.argv[1] or '').strip().lower()
    try:
        api_id = int(sys.argv[2])
    except Exception:
        print_json({'ok': False, 'error': 'Invalid api_id'}, 1)
    api_hash = str(sys.argv[3] or '').strip()
    session_path = str(sys.argv[4] or '').strip()
    extra = str(sys.argv[5] or '').strip() if len(sys.argv) > 5 else ''

    if not api_hash or not session_path:
        print_json({'ok': False, 'error': 'api_hash and session_path are required'}, 1)

    try:
        if mode == 'start':
            if not extra:
                print_json({'ok': False, 'error': 'state_path is required for start'}, 1)
            asyncio.run(run_start(api_id, api_hash, session_path, extra))
            print_json({'ok': True})
        elif mode == '2fa':
            if not extra:
                print_json({'ok': False, 'error': 'password is required for 2fa'}, 1)
            phone = asyncio.run(run_2fa(api_id, api_hash, session_path, extra))
            print_json({'ok': True, 'phone': phone})
        else:
            print_json({'ok': False, 'error': f'Unsupported mode: {mode}'}, 1)
    except Exception as e:
        print_json({'ok': False, 'error': str(e)}, 1)


if __name__ == '__main__':
    main()
