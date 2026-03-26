from flask import Flask, render_template, request, jsonify, send_file, Response, session, redirect, url_for, g
from flask_socketio import SocketIO, emit
import asyncio
import threading
import os
import json
import sqlite3
import io
import subprocess
import re
import base64
import hashlib
import hmac
import secrets
import shutil
import tempfile
from collections import deque
from datetime import datetime, date, timedelta, timezone
from urllib.parse import urlencode
from werkzeug.utils import secure_filename
from werkzeug.security import check_password_hash, generate_password_hash
import pandas as pd
import requests
from telethon import TelegramClient, errors
from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.functions.channels import JoinChannelRequest, InviteToChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import InputUser, InputPeerUser, PeerUser, InputPhoneContact
from telegram_bot import TelegramAccountManager, Database, OutreachManager, build_telethon_proxy_config
import logging
import time
import sys
import atexit
import csv
import uuid

try:
    import jwt
    from jwt import PyJWKClient
except Exception:
    jwt = None
    PyJWKClient = None

try:
    import fcntl
except Exception:
    fcntl = None

try:
    # Под eventlet стандартный threading может быть monkey-patched (green threads).
    # Для asyncio checker нужен настоящий OS-thread.
    import eventlet.patcher as _eventlet_patcher
    _native_threading = _eventlet_patcher.original('threading')
    NativeThread = _native_threading.Thread
    NativeLock = _native_threading.Lock
    NativeEvent = _native_threading.Event
except Exception:
    NativeThread = threading.Thread
    NativeLock = threading.Lock
    NativeEvent = threading.Event

def load_local_env(path='.env'):
    if not os.path.exists(path):
        return
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip("\"'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass

load_local_env()

# Fix for asyncio on macOS with Python 3.9+.
# На Linux под gunicorn/eventlet это вмешательство ломает вложенные loop'ы.
if sys.platform == 'darwin':
    import selectors
    selector = selectors.SelectSelector()
    loop = asyncio.SelectorEventLoop(selector)
    asyncio.set_event_loop(loop)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'change-me')
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['RESULTS_FOLDER'] = 'results'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)

_PROCESS_LOCK_FD = None

def acquire_single_process_lock():
    """Защита от запуска второго процесса приложения на одном сервере."""
    global _PROCESS_LOCK_FD
    if os.getenv('SINGLE_PROCESS_ONLY', '1') != '1':
        return
    if fcntl is None:
        logger.warning("fcntl недоступен, single-process lock отключен")
        return

    lock_path = os.path.abspath('outreach_app.lock')
    fd = open(lock_path, 'w')
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError("Приложение уже запущено (single-process lock)")

    fd.seek(0)
    fd.truncate()
    fd.write(str(os.getpid()))
    fd.flush()
    _PROCESS_LOCK_FD = fd

    def _release_lock():
        try:
            if _PROCESS_LOCK_FD and fcntl is not None:
                fcntl.flock(_PROCESS_LOCK_FD.fileno(), fcntl.LOCK_UN)
                _PROCESS_LOCK_FD.close()
        except Exception:
            pass

    atexit.register(_release_lock)

# Инициализируем базы данных
db = Database()
outreach = OutreachManager()

GLOBAL_TARGET_CHAT_INVITE = "https://t.me/voronkastore"
BILLING_TRIAL_DAYS = max(1, int(os.getenv('BILLING_TRIAL_DAYS', '7') or '7'))
BILLING_PLAN_DAYS = max(1, int(os.getenv('BILLING_PLAN_DAYS', '30') or '30'))
# Примерная конвертация тарифа 1890 RUB в Telegram Stars.
BILLING_MONTHLY_STARS = max(1, int(os.getenv('BILLING_MONTHLY_STARS', '1050') or '1050'))
BILLING_BOT_POLLING_ENABLED = os.getenv('BILLING_BOT_POLLING_ENABLED', '1') == '1'
SUBSCRIPTION_PAYLOAD_PREFIX = 'voronka_sub_v1'

def normalize_chat_ref(chat_ref: str) -> str:
    if not chat_ref:
        return chat_ref
    value = str(chat_ref).strip()
    if not value:
        return value
    # drop scheme + domain
    for prefix in (
        "https://t.me/",
        "http://t.me/",
        "https://telegram.me/",
        "http://telegram.me/",
        "t.me/",
        "telegram.me/",
    ):
        if value.startswith(prefix):
            value = value[len(prefix):]
            break
    value = value.strip()
    if value.startswith("@"):
        value = value[1:]
    # drop query params / fragments and trailing slash
    value = value.split("?", 1)[0].split("#", 1)[0].strip().strip("/")
    return value

# Фиксируем постоянный чат для проверки/прогрева user_id.
if db.get_config('outreach_target_chat') != GLOBAL_TARGET_CHAT_INVITE:
    db.set_config('outreach_target_chat', GLOBAL_TARGET_CHAT_INVITE)

def ensure_db_schema():
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tenants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                is_default BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id INTEGER NOT NULL,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'admin',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id)
            )
        ''')
        cursor.execute("PRAGMA table_info(users)")
        user_columns = [col[1] for col in cursor.fetchall()]
        if 'telegram_id' not in user_columns:
            cursor.execute('ALTER TABLE users ADD COLUMN telegram_id TEXT')
        if 'telegram_username' not in user_columns:
            cursor.execute('ALTER TABLE users ADD COLUMN telegram_username TEXT')
        if 'telegram_name' not in user_columns:
            cursor.execute('ALTER TABLE users ADD COLUMN telegram_name TEXT')
        if 'telegram_phone' not in user_columns:
            cursor.execute('ALTER TABLE users ADD COLUMN telegram_phone TEXT')
        if 'telegram_photo_url' not in user_columns:
            cursor.execute('ALTER TABLE users ADD COLUMN telegram_photo_url TEXT')
        if 'telegram_auth_at' not in user_columns:
            cursor.execute('ALTER TABLE users ADD COLUMN telegram_auth_at TIMESTAMP')
        if 'trial_started_at' not in user_columns:
            cursor.execute('ALTER TABLE users ADD COLUMN trial_started_at TIMESTAMP')
        if 'trial_ends_at' not in user_columns:
            cursor.execute('ALTER TABLE users ADD COLUMN trial_ends_at TIMESTAMP')
        if 'subscription_expires_at' not in user_columns:
            cursor.execute('ALTER TABLE users ADD COLUMN subscription_expires_at TIMESTAMP')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tenant_config (
                tenant_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                PRIMARY KEY (tenant_id, key),
                FOREIGN KEY(tenant_id) REFERENCES tenants(id)
            )
        ''')
        cursor.execute("PRAGMA table_info(outreach_campaigns)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'schedule' not in columns:
            cursor.execute('ALTER TABLE outreach_campaigns ADD COLUMN schedule TEXT')
        if 'tenant_id' not in columns:
            cursor.execute('ALTER TABLE outreach_campaigns ADD COLUMN tenant_id INTEGER DEFAULT 1')
        if 'created_by_user_id' not in columns:
            cursor.execute('ALTER TABLE outreach_campaigns ADD COLUMN created_by_user_id INTEGER')
        if 'base_id' not in columns:
            cursor.execute('ALTER TABLE outreach_campaigns ADD COLUMN base_id INTEGER')
        cursor.execute("PRAGMA table_info(outreach_contacts)")
        contact_columns = [col[1] for col in cursor.fetchall()]
        if 'username' not in contact_columns:
            cursor.execute('ALTER TABLE outreach_contacts ADD COLUMN username TEXT')
        if 'access_hash' not in contact_columns:
            cursor.execute('ALTER TABLE outreach_contacts ADD COLUMN access_hash TEXT')
        if 'tenant_id' not in contact_columns:
            cursor.execute('ALTER TABLE outreach_contacts ADD COLUMN tenant_id INTEGER DEFAULT 1')
        cursor.execute("PRAGMA table_info(outreach_messages)")
        msg_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in msg_columns:
            cursor.execute('ALTER TABLE outreach_messages ADD COLUMN tenant_id INTEGER DEFAULT 1')
        cursor.execute("PRAGMA table_info(conversations)")
        conv_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in conv_columns:
            cursor.execute('ALTER TABLE conversations ADD COLUMN tenant_id INTEGER DEFAULT 1')
        if 'is_unread' not in conv_columns:
            cursor.execute('ALTER TABLE conversations ADD COLUMN is_unread INTEGER DEFAULT 0')
        cursor.execute("PRAGMA table_info(account_stats)")
        account_columns = [col[1] for col in cursor.fetchall()]
        if 'outreach_daily_limit' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN outreach_daily_limit INTEGER DEFAULT 20')
        if 'checker_daily_limit' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN checker_daily_limit INTEGER DEFAULT 20')
        if 'tenant_id' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN tenant_id INTEGER DEFAULT 1')
        if 'added_by_user_id' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN added_by_user_id INTEGER')
        if 'first_name' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN first_name TEXT')
        if 'last_name' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN last_name TEXT')
        cursor.execute("PRAGMA table_info(proxies)")
        proxy_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in proxy_columns:
            cursor.execute('ALTER TABLE proxies ADD COLUMN tenant_id INTEGER DEFAULT 1')
        if 'added_by_user_id' not in proxy_columns:
            cursor.execute('ALTER TABLE proxies ADD COLUMN added_by_user_id INTEGER')
        cursor.execute("PRAGMA table_info(account_fingerprints)")
        fp_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in fp_columns:
            cursor.execute('ALTER TABLE account_fingerprints ADD COLUMN tenant_id INTEGER DEFAULT 1')
        cursor.execute("PRAGMA table_info(form_data)")
        form_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in form_columns:
            cursor.execute('ALTER TABLE form_data ADD COLUMN tenant_id INTEGER DEFAULT 1')
        cursor.execute("PRAGMA table_info(check_history)")
        history_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in history_columns:
            cursor.execute('ALTER TABLE check_history ADD COLUMN tenant_id INTEGER DEFAULT 1')
        cursor.execute("PRAGMA table_info(outreach_templates)")
        tmpl_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in tmpl_columns:
            cursor.execute('ALTER TABLE outreach_templates ADD COLUMN tenant_id INTEGER DEFAULT 1')
        cursor.execute("PRAGMA table_info(outreach_blacklist)")
        bl_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in bl_columns:
            cursor.execute('ALTER TABLE outreach_blacklist ADD COLUMN tenant_id INTEGER DEFAULT 1')
        cursor.execute("PRAGMA table_info(scheduler_campaign_locks)")
        lock_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in lock_columns:
            cursor.execute('ALTER TABLE scheduler_campaign_locks ADD COLUMN tenant_id INTEGER DEFAULT 1')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS outreach_bases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id INTEGER DEFAULT 1,
                created_by_user_id INTEGER,
                name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS outreach_base_contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id INTEGER DEFAULT 1,
                base_id INTEGER NOT NULL,
                phone TEXT,
                username TEXT,
                access_hash TEXT,
                name TEXT,
                company TEXT,
                position TEXT,
                source_file TEXT,
                user_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(base_id) REFERENCES outreach_bases(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dorks_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id INTEGER DEFAULT 1,
                user_id INTEGER,
                position TEXT NOT NULL,
                language TEXT NOT NULL,
                sources_json TEXT NOT NULL,
                base_name TEXT NOT NULL,
                results_limit INTEGER NOT NULL,
                queries_total INTEGER DEFAULT 0,
                requests_ok INTEGER DEFAULT 0,
                requests_failed INTEGER DEFAULT 0,
                found_total INTEGER DEFAULT 0,
                dedup_total INTEGER DEFAULT 0,
                imported INTEGER DEFAULT 0,
                skipped INTEGER DEFAULT 0,
                status TEXT DEFAULT 'ok',
                error_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dorks_run_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                tenant_id INTEGER DEFAULT 1,
                source TEXT NOT NULL,
                query_text TEXT NOT NULL,
                status TEXT DEFAULT 'ok',
                http_status INTEGER,
                result_items INTEGER DEFAULT 0,
                usernames_found INTEGER DEFAULT 0,
                error_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(run_id) REFERENCES dorks_runs(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS outreach_blacklist (
                tenant_id INTEGER DEFAULT 1,
                user_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduler_campaign_locks (
                tenant_id INTEGER DEFAULT 1,
                campaign_id INTEGER PRIMARY KEY,
                owner TEXT NOT NULL,
                locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS billing_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                telegram_user_id TEXT,
                invoice_payload TEXT,
                telegram_payment_charge_id TEXT,
                provider_payment_charge_id TEXT,
                amount_stars INTEGER DEFAULT 0,
                currency TEXT DEFAULT 'XTR',
                paid_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_update_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        ''')
        cursor.execute("SELECT id FROM tenants WHERE is_default = 1 LIMIT 1")
        row = cursor.fetchone()
        if row:
            default_tenant_id = int(row[0])
        else:
            cursor.execute("INSERT INTO tenants (name, is_default) VALUES ('default', 1)")
            default_tenant_id = int(cursor.lastrowid)

        for table in (
            'outreach_campaigns', 'outreach_contacts', 'conversations', 'account_stats',
            'proxies', 'account_fingerprints', 'form_data', 'check_history',
            'outreach_templates', 'outreach_blacklist', 'scheduler_campaign_locks',
            'outreach_messages'
            , 'outreach_bases', 'outreach_base_contacts', 'dorks_runs', 'dorks_run_queries'
        ):
            try:
                cursor.execute(f'UPDATE {table} SET tenant_id = ? WHERE tenant_id IS NULL', (default_tenant_id,))
            except Exception:
                pass

        cursor.execute('SELECT COUNT(*) FROM users')
        users_count = int(cursor.fetchone()[0] or 0)
        if users_count == 0:
            admin_username = str(os.getenv('DEFAULT_ADMIN_USERNAME', 'admin')).strip() or 'admin'
            admin_password = str(os.getenv('DEFAULT_ADMIN_PASSWORD', os.getenv('APP_PASSWORD', 'admin'))).strip() or 'admin'
            cursor.execute('''
                INSERT INTO users (
                    tenant_id, username, password_hash, role, is_active,
                    trial_started_at, trial_ends_at
                )
                VALUES (?, ?, ?, 'admin', 1, CURRENT_TIMESTAMP, datetime(CURRENT_TIMESTAMP, ?))
            ''', (default_tenant_id, admin_username, generate_password_hash(admin_password), f'+{BILLING_TRIAL_DAYS} day'))
            logger.warning("Created default admin user for multi-user mode: username='%s'", admin_username)

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_outreach_contacts_campaign_status ON outreach_contacts(campaign_id, status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_contact_direction_sent_at ON conversations(contact_id, direction, sent_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_campaign_direction_sent_at ON conversations(campaign_id, direction, sent_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_tenant_contact_msg ON conversations(tenant_id, contact_id, message_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_outreach_campaigns_tenant ON outreach_campaigns(tenant_id, status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_outreach_contacts_tenant ON outreach_contacts(tenant_id, campaign_id, status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_tenant ON conversations(tenant_id, campaign_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_account_stats_tenant ON account_stats(tenant_id, phone)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_proxies_tenant ON proxies(tenant_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_form_data_tenant ON form_data(tenant_id, saved_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_check_history_tenant ON check_history(tenant_id, check_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_templates_tenant ON outreach_templates(tenant_id, name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_outreach_bases_tenant ON outreach_bases(tenant_id, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_outreach_base_contacts_tenant ON outreach_base_contacts(tenant_id, base_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_outreach_messages_tenant_contact ON outreach_messages(tenant_id, contact_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_dorks_runs_tenant ON dorks_runs(tenant_id, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_dorks_run_queries_run ON dorks_run_queries(tenant_id, run_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_account_stats_added_by ON account_stats(tenant_id, added_by_user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_campaigns_created_by ON outreach_campaigns(tenant_id, created_by_user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_proxies_added_by ON proxies(tenant_id, added_by_user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_billing_payments_user ON billing_payments(tenant_id, user_id, paid_at)')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_billing_charge_unique ON billing_payments(telegram_payment_charge_id)')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_telegram_id_unique ON users(telegram_id) WHERE telegram_id IS NOT NULL')
        cursor.execute("UPDATE users SET role = 'user' WHERE role = 'viewer'")
        cursor.execute("UPDATE users SET role = 'admin' WHERE role = 'owner'")
        cursor.execute('''
            UPDATE users
            SET trial_started_at = COALESCE(trial_started_at, CURRENT_TIMESTAMP)
            WHERE trial_started_at IS NULL
        ''')
        cursor.execute(
            f'''
            UPDATE users
            SET trial_ends_at = datetime(trial_started_at, '+{BILLING_TRIAL_DAYS} day')
            WHERE trial_ends_at IS NULL
            '''
        )
        # Заполняем владельца прокси для старых записей.
        cursor.execute('''
            UPDATE proxies
            SET added_by_user_id = (
                SELECT a.added_by_user_id
                FROM account_stats a
                WHERE a.tenant_id = proxies.tenant_id
                  AND a.proxy_id = proxies.id
                  AND a.added_by_user_id IS NOT NULL
                GROUP BY a.added_by_user_id
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            WHERE added_by_user_id IS NULL
        ''')
        cursor.execute('''
            UPDATE proxies
            SET added_by_user_id = (
                SELECT u.id
                FROM users u
                WHERE u.tenant_id = proxies.tenant_id
                  AND lower(u.role) IN ('admin', 'owner')
                ORDER BY u.id
                LIMIT 1
            )
            WHERE added_by_user_id IS NULL
        ''')
        conn.commit()

ensure_db_schema()

try:
    with sqlite3.connect(db.db_path) as _conn:
        _cur = _conn.cursor()
        _cur.execute(
            '''
            INSERT INTO config (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ''',
            ('outreach_target_chat', GLOBAL_TARGET_CHAT_INVITE)
        )
        _cur.execute('SELECT id FROM tenants')
        for _row in _cur.fetchall():
            _tenant_id = int(_row[0])
            _cur.execute(
                '''
                INSERT INTO tenant_config (tenant_id, key, value) VALUES (?, ?, ?)
                ON CONFLICT(tenant_id, key) DO UPDATE SET value = excluded.value
                ''',
                (_tenant_id, 'outreach_target_chat', GLOBAL_TARGET_CHAT_INVITE)
            )
        _conn.commit()
except Exception as _cfg_err:
    logger.warning(f"Could not enforce global outreach_target_chat: {_cfg_err}")

# Импортируем новые менеджеры
from proxy_manager import ProxyManager
from fingerprint_manager import FingerprintManager
from outreach_scheduler import OutreachScheduler, render_spintax

proxy_manager = ProxyManager()
fingerprint_manager = FingerprintManager()
DEFAULT_API_ID = os.getenv('TELEGRAM_API_ID', '').strip()
DEFAULT_API_HASH = os.getenv('TELEGRAM_API_HASH', '').strip()
APP_PASSWORD = os.getenv('APP_PASSWORD', '').strip()
TELEGRAM_LOGIN_CLIENT_ID = os.getenv('TELEGRAM_LOGIN_CLIENT_ID', '').strip()
TELEGRAM_LOGIN_CLIENT_SECRET = os.getenv('TELEGRAM_LOGIN_CLIENT_SECRET', '').strip()
TELEGRAM_LOGIN_REDIRECT_URI = os.getenv('TELEGRAM_LOGIN_REDIRECT_URI', '').strip()
TELEGRAM_LOGIN_SCOPE = os.getenv('TELEGRAM_LOGIN_SCOPE', 'openid profile phone').strip() or 'openid profile phone'
TELEGRAM_LOGIN_BOT_USERNAME = os.getenv('TELEGRAM_LOGIN_BOT_USERNAME', '').strip().lstrip('@')
TELEGRAM_LOGIN_BOT_TOKEN = os.getenv('TELEGRAM_LOGIN_BOT_TOKEN', '').strip()
TELEGRAM_OIDC_DISCOVERY = 'https://oauth.telegram.org/.well-known/openid-configuration'
AUTH_EXEMPT_PATHS = {
    '/health',
    '/login',
    '/register',
    '/oferta',
    '/logout',
    '/api/auth/login',
    '/api/auth/me',
    '/api/auth/register',
    '/api/auth/telegram/start',
    '/api/auth/telegram/widget',
    '/auth/telegram/callback'
}

if DEFAULT_API_ID and DEFAULT_API_HASH:
    db.set_config('api_id', DEFAULT_API_ID)
    db.set_config('api_hash', DEFAULT_API_HASH)

def run_async(coro):
    """Безопасный запуск coroutine в синхронном Flask route.

    Если в текущем потоке уже запущен event loop (eventlet/gunicorn сценарии),
    выносим выполнение в отдельный поток с собственным loop.
    """
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if running_loop and running_loop.is_running():
        result_holder = {'result': None, 'error': None}
        done = NativeEvent()

        def _runner():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result_holder['result'] = loop.run_until_complete(coro)
            except Exception as e:
                result_holder['error'] = e
            finally:
                try:
                    loop.close()
                except Exception:
                    pass
                done.set()

        NativeThread(target=_runner, daemon=True).start()
        done.wait()
        if result_holder['error']:
            raise result_holder['error']
        return result_holder['result']

    return asyncio.run(coro)

def run_async_threadsafe(coro):
    """Всегда выполняет coroutine в отдельном потоке со своим event loop."""
    result_holder = {'result': None, 'error': None}
    done = NativeEvent()

    def _runner():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result_holder['result'] = loop.run_until_complete(coro)
        except Exception as e:
            result_holder['error'] = e
        finally:
            try:
                loop.close()
            except Exception:
                pass
            done.set()

    NativeThread(target=_runner, daemon=True).start()
    done.wait()
    if result_holder['error']:
        raise result_holder['error']
    return result_holder['result']

def get_api_credentials(payload=None):
    payload = payload or {}
    tenant_id = get_current_tenant_id()
    api_id = str(payload.get('api_id') or db.get_config('api_id', tenant_id=tenant_id) or DEFAULT_API_ID).strip()
    api_hash = str(payload.get('api_hash') or db.get_config('api_hash', tenant_id=tenant_id) or DEFAULT_API_HASH).strip()
    return api_id, api_hash

def _normalize_phone_value(value):
    raw = str(value or '').strip()
    if not raw:
        return None
    digits = ''.join(ch for ch in raw if ch.isdigit())
    if not digits:
        return None
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        digits = '7' + digits
    return f"+{digits}"

def _parse_checker_file(filepath: str):
    phones = []
    phone_metadata = {}
    parsed_rows = 0
    detected_columns = {
        'phone': None,
        'name': None,
        'company': None,
        'position': None
    }

    if filepath.lower().endswith('.txt'):
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                norm = _normalize_phone_value(line.strip())
                if norm:
                    phones.append(norm)
        # сохраняем порядок, убираем дубли
        phones = list(dict.fromkeys(phones))
        return phones, phone_metadata, parsed_rows, detected_columns

    if filepath.lower().endswith('.csv'):
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath)
    parsed_rows = int(len(df.index))
    df.columns = [str(c).strip() for c in df.columns]
    low_cols = {str(c).lower().strip(): str(c).strip() for c in df.columns}

    phone_col = next((orig for low, orig in low_cols.items() if 'phone' in low or 'tel' in low or 'номер' in low), None)
    # Важно: не путать `name` и `username`.
    preferred_name_keys = ['name', 'full_name', 'contact_name', 'имя', 'фио']
    name_col = next((low_cols.get(k) for k in preferred_name_keys if k in low_cols), None)
    if not name_col:
        name_col = next(
            (
                orig for low, orig in low_cols.items()
                if ('name' in low or 'имя' in low)
                and low not in ('username', 'telegram_username', 'tg_username', 'user_name')
                and 'username' not in low
            ),
            None
        )
    company_col = next((orig for low, orig in low_cols.items() if 'company' in low or 'компан' in low), None)
    position_col = next((orig for low, orig in low_cols.items() if 'position' in low or 'должн' in low or 'role' in low), None)

    detected_columns['phone'] = phone_col
    detected_columns['name'] = name_col
    detected_columns['company'] = company_col
    detected_columns['position'] = position_col

    if not phone_col:
        raise ValueError("Не найдена колонка с телефоном (phone/tel/номер)")

    for _, row in df.iterrows():
        phone = _normalize_phone_value(row.get(phone_col))
        if not phone:
            continue
        if phone not in phone_metadata:
            phones.append(phone)
            phone_metadata[phone] = {}
        if name_col:
            val = row.get(name_col)
            if pd.notna(val):
                phone_metadata[phone]['name'] = str(val).strip()
        if company_col:
            val = row.get(company_col)
            if pd.notna(val):
                phone_metadata[phone]['company'] = str(val).strip()
        if position_col:
            val = row.get(position_col)
            if pd.notna(val):
                phone_metadata[phone]['position'] = str(val).strip()

    return phones, phone_metadata, parsed_rows, detected_columns

async def ensure_outreach_accounts_in_global_target_chat(tenant_id: int, account_phones=None):
    api_id = str(db.get_config('api_id', tenant_id=tenant_id) or DEFAULT_API_ID).strip()
    api_hash = str(db.get_config('api_hash', tenant_id=tenant_id) or DEFAULT_API_HASH).strip()
    if not api_id or not api_hash:
        return {'success': 0, 'already': 0, 'failed': 0, 'total': 0}

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        if account_phones:
            normalized = [str(p).strip() for p in account_phones if str(p).strip()]
            if not normalized:
                return {'success': 0, 'already': 0, 'failed': 0, 'total': 0}
            placeholders = ",".join("?" for _ in normalized)
            cursor.execute(
                f'''
                SELECT phone, session_name
                FROM account_stats
                WHERE tenant_id = ?
                  AND account_type = 'outreach'
                  AND COALESCE(is_banned, 0) = 0
                  AND COALESCE(is_frozen, 0) = 0
                  AND phone IN ({placeholders})
                ''',
                [tenant_id, *normalized]
            )
        else:
            cursor.execute('''
                SELECT phone, session_name
                FROM account_stats
                WHERE tenant_id = ?
                  AND account_type = 'outreach'
                  AND COALESCE(is_banned, 0) = 0
                  AND COALESCE(is_frozen, 0) = 0
            ''', (tenant_id,))
        rows = cursor.fetchall()

    results = {'success': 0, 'already': 0, 'failed': 0, 'total': len(rows)}
    if not rows:
        return results

    chat_ref = normalize_chat_ref(GLOBAL_TARGET_CHAT_INVITE)
    for phone, session_name in rows:
        session_path = os.path.join('sessions', str(session_name or ''))
        if not session_name or not os.path.exists(session_path + '.session'):
            results['failed'] += 1
            continue
        client = TelegramClient(session_path, api_id=int(api_id), api_hash=api_hash)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                results['failed'] += 1
                continue
            try:
                if '+' in chat_ref or 'joinchat' in chat_ref:
                    hash_part = chat_ref.split('/')[-1].replace('+', '')
                    if 'joinchat/' in hash_part:
                        hash_part = hash_part.split('joinchat/')[-1]
                    await client(ImportChatInviteRequest(hash_part))
                    results['success'] += 1
                else:
                    entity = await client.get_entity(chat_ref)
                    await client(JoinChannelRequest(entity))
                    results['success'] += 1
            except errors.UserAlreadyParticipantError:
                results['already'] += 1
            except Exception as e:
                if 'USER_ALREADY_PARTICIPANT' in str(e):
                    results['already'] += 1
                else:
                    logger.warning(f"[{phone}] failed to join global target chat: {e}")
                    results['failed'] += 1
        except Exception as e:
            logger.warning(f"[{phone}] connect failed during global chat ensure: {e}")
            results['failed'] += 1
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
    return results

def filter_valid_outreach_accounts(selected_accounts):
    selected = [str(a).strip() for a in (selected_accounts or []) if str(a).strip()]
    if not selected:
        return []
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        placeholders = ",".join("?" for _ in selected)
        if is_admin_user(user):
            cursor.execute(
                f'''
                SELECT phone
                FROM account_stats
                WHERE phone IN ({placeholders})
                  AND tenant_id = ?
                  AND account_type = 'outreach'
                  AND COALESCE(is_banned, 0) = 0
                  AND COALESCE(is_frozen, 0) = 0
                ''',
                [*selected, tenant_id]
            )
        else:
            cursor.execute(
                f'''
                SELECT phone
                FROM account_stats
                WHERE phone IN ({placeholders})
                  AND tenant_id = ?
                  AND added_by_user_id = ?
                  AND account_type = 'outreach'
                  AND COALESCE(is_banned, 0) = 0
                  AND COALESCE(is_frozen, 0) = 0
                ''',
                [*selected, tenant_id, user_id]
            )
        allowed = {row[0] for row in cursor.fetchall()}
    # сохраняем порядок выбора в UI
    return [phone for phone in selected if phone in allowed]

def _count_followup_steps(strategy: dict) -> int:
    steps = (strategy or {}).get('steps') or []
    count = 0
    for step in steps:
        if not isinstance(step, dict):
            continue
        content = str(step.get('content') or '').strip()
        sid = None
        try:
            sid = int(step.get('id'))
        except (TypeError, ValueError):
            sid = None
        if content and sid and sid > 1:
            count += 1
    return count

def _normalize_campaign_schedule(schedule: dict, followup_steps_count: int) -> dict:
    raw = schedule if isinstance(schedule, dict) else {}
    start_time = str(raw.get('start_time') or '10:00').strip()
    end_time = str(raw.get('end_time') or '20:00').strip()
    days_raw = raw.get('days') or [1, 2, 3, 4, 5]
    days = []
    for d in days_raw:
        try:
            dv = int(d)
        except (TypeError, ValueError):
            continue
        if 0 <= dv <= 6 and dv not in days:
            days.append(dv)
    if not days:
        days = [1, 2, 3, 4, 5]
    try:
        ignore_after_hours = max(1, int(raw.get('ignore_after_hours') or 24))
    except (TypeError, ValueError):
        ignore_after_hours = 24
    if followup_steps_count > 0:
        ignore_after_hours = max(25, ignore_after_hours)
    return {
        'start_time': start_time,
        'end_time': end_time,
        'ignore_after_hours': ignore_after_hours,
        'days': days
    }

def _calculate_campaign_capacity(tenant_id: int, accounts: list, strategy: dict) -> dict:
    selected_accounts = [str(a).strip() for a in (accounts or []) if str(a).strip()]
    followup_steps = _count_followup_steps(strategy or {})
    waves_required = max(1, followup_steps + 1)
    total_accounts = len(selected_accounts)

    limits = []
    if selected_accounts:
        placeholders = ",".join("?" for _ in selected_accounts)
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f'''
                SELECT phone, COALESCE(outreach_daily_limit, 20)
                FROM account_stats
                WHERE tenant_id = ?
                  AND phone IN ({placeholders})
                ''',
                [tenant_id, *selected_accounts]
            )
            rows = cursor.fetchall()
        limit_by_phone = {str(r[0]): max(1, int(r[1] or 20)) for r in rows}
        limits = [limit_by_phone.get(phone, 20) for phone in selected_accounts]

    primary_accounts_per_day = total_accounts // waves_required if waves_required > 0 else 0
    per_account_limit_floor = min(limits) if limits else 20
    safe_new_per_day = primary_accounts_per_day * per_account_limit_floor
    shortage_for_rotation = max(0, waves_required - total_accounts)
    accounts_to_add_for_next_step = max(0, waves_required * (primary_accounts_per_day + 1) - total_accounts)

    return {
        'followup_steps': followup_steps,
        'waves_required': waves_required,
        'accounts_total': total_accounts,
        'primary_accounts_per_day': primary_accounts_per_day,
        'per_account_limit_floor': per_account_limit_floor,
        'safe_new_per_day': safe_new_per_day,
        'shortage_for_rotation': shortage_for_rotation,
        'accounts_to_add_for_next_step': accounts_to_add_for_next_step
    }

def _is_auth_exempt_path(path: str) -> bool:
    if path.startswith('/static/'):
        return True
    if path.startswith('/socket.io/'):
        return True
    if path in AUTH_EXEMPT_PATHS:
        return True
    return False

def get_default_tenant_id() -> int:
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM tenants WHERE is_default = 1 LIMIT 1')
        row = cursor.fetchone()
        if row:
            return int(row[0])
        cursor.execute("INSERT INTO tenants (name, is_default) VALUES ('default', 1)")
        conn.commit()
        return int(cursor.lastrowid)

def get_current_tenant_id() -> int:
    tid = getattr(g, 'tenant_id', None)
    if tid:
        return int(tid)
    sess_tid = session.get('tenant_id')
    if sess_tid:
        return int(sess_tid)
    return get_default_tenant_id()

def get_current_user():
    uid = session.get('user_id')
    if not uid:
        return None
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT
                id,
                tenant_id,
                username,
                role,
                is_active,
                telegram_id,
                trial_started_at,
                trial_ends_at,
                subscription_expires_at
            FROM users
            WHERE id = ?
        ''', (uid,))
        row = cursor.fetchone()
    if not row or not row[4]:
        return None
    role = (row[3] or '').strip().lower()
    if role == 'viewer':
        role = 'user'
    if role == 'owner':
        role = 'admin'
    return {
        'id': int(row[0]),
        'tenant_id': int(row[1]),
        'username': row[2],
        'role': role,
        'is_active': bool(row[4]),
        'telegram_id': str(row[5] or ''),
        'trial_started_at': row[6],
        'trial_ends_at': row[7],
        'subscription_expires_at': row[8]
    }

def is_admin_user(user=None) -> bool:
    u = user or get_current_user() or {}
    return (u.get('role') == 'admin')

def is_limited_user(user=None) -> bool:
    u = user or get_current_user() or {}
    return not is_admin_user(u)

def owns_account(phone: str, tenant_id: int, user_id: int) -> bool:
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1 FROM account_stats
            WHERE tenant_id = ? AND phone = ? AND added_by_user_id = ?
            LIMIT 1
        ''', (tenant_id, phone, user_id))
        return cursor.fetchone() is not None

def get_campaign_for_current_user(campaign_id: int):
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    campaign = outreach.get_campaign(campaign_id, tenant_id=tenant_id)
    if not campaign:
        return None
    if is_admin_user(user):
        return campaign
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1
            FROM outreach_campaigns
            WHERE id = ? AND tenant_id = ? AND created_by_user_id = ?
            LIMIT 1
        ''', (campaign_id, tenant_id, int(user.get('id') or 0)))
        if not cursor.fetchone():
            return None
    return campaign

def get_base_for_current_user(base_id: int):
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    base = outreach.get_base(base_id, tenant_id=tenant_id)
    if not base:
        return None
    if is_admin_user(user):
        return base
    if int(base.get('created_by_user_id') or 0) != int(user.get('id') or 0):
        return None
    return base

def require_role(*allowed_roles):
    user = get_current_user()
    if not user:
        return None, (jsonify({'error': 'Unauthorized'}), 401)
    if user.get('role') not in allowed_roles:
        return user, (jsonify({'error': 'Forbidden'}), 403)
    return user, None

_telegram_discovery_cache = {'at': 0.0, 'data': None}
_telegram_jwks_client = None
_telegram_jwks_uri = ''

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode('utf-8').rstrip('=')

def _telegram_login_credentials(tenant_id: int) -> tuple[str, str]:
    client_id = str(
        db.get_config('telegram_login_client_id', tenant_id=tenant_id)
        or db.get_config('telegram_client_id', tenant_id=tenant_id)
        or TELEGRAM_LOGIN_CLIENT_ID
    ).strip()
    client_secret = str(
        db.get_config('telegram_login_client_secret', tenant_id=tenant_id)
        or db.get_config('telegram_client_secret', tenant_id=tenant_id)
        or TELEGRAM_LOGIN_CLIENT_SECRET
    ).strip()
    return client_id, client_secret

def _telegram_widget_credentials(tenant_id: int) -> tuple[str, str]:
    bot_username = str(
        db.get_config('telegram_login_bot_username', tenant_id=tenant_id)
        or db.get_config('telegram_bot_username', tenant_id=tenant_id)
        or TELEGRAM_LOGIN_BOT_USERNAME
    ).strip().lstrip('@')
    bot_token = str(
        db.get_config('telegram_login_bot_token', tenant_id=tenant_id)
        or db.get_config('telegram_bot_token', tenant_id=tenant_id)
        or TELEGRAM_LOGIN_BOT_TOKEN
    ).strip()
    return bot_username, bot_token

def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _parse_db_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        raw = raw.replace('Z', '+00:00')
        dt = None
        for candidate in (raw, raw.replace(' ', 'T')):
            try:
                dt = datetime.fromisoformat(candidate)
                break
            except Exception:
                dt = None
        if dt is None:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

def _format_db_datetime(value: datetime) -> str:
    return value.strftime('%Y-%m-%d %H:%M:%S')

def _build_subscription_payload(tenant_id: int, user_id: int) -> str:
    nonce = secrets.token_hex(6)
    return f"{SUBSCRIPTION_PAYLOAD_PREFIX}:{int(tenant_id)}:{int(user_id)}:{int(time.time())}:{nonce}"

def _parse_subscription_payload(payload: str):
    raw = str(payload or '').strip()
    parts = raw.split(':')
    if len(parts) < 4:
        return None
    if parts[0] != SUBSCRIPTION_PAYLOAD_PREFIX:
        return None
    try:
        tenant_id = int(parts[1])
        user_id = int(parts[2])
        issued_at = int(parts[3])
    except Exception:
        return None
    return {
        'tenant_id': tenant_id,
        'user_id': user_id,
        'issued_at': issued_at,
        'raw': raw
    }

def _billing_state(trial_ends_at, subscription_expires_at, now=None) -> dict:
    ts_now = now or _utcnow_naive()
    trial_end_dt = _parse_db_datetime(trial_ends_at)
    sub_end_dt = _parse_db_datetime(subscription_expires_at)

    if sub_end_dt and sub_end_dt > ts_now:
        return {
            'active': True,
            'mode': 'subscription',
            'active_until': sub_end_dt,
            'message': f"Подписка активна до {sub_end_dt.strftime('%d.%m.%Y %H:%M')}"
        }
    if trial_end_dt and trial_end_dt > ts_now:
        return {
            'active': True,
            'mode': 'trial',
            'active_until': trial_end_dt,
            'message': f"Триал активен до {trial_end_dt.strftime('%d.%m.%Y %H:%M')}"
        }
    return {
        'active': False,
        'mode': 'expired',
        'active_until': sub_end_dt or trial_end_dt,
        'message': 'Триал закончился. Для входа оплатите подписку.'
    }

def _ensure_user_trial_window(cursor, user_id: int):
    cursor.execute(
        '''
        UPDATE users
        SET trial_started_at = COALESCE(trial_started_at, CURRENT_TIMESTAMP)
        WHERE id = ? AND trial_started_at IS NULL
        ''',
        (int(user_id),)
    )
    cursor.execute(
        f'''
        UPDATE users
        SET trial_ends_at = datetime(trial_started_at, '+{BILLING_TRIAL_DAYS} day')
        WHERE id = ? AND trial_ends_at IS NULL
        ''',
        (int(user_id),)
    )

def _telegram_bot_api_request(bot_token: str, method: str, payload: dict = None, timeout: int = 35):
    token = str(bot_token or '').strip()
    if not token:
        raise RuntimeError('Telegram bot token is not configured')
    url = f"https://api.telegram.org/bot{token}/{method}"
    response = requests.post(url, json=payload or {}, timeout=timeout)
    response.raise_for_status()
    data = response.json() or {}
    if not data.get('ok'):
        raise RuntimeError(str(data.get('description') or f'{method} failed'))
    return data.get('result')

def _create_subscription_invoice_link(user_id: int, tenant_id: int) -> str:
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id, tenant_id, telegram_id
            FROM users
            WHERE id = ? AND tenant_id = ?
            LIMIT 1
            ''',
            (int(user_id), int(tenant_id))
        )
        row = cursor.fetchone()
    if not row or not str(row[2] or '').strip():
        return ''

    _, bot_token = _telegram_widget_credentials(int(tenant_id))
    if not bot_token:
        return ''

    payload = _build_subscription_payload(int(tenant_id), int(user_id))
    invoice = {
        'title': 'воронка: подписка на 30 дней',
        'description': 'Доступ к сервису рассылок в Telegram на 30 дней.',
        'payload': payload,
        'currency': 'XTR',
        'prices': [{'label': 'Подписка 30 дней', 'amount': int(BILLING_MONTHLY_STARS)}]
    }
    try:
        return str(_telegram_bot_api_request(bot_token, 'createInvoiceLink', invoice, timeout=20) or '')
    except Exception as e:
        logger.error(f"Failed to create invoice link for user {user_id}: {e}")
        return ''

def _apply_successful_subscription_payment(parsed_payload: dict, message: dict, successful_payment: dict):
    tenant_id = int(parsed_payload['tenant_id'])
    user_id = int(parsed_payload['user_id'])
    telegram_user_id = str(((message or {}).get('from') or {}).get('id') or '').strip()
    charge_id = str(successful_payment.get('telegram_payment_charge_id') or '').strip()
    provider_charge_id = str(successful_payment.get('provider_payment_charge_id') or '').strip()
    amount_stars = int(successful_payment.get('total_amount') or 0)
    currency = str(successful_payment.get('currency') or 'XTR').strip() or 'XTR'

    now = _utcnow_naive()
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()

        if charge_id:
            cursor.execute(
                '''
                SELECT 1
                FROM billing_payments
                WHERE telegram_payment_charge_id = ?
                LIMIT 1
                ''',
                (charge_id,)
            )
            if cursor.fetchone():
                return False

        cursor.execute(
            '''
            SELECT telegram_id, subscription_expires_at
            FROM users
            WHERE id = ? AND tenant_id = ?
            LIMIT 1
            ''',
            (user_id, tenant_id)
        )
        row = cursor.fetchone()
        if not row:
            logger.warning(f"Billing payment ignored: user {user_id} not found")
            return False
        db_tg_id = str(row[0] or '').strip()
        if db_tg_id and telegram_user_id and db_tg_id != telegram_user_id:
            logger.warning(
                f"Billing payment ignored: telegram mismatch user={user_id} db={db_tg_id} msg={telegram_user_id}"
            )
            return False

        current_exp_dt = _parse_db_datetime(row[1])
        anchor = current_exp_dt if current_exp_dt and current_exp_dt > now else now
        new_expiry = anchor + timedelta(days=BILLING_PLAN_DAYS)

        cursor.execute(
            '''
            UPDATE users
            SET subscription_expires_at = ?
            WHERE id = ? AND tenant_id = ?
            ''',
            (_format_db_datetime(new_expiry), user_id, tenant_id)
        )
        cursor.execute(
            '''
            INSERT INTO billing_payments (
                tenant_id,
                user_id,
                telegram_user_id,
                invoice_payload,
                telegram_payment_charge_id,
                provider_payment_charge_id,
                amount_stars,
                currency,
                paid_at,
                raw_update_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            ''',
            (
                tenant_id,
                user_id,
                telegram_user_id,
                str(successful_payment.get('invoice_payload') or ''),
                charge_id or None,
                provider_charge_id or None,
                amount_stars,
                currency,
                json.dumps({'message': message, 'successful_payment': successful_payment}, ensure_ascii=False)
            )
        )
        conn.commit()

    logger.info(
        "Billing payment applied: tenant=%s user=%s stars=%s expires=%s",
        tenant_id,
        user_id,
        amount_stars,
        _format_db_datetime(new_expiry)
    )
    return True

def _handle_billing_update(bot_token: str, update: dict):
    if not isinstance(update, dict):
        return
    pre_checkout_query = update.get('pre_checkout_query') if isinstance(update.get('pre_checkout_query'), dict) else None
    if pre_checkout_query:
        query_id = str(pre_checkout_query.get('id') or '').strip()
        payload = _parse_subscription_payload(pre_checkout_query.get('invoice_payload'))
        from_id = str((pre_checkout_query.get('from') or {}).get('id') or '').strip()
        ok = False
        error_message = 'Платеж недоступен для этого аккаунта'
        if payload:
            with sqlite3.connect(db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    '''
                    SELECT telegram_id
                    FROM users
                    WHERE id = ? AND tenant_id = ?
                    LIMIT 1
                    ''',
                    (int(payload['user_id']), int(payload['tenant_id']))
                )
                row = cursor.fetchone()
            if row:
                db_tg_id = str(row[0] or '').strip()
                if not db_tg_id or db_tg_id == from_id:
                    ok = True
                    error_message = ''
        try:
            req = {'pre_checkout_query_id': query_id, 'ok': bool(ok)}
            if not ok and error_message:
                req['error_message'] = error_message[:200]
            _telegram_bot_api_request(bot_token, 'answerPreCheckoutQuery', req, timeout=15)
        except Exception as e:
            logger.error(f"Failed answerPreCheckoutQuery: {e}")

    message = update.get('message') if isinstance(update.get('message'), dict) else None
    if not message:
        return
    successful_payment = message.get('successful_payment') if isinstance(message.get('successful_payment'), dict) else None
    if not successful_payment:
        return
    payload = _parse_subscription_payload(successful_payment.get('invoice_payload'))
    if not payload:
        return
    try:
        _apply_successful_subscription_payment(payload, message, successful_payment)
    except Exception as e:
        logger.error(f"Failed to apply successful payment: {e}")

def _telegram_widget_validate(auth_payload: dict, bot_token: str) -> dict:
    if not bot_token:
        raise RuntimeError('Telegram bot token is not configured')
    payload = dict(auth_payload or {})
    check_hash = str(payload.get('hash') or '').strip()
    if not check_hash:
        raise RuntimeError('Telegram hash is required')

    # Optional freshness check (24h)
    auth_date_raw = str(payload.get('auth_date') or '').strip()
    if auth_date_raw:
        try:
            auth_ts = int(auth_date_raw)
            if abs(int(time.time()) - auth_ts) > 86400:
                raise RuntimeError('Telegram auth payload expired')
        except ValueError:
            raise RuntimeError('Invalid auth_date')

    data_check_items = []
    for key in sorted(payload.keys()):
        if key == 'hash':
            continue
        value = payload.get(key)
        if value is None:
            continue
        value_str = str(value)
        if value_str == '':
            continue
        data_check_items.append(f"{key}={value_str}")

    data_check_string = '\n'.join(data_check_items)
    secret_key = hashlib.sha256(bot_token.encode('utf-8')).digest()
    expected_hash = hmac.new(secret_key, data_check_string.encode('utf-8'), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_hash, check_hash):
        raise RuntimeError('Telegram auth hash mismatch')
    return payload

def _upsert_user_from_telegram(
    mode: str,
    tenant_id: int,
    tg_id: str,
    tg_username: str,
    tg_name: str,
    tg_phone: str,
    tg_picture: str
) -> dict:
    mode = str(mode or 'login').strip().lower()
    if mode not in ('login', 'register'):
        mode = 'login'

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()

        cursor.execute(
            '''
            SELECT
                id,
                tenant_id,
                username,
                role,
                is_active,
                telegram_id,
                trial_started_at,
                trial_ends_at,
                subscription_expires_at
            FROM users
            WHERE telegram_id = ?
            LIMIT 1
            ''',
            (tg_id,)
        )
        existing = cursor.fetchone()

        if not existing and tg_username:
            # Migration bridge: link an existing profile with same username (case-insensitive)
            # only if Telegram is not linked yet.
            cursor.execute(
                '''
                SELECT
                    id,
                    tenant_id,
                    username,
                    role,
                    is_active,
                    telegram_id,
                    trial_started_at,
                    trial_ends_at,
                    subscription_expires_at
                FROM users
                WHERE telegram_id IS NULL
                  AND lower(username) = lower(?)
                LIMIT 1
                ''',
                (tg_username,)
            )
            existing = cursor.fetchone()

        if existing:
            cursor.execute(
                '''
                UPDATE users
                SET telegram_id = COALESCE(telegram_id, ?),
                    telegram_username = ?,
                    telegram_name = ?,
                    telegram_phone = ?,
                    telegram_photo_url = ?,
                    telegram_auth_at = CURRENT_TIMESTAMP
                WHERE id = ?
                ''',
                (tg_id, tg_username, tg_name, tg_phone, tg_picture, int(existing[0]))
            )
            _ensure_user_trial_window(cursor, int(existing[0]))
            cursor.execute(
                '''
                SELECT
                    id,
                    tenant_id,
                    username,
                    role,
                    is_active,
                    telegram_id,
                    trial_started_at,
                    trial_ends_at,
                    subscription_expires_at
                FROM users
                WHERE id = ?
                LIMIT 1
                ''',
                (int(existing[0]),)
            )
            existing = cursor.fetchone()
            conn.commit()

            if not bool(existing[4]):
                return {
                    'status': 'pending',
                    'message': 'Заявка найдена, ожидайте активацию админом',
                    'user_id': int(existing[0])
                }

            billing = _billing_state(existing[7], existing[8])
            if not billing.get('active'):
                payment_url = _create_subscription_invoice_link(int(existing[0]), int(existing[1]))
                return {
                    'status': 'payment_required',
                    'message': billing.get('message') or 'Триал закончился. Оплатите подписку.',
                    'payment_url': payment_url,
                    'stars_price': BILLING_MONTHLY_STARS,
                    'trial_days': BILLING_TRIAL_DAYS
                }

            session['user_id'] = int(existing[0])
            session['tenant_id'] = int(existing[1])
            return {
                'status': 'ok',
                'next': '/',
                'user': {
                    'id': int(existing[0]),
                    'tenant_id': int(existing[1]),
                    'username': str(existing[2] or ''),
                    'role': ('admin' if str(existing[3] or '').lower() in ('admin', 'owner') else 'user')
                }
            }

        if mode == 'login':
            return {
                'status': 'not_found',
                'message': 'Пользователь не найден. Сначала зарегистрируйтесь через Telegram'
            }

        base_username = _telegram_username_base(tg_username, tg_id)
        username = _telegram_unique_username(cursor, base_username)
        random_password = secrets.token_urlsafe(32)
        cursor.execute(
            '''
            INSERT INTO users (
                tenant_id, username, password_hash, role, is_active,
                telegram_id, telegram_username, telegram_name, telegram_phone, telegram_photo_url, telegram_auth_at,
                trial_started_at, trial_ends_at
            )
            VALUES (?, ?, ?, 'user', 0, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, datetime(CURRENT_TIMESTAMP, ?))
            ''',
            (
                tenant_id,
                username,
                generate_password_hash(random_password),
                tg_id,
                tg_username,
                tg_name,
                tg_phone,
                tg_picture,
                f'+{BILLING_TRIAL_DAYS} day'
            )
        )
        conn.commit()
        return {
            'status': 'pending',
            'message': 'Регистрация через Telegram создана. Ожидайте активацию админом',
            'username': username
        }

def _telegram_discovery() -> dict:
    now = time.time()
    cached = _telegram_discovery_cache.get('data')
    if cached and (now - float(_telegram_discovery_cache.get('at') or 0)) < 3600:
        return cached
    response = requests.get(TELEGRAM_OIDC_DISCOVERY, timeout=20)
    response.raise_for_status()
    data = response.json() or {}
    if not data.get('authorization_endpoint') or not data.get('token_endpoint') or not data.get('jwks_uri'):
        raise RuntimeError('Telegram OIDC discovery is incomplete')
    _telegram_discovery_cache['data'] = data
    _telegram_discovery_cache['at'] = now
    return data

def _telegram_redirect_uri() -> str:
    if TELEGRAM_LOGIN_REDIRECT_URI:
        return TELEGRAM_LOGIN_REDIRECT_URI
    return url_for('telegram_oauth_callback', _external=True)

def _telegram_id_token_claims(id_token: str, client_id: str, nonce=None) -> dict:
    if jwt is None or PyJWKClient is None:
        raise RuntimeError('PyJWT is required for Telegram OIDC login')
    discovery = _telegram_discovery()
    issuer = str(discovery.get('issuer') or 'https://oauth.telegram.org').strip()
    jwks_uri = str(discovery.get('jwks_uri') or '').strip()
    if not jwks_uri:
        raise RuntimeError('Telegram JWKS URI not found')

    global _telegram_jwks_client, _telegram_jwks_uri
    if (_telegram_jwks_client is None) or (_telegram_jwks_uri != jwks_uri):
        _telegram_jwks_client = PyJWKClient(jwks_uri)
        _telegram_jwks_uri = jwks_uri

    signing_key = _telegram_jwks_client.get_signing_key_from_jwt(id_token)
    claims = jwt.decode(
        id_token,
        signing_key.key,
        algorithms=['RS256', 'ES256', 'EdDSA'],
        audience=str(client_id),
        issuer=issuer,
        options={'require': ['exp', 'iat']}
    )
    if nonce:
        token_nonce = str(claims.get('nonce') or '')
        if token_nonce != str(nonce):
            raise RuntimeError('Invalid Telegram nonce')
    return claims

def _telegram_username_base(preferred_username: str, tg_id: str) -> str:
    candidate = re.sub(r'[^a-zA-Z0-9_\\.-]', '_', str(preferred_username or '').strip())
    candidate = candidate.strip('._-')
    if not candidate:
        candidate = f'tg_{tg_id}'
    if len(candidate) > 64:
        candidate = candidate[:64]
    if len(candidate) < 3:
        candidate = f'tg_{tg_id}'
    return candidate

def _telegram_unique_username(cursor, base_username: str) -> str:
    username = base_username
    suffix = 1
    while True:
        cursor.execute('SELECT 1 FROM users WHERE username = ? LIMIT 1', (username,))
        if not cursor.fetchone():
            return username
        suffix += 1
        username = f"{base_username}_{suffix}"
        if len(username) > 64:
            username = username[:64]

def _telegram_auth_redirect(endpoint: str, message: str = '', error: str = ''):
    query = {}
    if message:
        query['message'] = message
    if error:
        query['error'] = error
    target = url_for(endpoint)
    if query:
        target = f"{target}?{urlencode(query)}"
    return redirect(target)

@app.context_processor
def inject_current_user():
    return {'current_user': get_current_user()}

@app.before_request
def enforce_session_auth():
    if _is_auth_exempt_path(request.path):
        return None

    user = get_current_user()
    if user:
        billing = _billing_state(user.get('trial_ends_at'), user.get('subscription_expires_at'))
        if not billing.get('active'):
            session.clear()
            if request.path.startswith('/api/'):
                return jsonify({
                    'error': 'Subscription required',
                    'status': 'payment_required',
                    'message': billing.get('message') or 'Триал закончился. Оплатите подписку.',
                    'stars_price': BILLING_MONTHLY_STARS
                }), 402
            return _telegram_auth_redirect(
                'login_page',
                error=billing.get('message') or 'Триал закончился. Оплатите подписку.'
            )

        g.current_user = user
        g.tenant_id = int(user['tenant_id'])
        if is_limited_user(user):
            forbidden_prefixes = (
                '/api/auth/users',
                '/api/admin/',
                '/api/outreach/scheduler/'
            )
            if any(request.path.startswith(p) for p in forbidden_prefixes):
                return jsonify({'error': 'Forbidden'}), 403
        return None

    if request.path.startswith('/api/'):
        return jsonify({'error': 'Unauthorized'}), 401
    return redirect(url_for('login_page'))

checker_process = None
checker_status = {
    'running': False,
    'current_phone': '',
    'processed': 0,
    'total': 0,
    'input_total': 0,
    'unique_total': 0,
    'skipped_duplicates': 0,
    'skipped_existing_in_base': 0,
    'skipped_history_registered': 0,
    'skipped_history_not_registered': 0,
    'skipped_history_total': 0,
    'registered': 0,
    'not_registered': 0,
    'errors': 0,
    'test_checks': 0,
    'accounts': [],
    'start_time': None,
    'estimated_time': None,
    'recent_results': []
}

def push_recent_result(payload: dict):
    if payload is None:
        return
    recent = checker_status.get('recent_results') or []
    recent.append(payload)
    if len(recent) > 200:
        recent = recent[-200:]
    checker_status['recent_results'] = recent
checker_emit_queue = deque()
checker_emit_lock = NativeLock()
checker_emitter_started = False
checker_emitter_start_lock = NativeLock()
ENABLE_CHECKER_SOCKET_EVENTS = os.getenv('ENABLE_CHECKER_SOCKET_EVENTS', '0') == '1'

qr_auth_sessions = {}
qr_auth_lock = NativeLock()

scheduler_thread = None
scheduler = None
last_scheduler_autostart_check_at = 0.0
SCHEDULER_AUTOSTART_CHECK_INTERVAL_SECONDS = 10

billing_bot_thread = None
billing_bot_stop_event = NativeEvent()
last_billing_worker_check_at = 0.0
BILLING_WORKER_CHECK_INTERVAL_SECONDS = 15

def is_scheduler_running():
    return scheduler_thread is not None and scheduler_thread.is_alive()

def ensure_scheduler_running():
    global scheduler_thread, scheduler
    if is_scheduler_running():
        return False
    scheduler = OutreachScheduler()
    def run_scheduler():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(scheduler.start())
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            loop.close()
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    return True

def queue_checker_emit(event_name: str, payload=None):
    if not ENABLE_CHECKER_SOCKET_EVENTS:
        return
    with checker_emit_lock:
        checker_emit_queue.append((event_name, payload if payload is not None else {}))

def checker_emit_worker():
    while True:
        batch = []
        with checker_emit_lock:
            while checker_emit_queue and len(batch) < 200:
                batch.append(checker_emit_queue.popleft())
        for event_name, payload in batch:
            try:
                socketio.emit(event_name, payload)
            except Exception as e:
                logger.warning(f"Checker emitter failed ({event_name}): {e}")
        socketio.sleep(0.01 if batch else 0.05)

def ensure_checker_emitter_started():
    if not ENABLE_CHECKER_SOCKET_EVENTS:
        return
    global checker_emitter_started
    if checker_emitter_started:
        return
    with checker_emitter_start_lock:
        if checker_emitter_started:
            return
        socketio.start_background_task(checker_emit_worker)
        checker_emitter_started = True

def _billing_updates_offset_get() -> int:
    raw = str(db.get_config('billing_bot_updates_offset', default='0') or '0').strip()
    try:
        value = int(raw)
    except Exception:
        value = 0
    return max(0, value)

def _billing_updates_offset_set(value: int):
    db.set_config('billing_bot_updates_offset', str(max(0, int(value))))

def _billing_poll_once(bot_token: str):
    offset = _billing_updates_offset_get()
    payload = {
        'offset': offset,
        'timeout': 25,
        'allowed_updates': ['pre_checkout_query', 'message']
    }
    updates = _telegram_bot_api_request(bot_token, 'getUpdates', payload, timeout=40)
    if not isinstance(updates, list) or not updates:
        return
    max_offset = offset
    for update in updates:
        if not isinstance(update, dict):
            continue
        try:
            update_id = int(update.get('update_id') or 0)
        except Exception:
            update_id = 0
        if update_id > 0:
            max_offset = max(max_offset, update_id + 1)
        _handle_billing_update(bot_token, update)
    if max_offset != offset:
        _billing_updates_offset_set(max_offset)

def _billing_bot_worker_loop(bot_token: str, stop_event):
    try:
        try:
            _telegram_bot_api_request(bot_token, 'deleteWebhook', {'drop_pending_updates': False}, timeout=20)
        except Exception as e:
            logger.warning(f"Billing worker: deleteWebhook skipped ({e})")

        while not stop_event.is_set():
            try:
                _billing_poll_once(bot_token)
            except Exception as e:
                logger.error(f"Billing worker poll error: {e}")
                time.sleep(5)
    except Exception as e:
        logger.error(f"Billing worker stopped unexpectedly: {e}")

def is_billing_worker_running() -> bool:
    return billing_bot_thread is not None and billing_bot_thread.is_alive()

def ensure_billing_worker_running():
    global billing_bot_thread, billing_bot_stop_event
    if not BILLING_BOT_POLLING_ENABLED:
        return False
    if is_billing_worker_running():
        return False
    tenant_id = get_default_tenant_id()
    _, bot_token = _telegram_widget_credentials(tenant_id)
    if not bot_token:
        return False

    billing_bot_stop_event = NativeEvent()

    def run_worker():
        _billing_bot_worker_loop(bot_token, billing_bot_stop_event)

    billing_bot_thread = NativeThread(target=run_worker, daemon=True)
    billing_bot_thread.start()
    logger.info("▶ Billing bot worker started")
    return True


@app.before_request
def ensure_scheduler_autostart():
    """
    Автостарт scheduler для gunicorn/eventlet.
    Блок __main__ при таком запуске не исполняется, поэтому проверяем
    периодически на HTTP-запросах и поднимаем scheduler, если он остановился.
    """
    global last_scheduler_autostart_check_at
    now_mono = time.monotonic()
    if now_mono - last_scheduler_autostart_check_at < SCHEDULER_AUTOSTART_CHECK_INTERVAL_SECONDS:
        return
    last_scheduler_autostart_check_at = now_mono
    try:
        auto_start_scheduler = os.getenv('AUTO_START_SCHEDULER', '0') == '1'
        if auto_start_scheduler and has_active_campaigns() and not is_scheduler_running():
            started = ensure_scheduler_running()
            if started:
                logger.info("▶ Scheduler autostarted from before_request hook")
    except Exception as e:
        logger.error(f"Failed scheduler autostart hook: {e}")

@app.before_request
def ensure_billing_worker_autostart():
    global last_billing_worker_check_at
    now_mono = time.monotonic()
    if now_mono - last_billing_worker_check_at < BILLING_WORKER_CHECK_INTERVAL_SECONDS:
        return
    last_billing_worker_check_at = now_mono
    try:
        ensure_billing_worker_running()
    except Exception as e:
        logger.error(f"Failed billing worker autostart hook: {e}")

def has_active_campaigns():
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM outreach_campaigns WHERE status = 'active'")
            return (cursor.fetchone()[0] or 0) > 0
    except Exception as e:
        logger.error(f"Failed to check active campaigns: {e}")
        return False

class TelegramCheckerThread(NativeThread):
    def __init__(
        self,
        api_id,
        api_hash,
        target_chat,
        phones_list,
        delay=2,
        tenant_id: int = 1,
        target_base_id: int = None,
        phone_metadata: dict = None
    ):
        NativeThread.__init__(self)
        self.api_id = api_id
        self.api_hash = api_hash
        self.target_chat = normalize_chat_ref(target_chat)
        self.phones_list = phones_list
        self.raw_total = len(phones_list or [])
        self.delay = delay
        self.tenant_id = int(tenant_id or 1)
        self.target_base_id = int(target_base_id) if target_base_id else None
        self.phone_metadata = phone_metadata or {}
        self.loop = None
        self.processed_count = 0
        self.non_writable_chat_clients = set()
        self._write_forbidden_reported = False

    def _emit(self, event_name: str, payload=None):
        # Под gunicorn+eventlet checker выполняется в отдельном OS-thread.
        # Кросс-тред socket emit может падать greenlet.error.
        # UI в любом случае восстанавливает прогресс через /api/status polling.
        queue_checker_emit(event_name, payload)

    def _remember_checker_file(self, filename: str, file_type: str):
        if not filename:
            return
        key = 'checker_last_files'
        now_iso = datetime.now().isoformat()
        try:
            raw = db.get_config(key, default='[]', tenant_id=self.tenant_id)
            items = json.loads(raw) if raw else []
            if not isinstance(items, list):
                items = []
        except Exception:
            items = []

        items = [it for it in items if str((it or {}).get('filename') or '') != filename]
        items.insert(0, {
            'filename': filename,
            'type': file_type or 'file',
            'created_at': now_iso
        })
        items = items[:20]
        try:
            db.set_config(key, json.dumps(items, ensure_ascii=False), tenant_id=self.tenant_id)
        except Exception as e:
            logger.warning(f"Failed to persist checker file list (tenant={self.tenant_id}): {e}")

    async def _send_contact_card_via_any_checker(self, manager, payload: dict, preferred_client=None) -> bool:
        accounts = list(getattr(manager, 'accounts', []) or [])
        if not accounts:
            return False

        ordered = []
        if preferred_client is not None:
            for client, stats in accounts:
                if client is preferred_client:
                    ordered.append((client, stats))
                    break
        for client, stats in accounts:
            if preferred_client is not None and client is preferred_client:
                continue
            ordered.append((client, stats))

        had_attempt = False
        non_forbidden_error_seen = False
        for client, stats in ordered:
            client_key = id(client)
            if client_key in self.non_writable_chat_clients and client is not preferred_client:
                continue

            send_payload = dict(payload or {})
            send_payload['account_used'] = stats.phone or send_payload.get('account_used')
            ok, err_code = await manager.send_contact_card(
                client,
                self.target_chat,
                send_payload,
                return_error=True
            )
            had_attempt = True
            if ok:
                return True
            if err_code == 'chat_write_forbidden':
                self.non_writable_chat_clients.add(client_key)
            else:
                non_forbidden_error_seen = True

        if had_attempt and not non_forbidden_error_seen and not self._write_forbidden_reported:
            self._write_forbidden_reported = True
            self._emit('warning', {
                'message': 'Нет прав писать в целевой чат: карточки не отправляются. '
                           'Проверьте, что checker-аккаунтам разрешено отправлять сообщения в этот чат.'
            })
        return False

    async def _checker_account_can_send_card(self, client) -> bool:
        """Проверка, что аккаунт реально может отправлять визитки (media) в target chat."""
        try:
            chat_entity = await client.get_entity(self.target_chat)
        except Exception:
            return False
        try:
            perms = await client.get_permissions(chat_entity, 'me')
        except Exception as e:
            error_text = str(e).lower()
            if 'not a member' in error_text or 'usernotparticipant' in error_text:
                return False
            # Консервативно: если не удалось определить права, аккаунт не используем.
            return False

        if getattr(perms, 'is_banned', False):
            return False
        banned = getattr(perms, 'banned_rights', None)
        if banned:
            if getattr(banned, 'send_messages', False):
                return False
            if getattr(banned, 'send_media', False):
                return False
        return True

    async def _invite_checker_accounts_via_admin(self, blocked_pairs):
        """
        Пытается пригласить checker-аккаунты в target chat через админскую service-сессию.
        Используется как fallback, когда checker-аккаунт сам не может писать в чат.
        """
        inviter_phone = "19494793880"
        if not blocked_pairs:
            return 0

        session_name = None
        inviter_tenant_id = None
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT tenant_id, session_name
                FROM account_stats
                WHERE phone = ?
                  AND is_banned = 0
                  AND is_frozen = 0
                ORDER BY
                    CASE WHEN tenant_id = ? THEN 0 ELSE 1 END,
                    CASE WHEN account_type = 'service' THEN 0 ELSE 1 END,
                    tenant_id ASC
                LIMIT 1
                ''',
                (inviter_phone, self.tenant_id)
            )
            row = cursor.fetchone()
            if row and row[1]:
                inviter_tenant_id = row[0]
                session_name = str(row[1]).strip()

        if not session_name:
            logger.warning("Admin inviter session not found for phone 19494793880")
            return 0

        session_path = os.path.join('sessions', session_name)
        if not os.path.exists(session_path + '.session'):
            logger.warning("Admin inviter session file not found for phone 19494793880")
            return 0

        inviter = TelegramClient(session_path, api_id=int(self.api_id), api_hash=self.api_hash)
        invited_count = 0
        try:
            await inviter.connect()
            if not await inviter.is_user_authorized():
                logger.warning("Admin inviter session 19494793880 is not authorized")
                return 0
            logger.info(
                "Admin inviter selected: phone=19494793880 session=%s tenant_id=%s",
                session_name,
                inviter_tenant_id
            )

            try:
                chat_entity = await inviter.get_entity(self.target_chat)
            except Exception as e:
                logger.warning(f"Admin inviter cannot resolve target chat {self.target_chat}: {e}")
                return 0

            for checker_client, checker_stats in blocked_pairs:
                try:
                    me = await checker_client.get_me()
                    if not me or not getattr(me, 'id', None) or not getattr(me, 'access_hash', None):
                        continue
                    input_user = InputUser(user_id=me.id, access_hash=me.access_hash)
                    try:
                        await inviter(InviteToChannelRequest(channel=chat_entity, users=[input_user]))
                        invited_count += 1
                        try:
                            manager_db = getattr(self, '_manager_db', None)
                            if manager_db:
                                manager_db.mark_chat_joined(checker_stats.phone, self.target_chat)
                        except Exception:
                            pass
                        logger.info(f"Admin inviter added checker {checker_stats.phone} to target chat")
                    except errors.UserAlreadyParticipantError:
                        invited_count += 1
                        try:
                            manager_db = getattr(self, '_manager_db', None)
                            if manager_db:
                                manager_db.mark_chat_joined(checker_stats.phone, self.target_chat)
                        except Exception:
                            pass
                    except Exception as e:
                        logger.warning(f"Admin inviter failed for checker {checker_stats.phone}: {e}")
                    await asyncio.sleep(0.25)
                except Exception as e:
                    logger.warning(f"Cannot resolve checker self-user for invite ({checker_stats.phone}): {e}")
        except Exception as e:
            logger.warning(f"Admin inviter flow failed: {e}")
        finally:
            try:
                await inviter.disconnect()
            except Exception:
                pass
        return invited_count
        
    def run(self):
        global checker_status
        try:
            # Всегда запускаем отдельный event loop внутри checker-потока.
            # Попытка планировать задачу в чужой loop приводила к зависанию статуса
            # "running=true" без фактического выполнения проверки.
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.run_checker())
        except Exception as e:
            logger.exception(f"Checker thread crashed: {e}")
            checker_status['running'] = False
            self._emit('error', {'message': f'Ошибка запуска проверки: {str(e)}'})
            self._emit('checker_stopped')
        finally:
            if self.loop:
                try:
                    self.loop.close()
                except Exception:
                    pass

    async def run_checker(self):
        global checker_status
        
        manager = TelegramAccountManager(
            self.api_id,
            self.api_hash,
            'sessions',
            tenant_id=self.tenant_id
        )
        self._manager_db = manager.db
        checker_status['start_time'] = datetime.now().isoformat()
        checker_status['recent_results'] = []

        try:
            loaded = await asyncio.wait_for(
                manager.load_accounts(auto_test_new_sessions=False),
                timeout=120
            )
        except asyncio.TimeoutError:
            self._emit('error', {'message': 'Таймаут инициализации checker-аккаунтов (120с)'})
            checker_status['running'] = False
            self._emit('checker_stopped')
            return
        except Exception as e:
            self._emit('error', {'message': f'Ошибка инициализации аккаунтов: {str(e)}'})
            checker_status['running'] = False
            self._emit('checker_stopped')
            return

        if not loaded:
            self._emit('error', {'message': 'Не удалось загрузить аккаунты'})
            checker_status['running'] = False
            return

        # Для checker важен не только ImportContacts, но и публикация визиток в target chat.
        # Используем в проверке только аккаунты, которые реально могут отправлять media в target chat.
        try:
            await manager.ensure_all_accounts_in_chat(self.target_chat)
        except Exception as e:
            logger.warning(f"Не удалось выполнить массовое вступление checker-аккаунтов в target chat: {e}")

        writable_accounts = []
        blocked_pairs = []
        for client_obj, stats_obj in list(manager.accounts):
            can_send_card = await self._checker_account_can_send_card(client_obj)
            if can_send_card:
                writable_accounts.append((client_obj, stats_obj))
            else:
                blocked_pairs.append((client_obj, stats_obj))

        # Пытаемся довступить blocked checker-аккаунты в target chat через админ-инвайтер.
        if blocked_pairs:
            invited = await self._invite_checker_accounts_via_admin(blocked_pairs)
            if invited:
                await asyncio.sleep(1.0)
                reblocked = []
                for client_obj, stats_obj in blocked_pairs:
                    can_send_card = await self._checker_account_can_send_card(client_obj)
                    if can_send_card:
                        writable_accounts.append((client_obj, stats_obj))
                    else:
                        reblocked.append((client_obj, stats_obj))
                blocked_pairs = reblocked
                logger.info(f"Checker recheck after admin invite: invited={invited}, still_blocked={len(blocked_pairs)}")

        blocked_accounts = [stats_obj.phone or stats_obj.session_name or 'unknown' for _, stats_obj in blocked_pairs]
        if blocked_accounts:
            logger.warning(
                f"Checker accounts excluded (cannot send card to target chat): {len(blocked_accounts)} "
                f"-> {blocked_accounts}"
            )
            self._emit('warning', {
                'message': (
                    f'Исключено checker-аккаунтов без права отправки визиток в целевой чат: '
                    f'{len(blocked_accounts)}'
                )
            })

        manager.accounts = writable_accounts
        if not manager.accounts:
            self._emit('error', {'message': 'Нет checker-аккаунтов с правом отправки визиток в целевой чат'})
            checker_status['running'] = False
            self._emit('checker_stopped')
            return
        
        # Нормализуем и дедуплицируем входной список на стороне сервера,
        # чтобы не тратить лимиты на дубликаты/разные форматы одного номера.
        normalized_unique = []
        seen_numbers = set()
        for raw_phone in self.phones_list:
            norm = _normalize_phone_value(raw_phone)
            if not norm or norm in seen_numbers:
                continue
            seen_numbers.add(norm)
            normalized_unique.append(norm)
        self.phones_list = normalized_unique

        raw_total = int(self.raw_total or 0)
        unique_total = len(self.phones_list)
        checker_status['input_total'] = raw_total
        checker_status['unique_total'] = unique_total
        checker_status['skipped_duplicates'] = max(0, raw_total - unique_total)
        checker_status['skipped_existing_in_base'] = 0
        checker_status['skipped_history_registered'] = 0
        checker_status['skipped_history_not_registered'] = 0
        checker_status['skipped_history_total'] = 0

        prefilled_registered = 0
        prefilled_not_registered = 0
        skipped_existing_in_base = 0

        if self.target_base_id:
            # 1) Не проверяем то, что уже присутствует в выбранной базе.
            try:
                existing_in_base = outreach.get_base_existing_phones(
                    base_id=self.target_base_id,
                    phones=self.phones_list,
                    tenant_id=self.tenant_id
                )
            except Exception as e:
                logger.warning(f"Не удалось получить список телефонов базы {self.target_base_id}: {e}")
                existing_in_base = set()

            if existing_in_base:
                old_len = len(self.phones_list)
                self.phones_list = [p for p in self.phones_list if p not in existing_in_base]
                skipped_existing_in_base = old_len - len(self.phones_list)
                checker_status['skipped_existing_in_base'] = skipped_existing_in_base
                self._emit('warning', {
                    'message': f'{skipped_existing_in_base} номеров уже есть в выбранной базе и пропущены'
                })

            # 2) Для оставшихся подтягиваем последний результат проверки из истории.
            try:
                latest_results = manager.db.get_latest_results_for_phones(
                    self.phones_list,
                    tenant_id=self.tenant_id
                )
            except Exception as e:
                logger.warning(f"Не удалось получить кеш истории проверок: {e}")
                latest_results = {}

            cached_contacts = []
            cached_registered_set = set()
            cached_not_registered_set = set()

            for norm in self.phones_list:
                cached = latest_results.get(norm)
                if not cached:
                    continue
                if bool(cached.get('registered')):
                    meta = self.phone_metadata.get(norm, {})
                    cached_contacts.append({
                        'phone': norm,
                        'user_id': cached.get('user_id'),
                        'username': cached.get('username'),
                        'name': meta.get('name') or cached.get('first_name'),
                        'company': meta.get('company'),
                        'position': meta.get('position'),
                        'access_hash': None
                    })
                    cached_registered_set.add(norm)
                else:
                    cached_not_registered_set.add(norm)

            if cached_contacts:
                try:
                    add_result = outreach.add_base_contacts(
                        base_id=self.target_base_id,
                        contacts=cached_contacts,
                        tenant_id=self.tenant_id,
                        source_file='checker:history_cache'
                    )
                    imported_cached = int(add_result.get('imported', 0))
                    updated_cached = int(add_result.get('updated', 0))
                    skipped_cached = int(add_result.get('skipped', 0))
                    logger.info(
                        f"📦 База {self.target_base_id}: history_cache imported={imported_cached}, "
                        f"updated={updated_cached}, skipped={skipped_cached}, from_history={len(cached_registered_set)}"
                    )
                except Exception as e:
                    logger.warning(f"Не удалось сохранить history-cache в базу {self.target_base_id}: {e}")

                # Важно: для отправки по user_id контакт должен оказаться в целевом чате.
                # Поэтому контакты, поднятые из истории, тоже публикуем как карточки в target chat.
                cached_cards_sent = 0
                cached_cards_failed = 0
                try:
                    if manager.accounts:
                        try:
                            await manager.ensure_all_accounts_in_chat(self.target_chat)
                        except Exception as e:
                            logger.warning(f"Не удалось синхронизировать checker-аккаунты с target chat: {e}")
                        per_card_delay = max(0.35, float(self.delay or 0))

                        for cached in cached_contacts:
                            payload = {
                                'phone': cached.get('phone'),
                                'user_id': cached.get('user_id'),
                                'username': cached.get('username'),
                                'first_name': cached.get('name') or 'Контакт',
                                'last_name': '',
                                'account_used': 'history_cache',
                                'is_test': False
                            }
                            sent = await self._send_contact_card_via_any_checker(manager, payload)
                            if sent:
                                cached_cards_sent += 1
                            else:
                                cached_cards_failed += 1
                            await asyncio.sleep(per_card_delay)

                        logger.info(
                            f"📨 Target chat sync from history-cache: sent={cached_cards_sent}, "
                            f"failed={cached_cards_failed}, total={len(cached_contacts)}"
                        )
                except Exception as e:
                    logger.warning(f"Ошибка отправки history-cache карточек в target chat: {e}")

            prefilled_registered = len(cached_registered_set)
            prefilled_not_registered = len(cached_not_registered_set)
            checker_status['skipped_history_registered'] = prefilled_registered
            checker_status['skipped_history_not_registered'] = prefilled_not_registered
            checker_status['skipped_history_total'] = prefilled_registered + prefilled_not_registered

            cached_any = cached_registered_set | cached_not_registered_set
            if cached_any:
                self.phones_list = [p for p in self.phones_list if p not in cached_any]
                self._emit('warning', {
                    'message': (
                        f'Из истории пропущено: найдено {prefilled_registered}, '
                        f'не найдено {prefilled_not_registered}'
                    )
                })

        else:
            # Без сохранения в базу также не гоняем номера по второму кругу:
            # если номер уже проверялся раньше (найден или нет), пропускаем.
            try:
                latest_results = manager.db.get_latest_results_for_phones(
                    self.phones_list,
                    tenant_id=self.tenant_id
                )
            except Exception as e:
                logger.warning(f"Не удалось получить кеш истории проверок: {e}")
                latest_results = {}

            if latest_results:
                cached_checked = set(latest_results.keys())
                prefilled_registered = sum(
                    1 for p in cached_checked if bool(latest_results.get(p, {}).get('registered'))
                )
                prefilled_not_registered = len(cached_checked) - prefilled_registered
                checker_status['skipped_history_registered'] = prefilled_registered
                checker_status['skipped_history_not_registered'] = prefilled_not_registered
                checker_status['skipped_history_total'] = prefilled_registered + prefilled_not_registered
                self.phones_list = [p for p in self.phones_list if p not in cached_checked]
                self._emit('warning', {
                    'message': (
                        f'Пропущены ранее проверенные номера: найдено {prefilled_registered}, '
                        f'не найдено {prefilled_not_registered}'
                    )
                })

        # Учитываем кэш-результаты в общей статистике текущего запуска.
        checker_status['registered'] += prefilled_registered
        checker_status['not_registered'] += prefilled_not_registered

        if not self.phones_list:
            if prefilled_registered > 0 or prefilled_not_registered > 0 or skipped_existing_in_base > 0:
                self._emit('checker_complete', {
                    'registered': prefilled_registered,
                    'not_registered': prefilled_not_registered,
                    'errors': 0,
                    'invalid': 0,
                    'test_checks': checker_status['test_checks']
                })
                await manager.close_all()
                checker_status['running'] = False
                self._emit('checker_stopped')
                return

            self._emit('error', {'message': 'Нет новых номеров для проверки'})
            checker_status['running'] = False
            self._emit('checker_stopped')
            return
        
        checker_status['accounts'] = []
        for client, stats in manager.accounts:
            is_working = True
            checker_status['accounts'].append({
                'phone': stats.phone,
                'checked_today': stats.checked_today,
                'total_checked': stats.total_checked,
                'can_use': stats.can_use_today(),
                'account_type': stats.account_type,
                'test_quality': stats.test_quality,
                'daily_limit': stats.get_daily_limit(),
                'is_working': is_working
            })
        
        self._emit('accounts_update', checker_status['accounts'])
        
        results = {
            'registered': [],
            'not_registered': [],
            'errors': [],
            'invalid': []
        }
        
        processed = 0
        total = len(self.phones_list)
        checker_status['total'] = total
        checker_status['start_time'] = checker_status.get('start_time') or datetime.now().isoformat()
        
        for i, phone in enumerate(self.phones_list):
            if not checker_status['running']:
                break
            
            is_test = False
            if i > 0 and i % 15 == 0:
                is_test = True
                test_phone = manager.get_next_test_phone()
                logger.info(f"🔍 Тестовая проверка аккаунтов с номером {test_phone}")
                
                for client, stats in manager.accounts:
                    if stats.can_use_today():
                        test_result = await manager.check_phone(client, stats, test_phone, is_test=True)
                        if test_result and test_result.get('registered'):
                            logger.info(f"✅ Аккаунт {stats.phone} прошел тест")
                        elif test_result and not test_result.get('registered'):
                            logger.warning(f"⚠️ Аккаунт {stats.phone} не прошел тест")
                
                checker_status['accounts'] = []
                for client, stats in manager.accounts:
                    is_working = True
                    checker_status['accounts'].append({
                        'phone': stats.phone,
                        'checked_today': stats.checked_today,
                        'total_checked': stats.total_checked,
                        'can_use': stats.can_use_today(),
                        'account_type': stats.account_type,
                        'test_quality': stats.test_quality,
                        'daily_limit': stats.get_daily_limit(),
                        'is_working': is_working
                    })
                self._emit('accounts_update', checker_status['accounts'])
                
                phone = self.phones_list[i]
            
            processed += 1
            checker_status['current_phone'] = phone
            checker_status['processed'] = processed
            if is_test:
                checker_status['test_checks'] += 1
            
            if processed > 0 and checker_status['start_time']:
                try:
                    elapsed = (datetime.now() - datetime.fromisoformat(checker_status['start_time'])).total_seconds()
                    avg_time = elapsed / processed
                    remaining = avg_time * (total - processed)
                    checker_status['estimated_time'] = remaining
                except Exception:
                    checker_status['estimated_time'] = None
            
            self._emit('progress_update', checker_status)
            
            account_data = await manager.get_next_account()
            if not account_data:
                self._emit('warning', {'message': 'Нет доступных аккаунтов'})
                break
            
            client, stats = account_data
            result = await manager.check_phone(client, stats, phone, is_test)
            
            if result is None:
                results['errors'].append(phone)
                checker_status['errors'] += 1
                payload = {
                    'phone': phone,
                    'status': 'error',
                    'message': 'Ошибка проверки'
                }
                push_recent_result(payload)
                self._emit('phone_result', payload)
            elif 'error' in result:
                results['invalid'].append(phone)
                payload = {
                    'phone': phone,
                    'status': 'invalid',
                    'message': f"Ошибка: {result['error']}"
                }
                push_recent_result(payload)
                self._emit('phone_result', payload)
            elif result['registered']:
                results['registered'].append(result)
                checker_status['registered'] += 1
                
                sent_to_chat = await self._send_contact_card_via_any_checker(
                    manager,
                    result,
                    preferred_client=client
                )
                if sent_to_chat and self.target_base_id:
                    phone_key = _normalize_phone_value(result.get('phone'))
                    meta = self.phone_metadata.get(phone_key, {}) if phone_key else {}
                    contact = {
                        'phone': phone_key,
                        'user_id': result.get('user_id'),
                        'username': result.get('username'),
                        'name': meta.get('name') or result.get('first_name'),
                        'company': meta.get('company'),
                        'position': meta.get('position'),
                        'access_hash': None
                    }
                    try:
                        add_result = outreach.add_base_contacts(
                            base_id=self.target_base_id,
                            contacts=[contact],
                            tenant_id=self.tenant_id,
                            source_file='checker:file_to_base'
                        )
                        imported_now = int(add_result.get('imported', 0))
                        updated_now = int(add_result.get('updated', 0))
                        skipped_now = int(add_result.get('skipped', 0))
                        if imported_now + updated_now == 0:
                            logger.info(
                                f"База {self.target_base_id}: контакт {phone_key} не добавлен (duplicate/skip), "
                                f"skipped={skipped_now}"
                            )
                    except Exception as e:
                        logger.warning(f"Не удалось сохранить контакт в базу {self.target_base_id}: {e}")
                
                payload = {
                    'phone': phone,
                    'status': 'registered',
                    'is_test': result.get('is_test', False),
                    'user_info': {
                        'user_id': result['user_id'],
                        'username': result.get('username'),
                        'first_name': result.get('first_name'),
                        'last_name': result.get('last_name')
                    }
                }
                push_recent_result(payload)
                self._emit('phone_result', payload)
            else:
                results['not_registered'].append(phone)
                checker_status['not_registered'] += 1
                payload = {
                    'phone': phone,
                    'status': 'not_registered',
                    'is_test': result.get('is_test', False),
                    'message': 'Не зарегистрирован'
                }
                push_recent_result(payload)
                self._emit('phone_result', payload)
            
            await asyncio.sleep(self.delay)
            
            checker_status['accounts'] = []
            for client, stats in manager.accounts:
                is_working = True
                checker_status['accounts'].append({
                    'phone': stats.phone,
                    'checked_today': stats.checked_today,
                    'total_checked': stats.total_checked,
                    'can_use': stats.can_use_today(),
                    'account_type': stats.account_type,
                    'test_quality': stats.test_quality,
                    'daily_limit': stats.get_daily_limit(),
                    'is_working': is_working
                })
            self._emit('accounts_update', checker_status['accounts'])
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if results['registered']:
            df_reg = pd.DataFrame(results['registered'])
            filename = f'registered_{timestamp}.csv'
            filepath = os.path.join(app.config['RESULTS_FOLDER'], filename)
            df_reg.to_csv(filepath, index=False, encoding='utf-8')
            self._remember_checker_file(filename, 'registered')
            self._emit('file_ready', {'filename': filename, 'type': 'registered', 'created_at': datetime.now().isoformat()})
        
        if results['not_registered']:
            filename = f'not_registered_{timestamp}.txt'
            filepath = os.path.join(app.config['RESULTS_FOLDER'], filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(results['not_registered']))
            self._remember_checker_file(filename, 'not_registered')
            self._emit('file_ready', {'filename': filename, 'type': 'not_registered', 'created_at': datetime.now().isoformat()})
        
        if results['errors']:
            filename = f'errors_{timestamp}.txt'
            filepath = os.path.join(app.config['RESULTS_FOLDER'], filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(results['errors']))
            self._remember_checker_file(filename, 'errors')
            self._emit('file_ready', {'filename': filename, 'type': 'errors', 'created_at': datetime.now().isoformat()})
        
        self._emit('checker_complete', {
            'registered': prefilled_registered + len(results['registered']),
            'not_registered': prefilled_not_registered + len(results['not_registered']),
            'errors': len(results['errors']),
            'invalid': len(results['invalid']),
            'test_checks': checker_status['test_checks']
        })
        
        await manager.close_all()
        checker_status['running'] = False
        self._emit('checker_stopped')

def _is_checker_thread_alive() -> bool:
    global checker_process
    try:
        return bool(checker_process and checker_process.is_alive())
    except Exception:
        return False

def _refresh_checker_running_flag():
    """Сбрасывает залипший running, если checker-поток уже завершился."""
    global checker_status
    if checker_status.get('running') and not _is_checker_thread_alive():
        checker_status['running'] = False
        checker_status['estimated_time'] = None
        checker_status['current_phone'] = checker_status.get('current_phone') or ''


# ===== ОСНОВНЫЕ ЭНДПОИНТЫ =====

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/outreach')
def outreach_page():
    return render_template('outreach.html')

@app.route('/replies')
def replies_page():
    return render_template('replies.html')

@app.route('/bases')
def bases_page():
    return render_template('bases.html')

@app.route('/accounts')
def accounts_page():
    return render_template('accounts.html')

@app.route('/proxies')
def proxies_page():
    return render_template('proxies.html')

@app.route('/users')
def users_page():
    user, err = require_role('admin')
    if err:
        return redirect(url_for('index'))
    return render_template('users.html')

@app.route('/login')
def login_page():
    if get_current_user():
        return redirect(url_for('index'))
    tenant_id = get_default_tenant_id()
    bot_username, _ = _telegram_widget_credentials(tenant_id)
    return render_template(
        'login.html',
        telegram_bot_username=bot_username,
        auth_message=str(request.args.get('message') or '').strip(),
        auth_error=str(request.args.get('error') or '').strip()
    )

@app.route('/register')
def register_page():
    if get_current_user():
        return redirect(url_for('index'))
    tenant_id = get_default_tenant_id()
    bot_username, _ = _telegram_widget_credentials(tenant_id)
    return render_template(
        'register.html',
        telegram_bot_username=bot_username,
        auth_message=str(request.args.get('message') or '').strip(),
        auth_error=str(request.args.get('error') or '').strip()
    )

@app.route('/oferta')
def oferta_page():
    return render_template('oferta.html')

@app.route('/logout', methods=['GET', 'POST'])
def logout_page():
    session.clear()
    if request.path.startswith('/api/'):
        return jsonify({'status': 'ok'})
    return redirect(url_for('login_page'))

@app.route('/api/auth/telegram/start', methods=['GET'])
def api_telegram_auth_start():
    mode = str(request.args.get('mode') or 'login').strip().lower()
    if mode not in ('login', 'register'):
        mode = 'login'
    target = 'login_page' if mode == 'login' else 'register_page'
    return _telegram_auth_redirect(
        target,
        message='Используйте кнопку Telegram Login на странице для входа'
    )

@app.route('/api/auth/telegram/widget', methods=['POST'])
def api_telegram_auth_widget():
    data = request.get_json(silent=True) or {}
    mode = str(data.get('mode') or 'login').strip().lower()
    auth_data = data.get('auth') if isinstance(data.get('auth'), dict) else data

    tenant_id = get_default_tenant_id()
    _, bot_token = _telegram_widget_credentials(tenant_id)
    if not bot_token:
        return jsonify({'error': 'Telegram bot token не настроен'}), 500

    try:
        payload = _telegram_widget_validate(auth_data, bot_token)
    except Exception as e:
        logger.warning(f"Telegram widget auth validation failed: {e}")
        return jsonify({'error': 'Невалидные данные Telegram авторизации'}), 401

    tg_id = str(payload.get('id') or '').strip()
    if not tg_id:
        return jsonify({'error': 'Telegram ID отсутствует'}), 400

    tg_username = str(payload.get('username') or '').strip()
    full_name = ' '.join(
        p for p in [
            str(payload.get('first_name') or '').strip(),
            str(payload.get('last_name') or '').strip()
        ] if p
    ).strip()
    tg_phone = str(payload.get('phone_number') or '').strip()
    tg_picture = str(payload.get('photo_url') or '').strip()

    result = _upsert_user_from_telegram(
        mode=mode,
        tenant_id=tenant_id,
        tg_id=tg_id,
        tg_username=tg_username,
        tg_name=full_name,
        tg_phone=tg_phone,
        tg_picture=tg_picture
    )
    if result.get('status') == 'ok':
        return jsonify({'status': 'ok', 'next': result.get('next') or '/'})
    if result.get('status') == 'pending':
        return jsonify({'status': 'pending', 'message': result.get('message') or 'Ожидайте активацию'}), 200
    if result.get('status') == 'payment_required':
        return jsonify({
            'status': 'payment_required',
            'message': result.get('message') or 'Требуется оплата подписки',
            'payment_url': result.get('payment_url') or '',
            'stars_price': int(result.get('stars_price') or BILLING_MONTHLY_STARS),
            'trial_days': int(result.get('trial_days') or BILLING_TRIAL_DAYS)
        }), 402
    if result.get('status') == 'not_found':
        return jsonify({'status': 'not_found', 'error': result.get('message') or 'Пользователь не найден'}), 404
    return jsonify({'error': 'Ошибка Telegram авторизации'}), 500

@app.route('/auth/telegram/callback', methods=['GET'])
def telegram_oauth_callback():
    return _telegram_auth_redirect(
        'login_page',
        message='Используйте кнопку Telegram Login на странице входа'
    )

@app.route('/api/auth/login', methods=['POST'])
def api_login():
    return jsonify({'error': 'Доступно только через Telegram Login'}), 410

@app.route('/api/auth/register', methods=['POST'])
def api_register():
    return jsonify({'error': 'Доступно только через Telegram Login'}), 410

@app.route('/api/auth/me', methods=['GET'])
def api_me():
    user = get_current_user()
    if not user:
        return jsonify({'authenticated': False}), 200
    billing = _billing_state(user.get('trial_ends_at'), user.get('subscription_expires_at'))
    return jsonify({'authenticated': True, 'user': user, 'billing': billing})

@app.route('/api/billing/status', methods=['GET'])
def api_billing_status():
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    billing = _billing_state(user.get('trial_ends_at'), user.get('subscription_expires_at'))
    return jsonify({
        'status': 'ok',
        'billing': {
            'active': bool(billing.get('active')),
            'mode': str(billing.get('mode') or ''),
            'active_until': _format_db_datetime(billing['active_until']) if billing.get('active_until') else None,
            'message': billing.get('message') or '',
            'stars_price': BILLING_MONTHLY_STARS,
            'trial_days': BILLING_TRIAL_DAYS
        }
    })

@app.route('/api/billing/checkout', methods=['POST'])
def api_billing_checkout():
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    invoice_url = _create_subscription_invoice_link(int(user.get('id') or 0), int(user.get('tenant_id') or 0))
    if not invoice_url:
        return jsonify({'error': 'Не удалось создать ссылку на оплату'}), 500
    return jsonify({
        'status': 'ok',
        'payment_url': invoice_url,
        'stars_price': BILLING_MONTHLY_STARS,
        'plan_days': BILLING_PLAN_DAYS
    })

@app.route('/api/auth/change-password', methods=['POST'])
def api_change_password():
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json(silent=True) or {}
    old_password = str(data.get('old_password') or '')
    new_password = str(data.get('new_password') or '')
    if len(new_password) < 6:
        return jsonify({'error': 'new_password must be at least 6 chars'}), 400
    if not old_password:
        return jsonify({'error': 'old_password is required'}), 400
    if int(user.get('id') or 0) <= 0:
        return jsonify({'error': 'Password change unavailable for basic auth user'}), 400

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT password_hash FROM users WHERE id = ? AND tenant_id = ?', (user['id'], user['tenant_id']))
        row = cursor.fetchone()
        if not row or not check_password_hash(row[0], old_password):
            return jsonify({'error': 'Invalid current password'}), 401
        cursor.execute(
            'UPDATE users SET password_hash = ? WHERE id = ? AND tenant_id = ?',
            (generate_password_hash(new_password), user['id'], user['tenant_id'])
        )
        conn.commit()
    return jsonify({'status': 'ok'})

@app.route('/api/auth/users', methods=['GET'])
def api_list_users():
    user, err = require_role('admin')
    if err:
        return err
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT
                u.id,
                u.username,
                u.role,
                u.is_active,
                u.created_at,
                COALESCE((
                    SELECT COUNT(*)
                    FROM account_stats a
                    WHERE a.tenant_id = u.tenant_id
                      AND a.added_by_user_id = u.id
                ), 0) AS sessions_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM outreach_campaigns c
                    WHERE c.tenant_id = u.tenant_id
                      AND c.created_by_user_id = u.id
                ), 0) AS campaigns_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM outreach_campaigns c
                    WHERE c.tenant_id = u.tenant_id
                      AND c.created_by_user_id = u.id
                      AND c.status = 'active'
                ), 0) AS active_campaigns_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM conversations cv
                    JOIN outreach_campaigns c
                      ON c.id = cv.campaign_id
                     AND c.tenant_id = cv.tenant_id
                    WHERE cv.tenant_id = u.tenant_id
                      AND c.created_by_user_id = u.id
                      AND cv.direction = 'outgoing'
                ), 0) AS sent_total,
                COALESCE((
                    SELECT COUNT(*)
                    FROM conversations cv
                    JOIN outreach_campaigns c
                      ON c.id = cv.campaign_id
                     AND c.tenant_id = cv.tenant_id
                    WHERE cv.tenant_id = u.tenant_id
                      AND c.created_by_user_id = u.id
                      AND cv.direction = 'outgoing'
                      AND COALESCE(cv.is_followup, 0) = 0
                ), 0) AS sent_initial,
                COALESCE((
                    SELECT COUNT(*)
                    FROM conversations cv
                    JOIN outreach_campaigns c
                      ON c.id = cv.campaign_id
                     AND c.tenant_id = cv.tenant_id
                    WHERE cv.tenant_id = u.tenant_id
                      AND c.created_by_user_id = u.id
                      AND cv.direction = 'outgoing'
                      AND COALESCE(cv.is_followup, 0) = 1
                ), 0) AS sent_followup,
                COALESCE((
                    SELECT COUNT(*)
                    FROM conversations cv
                    JOIN outreach_campaigns c
                      ON c.id = cv.campaign_id
                     AND c.tenant_id = cv.tenant_id
                    WHERE cv.tenant_id = u.tenant_id
                      AND c.created_by_user_id = u.id
                      AND cv.direction = 'outgoing'
                      AND date(cv.sent_at) = date('now')
                ), 0) AS sent_today
            FROM users u
            WHERE u.tenant_id = ?
            ORDER BY u.id
        ''', (user['tenant_id'],))
        rows = cursor.fetchall()
    return jsonify([
        {
            'id': int(r[0]),
            'username': r[1],
            'role': ('admin' if str(r[2] or '').lower() in ('owner', 'admin') else 'user'),
            'is_active': bool(r[3]),
            'created_at': r[4],
            'sessions_count': int(r[5] or 0),
            'campaigns_count': int(r[6] or 0),
            'active_campaigns_count': int(r[7] or 0),
            'sent_total': int(r[8] or 0),
            'sent_initial': int(r[9] or 0),
            'sent_followup': int(r[10] or 0),
            'sent_today': int(r[11] or 0)
        }
        for r in rows
    ])

@app.route('/api/admin/users/overview', methods=['GET'])
def api_admin_users_overview():
    actor, err = require_role('admin')
    if err:
        return err
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT
                COUNT(*) AS users_total,
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS users_active,
                SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS users_pending
            FROM users
            WHERE tenant_id = ?
        ''', (actor['tenant_id'],))
        users_total, users_active, users_pending = cursor.fetchone()

        cursor.execute('SELECT COUNT(*) FROM account_stats WHERE tenant_id = ?', (actor['tenant_id'],))
        sessions_total = int(cursor.fetchone()[0] or 0)

        cursor.execute('SELECT COUNT(*) FROM outreach_campaigns WHERE tenant_id = ?', (actor['tenant_id'],))
        campaigns_total = int(cursor.fetchone()[0] or 0)

        cursor.execute('SELECT COUNT(*) FROM outreach_campaigns WHERE tenant_id = ? AND status = ?', (actor['tenant_id'], 'active'))
        campaigns_active = int(cursor.fetchone()[0] or 0)

        cursor.execute('''
            SELECT COUNT(*)
            FROM conversations
            WHERE tenant_id = ? AND direction = 'outgoing'
        ''', (actor['tenant_id'],))
        sends_total = int(cursor.fetchone()[0] or 0)

        cursor.execute('''
            SELECT COUNT(*)
            FROM conversations
            WHERE tenant_id = ? AND direction = 'outgoing' AND date(sent_at) = date('now')
        ''', (actor['tenant_id'],))
        sends_today = int(cursor.fetchone()[0] or 0)

    return jsonify({
        'summary': {
            'users_total': int(users_total or 0),
            'users_active': int(users_active or 0),
            'users_pending': int(users_pending or 0),
            'sessions_total': sessions_total,
            'campaigns_total': campaigns_total,
            'campaigns_active': campaigns_active,
            'sends_total': sends_total,
            'sends_today': sends_today
        }
    })

@app.route('/api/auth/users', methods=['POST'])
def api_create_user():
    actor, err = require_role('admin')
    if err:
        return err

    data = request.get_json(silent=True) or {}
    username = str(data.get('username') or '').strip()
    password = str(data.get('password') or '')
    role = str(data.get('role') or 'user').strip().lower()
    if role not in ('admin', 'user'):
        role = 'user'
    if not username:
        return jsonify({'error': 'username is required'}), 400
    if len(password) < 6:
        return jsonify({'error': 'password must be at least 6 chars'}), 400
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT 1 FROM users WHERE tenant_id = ? AND username = ?',
            (actor['tenant_id'], username)
        )
        if cursor.fetchone():
            return jsonify({'error': 'Username already exists'}), 409
        cursor.execute('''
            INSERT INTO users (tenant_id, username, password_hash, role, is_active)
            VALUES (?, ?, ?, ?, 1)
        ''', (actor['tenant_id'], username, generate_password_hash(password), role))
        conn.commit()
        user_id = int(cursor.lastrowid)
    return jsonify({'status': 'created', 'id': user_id})

@app.route('/api/auth/users/<int:user_id>', methods=['PATCH'])
def api_update_user(user_id: int):
    actor, err = require_role('admin')
    if err:
        return err
    data = request.get_json(silent=True) or {}

    updates = []
    params = []

    if 'role' in data:
        role = str(data.get('role') or '').strip().lower()
        if role not in ('admin', 'user'):
            return jsonify({'error': 'Invalid role'}), 400
        updates.append('role = ?')
        params.append(role)

    if 'is_active' in data:
        is_active = bool(data.get('is_active'))
        updates.append('is_active = ?')
        params.append(1 if is_active else 0)

    if 'password' in data:
        password = str(data.get('password') or '')
        if len(password) < 6:
            return jsonify({'error': 'password must be at least 6 chars'}), 400
        updates.append('password_hash = ?')
        params.append(generate_password_hash(password))

    if not updates:
        return jsonify({'error': 'No updates provided'}), 400

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT id, role FROM users WHERE id = ? AND tenant_id = ?',
            (user_id, actor['tenant_id'])
        )
        target = cursor.fetchone()
        if not target:
            return jsonify({'error': 'User not found'}), 404
        if int(target[0]) == int(actor.get('id') or 0) and 'is_active' in data and not bool(data.get('is_active')):
            return jsonify({'error': 'Cannot deactivate yourself'}), 400

        params.extend([user_id, actor['tenant_id']])
        cursor.execute(
            f"UPDATE users SET {', '.join(updates)} WHERE id = ? AND tenant_id = ?",
            params
        )
        conn.commit()
    return jsonify({'status': 'updated'})

@app.route('/api/auth/users/<int:user_id>', methods=['DELETE'])
def api_delete_user(user_id: int):
    actor, err = require_role('admin')
    if err:
        return err
    if int(actor.get('id') or 0) == int(user_id):
        return jsonify({'error': 'Cannot delete yourself'}), 400

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT role FROM users WHERE id = ? AND tenant_id = ?',
            (user_id, actor['tenant_id'])
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'User not found'}), 404
        cursor.execute(
            'DELETE FROM users WHERE id = ? AND tenant_id = ?',
            (user_id, actor['tenant_id'])
        )
        conn.commit()
    return jsonify({'status': 'deleted'})


# ===== API ЭНДПОИНТЫ =====

@app.route('/api/status')
def get_status():
    _refresh_checker_running_flag()
    return jsonify(checker_status)

@app.route('/api/checker/accounts', methods=['GET'])
def get_checker_accounts_live():
    """
    Синхронный список checker-аккаунтов для UI.
    Без запуска async-контуров (иначе в eventlet/gunicorn возможен конфликт loop).
    """
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    accounts = []

    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                    phone,
                    session_name,
                    COALESCE(total_checks, 0),
                    COALESCE(is_banned, 0),
                    COALESCE(is_frozen, 0),
                    COALESCE(test_quality, 'unknown'),
                    COALESCE(checker_daily_limit, 20),
                    COALESCE(last_check_date, '')
                FROM account_stats
                WHERE tenant_id = ?
                  AND account_type = 'checker'
            ''' + (
                ''
                if is_admin_user(user)
                else ' AND (added_by_user_id = ? OR COALESCE(added_by_user_id, 0) = 0)'
            ) + ' ORDER BY phone',
                (tenant_id,) if is_admin_user(user) else (tenant_id, user_id)
            )
            rows = cursor.fetchall()

        for row in rows:
            phone = str(row[0] or '').strip()
            session_name = str(row[1] or '').strip()
            total_checked = int(row[2] or 0)
            is_banned = bool(row[3])
            is_frozen = bool(row[4])
            test_quality = str(row[5] or 'unknown').strip().lower()
            checker_daily_limit = max(1, int(row[6] or 20))
            session_exists = bool(session_name and os.path.exists(os.path.join('sessions', f'{session_name}.session')))

            # Для checker-экрана показываем только рабочие аккаунты:
            # есть сессия, нет бана/фриза и аккаунт не помечен как bad.
            # unknown считаем рабочим, потому что качество может быть не проставлено
            # после миграции/переноса, но аккаунт физически доступен.
            is_working = session_exists and not is_banned and not is_frozen and test_quality != 'bad'
            if not is_working:
                continue

            try:
                # sync=True держит checked_today/last_check_date в БД актуальными
                # и устраняет рассинхрон счётчиков после перезагрузки интерфейса.
                usage = db.get_effective_account_usage(phone, tenant_id=tenant_id, sync=True)
            except Exception:
                usage = {}
            checked_today = int(usage.get('checked_today') or 0)
            effective_daily_limit = int(usage.get('checker_daily_limit') or checker_daily_limit)
            if checked_today > effective_daily_limit:
                checked_today = effective_daily_limit

            accounts.append({
                'phone': phone,
                'session_name': session_name,
                'checked_today': checked_today,
                'total_checked': total_checked,
                'account_type': 'checker',
                'test_quality': test_quality,
                'daily_limit': effective_daily_limit,
                'can_use': checked_today < effective_daily_limit,
                'is_working': is_working
            })
    except Exception as e:
        logger.error(f"Error in /api/checker/accounts: {e}")
        return jsonify({'error': str(e)}), 500

    return jsonify(accounts)

@app.route('/api/accounts')
def get_accounts():
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    accounts = []
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            if is_admin_user(user):
                cursor.execute(
                    'SELECT phone, session_name FROM account_stats WHERE tenant_id = ? ORDER BY phone',
                    (tenant_id,)
                )
            else:
                cursor.execute(
                    'SELECT phone, session_name FROM account_stats WHERE tenant_id = ? AND added_by_user_id = ? ORDER BY phone',
                    (tenant_id, user_id)
                )
            for phone, session_name in cursor.fetchall():
                accounts.append({'name': session_name or phone, 'file': f"{session_name}.session" if session_name else ''})
    except Exception:
        pass
    return jsonify(accounts)

@app.route('/api/accounts/detailed', methods=['GET'])
def get_accounts_detailed():
    """Получение детальной информации об аккаунтах"""
    accounts = []
    for_checker = str(request.args.get('for_checker') or '').strip().lower() in {'1', 'true', 'yes', 'on'}
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    SELECT a.phone, a.session_name, a.account_type, a.total_checks, a.test_quality,
                           a.last_check, 
                           COALESCE(a.is_banned, 0) as is_banned,
                           COALESCE(a.is_frozen, 0) as is_frozen,
                           a.proxy_id,
                           a.test_history, a.last_check as last_used, 
                           COALESCE(a.checked_today, 0) as checked_today,
                           a.last_check_date,
                           COALESCE(a.outreach_daily_limit, 20) as outreach_daily_limit,
                           COALESCE(a.checker_daily_limit, 20) as checker_daily_limit,
                           f.device_model, f.app_version, f.system_version, f.lang_code,
                           a.tenant_id as account_tenant_id,
                           COALESCE(a.first_name, '') as first_name,
                           COALESCE(a.last_name, '') as last_name
                    FROM account_stats a
                    LEFT JOIN account_fingerprints f ON a.phone = f.account_phone
                    WHERE a.tenant_id = ?
                ''' + ('' if is_admin_user(user) else ' AND a.added_by_user_id = ?'), (tenant_id,) if is_admin_user(user) else (tenant_id, user_id))
            except sqlite3.OperationalError as e:
                logger.error(f"Database error: {e}")
                # Пробуем без новых колонок
                cursor.execute('''
                    SELECT a.phone, a.session_name, a.account_type, a.total_checks, a.test_quality,
                           a.last_check, 
                           0 as is_banned,
                           0 as is_frozen,
                           a.proxy_id,
                           a.test_history, a.last_check as last_used, 
                           0 as checked_today,
                           date('now') as last_check_date,
                           20 as outreach_daily_limit,
                           20 as checker_daily_limit,
                           f.device_model, f.app_version, f.system_version, f.lang_code,
                           a.tenant_id as account_tenant_id,
                           '' as first_name,
                           '' as last_name
                    FROM account_stats a
                    LEFT JOIN account_fingerprints f ON a.phone = f.account_phone
                    WHERE a.tenant_id = ?
                ''' + ('' if is_admin_user(user) else ' AND a.added_by_user_id = ?'), (tenant_id,) if is_admin_user(user) else (tenant_id, user_id))
            
            for row in cursor.fetchall():
                phone = row[0]
                session_name = row[1]
                account_type = row[2] or 'checker'
                is_banned = bool(row[6])
                is_frozen = bool(row[7])
                test_quality = row[4] or 'unknown'

                usage = db.get_effective_account_usage(phone, tenant_id=tenant_id, sync=False)
                checked_today = int(usage.get('checked_today') or 0)
                last_check_date = usage.get('last_check_date')
                checker_daily_limit = int(usage.get('checker_daily_limit') or 20)
                outreach_daily_limit = int(usage.get('outreach_daily_limit') or 20)
                effective_daily_limit = int(usage.get('effective_daily_limit') or 20)
                can_use_today = checked_today < effective_daily_limit

                session_exists = bool(session_name and os.path.exists(os.path.join('sessions', f'{session_name}.session')))
                is_working_checker = (
                    account_type == 'checker'
                    and not is_banned
                    and not is_frozen
                    and session_exists
                    and str(test_quality).lower() == 'good'
                )

                account_item = {
                    'phone': phone,
                    'session_name': session_name,
                    'account_type': account_type,
                    'total_checked': row[3] or 0,
                    'test_quality': test_quality,
                    'last_check': row[5],
                    'is_banned': is_banned,
                    'is_frozen': is_frozen,
                    'proxy_id': row[8],
                    'test_history': json.loads(row[9]) if row[9] else [],
                    'last_used': row[10],
                    'checked_today': checked_today,
                    'last_check_date': last_check_date,
                    'checker_daily_limit': checker_daily_limit,
                    'outreach_daily_limit': outreach_daily_limit,
                    'effective_daily_limit': effective_daily_limit,
                    'can_use_today': can_use_today,
                    'is_working_checker': is_working_checker,
                    'session_exists': session_exists,
                    'first_name': row[20] if len(row) > 20 else '',
                    'last_name': row[21] if len(row) > 21 else '',
                    'fingerprint': {
                        'device_model': row[15],
                        'app_version': row[16],
                        'system_version': row[17],
                        'lang_code': row[18]
                    } if len(row) > 15 and row[15] else None
                }

                if for_checker and not is_working_checker:
                    continue
                accounts.append(account_item)
    except Exception as e:
        logger.error(f"Error in get_accounts_detailed: {e}")
        return jsonify({'error': str(e)}), 500
    
    return jsonify(accounts)

def _register_account_session(
    tenant_id: int,
    actor_id: int,
    phone: str,
    session_name: str,
    first_name: str = '',
    last_name: str = ''
):
    """Регистрирует session в account_stats, если аккаунта ещё нет."""
    first_name = str(first_name or '').strip()
    last_name = str(last_name or '').strip()
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT phone, session_name FROM account_stats WHERE tenant_id = ? AND phone = ?',
            (tenant_id, phone)
        )
        existing = cursor.fetchone()
        if existing:
            cursor.execute(
                '''
                UPDATE account_stats
                SET session_name = ?,
                    first_name = COALESCE(NULLIF(?, ''), first_name),
                    last_name = COALESCE(NULLIF(?, ''), last_name)
                WHERE tenant_id = ? AND phone = ?
                ''',
                (
                    session_name or (existing[1] or ''),
                    first_name,
                    last_name,
                    tenant_id,
                    phone
                )
            )
            conn.commit()
            return {
                'status': 'exists',
                'phone': existing[0],
                'session_name': session_name or (existing[1] or ''),
                'message': 'Аккаунт уже зарегистрирован'
            }

        cursor.execute('''
            INSERT INTO account_stats
            (tenant_id, phone, session_name, account_type, is_banned, is_frozen, checked_today, last_check_date, added_by_user_id, first_name, last_name)
            VALUES (?, ?, ?, 'checker', 0, 0, 0, ?, ?, ?, ?)
        ''', (
            tenant_id,
            phone,
            session_name,
            date.today().isoformat(),
            actor_id if actor_id > 0 else None,
            first_name,
            last_name
        ))
        conn.commit()

    try:
        fingerprint = fingerprint_manager.generate_fingerprint('checker')
        fingerprint_manager.save_fingerprint(phone, fingerprint)
    except Exception as e:
        logger.error(f"Error creating fingerprint for account {phone}: {e}")

    return {
        'status': 'uploaded',
        'phone': phone,
        'session_name': session_name
    }

def _cleanup_qr_auth_sessions():
    now_ts = time.time()
    stale_tokens = []
    with qr_auth_lock:
        for token, rec in qr_auth_sessions.items():
            status = rec.get('status')
            created_at = float(rec.get('created_at') or now_ts)
            finished_at = float(rec.get('finished_at') or 0)
            max_age = 900 if status in ('done', 'error', 'expired', 'cancelled') else 360
            ref_ts = finished_at or created_at
            if now_ts - ref_ts > max_age:
                stale_tokens.append(token)

        for token in stale_tokens:
            rec = qr_auth_sessions.pop(token, None)
            if not rec:
                continue
            pid = int(rec.get('worker_pid') or 0)
            if pid > 0:
                try:
                    os.kill(pid, 15)
                except Exception:
                    pass

def _qr_worker_script_path() -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    helper = os.path.join(base_dir, 'tools', 'qr_auth_worker.py')
    if os.path.exists(helper):
        return helper
    helper = os.path.join(base_dir, 'qr_auth_worker.py')
    if os.path.exists(helper):
        return helper
    raise RuntimeError('QR worker script not found')

def _run_qr_worker_2fa(api_id: int, api_hash: str, session_path: str, password: str):
    worker = _qr_worker_script_path()
    cmd = [sys.executable, worker, '2fa', str(api_id), api_hash, session_path, password]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=80)
    payload = {}
    try:
        payload = json.loads((proc.stdout or '').strip() or '{}')
    except Exception:
        payload = {}
    if proc.returncode != 0 or not payload.get('ok', False):
        raise RuntimeError(payload.get('error') or proc.stderr.strip() or proc.stdout.strip() or 'qr 2fa failed')
    return payload

def _read_qr_worker_state(state_path: str):
    if not state_path or not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, 'r', encoding='utf-8') as f:
            return json.load(f) or {}
    except Exception:
        return {}

@app.route('/api/accounts/qr/start', methods=['POST'])
def start_qr_account_auth():
    """Старт QR-логина через отдельный worker-процесс Telethon."""
    data = request.get_json(silent=True) or {}
    api_id, api_hash = get_api_credentials(data)
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400

    tenant_id = get_current_tenant_id()
    actor = get_current_user() or {}
    actor_id = int(actor.get('id') or 0)
    _cleanup_qr_auth_sessions()

    session_name = f"qr_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    os.makedirs('sessions', exist_ok=True)
    session_path = os.path.join('sessions', session_name)
    os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)
    token = uuid.uuid4().hex
    state_path = os.path.join(app.config['RESULTS_FOLDER'], f'qr_state_{token}.json')

    try:
        worker = _qr_worker_script_path()
        cmd = [sys.executable, worker, 'start', str(int(api_id)), api_hash, session_path, state_path]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        logger.error(f"Ошибка старта QR авторизации: {e}")
        return jsonify({'error': str(e)}), 500

    qr_url = ''
    worker_state = 'pending'
    worker_message = ''
    # QR может появляться не мгновенно (сетевые задержки/нагрузка Telegram),
    # поэтому не считаем это ошибкой на старте и даем worker-у больше времени.
    for _ in range(80):  # до ~20 сек
        state = _read_qr_worker_state(state_path)
        worker_state = str(state.get('status') or 'pending').strip().lower()
        worker_message = str(state.get('message') or state.get('error') or '').strip()
        qr_url = str(state.get('qr_url') or '').strip()
        if qr_url or worker_state in ('done', '2fa_required', 'error', 'expired'):
            break
        time.sleep(0.25)

    if worker_state in ('error', 'expired'):
        try:
            os.kill(proc.pid, 15)
        except Exception:
            pass
        message = worker_message or 'Не удалось получить QR-код'
        return jsonify({'error': message}), 500

    with qr_auth_lock:
        qr_auth_sessions[token] = {
            'status': 'pending' if worker_state not in ('2fa_required',) else '2fa_required',
            'tenant_id': tenant_id,
            'actor_id': actor_id,
            'session_name': session_name,
            'qr_url': qr_url,
            'api_id': int(api_id),
            'api_hash': api_hash,
            'state_path': state_path,
            'worker_pid': int(proc.pid),
            'created_at': time.time(),
            'finished_at': None,
            'message': worker_message
        }

    return jsonify({
        'status': 'pending' if worker_state not in ('2fa_required',) else '2fa_required',
        'token': token,
        'qr_url': qr_url,
        'session_name': session_name,
        'message': worker_message
    })

@app.route('/api/accounts/qr/status/<token>', methods=['GET'])
def get_qr_account_auth_status(token):
    _cleanup_qr_auth_sessions()
    with qr_auth_lock:
        rec = qr_auth_sessions.get(token)
    if not rec:
        return jsonify({'error': 'QR-сессия не найдена или истекла'}), 404

    if rec.get('status') in ('done', '2fa_required', 'error', 'expired'):
        return jsonify({
            'status': rec.get('status'),
            'message': rec.get('message') or '',
            'phone': rec.get('phone'),
            'session_name': rec.get('session_name'),
            'qr_url': rec.get('qr_url') if rec.get('status') == 'pending' else None,
            'register_result': rec.get('register_result')
        })

    state = _read_qr_worker_state(str(rec.get('state_path') or ''))
    status = str(state.get('status') or 'pending').strip().lower()
    message = str(state.get('message') or '').strip()
    if not status:
        status = 'pending'

    if status == 'done':
        phone = str(state.get('phone') or '').strip()
        if not phone:
            status = 'error'
            message = message or 'Не удалось получить номер телефона'
        else:
            reg_result = _register_account_session(
                int(rec.get('tenant_id') or 1),
                int(rec.get('actor_id') or 0),
                phone,
                str(rec.get('session_name') or ''),
                str(state.get('first_name') or ''),
                str(state.get('last_name') or '')
            )
            with qr_auth_lock:
                current = qr_auth_sessions.get(token)
                if current:
                    current['status'] = 'done'
                    current['phone'] = phone
                    current['register_result'] = reg_result
                    current['message'] = reg_result.get('message') or 'QR авторизация завершена'
                    current['finished_at'] = time.time()
            return jsonify({
                'status': 'done',
                'phone': phone,
                'session_name': rec.get('session_name'),
                'register_result': reg_result,
                'message': reg_result.get('message') or ''
            })

    if status in ('2fa_required', 'expired', 'error'):
        if status == '2fa_required' and not message:
            message = 'Нужен пароль двухфакторной аутентификации'
        with qr_auth_lock:
            current = qr_auth_sessions.get(token)
            if current:
                current['status'] = status
                current['message'] = message
                current['finished_at'] = time.time()
        http_code = 500 if status == 'error' else 200
        return jsonify({'status': status, 'message': message}), http_code

    return jsonify({
        'status': 'pending',
        'message': message,
        'session_name': rec.get('session_name'),
        'qr_url': str(state.get('qr_url') or rec.get('qr_url') or '')
    })

@app.route('/api/accounts/qr/2fa', methods=['POST'])
def complete_qr_account_auth_2fa():
    data = request.get_json(silent=True) or {}
    token = str(data.get('token') or '').strip()
    password = str(data.get('password') or '')
    if not token:
        return jsonify({'error': 'token обязателен'}), 400
    if not password:
        return jsonify({'error': 'password обязателен'}), 400

    with qr_auth_lock:
        rec = qr_auth_sessions.get(token)
    if not rec:
        return jsonify({'error': 'QR-сессия не найдена'}), 404
    if rec.get('status') != '2fa_required':
        return jsonify({'error': 'Для этой сессии 2FA не требуется'}), 400

    try:
        session_path = os.path.join('sessions', str(rec.get('session_name') or ''))
        result_2fa = _run_qr_worker_2fa(
            int(rec.get('api_id') or 0),
            str(rec.get('api_hash') or ''),
            session_path,
            password
        )
        phone = str(result_2fa.get('phone') or '').strip()
        if not phone:
            raise RuntimeError('Не удалось получить номер после 2FA')
        result = _register_account_session(
            int(rec.get('tenant_id') or 1),
            int(rec.get('actor_id') or 0),
            phone,
            str(rec.get('session_name') or ''),
            str(result_2fa.get('first_name') or ''),
            str(result_2fa.get('last_name') or '')
        )
        with qr_auth_lock:
            current = qr_auth_sessions.get(token)
            if current is not None:
                current['status'] = 'done'
                current['phone'] = phone
                current['register_result'] = result
                current['message'] = result.get('message') or 'QR 2FA авторизация завершена'
                current['finished_at'] = time.time()
        return jsonify({
            'status': 'done',
            'phone': phone,
            'register_result': result
        })
    except Exception as e:
        logger.error(f"Ошибка 2FA для QR-сессии {token}: {e}")
        with qr_auth_lock:
            current = qr_auth_sessions.get(token)
            if current is not None:
                current['status'] = '2fa_required'
                current['message'] = f'Ошибка 2FA: {str(e)}'
        return jsonify({'error': str(e)}), 400

@app.route('/api/accounts/qr/cancel', methods=['POST'])
def cancel_qr_account_auth():
    data = request.get_json(silent=True) or {}
    token = str(data.get('token') or '').strip()
    if not token:
        return jsonify({'error': 'token обязателен'}), 400

    with qr_auth_lock:
        rec = qr_auth_sessions.pop(token, None)
    if not rec:
        return jsonify({'status': 'ok'})

    pid = int(rec.get('worker_pid') or 0)
    if pid > 0:
        try:
            os.kill(pid, 15)
        except Exception:
            pass

    state_path = str(rec.get('state_path') or '')
    if state_path and os.path.exists(state_path):
        try:
            os.remove(state_path)
        except Exception:
            pass

    # Если аккаунт не был завершен, очищаем временный session-файл.
    if rec.get('status') not in ('done',):
        session_name = str(rec.get('session_name') or '').strip()
        if session_name:
            session_file = os.path.join('sessions', f'{session_name}.session')
            session_journal = f"{session_file}-journal"
            try:
                if os.path.exists(session_file):
                    os.remove(session_file)
            except Exception:
                pass
            try:
                if os.path.exists(session_journal):
                    os.remove(session_journal)
            except Exception:
                pass

    return jsonify({'status': 'ok'})

@app.route('/api/accounts/scan', methods=['POST'])
def scan_new_sessions():
    """Сканирование новых сессий в папке sessions"""
    from fingerprint_manager import FingerprintManager
    data = request.get_json(silent=True) or {}
    api_id, api_hash = get_api_credentials(data)
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400
    
    new_accounts = []
    errors = []
    skipped = []
    tenant_id = get_current_tenant_id()
    actor = get_current_user() or {}
    actor_id = int(actor.get('id') or 0)
    
    # Получаем список файлов сессий
    session_files = []
    if os.path.exists('sessions'):
        for file in os.listdir('sessions'):
            if file.endswith('.session'):
                session_name = file[:-8]
                session_files.append(session_name)
    
    logger.info(f"Найдено сессий в папке: {session_files}")
    
    # Проверяем, какие сессии уже есть в БД
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            for session_name in session_files:
                cursor.execute('SELECT phone FROM account_stats WHERE tenant_id = ? AND session_name = ?', (tenant_id, session_name))
                existing = cursor.fetchone()
                if existing:
                    logger.info(f"Сессия {session_name} уже есть в БД с номером {existing[0]}, пропускаем")
                    skipped.append(session_name)
    except Exception as e:
        logger.error(f"Error checking existing sessions: {e}")
    
    # Сканируем новые сессии
    for session_name in session_files:
        if session_name in skipped:
            continue
            
        try:
            from telethon import TelegramClient
            
            async def get_account_meta():
                client = None
                try:
                    client = TelegramClient(f'sessions/{session_name}', int(api_id), api_hash)
                    await client.connect()
                    if await client.is_user_authorized():
                        me = await client.get_me()
                        return {
                            'phone': str(getattr(me, 'phone', '') or '').strip(),
                            'first_name': str(getattr(me, 'first_name', '') or '').strip(),
                            'last_name': str(getattr(me, 'last_name', '') or '').strip()
                        }
                    return None
                finally:
                    if client:
                        await client.disconnect()
            
            account_meta = run_async(get_account_meta())
            phone = str((account_meta or {}).get('phone') or '').strip()
            first_name = str((account_meta or {}).get('first_name') or '').strip()
            last_name = str((account_meta or {}).get('last_name') or '').strip()
            
            if phone:
                logger.info(f"Получен номер {phone} для сессии {session_name}")
                
                # Вставляем в БД
                with sqlite3.connect(db.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT OR IGNORE INTO account_stats 
                        (tenant_id, phone, session_name, account_type, is_banned, is_frozen, checked_today, last_check_date, added_by_user_id, first_name, last_name)
                        VALUES (?, ?, ?, 'checker', 0, 0, 0, ?, ?, ?, ?)
                    ''', (
                        tenant_id,
                        phone,
                        session_name,
                        date.today().isoformat(),
                        actor_id if actor_id > 0 else None,
                        first_name,
                        last_name
                    ))
                    cursor.execute(
                        '''
                        UPDATE account_stats
                        SET first_name = COALESCE(NULLIF(?, ''), first_name),
                            last_name = COALESCE(NULLIF(?, ''), last_name)
                        WHERE tenant_id = ? AND phone = ?
                        ''',
                        (first_name, last_name, tenant_id, phone)
                    )
                    conn.commit()
                
                # Создаем fingerprint
                try:
                    fingerprint_manager = FingerprintManager()
                    fingerprint = fingerprint_manager.generate_fingerprint('checker')
                    fingerprint_manager.save_fingerprint(phone, fingerprint)
                except Exception as e:
                    logger.error(f"Error creating fingerprint for {phone}: {e}")
                
                new_accounts.append(phone)
            else:
                logger.warning(f"Сессия {session_name} не авторизована или не удалось получить номер")
                errors.append(session_name)
                
        except Exception as e:
            logger.error(f"Ошибка при сканировании {session_name}: {e}")
            errors.append(session_name)
            # Небольшая задержка, чтобы избежать блокировок
            time.sleep(0.5)
            continue
    
    return jsonify({
        'new': len(new_accounts), 
        'accounts': new_accounts,
        'errors': errors,
        'skipped': skipped,
        'total_sessions': len(session_files)
    })

@app.route('/api/accounts/upload-session', methods=['POST'])
def upload_session_file():
    """Загрузка .session файла через UI и регистрация аккаунта в БД."""
    from fingerprint_manager import FingerprintManager
    tenant_id = get_current_tenant_id()
    actor = get_current_user() or {}
    actor_id = int(actor.get('id') or 0)
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не передан'}), 400

    data = request.form or {}
    api_id, api_hash = get_api_credentials(data)
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400

    file = request.files['file']
    if not file or not file.filename:
        return jsonify({'error': 'Пустой файл'}), 400

    filename = secure_filename(file.filename)
    if not filename.lower().endswith('.session'):
        return jsonify({'error': 'Поддерживаются только файлы .session'}), 400

    os.makedirs('sessions', exist_ok=True)
    session_name = filename[:-8]
    session_path = os.path.join('sessions', f'{session_name}.session')

    if os.path.exists(session_path):
        return jsonify({'error': f'Сессия {session_name} уже существует'}), 409

    try:
        file.save(session_path)
    except Exception as e:
        logger.error(f"Не удалось сохранить загруженный session-файл: {e}")
        return jsonify({'error': f'Не удалось сохранить файл: {str(e)}'}), 500

    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        helper = os.path.join(base_dir, 'tools', 'extract_session_phone.py')
        if not os.path.exists(helper):
            helper = os.path.join(base_dir, 'extract_session_phone.py')
        proc = subprocess.run(
            [sys.executable, helper, f'sessions/{session_name}', str(int(api_id)), api_hash],
            capture_output=True,
            text=True,
            timeout=45
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or 'session probe failed')
        payload = json.loads(proc.stdout.strip() or '{}')
        phone = str(payload.get('phone') or '').strip() if payload.get('ok') else ''
        first_name = str(payload.get('first_name') or '').strip()
        last_name = str(payload.get('last_name') or '').strip()

        if not phone:
            try:
                os.remove(session_path)
            except Exception:
                pass
            return jsonify({'error': 'Сессия не авторизована или не удалось получить номер'}), 400

        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT phone FROM account_stats WHERE tenant_id = ? AND phone = ?', (tenant_id, phone))
            if cursor.fetchone():
                cursor.execute(
                    '''
                    UPDATE account_stats
                    SET session_name = ?,
                        first_name = COALESCE(NULLIF(?, ''), first_name),
                        last_name = COALESCE(NULLIF(?, ''), last_name)
                    WHERE tenant_id = ? AND phone = ?
                    ''',
                    (session_name, first_name, last_name, tenant_id, phone)
                )
                conn.commit()
                return jsonify({
                    'status': 'exists',
                    'phone': phone,
                    'session_name': session_name,
                    'message': 'Аккаунт уже зарегистрирован'
                }), 200

            cursor.execute('''
                INSERT INTO account_stats
                (tenant_id, phone, session_name, account_type, is_banned, is_frozen, checked_today, last_check_date, added_by_user_id, first_name, last_name)
                VALUES (?, ?, ?, 'checker', 0, 0, 0, ?, ?, ?, ?)
            ''', (
                tenant_id,
                phone,
                session_name,
                date.today().isoformat(),
                actor_id if actor_id > 0 else None,
                first_name,
                last_name
            ))
            conn.commit()

        try:
            fingerprint_manager = FingerprintManager()
            fingerprint = fingerprint_manager.generate_fingerprint('checker')
            fingerprint_manager.save_fingerprint(phone, fingerprint)
        except Exception as e:
            logger.error(f"Error creating fingerprint for uploaded session {phone}: {e}")

        return jsonify({
            'status': 'uploaded',
            'phone': phone,
            'session_name': session_name
        })
    except Exception as e:
        logger.error(f"Ошибка обработки загруженной сессии {session_name}: {e}")
        try:
            if os.path.exists(session_path):
                os.remove(session_path)
        except Exception:
            pass
        return jsonify({'error': str(e)}), 500

async def test_account_async(phone):
    """Тестирование конкретного аккаунта"""
    from telegram_bot import TelegramAccountManager
    api_id, api_hash = get_api_credentials()
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400
    
    session_name = None
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            if is_admin_user(user):
                cursor.execute('SELECT session_name FROM account_stats WHERE tenant_id = ? AND phone = ?', (tenant_id, phone))
            else:
                cursor.execute('''
                    SELECT session_name FROM account_stats
                    WHERE tenant_id = ? AND phone = ? AND added_by_user_id = ?
                ''', (tenant_id, phone, user_id))
            row = cursor.fetchone()
            if row:
                session_name = row[0]
    except Exception as e:
        logger.error(f"Error getting session name: {e}")
        return jsonify({'error': 'Database error'}), 500
    
    if not session_name:
        return jsonify({'error': 'Account not found'}), 404
    
    try:
        from telethon import TelegramClient
        client = None
        try:
            client = TelegramClient(f'sessions/{session_name}', int(api_id), api_hash)
            await client.connect()
            
            if not await client.is_user_authorized():
                return jsonify({'working': False, 'reason': 'not_authorized'})
            
            from telegram_bot import VALID_TEST_PHONE
            from telethon.tl.functions.contacts import ImportContactsRequest, DeleteContactsRequest
            from telethon.tl.types import InputPhoneContact
            
            contact = InputPhoneContact(0, VALID_TEST_PHONE, "Test", "User")
            result = await client(ImportContactsRequest([contact]))
            imported_users = list(getattr(result, 'users', []) or [])
            working = len(imported_users) > 0

            if imported_users:
                try:
                    await client(DeleteContactsRequest(id=imported_users))
                except Exception as cleanup_error:
                    logger.warning(f"Failed to delete test contact for {phone}: {cleanup_error}")
            
            with sqlite3.connect(db.db_path) as conn:
                cursor = conn.cursor()
                quality = 'good' if working else 'bad'
                cursor.execute('''
                    UPDATE account_stats 
                    SET test_quality = ?, last_test_date = CURRENT_TIMESTAMP
                    WHERE tenant_id = ? AND phone = ?
                ''', (quality, tenant_id, phone))
                conn.commit()
            
            return jsonify({'working': working})
        finally:
            if client:
                await client.disconnect()
        
    except Exception as e:
        logger.error(f"Error testing account {phone}: {e}")
        return jsonify({'working': False, 'error': str(e)})

@app.route('/api/accounts/<path:phone>/test', methods=['POST'])
def test_account(phone):
    return run_async(test_account_async(phone))

@app.route('/api/accounts/test-all', methods=['POST'])
def test_all_accounts():
    return run_async(test_all_accounts_async())

async def test_all_accounts_async():
    """Тестирование всех аккаунтов"""
    accounts = []
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            if is_admin_user(user):
                cursor.execute('SELECT phone FROM account_stats WHERE tenant_id = ?', (tenant_id,))
            else:
                cursor.execute('SELECT phone FROM account_stats WHERE tenant_id = ? AND added_by_user_id = ?', (tenant_id, user_id))
            accounts = cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting accounts: {e}")
        return jsonify({'error': str(e)}), 500
    
    results = {'working': 0, 'failed': 0, 'total': len(accounts)}
    
    for (phone,) in accounts:
        try:
            response = await test_account_async(phone)
            # Внимание: здесь нужно получить JSON из response
            if isinstance(response, tuple):
                # Если вернулся tuple (response, status_code)
                if response[1] == 200:
                    data = response[0].json
                    if data.get('working'):
                        results['working'] += 1
                    else:
                        results['failed'] += 1
                else:
                    results['failed'] += 1
            else:
                # Если вернулся Response object
                if response.status_code == 200:
                    data = response.json
                    if data.get('working'):
                        results['working'] += 1
                    else:
                        results['failed'] += 1
                else:
                    results['failed'] += 1
        except Exception as e:
            logger.error(f"Error testing account {phone}: {e}")
            results['failed'] += 1
        
        await asyncio.sleep(2)
    
    return jsonify(results)

@app.route('/api/accounts/assign-proxy', methods=['POST'])
def assign_proxy_to_accounts():
    """Назначение прокси выбранным аккаунтам"""
    data = request.json or {}
    phones = data.get('phones', [])
    proxy_id = data.get('proxy_id')

    if not phones:
        return jsonify({'error': 'Missing phones'}), 400
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)

    if proxy_id is not None:
        try:
            proxy_id = int(proxy_id)
        except (TypeError, ValueError):
            return jsonify({'error': 'Invalid proxy_id'}), 400
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT 1 FROM proxies
                WHERE tenant_id = ? AND id = ? AND added_by_user_id = ?
                LIMIT 1
                ''',
                (tenant_id, proxy_id, user_id)
            )
            if not cursor.fetchone():
                return jsonify({'error': 'Прокси не найден или недоступен'}), 404
    
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            for phone in phones:
                cursor.execute('''
                    UPDATE account_stats
                    SET proxy_id = ?
                    WHERE tenant_id = ? AND phone = ? AND added_by_user_id = ?
                ''', (proxy_id, tenant_id, phone, user_id))
            conn.commit()
    except Exception as e:
        logger.error(f"Error assigning proxy: {e}")
        return jsonify({'error': str(e)}), 500
    
    return jsonify({'updated': len(phones)})


@app.route('/api/accounts/unassign-proxy', methods=['POST'])
def unassign_proxy_from_accounts():
    data = request.json or {}
    phones = data.get('phones', [])
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    if not phones:
        return jsonify({'error': 'Missing phones'}), 400

    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            for phone in phones:
                cursor.execute('''
                    UPDATE account_stats
                    SET proxy_id = NULL
                    WHERE tenant_id = ? AND phone = ? AND added_by_user_id = ?
                ''', (tenant_id, phone, user_id))
            conn.commit()
    except Exception as e:
        logger.error(f"Error unassigning proxy: {e}")
        return jsonify({'error': str(e)}), 500

    return jsonify({'updated': len(phones)})

@app.route('/api/config', methods=['GET'])
def get_config():
    tenant_id = get_current_tenant_id()
    form_data = db.load_form_data(tenant_id=tenant_id)
    form_data['target_chat'] = GLOBAL_TARGET_CHAT_INVITE
    return jsonify(form_data)

@app.route('/api/save_config', methods=['POST'])
def save_config():
    data = request.get_json(silent=True) or {}
    tenant_id = get_current_tenant_id()
    api_id, api_hash = get_api_credentials(data)
    db.save_form_data(
        api_id=api_id,
        api_hash=api_hash,
        target_chat=GLOBAL_TARGET_CHAT_INVITE,
        phones_text=data.get('phones_text', ''),
        delay=1,
        tenant_id=tenant_id
    )
    db.set_config('api_id', api_id, tenant_id=tenant_id)
    db.set_config('api_hash', api_hash, tenant_id=tenant_id)
    db.set_config('outreach_target_chat', GLOBAL_TARGET_CHAT_INVITE, tenant_id=tenant_id)
    return jsonify({'status': 'saved'})

@app.route('/api/history', methods=['GET'])
def get_history():
    limit = request.args.get('limit', 100, type=int)
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    history = db.get_history(limit, tenant_id=tenant_id)
    if not is_admin_user(user):
        user_id = int(user.get('id') or 0)
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT phone FROM account_stats WHERE tenant_id = ? AND added_by_user_id = ?',
                (tenant_id, user_id)
            )
            own_phones = {row[0] for row in cursor.fetchall()}
        history = [h for h in history if (h.get('account_used') in own_phones)]
    return jsonify(history)

@app.route('/api/phone_stats', methods=['GET'])
def get_phone_stats():
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        if is_admin_user(user):
            cursor.execute('SELECT COUNT(*) FROM check_history WHERE tenant_id = ? AND registered = 1', (tenant_id,))
            found = int(cursor.fetchone()[0] or 0)
            cursor.execute('SELECT COUNT(*) FROM check_history WHERE tenant_id = ? AND registered = 0', (tenant_id,))
            not_found = int(cursor.fetchone()[0] or 0)
            cursor.execute('SELECT COUNT(*) FROM check_history WHERE tenant_id = ?', (tenant_id,))
            total = int(cursor.fetchone()[0] or 0)
        else:
            user_id = int(user.get('id') or 0)
            cursor.execute(
                'SELECT phone FROM account_stats WHERE tenant_id = ? AND added_by_user_id = ?',
                (tenant_id, user_id)
            )
            own_phones = [row[0] for row in cursor.fetchall()]
            if not own_phones:
                return jsonify({'found': 0, 'not_found': 0, 'total': 0})
            placeholders = ",".join("?" for _ in own_phones)
            cursor.execute(
                f'SELECT COUNT(*) FROM check_history WHERE tenant_id = ? AND account_used IN ({placeholders}) AND registered = 1',
                [tenant_id, *own_phones]
            )
            found = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                f'SELECT COUNT(*) FROM check_history WHERE tenant_id = ? AND account_used IN ({placeholders}) AND registered = 0',
                [tenant_id, *own_phones]
            )
            not_found = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                f'SELECT COUNT(*) FROM check_history WHERE tenant_id = ? AND account_used IN ({placeholders})',
                [tenant_id, *own_phones]
            )
            total = int(cursor.fetchone()[0] or 0)
    return jsonify({
        'found': found,
        'not_found': not_found,
        'total': total
    })

@app.route('/api/join_chat', methods=['POST'])
def join_chat():
    return run_async(join_chat_async())

async def join_chat_async():
    data = request.get_json(silent=True) or {}
    chat_username = data.get('chat_username')
    tenant_id = get_current_tenant_id()
    api_id, api_hash = get_api_credentials(data)
    
    if not chat_username:
        return jsonify({'error': 'Не указан username чата'}), 400
    
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400
    
    manager = TelegramAccountManager(api_id, api_hash, 'sessions', tenant_id=tenant_id)
    
    if not await manager.load_accounts():
        return jsonify({'error': 'Не удалось загрузить аккаунты'}), 400
    
    results = await manager.ensure_all_accounts_in_chat(chat_username)
    
    await manager.close_all()
    
    return jsonify(results)

@app.route('/api/start', methods=['POST'])
def start_checker():
    global checker_process, checker_status
    ensure_checker_emitter_started()
    _refresh_checker_running_flag()
    if checker_status['running']:
        return jsonify({'error': 'Проверка уже запущена'}), 400
    
    data = request.get_json(silent=True) or {}
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    api_id, api_hash = get_api_credentials(data)
    target_chat = GLOBAL_TARGET_CHAT_INVITE
    phones_text = data.get('phones')
    # Для checker фиксируем безопасный постоянный темп.
    delay = 1
    save_to_base_raw = data.get('save_to_base')
    save_to_base = save_to_base_raw is True or str(save_to_base_raw).strip().lower() in {'1', 'true', 'yes', 'on'}
    requested_base_id = data.get('base_id')
    requested_new_base_name = str(data.get('new_base_name') or '').strip()
    # Если явно передан base_id — считаем, что пользователь хочет обогащать базу,
    # даже если фронт не передал/сбросил save_to_base флаг.
    if requested_base_id not in (None, '', 0, '0'):
        save_to_base = True
    raw_phone_metadata = data.get('phone_metadata') or {}
    phone_metadata = {}
    if isinstance(raw_phone_metadata, dict):
        for key, value in raw_phone_metadata.items():
            norm_key = _normalize_phone_value(key)
            if not norm_key:
                continue
            if isinstance(value, dict):
                phone_metadata[norm_key] = {
                    'name': str(value.get('name') or '').strip() or None,
                    'company': str(value.get('company') or '').strip() or None,
                    'position': str(value.get('position') or '').strip() or None
                }
            else:
                phone_metadata[norm_key] = {}
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400

    if not phones_text:
        return jsonify({'error': 'Нет номеров для проверки'}), 400
    
    phones_list = [p.strip() for p in phones_text.split('\n') if p.strip()]
    
    if not phones_list:
        return jsonify({'error': 'Нет номеров для проверки'}), 400

    target_base_id = None
    if save_to_base:
        if requested_base_id:
            try:
                target_base_id = int(requested_base_id)
            except Exception:
                return jsonify({'error': 'Некорректный base_id'}), 400
            if not get_base_for_current_user(target_base_id):
                return jsonify({'error': 'База недоступна'}), 403
        else:
            base_name = requested_new_base_name or f"Проверка {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            target_base_id = outreach.create_base(
                name=base_name,
                tenant_id=tenant_id,
                created_by_user_id=user_id
            )
    logger.info(
        "Checker start: tenant=%s user=%s phones=%s save_to_base=%s requested_base_id=%s target_base_id=%s new_base_name=%s",
        tenant_id, user_id, len(phones_list), save_to_base, requested_base_id, target_base_id, requested_new_base_name
    )
    
    db.save_form_data(api_id, api_hash, target_chat, phones_text, delay, tenant_id=tenant_id)
    db.set_config('api_id', api_id, tenant_id=tenant_id)
    db.set_config('api_hash', api_hash, tenant_id=tenant_id)
    db.set_config('outreach_target_chat', target_chat, tenant_id=tenant_id)
    db.set_config('checker_last_files', '[]', tenant_id=tenant_id)
    
    checker_status = {
        'running': True,
        'current_phone': '',
        'processed': 0,
        'total': len(phones_list),
        'input_total': len(phones_list),
        'unique_total': len(phones_list),
        'skipped_duplicates': 0,
        'skipped_existing_in_base': 0,
        'skipped_history_registered': 0,
        'skipped_history_not_registered': 0,
        'skipped_history_total': 0,
        'registered': 0,
        'not_registered': 0,
        'errors': 0,
        'test_checks': 0,
        'accounts': [],
        'start_time': None,
        'estimated_time': None,
        'recent_results': []
    }
    
    checker_process = TelegramCheckerThread(
        api_id=api_id,
        api_hash=api_hash,
        target_chat=target_chat,
        phones_list=phones_list,
        delay=delay,
        tenant_id=tenant_id,
        target_base_id=target_base_id,
        phone_metadata=phone_metadata
    )
    checker_process.start()
    
    response = {'status': 'started', 'total': len(phones_list)}
    if target_base_id:
        response['base_id'] = target_base_id
    return jsonify(response)

@app.route('/api/stop', methods=['POST'])
def stop_checker():
    global checker_status
    checker_status['running'] = False
    return jsonify({'status': 'stopping'})

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'Нет файла'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Нет выбранного файла'}), 400
    
    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        phones = []
        phone_metadata = {}
        parsed_rows = 0
        detected_columns = {}
        try:
            phones, phone_metadata, parsed_rows, detected_columns = _parse_checker_file(filepath)
        except Exception as e:
            return jsonify({'error': str(e)}), 400
        finally:
            try:
                os.remove(filepath)
            except Exception:
                pass

        return jsonify({
            'phones': phones,
            'phone_metadata': phone_metadata,
            'total': len(phones),
            'parsed_rows': parsed_rows,
            'detected_columns': detected_columns
        })


@app.route('/api/files', methods=['GET'])
def list_checker_files():
    tenant_id = get_current_tenant_id()
    scope = str(request.args.get('scope') or 'checker_last').strip().lower()
    if scope != 'checker_last':
        return jsonify([])

    items = []
    try:
        raw = db.get_config('checker_last_files', default='[]', tenant_id=tenant_id)
        parsed = json.loads(raw) if raw else []
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        items = []

    safe_items = []
    for item in items:
        if not isinstance(item, dict):
            continue
        filename = os.path.basename(str(item.get('filename') or '').strip())
        if not filename:
            continue
        full_path = os.path.join(app.config['RESULTS_FOLDER'], filename)
        if not os.path.exists(full_path):
            continue
        safe_items.append({
            'filename': filename,
            'type': str(item.get('type') or 'file'),
            'created_at': item.get('created_at')
        })

    # Fallback для старых запусков (до персистентного сохранения списка файлов).
    if not safe_items:
        try:
            all_files = []
            for name in os.listdir(app.config['RESULTS_FOLDER']):
                if not (
                    name.startswith('registered_')
                    or name.startswith('not_registered_')
                    or name.startswith('errors_')
                ):
                    continue
                full_path = os.path.join(app.config['RESULTS_FOLDER'], name)
                if not os.path.isfile(full_path):
                    continue
                mtime = os.path.getmtime(full_path)
                all_files.append((name, mtime))

            if all_files:
                all_files.sort(key=lambda x: x[1], reverse=True)
                latest_mtime = all_files[0][1]
                for name, mtime in all_files:
                    # Берем файлы самого свежего запуска (окно 5 секунд).
                    if latest_mtime - mtime > 5:
                        continue
                    ftype = 'file'
                    if name.startswith('registered_'):
                        ftype = 'registered'
                    elif name.startswith('not_registered_'):
                        ftype = 'not_registered'
                    elif name.startswith('errors_'):
                        ftype = 'errors'
                    safe_items.append({
                        'filename': name,
                        'type': ftype,
                        'created_at': datetime.fromtimestamp(mtime).isoformat()
                    })
        except Exception:
            pass

    return jsonify(safe_items)


@app.route('/api/download/<filename>')
def download_file(filename):
    return send_file(
        os.path.join(app.config['RESULTS_FOLDER'], filename),
        as_attachment=True
    )


# ===== ЭНДПОИНТЫ ДЛЯ ПРОКСИ =====

@app.route('/api/proxies', methods=['GET'])
def get_proxies():
    """Получение списка прокси"""
    try:
        tenant_id = get_current_tenant_id()
        user = get_current_user() or {}
        user_id = int(user.get('id') or 0)
        if user_id <= 0:
            return jsonify({'error': 'Unauthorized'}), 401
        proxies = []
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, ip, port, protocol, username, password, country, is_active, last_used, created_at
                FROM proxies
                WHERE tenant_id = ? AND added_by_user_id = ?
                ORDER BY created_at DESC
            ''', (tenant_id, user_id))
            for row in cursor.fetchall():
                proxy = {
                    'id': row[0], 'ip': row[1], 'port': row[2], 'protocol': row[3],
                    'username': row[4], 'password': row[5], 'country': row[6],
                    'is_active': bool(row[7]), 'last_used': row[8], 'created_at': row[9]
                }
                cursor.execute(
                    '''
                    SELECT phone FROM account_stats
                    WHERE tenant_id = ? AND added_by_user_id = ? AND proxy_id = ?
                    LIMIT 1
                    ''',
                    (tenant_id, user_id, proxy['id'])
                )
                account = cursor.fetchone()
                proxy['assigned_to'] = account[0] if account else None
                proxies.append(proxy)
        
        return jsonify(proxies)
    except Exception as e:
        logger.error(f"Error getting proxies: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies', methods=['POST'])
def add_proxy():
    """Добавление нового прокси"""
    data = request.get_json(silent=True) or {}
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    if user_id <= 0:
        return jsonify({'error': 'Unauthorized'}), 401
    try:
        ip = str(data.get('ip') or '').strip()
        port_raw = data.get('port')
        try:
            port = int(port_raw)
        except Exception:
            port = None
        protocol = (data.get('protocol') or 'socks5').strip().lower()
        if not ip or not port:
            return jsonify({'error': 'Укажите IP и порт'}), 400
        if port <= 0:
            return jsonify({'error': 'Неверный порт'}), 400
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id FROM proxies
                WHERE tenant_id = ? AND added_by_user_id = ? AND ip = ? AND port = ? AND protocol = ?
            ''', (tenant_id, user_id, ip, port, protocol))
            if cursor.fetchone():
                return jsonify({'error': 'Прокси уже существует'}), 400
            cursor.execute('''
                INSERT INTO proxies (tenant_id, added_by_user_id, ip, port, protocol, username, password, country, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            ''', (
                tenant_id,
                user_id,
                ip,
                port,
                protocol,
                data.get('username'),
                data.get('password'),
                data.get('country')
            ))
            conn.commit()
            return jsonify({'id': cursor.lastrowid, 'status': 'added'})
    except Exception as e:
        logger.error(f"Error adding proxy: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/<int:proxy_id>', methods=['PUT'])
def update_proxy(proxy_id):
    """Обновление прокси"""
    data = request.json
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    if user_id <= 0:
        return jsonify({'error': 'Unauthorized'}), 401
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE proxies 
                SET ip=?, port=?, protocol=?, username=?, password=?, country=?
                WHERE id=? AND tenant_id = ? AND added_by_user_id = ?
            ''', (
                data['ip'], data['port'], data.get('protocol', 'socks5'),
                data.get('username'), data.get('password'), data.get('country'),
                proxy_id, tenant_id, user_id
            ))
            if cursor.rowcount <= 0:
                return jsonify({'error': 'Прокси не найден или недоступен'}), 404
            conn.commit()
        return jsonify({'status': 'updated'})
    except Exception as e:
        logger.error(f"Error updating proxy: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/<int:proxy_id>', methods=['DELETE'])
def delete_proxy(proxy_id):
    """Удаление прокси"""
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    if user_id <= 0:
        return jsonify({'error': 'Unauthorized'}), 401
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                UPDATE account_stats
                SET proxy_id = NULL
                WHERE tenant_id = ? AND added_by_user_id = ? AND proxy_id = ?
                ''',
                (tenant_id, user_id, proxy_id)
            )
            cursor.execute(
                '''
                DELETE FROM proxies
                WHERE tenant_id = ? AND id = ? AND added_by_user_id = ?
                ''',
                (tenant_id, proxy_id, user_id)
            )
            if cursor.rowcount <= 0:
                return jsonify({'error': 'Прокси не найден или недоступен'}), 404
            conn.commit()
        return jsonify({'status': 'deleted'})
    except Exception as e:
        logger.error(f"Error deleting proxy: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/<int:proxy_id>/test', methods=['POST'])
def test_proxy(proxy_id):
    return run_async(test_proxy_async(proxy_id))

async def test_proxy_async(proxy_id):
    """Тестирование прокси"""
    try:
        tenant_id = get_current_tenant_id()
        user = get_current_user() or {}
        user_id = int(user.get('id') or 0)
        if user_id <= 0:
            return jsonify({'error': 'Unauthorized'}), 401
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT 1 FROM proxies
                WHERE tenant_id = ? AND id = ? AND added_by_user_id = ?
                ''',
                (tenant_id, proxy_id, user_id)
            )
            if not cursor.fetchone():
                return jsonify({'error': 'Proxy not found'}), 404
        working = await proxy_manager.test_proxy(proxy_id)
        return jsonify({'working': working})
    except Exception as e:
        logger.error(f"Error testing proxy: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/test-all', methods=['POST'])
def test_all_proxies():
    return run_async(test_all_proxies_async())

async def test_all_proxies_async():
    """Тестирование всех прокси"""
    try:
        tenant_id = get_current_tenant_id()
        user = get_current_user() or {}
        user_id = int(user.get('id') or 0)
        if user_id <= 0:
            return jsonify({'error': 'Unauthorized'}), 401
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT id FROM proxies
                WHERE tenant_id = ? AND added_by_user_id = ?
                ''',
                (tenant_id, user_id)
            )
            proxy_ids = [int(r[0]) for r in cursor.fetchall()]
        results = {'working': 0, 'failed': 0, 'total': len(proxy_ids)}
        for pid in proxy_ids:
            ok = await proxy_manager.test_proxy(pid)
            if ok:
                results['working'] += 1
            else:
                results['failed'] += 1
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error testing all proxies: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/import', methods=['POST'])
def import_proxies():
    """Импорт прокси из файла"""
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    if user_id <= 0:
        return jsonify({'error': 'Unauthorized'}), 401
    if 'file' not in request.files:
        return jsonify({'error': 'No file'}), 400
    
    file = request.files['file']
    try:
        content = file.read().decode('utf-8')
    except Exception as e:
        return jsonify({'error': f'Error reading file: {e}'}), 400
    
    proxies = []
    for line in content.split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        parts = line.split(':')
        if len(parts) >= 2:
            proxy = {
                'ip': parts[0],
                'port': int(parts[1]),
                'protocol': parts[2] if len(parts) > 2 else 'socks5',
                'username': parts[3] if len(parts) > 3 else None,
                'password': parts[4] if len(parts) > 4 else None,
                'country': parts[5] if len(parts) > 5 else None
            }
            proxies.append(proxy)
    
    try:
        added = 0
        skipped = 0
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            for p in proxies:
                cursor.execute(
                    '''
                    SELECT id FROM proxies
                    WHERE tenant_id = ? AND added_by_user_id = ? AND ip = ? AND port = ? AND protocol = ?
                    ''',
                    (tenant_id, user_id, p['ip'], p['port'], p.get('protocol', 'socks5'))
                )
                if cursor.fetchone():
                    skipped += 1
                    continue
                cursor.execute('''
                    INSERT INTO proxies (tenant_id, added_by_user_id, ip, port, protocol, username, password, country, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                ''', (tenant_id, user_id, p['ip'], p['port'], p.get('protocol', 'socks5'), p.get('username'), p.get('password'), p.get('country')))
                added += 1
            conn.commit()
        return jsonify({'added': added, 'skipped': skipped, 'total': len(proxies)})
    except Exception as e:
        logger.error(f"Error importing proxies: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/delete-inactive', methods=['DELETE'])
def delete_inactive_proxies():
    """Удаление всех неактивных прокси"""
    try:
        tenant_id = get_current_tenant_id()
        user = get_current_user() or {}
        user_id = int(user.get('id') or 0)
        if user_id <= 0:
            return jsonify({'error': 'Unauthorized'}), 401
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                DELETE FROM proxies
                WHERE tenant_id = ? AND added_by_user_id = ? AND is_active = FALSE
                ''',
                (tenant_id, user_id)
            )
            deleted = cursor.rowcount
            conn.commit()
        return jsonify({'deleted': deleted})
    except Exception as e:
        logger.error(f"Error deleting inactive proxies: {e}")
        return jsonify({'error': str(e)}), 500


# ===== ЭНДПОИНТЫ ДЛЯ АУТРИЧА =====

@app.route('/api/bases', methods=['GET'])
def get_bases():
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    rows = outreach.get_bases(tenant_id=tenant_id)
    if is_admin_user(user):
        return jsonify(rows)
    uid = int(user.get('id') or 0)
    return jsonify([b for b in rows if int(b.get('created_by_user_id') or 0) == uid])

@app.route('/api/bases', methods=['POST'])
def create_base():
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    data = request.get_json(silent=True) or {}
    name = str(data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Название базы обязательно'}), 400
    base_id = outreach.create_base(
        name=name,
        tenant_id=tenant_id,
        created_by_user_id=int(user.get('id') or 0) if not is_admin_user(user) else int(user.get('id') or 0)
    )
    return jsonify({'id': base_id, 'status': 'created'})

@app.route('/api/bases/<int:base_id>', methods=['GET'])
def get_base(base_id: int):
    base = get_base_for_current_user(base_id)
    if not base:
        return jsonify({'error': 'Base not found'}), 404
    return jsonify(base)

@app.route('/api/bases/<int:base_id>', methods=['DELETE'])
def delete_base(base_id: int):
    base = get_base_for_current_user(base_id)
    if not base:
        return jsonify({'error': 'Base not found'}), 404
    tenant_id = get_current_tenant_id()
    outreach.delete_base(base_id=base_id, tenant_id=tenant_id)
    return jsonify({'status': 'deleted', 'base_id': base_id})

@app.route('/api/bases/<int:base_id>/contacts', methods=['GET'])
def get_base_contacts(base_id: int):
    base = get_base_for_current_user(base_id)
    if not base:
        return jsonify({'error': 'Base not found'}), 404
    tenant_id = get_current_tenant_id()
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    return jsonify(outreach.get_base_contacts(base_id=base_id, limit=limit, offset=offset, tenant_id=tenant_id))

@app.route('/api/bases/<int:base_id>/export', methods=['GET'])
def export_base_contacts(base_id: int):
    base = get_base_for_current_user(base_id)
    if not base:
        return jsonify({'error': 'Base not found'}), 404
    tenant_id = get_current_tenant_id()
    fmt = str(request.args.get('format') or 'csv').strip().lower()
    if fmt not in ('csv', 'xlsx'):
        return jsonify({'error': 'format must be csv or xlsx'}), 400

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, username, phone, name, company, position, access_hash, created_at
            FROM outreach_base_contacts
            WHERE tenant_id = ? AND base_id = ?
            ORDER BY id ASC
        ''', (tenant_id, base_id))
        rows = cursor.fetchall()

    columns = ['user_id', 'username', 'phone', 'name', 'company', 'position', 'access_hash', 'created_at']
    df = pd.DataFrame(rows, columns=columns)

    safe_name = secure_filename(str(base.get('name') or f'base_{base_id}')) or f'base_{base_id}'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if fmt == 'xlsx':
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='contacts')
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'{safe_name}_{timestamp}.xlsx'
        )

    csv_data = df.to_csv(index=False)
    return Response(
        csv_data,
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename={safe_name}_{timestamp}.csv'}
    )

@app.route('/api/bases/upload', methods=['POST'])
def upload_base_contacts():
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    if 'file' not in request.files:
        return jsonify({'error': 'No file'}), 400
    file = request.files['file']
    base_name = str(request.form.get('base_name') or '').strip()
    if not base_name:
        return jsonify({'error': 'base_name is required'}), 400

    filename = secure_filename(file.filename or 'contacts.csv')
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    try:
        validation = outreach.validate_contacts_file(filepath)
        if not validation.get('ok'):
            return jsonify({'error': validation.get('error') or 'Файл не прошёл валидацию'}), 400
        base_id = outreach.create_base(
            name=base_name,
            tenant_id=tenant_id,
            created_by_user_id=int(user.get('id') or 0)
        )
        result = outreach.import_contacts_to_base(base_id=base_id, file_path=filepath, tenant_id=tenant_id)
        result['base_id'] = base_id
        result['validation'] = validation
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

def _get_service_session_for_parsing(tenant_id: int):
    api_id, api_hash = get_api_credentials()
    if not api_id or not api_hash:
        raise RuntimeError('API credentials are not configured')

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT phone, session_name
            FROM account_stats
            WHERE tenant_id = ?
              AND account_type = 'service'
              AND COALESCE(is_banned, 0) = 0
              AND COALESCE(is_frozen, 0) = 0
            ORDER BY rowid DESC
            LIMIT 1
        ''', (tenant_id,))
        row = cursor.fetchone()
    if not row:
        raise RuntimeError('Нет доступных service-сессий для парсинга')

    session_name = row[1] or row[0]
    return int(api_id), api_hash, session_name

def _parse_chat_members_with_service(tenant_id: int, chat_ref: str, mode: str = 'auto', messages_limit: int = 3000):
    api_id, api_hash, session_name = _get_service_session_for_parsing(tenant_id)
    script_path = os.path.abspath(os.path.join('tools', 'parse_chat_members.py'))
    if not os.path.exists(script_path):
        raise RuntimeError(f'Parser script not found: {script_path}')

    session_path = os.path.abspath(os.path.join('sessions', session_name))
    cmd = [
        sys.executable,
        script_path,
        '--session', session_path,
        '--api-id', str(api_id),
        '--api-hash', api_hash,
        '--chat', chat_ref,
        '--mode', str(mode or 'auto'),
        '--messages-limit', str(int(messages_limit or 3000))
    ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=300
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or '').strip()
        raise RuntimeError(err or 'Chat parse failed')
    try:
        data = json.loads((proc.stdout or '[]').strip() or '[]')
    except Exception as e:
        raise RuntimeError(f'Некорректный ответ парсера: {e}') from e
    if not isinstance(data, list):
        raise RuntimeError('Некорректный формат данных парсера')
    return data


def _build_dork_queries(position: str, language: str, sources: list[str]) -> list[tuple[str, str]]:
    role = position.strip()
    lang = (language or 'RU').strip().upper()
    selected_sources = [str(s or '').strip().lower() for s in (sources or [])]
    out: list[tuple[str, str]] = []

    telegram_templates = [
        '"Send message" {role} site:t.me',
        '"Send message" "{role}" site:t.me',
        '"Send message" "{role} в" site:t.me',
        '"Send message" "{role} in" site:t.me',
        '"Send message" "{role} at" site:t.me',
        '"Отправить сообщение" "{role}" site:t.me'
    ]
    linkedin_templates = [
        '"{role}" "telegram: @" site:linkedin.com',
        '"{role}" "t.me/" site:linkedin.com',
        '"{role}" "Telegram" site:linkedin.com',
        '"{role} in" "telegram" site:linkedin.com',
        '"{role} at" "telegram" site:linkedin.com'
    ]
    tenchat_templates = [
        '"{role}" "telegram" site:tenchat.ru',
        '"{role}" "t.me/" site:tenchat.ru',
        '"{role}" "telegram: @" site:tenchat.ru',
        '"{role} в" "telegram" site:tenchat.ru',
        '"{role} in" "telegram" site:tenchat.ru',
        '"{role} at" "telegram" site:tenchat.ru'
    ]

    per_source = {
        'telegram': telegram_templates,
        'linkedin': linkedin_templates,
        'tenchat': tenchat_templates
    }

    # Сохраняем "полный" пул комбинаций, но чуть поджимаем по языку.
    def _allow(q: str) -> bool:
        if lang == 'RU':
            return (' в"' in q) or ('Отправить сообщение' in q) or ('Send message' in q)
        if lang == 'ENG':
            return (' in"' in q) or (' at"' in q) or ('Send message' in q) or ('telegram' in q.lower())
        return True

    for source in selected_sources:
        for tpl in per_source.get(source, []):
            q = tpl.format(role=role)
            if _allow(q):
                out.append((source, q))

    # Дедуп по query
    seen = set()
    uniq = []
    for source, q in out:
        key = q.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append((source, q))
    return uniq


_TG_USERNAME_RE = re.compile(r'(?<![A-Za-z0-9_])@([A-Za-z][A-Za-z0-9_]{4,31})(?![A-Za-z0-9_])')
_TG_URL_RE = re.compile(r'(?:https?://)?(?:t(?:elegram)?\.me)/([A-Za-z][A-Za-z0-9_]{4,31})', re.IGNORECASE)


def _extract_usernames_from_result(link: str, title: str, snippet: str) -> list[str]:
    text = ' '.join([link or '', title or '', snippet or ''])
    candidates = set()
    for m in _TG_URL_RE.findall(text):
        u = (m or '').strip()
        if u and u.lower() not in {'joinchat', 'share', 's', 'c'}:
            candidates.add(u)
    for m in _TG_USERNAME_RE.findall(text):
        u = (m or '').strip()
        if u:
            candidates.add(u)
    return sorted(candidates)


def _sync_campaign_from_base_safe(campaign_id: int, base_id, tenant_id: int, reset: bool = False):
    if not base_id:
        return {'imported': 0, 'skipped': 0}
    try:
        return outreach.sync_campaign_contacts_from_base(
            campaign_id=campaign_id,
            base_id=int(base_id),
            tenant_id=tenant_id,
            reset=reset
        )
    except Exception as e:
        logger.error(
            "Failed to sync campaign %s from base %s (tenant=%s, reset=%s): %s",
            campaign_id, base_id, tenant_id, reset, e
        )
        return {'imported': 0, 'skipped': 0, 'error': str(e)}

@app.route('/api/bases/parse-chat', methods=['POST'])
def parse_chat_to_base():
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    data = request.get_json(silent=True) or {}
    chat_ref = str(data.get('chat') or '').strip()
    base_name = str(data.get('base_name') or '').strip()
    mode = str(data.get('mode') or 'auto').strip().lower()
    try:
        messages_limit = int(data.get('messages_limit') or 3000)
    except Exception:
        return jsonify({'error': 'messages_limit must be integer'}), 400
    if mode not in ('auto', 'participants', 'authors'):
        return jsonify({'error': 'mode must be auto|participants|authors'}), 400
    messages_limit = max(100, min(10000, messages_limit))
    if not chat_ref:
        return jsonify({'error': 'chat is required'}), 400
    if not base_name:
        return jsonify({'error': 'base_name is required'}), 400
    try:
        contacts = _parse_chat_members_with_service(tenant_id, chat_ref, mode=mode, messages_limit=messages_limit)
        if not contacts:
            return jsonify({'error': 'Не удалось собрать контакты из чата'}), 400
        base_id = outreach.create_base(name=base_name, tenant_id=tenant_id, created_by_user_id=int(user.get('id') or 0))
        result = outreach.add_base_contacts(base_id=base_id, contacts=contacts, tenant_id=tenant_id, source_file=f'chat:{chat_ref}')
        result['base_id'] = base_id
        result['mode'] = mode
        result['messages_limit'] = messages_limit
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/bases/dorks/search', methods=['POST'])
def collect_base_by_dorks():
    user = get_current_user() or {}
    if not is_admin_user(user):
        return jsonify({'error': 'Функция временно доступна только admin'}), 403

    tenant_id = get_current_tenant_id()
    data = request.get_json(silent=True) or {}
    position = str(data.get('position') or '').strip()
    language = str(data.get('language') or 'RU').strip().upper()
    sources = data.get('sources') or []
    base_name = str(data.get('base_name') or '').strip()
    try:
        results_limit = int(data.get('results_limit') or 300)
    except Exception:
        return jsonify({'error': 'results_limit must be integer'}), 400

    if not position:
        return jsonify({'error': 'position is required'}), 400
    if language not in ('RU', 'ENG'):
        return jsonify({'error': 'language must be RU or ENG'}), 400
    if not isinstance(sources, list) or not sources:
        return jsonify({'error': 'sources must be non-empty list'}), 400
    if not base_name:
        base_name = f'Dorks: {position}'
    results_limit = max(50, min(2000, results_limit))

    serper_api_key = os.getenv('SERPER_API_KEY', '').strip()
    if not serper_api_key:
        return jsonify({'error': 'SERPER_API_KEY is not configured on server'}), 400

    queries = _build_dork_queries(position=position, language=language, sources=sources)
    if not queries:
        return jsonify({'error': 'Нет валидных комбинаций запросов'}), 400

    per_query_num = 20
    found_candidates = []
    requests_ok = 0
    requests_failed = 0
    query_stats = []

    headers = {
        'X-API-KEY': serper_api_key,
        'Content-Type': 'application/json'
    }
    for source, query in queries:
        try:
            response = requests.post(
                'https://google.serper.dev/search',
                headers=headers,
                json={'q': query, 'num': per_query_num},
                timeout=25
            )
            if response.status_code != 200:
                requests_failed += 1
                query_stats.append({
                    'source': source,
                    'query': query,
                    'status': 'error',
                    'http_status': int(response.status_code),
                    'result_items': 0,
                    'usernames_found': 0,
                    'error_text': f'http_{response.status_code}'
                })
                continue
            payload = response.json() if response.content else {}
            organic = payload.get('organic') or []
            requests_ok += 1
            usernames_found_for_query = 0
            for item in organic:
                link = str(item.get('link') or '').strip()
                title = str(item.get('title') or '').strip()
                snippet = str(item.get('snippet') or '').strip()
                usernames = _extract_usernames_from_result(link=link, title=title, snippet=snippet)
                for u in usernames:
                    usernames_found_for_query += 1
                    found_candidates.append({
                        'username': u,
                        'name': None,
                        'position': position,
                        'company': source,
                        'source_url': link,
                        'matched_query': query
                    })
            query_stats.append({
                'source': source,
                'query': query,
                'status': 'ok',
                'http_status': int(response.status_code),
                'result_items': len(organic),
                'usernames_found': usernames_found_for_query,
                'error_text': None
            })
        except Exception:
            requests_failed += 1
            query_stats.append({
                'source': source,
                'query': query,
                'status': 'error',
                'http_status': None,
                'result_items': 0,
                'usernames_found': 0,
                'error_text': 'request_exception'
            })

    # Дедуп по username.
    unique_by_username = {}
    for item in found_candidates:
        uname = (item.get('username') or '').strip().lstrip('@')
        if not uname:
            continue
        key = uname.lower()
        if key not in unique_by_username:
            unique_by_username[key] = item
    deduped = list(unique_by_username.values())[:results_limit]

    base_id = None
    imported = 0
    skipped = 0
    status = 'ok'
    error_text = None

    if deduped:
        base_id = outreach.create_base(
            name=base_name,
            tenant_id=tenant_id,
            created_by_user_id=int(user.get('id') or 0)
        )
        contacts = [{
            'phone': None,
            'username': d['username'],
            'access_hash': None,
            'name': d.get('name'),
            'company': d.get('company'),
            'position': d.get('position'),
            'user_id': None
        } for d in deduped]
        result = outreach.add_base_contacts(
            base_id=base_id,
            contacts=contacts,
            tenant_id=tenant_id,
            source_file=f'dorks:{position}'
        )
        imported = int(result.get('imported', 0) or 0)
        skipped = int(result.get('skipped', 0) or 0)
    else:
        status = 'empty'
        error_text = 'По запросам ничего не найдено'

    # Временный аудит dorks-запусков для настройки.
    run_id = None
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO dorks_runs (
                tenant_id, user_id, position, language, sources_json, base_name, results_limit,
                queries_total, requests_ok, requests_failed, found_total, dedup_total,
                imported, skipped, status, error_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            tenant_id, int(user.get('id') or 0), position, language, json.dumps(sources, ensure_ascii=False),
            base_name, results_limit, len(queries), requests_ok, requests_failed, len(found_candidates),
            len(deduped), imported, skipped, status, error_text
        ))
        run_id = int(cursor.lastrowid)
        for q in query_stats:
            cursor.execute('''
                INSERT INTO dorks_run_queries (
                    run_id, tenant_id, source, query_text, status, http_status, result_items, usernames_found, error_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                run_id, tenant_id, str(q.get('source') or ''), str(q.get('query') or ''),
                str(q.get('status') or 'ok'), q.get('http_status'),
                int(q.get('result_items') or 0), int(q.get('usernames_found') or 0),
                q.get('error_text')
            ))
        conn.commit()

    resp = {
        'run_id': run_id,
        'base_id': base_id,
        'queries_total': len(queries),
        'requests_ok': requests_ok,
        'requests_failed': requests_failed,
        'found_total': len(found_candidates),
        'dedup_total': len(deduped),
        'imported': imported,
        'skipped': skipped,
        'query_stats': query_stats
    }
    if status == 'empty':
        return jsonify({'error': error_text, **resp}), 400
    return jsonify(resp)


@app.route('/api/bases/dorks/runs', methods=['GET'])
def get_dorks_runs():
    user = get_current_user() or {}
    if not is_admin_user(user):
        return jsonify({'error': 'Forbidden'}), 403
    tenant_id = get_current_tenant_id()
    limit = request.args.get('limit', 20, type=int)
    limit = max(1, min(200, int(limit or 20)))

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, position, language, sources_json, base_name, results_limit,
                   queries_total, requests_ok, requests_failed, found_total, dedup_total,
                   imported, skipped, status, error_text, created_at
            FROM dorks_runs
            WHERE tenant_id = ?
            ORDER BY id DESC
            LIMIT ?
        ''', (tenant_id, limit))
        rows = cursor.fetchall()

    result = []
    for r in rows:
        try:
            sources = json.loads(r[3]) if r[3] else []
        except Exception:
            sources = []
        result.append({
            'id': int(r[0]),
            'position': r[1],
            'language': r[2],
            'sources': sources,
            'base_name': r[4],
            'results_limit': int(r[5] or 0),
            'queries_total': int(r[6] or 0),
            'requests_ok': int(r[7] or 0),
            'requests_failed': int(r[8] or 0),
            'found_total': int(r[9] or 0),
            'dedup_total': int(r[10] or 0),
            'imported': int(r[11] or 0),
            'skipped': int(r[12] or 0),
            'status': r[13],
            'error_text': r[14],
            'created_at': r[15]
        })
    return jsonify(result)


@app.route('/api/bases/dorks/runs/<int:run_id>', methods=['GET'])
def get_dorks_run_details(run_id: int):
    user = get_current_user() or {}
    if not is_admin_user(user):
        return jsonify({'error': 'Forbidden'}), 403
    tenant_id = get_current_tenant_id()
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, position, language, sources_json, base_name, results_limit,
                   queries_total, requests_ok, requests_failed, found_total, dedup_total,
                   imported, skipped, status, error_text, created_at
            FROM dorks_runs
            WHERE tenant_id = ? AND id = ?
            LIMIT 1
        ''', (tenant_id, run_id))
        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Run not found'}), 404
        cursor.execute('''
            SELECT source, query_text, status, http_status, result_items, usernames_found, error_text, created_at
            FROM dorks_run_queries
            WHERE tenant_id = ? AND run_id = ?
            ORDER BY id ASC
        ''', (tenant_id, run_id))
        qrows = cursor.fetchall()

    try:
        sources = json.loads(row[3]) if row[3] else []
    except Exception:
        sources = []
    return jsonify({
        'run': {
            'id': int(row[0]),
            'position': row[1],
            'language': row[2],
            'sources': sources,
            'base_name': row[4],
            'results_limit': int(row[5] or 0),
            'queries_total': int(row[6] or 0),
            'requests_ok': int(row[7] or 0),
            'requests_failed': int(row[8] or 0),
            'found_total': int(row[9] or 0),
            'dedup_total': int(row[10] or 0),
            'imported': int(row[11] or 0),
            'skipped': int(row[12] or 0),
            'status': row[13],
            'error_text': row[14],
            'created_at': row[15]
        },
        'queries': [{
            'source': q[0],
            'query': q[1],
            'status': q[2],
            'http_status': q[3],
            'result_items': int(q[4] or 0),
            'usernames_found': int(q[5] or 0),
            'error_text': q[6],
            'created_at': q[7]
        } for q in qrows]
    })

@app.route('/api/outreach/campaigns', methods=['GET'])
def get_outreach_campaigns():
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    campaigns = outreach.get_campaigns(tenant_id=tenant_id)
    if is_admin_user(user):
        return jsonify(campaigns)
    user_id = int(user.get('id') or 0)
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT id FROM outreach_campaigns WHERE tenant_id = ? AND created_by_user_id = ?',
            (tenant_id, user_id)
        )
        own_ids = {int(r[0]) for r in cursor.fetchall()}
    campaigns = [c for c in campaigns if int(c.get('id') or 0) in own_ids]
    return jsonify(campaigns)

@app.route('/api/outreach/campaigns', methods=['POST'])
def create_outreach_campaign():
    tenant_id = get_current_tenant_id()
    actor = get_current_user() or {}
    actor_id = int(actor.get('id') or 0)
    data = request.json or {}
    name = str(data.get('name', '')).strip()
    template = str(data.get('template', '')).strip()
    if not name:
        return jsonify({'error': 'Название кампании обязательно'}), 400
    if not template:
        return jsonify({'error': 'Текст сообщения обязателен'}), 400
    accounts = filter_valid_outreach_accounts(data.get('accounts', []))
    if not accounts:
        return jsonify({'error': 'Выберите минимум один активный outreach-аккаунт'}), 400
    strategy = data.get('strategy') or {'steps': []}
    followup_steps_count = _count_followup_steps(strategy)
    schedule = _normalize_campaign_schedule(data.get('schedule') or {}, followup_steps_count)
    capacity = _calculate_campaign_capacity(tenant_id, accounts, strategy)
    daily_limit_auto = int(capacity.get('safe_new_per_day') or 0)
    base_id_raw = data.get('base_id')
    base_id = None
    if base_id_raw not in (None, '', 0, '0'):
        try:
            base_id = int(base_id_raw)
        except (TypeError, ValueError):
            return jsonify({'error': 'Некорректный base_id'}), 400
        if not get_base_for_current_user(base_id):
            return jsonify({'error': 'База не найдена'}), 404
    try:
        campaign_id = outreach.create_campaign(
            name=name,
            message_template=template,
            accounts=accounts,
            daily_limit=daily_limit_auto,
            delay_min=int(data.get('delayMin', 5)),
            delay_max=int(data.get('delayMax', 15)),
            strategy=strategy,
            schedule=schedule,
            tenant_id=tenant_id,
            created_by_user_id=actor_id if actor_id > 0 else None,
            base_id=base_id
        )
        _sync_campaign_from_base_safe(
            campaign_id=campaign_id,
            base_id=base_id,
            tenant_id=tenant_id,
            reset=True
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({'id': campaign_id, 'capacity': capacity})

@app.route('/api/outreach/campaign/<int:campaign_id>', methods=['PUT'])
def update_outreach_campaign(campaign_id):
    tenant_id = get_current_tenant_id()
    data = request.get_json(silent=True) or {}
    campaign = get_campaign_for_current_user(campaign_id)
    if not campaign:
        return jsonify({'error': 'Campaign not found'}), 404

    name = str(data.get('name', campaign['name'])).strip()
    template = str(data.get('template', campaign['message_template'])).strip()
    if not name:
        return jsonify({'error': 'Название кампании обязательно'}), 400
    if not template:
        return jsonify({'error': 'Текст сообщения обязателен'}), 400

    requested_accounts = data.get('accounts', campaign.get('accounts', []))
    accounts = filter_valid_outreach_accounts(requested_accounts)

    strategy = data.get('strategy', campaign.get('strategy') or {'steps': []})
    followup_steps_count = _count_followup_steps(strategy)
    schedule = _normalize_campaign_schedule(data.get('schedule', campaign.get('schedule') or {}), followup_steps_count)
    base_id_raw = data.get('base_id', campaign.get('base_id'))
    base_id = None
    if base_id_raw not in (None, '', 0, '0'):
        try:
            base_id = int(base_id_raw)
        except (TypeError, ValueError):
            return jsonify({'error': 'Некорректный base_id'}), 400
        if not get_base_for_current_user(base_id):
            return jsonify({'error': 'База не найдена'}), 404
    try:
        delay_min = max(0, int(data.get('delayMin', campaign.get('delay_min', 5))))
        delay_max = max(delay_min, int(data.get('delayMax', campaign.get('delay_max', 15))))
    except (TypeError, ValueError):
        return jsonify({'error': 'Некорректные лимиты/задержка'}), 400
    capacity = _calculate_campaign_capacity(tenant_id, accounts, strategy)
    daily_limit = int(capacity.get('safe_new_per_day') or 0)

    try:
        old_base_id = int(campaign.get('base_id') or 0)
        outreach.update_campaign(
            campaign_id=campaign_id,
            name=name,
            message_template=template,
            accounts=accounts,
            daily_limit=daily_limit,
            delay_min=delay_min,
            delay_max=delay_max,
            strategy=strategy,
            schedule=schedule,
            tenant_id=tenant_id,
            base_id=base_id
        )
        new_base_id = int(base_id or 0)
        if old_base_id != new_base_id:
            with sqlite3.connect(db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM conversations WHERE tenant_id = ? AND campaign_id = ?', (tenant_id, campaign_id))
                cursor.execute('DELETE FROM outreach_contacts WHERE tenant_id = ? AND campaign_id = ?', (tenant_id, campaign_id))
                cursor.execute('''
                    UPDATE outreach_campaigns
                    SET total_contacts = 0, sent_count = 0, reply_count = 0, meeting_count = 0
                    WHERE tenant_id = ? AND id = ?
                ''', (tenant_id, campaign_id))
                conn.commit()
            _sync_campaign_from_base_safe(
                campaign_id=campaign_id,
                base_id=new_base_id if new_base_id > 0 else None,
                tenant_id=tenant_id,
                reset=True
            )
        elif new_base_id > 0:
            # Если база не менялась, но контактов нет (например, после старого бага) — догружаем.
            with sqlite3.connect(db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT COUNT(*) FROM outreach_contacts WHERE tenant_id = ? AND campaign_id = ?',
                    (tenant_id, campaign_id)
                )
                current_contacts = int(cursor.fetchone()[0] or 0)
            if current_contacts == 0:
                _sync_campaign_from_base_safe(
                    campaign_id=campaign_id,
                    base_id=new_base_id,
                    tenant_id=tenant_id,
                    reset=True
                )
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({
        'status': 'updated',
        'campaign': outreach.get_campaign(campaign_id, tenant_id=tenant_id),
        'capacity': capacity
    })


@app.route('/api/outreach/campaign/<int:campaign_id>', methods=['DELETE'])
def delete_outreach_campaign(campaign_id):
    tenant_id = get_current_tenant_id()
    campaign = get_campaign_for_current_user(campaign_id)
    if not campaign:
        return jsonify({'error': 'Campaign not found'}), 404

    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM conversations WHERE campaign_id = ? AND tenant_id = ?', (campaign_id, tenant_id))
            cursor.execute('DELETE FROM outreach_contacts WHERE campaign_id = ? AND tenant_id = ?', (campaign_id, tenant_id))
            cursor.execute('DELETE FROM scheduler_campaign_locks WHERE campaign_id = ? AND tenant_id = ?', (campaign_id, tenant_id))
            cursor.execute('DELETE FROM outreach_campaigns WHERE id = ? AND tenant_id = ?', (campaign_id, tenant_id))
            conn.commit()
    except Exception as e:
        logger.error(f"Error deleting campaign {campaign_id}: {e}")
        return jsonify({'error': str(e)}), 500

    if not has_active_campaigns():
        global scheduler, scheduler_thread
        if scheduler:
            scheduler.stop()
            scheduler = None
        scheduler_thread = None

    return jsonify({'status': 'deleted', 'campaign_id': campaign_id})

@app.route('/api/outreach/campaign/<int:campaign_id>', methods=['GET'])
def get_outreach_campaign(campaign_id):
    campaign = get_campaign_for_current_user(campaign_id)
    if not campaign:
        return jsonify({'error': 'Campaign not found'}), 404
    return jsonify(campaign)

@app.route('/api/outreach/campaign/<int:campaign_id>/contacts')
def get_campaign_contacts(campaign_id):
    tenant_id = get_current_tenant_id()
    campaign = get_campaign_for_current_user(campaign_id)
    if not campaign:
        return jsonify({'error': 'Campaign not found'}), 404
    base_id = int(campaign.get('base_id') or 0)
    if base_id > 0:
        try:
            with sqlite3.connect(db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT COUNT(*) FROM outreach_contacts WHERE tenant_id = ? AND campaign_id = ?',
                    (tenant_id, campaign_id)
                )
                current_contacts = int(cursor.fetchone()[0] or 0)
            if current_contacts == 0:
                _sync_campaign_from_base_safe(
                    campaign_id=campaign_id,
                    base_id=base_id,
                    tenant_id=tenant_id,
                    reset=True
                )
        except Exception as e:
            logger.error("Failed lazy sync for campaign %s: %s", campaign_id, e)
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    return jsonify(outreach.get_campaign_contacts(campaign_id, limit=limit, offset=offset, tenant_id=tenant_id))

def _parse_bool_arg(value) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')

def _normalize_reply_username(username):
    if not username:
        return None
    val = str(username).strip()
    if not val:
        return None
    return val if val.startswith('@') else f"@{val}"

def _normalize_reply_phone(phone):
    raw = str(phone or '').strip()
    if not raw:
        return None
    digits = ''.join(ch for ch in raw if ch.isdigit())
    if not digits:
        return None
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        digits = '7' + digits
    return f"+{digits}" if digits else None

def _cleanup_temp_session_files(temp_base: str):
    if not temp_base:
        return
    for suffix in ('.session', '.session-journal', '.session-shm', '.session-wal'):
        try:
            os.remove(temp_base + suffix)
        except FileNotFoundError:
            pass
        except Exception:
            pass

def _get_target_chat_for_tenant(tenant_id: int) -> str:
    target_chat = str(db.get_config('outreach_target_chat', tenant_id=tenant_id) or '').strip()
    if not target_chat:
        target_chat = str(db.get_config('outreach_target_chat') or '').strip()
    if not target_chat:
        target_chat = str(os.getenv('OUTREACH_TARGET_CHAT') or '').strip()
    if not target_chat:
        target_chat = GLOBAL_TARGET_CHAT_INVITE
    return normalize_chat_ref(target_chat)

def _resolve_account_display_name(tenant_id: int, account_phone: str):
    phone = str(account_phone or '').strip()
    if not phone:
        return None
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT first_name, last_name, session_name
            FROM account_stats
            WHERE tenant_id = ? AND phone = ?
            LIMIT 1
            ''',
            (int(tenant_id), phone)
        )
        row = cursor.fetchone()
    if not row:
        return phone
    first_name = str(row[0] or '').strip()
    last_name = str(row[1] or '').strip()
    session_name = str(row[2] or '').strip()
    full_name = " ".join(part for part in (first_name, last_name) if part).strip()
    if full_name:
        return full_name
    if session_name:
        return session_name
    return phone

def _get_replies_contact_for_current_user(contact_id: int):
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    is_admin = is_admin_user(user)

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        where_parts = [
            'oc.id = ?',
            'oc.tenant_id = ?'
        ]
        params = [int(contact_id), int(tenant_id)]
        if not is_admin:
            where_parts.append('cp.created_by_user_id = ?')
            params.append(user_id)

        cursor.execute(
            f'''
            SELECT
                oc.id,
                oc.tenant_id,
                oc.campaign_id,
                cp.name AS campaign_name,
                COALESCE(NULLIF(oc.name, ''), NULLIF(oc.username, ''), NULLIF(oc.phone, ''), CAST(oc.user_id AS TEXT), '—') AS contact_name,
                oc.status,
                oc.user_id,
                oc.username,
                oc.phone,
                oc.access_hash,
                COALESCE(
                    NULLIF(TRIM(oc.account_used), ''),
                    (
                        SELECT NULLIF(TRIM(om.account_used), '')
                        FROM outreach_messages om
                        WHERE om.tenant_id = oc.tenant_id
                          AND om.contact_id = oc.id
                          AND COALESCE(om.is_incoming, 0) = 0
                          AND om.account_used IS NOT NULL
                          AND TRIM(om.account_used) != ''
                        ORDER BY datetime(om.sent_at) DESC, om.id DESC
                        LIMIT 1
                    ),
                    (
                        SELECT NULLIF(TRIM(json_extract(cv.metadata, '$.account_used')), '')
                        FROM conversations cv
                        WHERE cv.tenant_id = oc.tenant_id
                          AND cv.contact_id = oc.id
                          AND cv.direction = 'outgoing'
                          AND cv.metadata IS NOT NULL
                          AND json_extract(cv.metadata, '$.account_used') IS NOT NULL
                        ORDER BY datetime(cv.sent_at) DESC, cv.id DESC
                        LIMIT 1
                    )
                ) AS reply_account
            FROM outreach_contacts oc
            JOIN outreach_campaigns cp
              ON cp.id = oc.campaign_id
             AND cp.tenant_id = oc.tenant_id
            WHERE {' AND '.join(where_parts)}
            LIMIT 1
            ''',
            params
        )
        row = cursor.fetchone()

    if not row:
        return None
    account_used = str(row[10] or '').strip() if row[10] else None
    account_name = _resolve_account_display_name(int(row[1]), account_used) if account_used else None
    return {
        'contact_id': int(row[0]),
        'tenant_id': int(row[1]),
        'campaign_id': int(row[2]),
        'campaign_name': str(row[3] or ''),
        'name': str(row[4] or '—'),
        'status': str(row[5] or ''),
        'user_id': int(row[6]) if row[6] is not None else None,
        'username': str(row[7] or '') if row[7] else None,
        'phone': str(row[8] or '') if row[8] else None,
        'access_hash': str(row[9] or '') if row[9] else None,
        'account_used': account_used,
        'account_name': account_name
    }

def _get_reply_account_config(tenant_id: int, account_phone: str, user: dict):
    if not account_phone:
        return None
    uid = int((user or {}).get('id') or 0)
    is_admin = is_admin_user(user)
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        where_parts = [
            'a.tenant_id = ?',
            'a.phone = ?',
            "a.account_type = 'outreach'",
            'COALESCE(a.is_banned, 0) = 0',
            'COALESCE(a.is_frozen, 0) = 0'
        ]
        params = [int(tenant_id), str(account_phone).strip()]
        if not is_admin:
            where_parts.append('a.added_by_user_id = ?')
            params.append(uid)
        cursor.execute(
            f'''
            SELECT
                a.phone,
                a.session_name,
                a.proxy_id,
                COALESCE(f.device_model, 'Desktop') AS device_model,
                COALESCE(f.app_version, '4.16.30') AS app_version,
                COALESCE(f.lang_code, 'ru') AS lang_code,
                COALESCE(f.system_lang_code, 'ru') AS system_lang_code
            FROM account_stats a
            LEFT JOIN account_fingerprints f
              ON f.account_phone = a.phone
             AND f.tenant_id = a.tenant_id
            WHERE {' AND '.join(where_parts)}
            LIMIT 1
            ''',
            params
        )
        row = cursor.fetchone()
        if not row:
            return None

        proxy_cfg = None
        proxy_id = row[2]
        if proxy_id:
            cursor.execute(
                '''
                SELECT ip, port, protocol, username, password, is_active
                FROM proxies
                WHERE id = ? AND tenant_id = ?
                LIMIT 1
                ''',
                (proxy_id, int(tenant_id))
            )
            p = cursor.fetchone()
            if p and int(p[5] or 0) == 1:
                proxy_cfg = build_telethon_proxy_config({
                    'ip': p[0],
                    'port': p[1],
                    'protocol': p[2],
                    'username': p[3],
                    'password': p[4],
                    'is_active': bool(p[5])
                })

    return {
        'phone': str(row[0] or '').strip(),
        'session_name': str(row[1] or '').strip(),
        'proxy': proxy_cfg,
        'device_model': str(row[3] or 'Desktop'),
        'app_version': str(row[4] or '4.16.30'),
        'lang_code': str(row[5] or 'ru'),
        'system_lang_code': str(row[6] or 'ru')
    }

async def _connect_reply_client_async(account_cfg: dict, api_id: str, api_hash: str):
    session_name = str((account_cfg or {}).get('session_name') or '').strip()
    if not session_name:
        raise RuntimeError('У аккаунта не найден session_name')
    session_src = os.path.join('sessions', session_name)
    session_src_file = session_src + '.session'
    if not os.path.exists(session_src_file):
        raise RuntimeError(f'Файл сессии не найден: {session_name}.session')

    os.makedirs(os.path.join(tempfile.gettempdir(), 'outreach_reply_sessions'), exist_ok=True)
    temp_base = os.path.join(
        tempfile.gettempdir(),
        'outreach_reply_sessions',
        f"{session_name}_{uuid.uuid4().hex}"
    )
    shutil.copy2(session_src_file, temp_base + '.session')

    common_kwargs = {
        'device_model': account_cfg.get('device_model') or 'Desktop',
        'app_version': account_cfg.get('app_version') or '4.16.30',
        'lang_code': account_cfg.get('lang_code') or 'ru',
        'system_lang_code': account_cfg.get('system_lang_code') or 'ru'
    }

    last_error = None
    for mode in ('proxy', 'direct'):
        kwargs = dict(common_kwargs)
        if mode == 'proxy':
            proxy = account_cfg.get('proxy')
            if not proxy:
                continue
            kwargs['proxy'] = proxy
        client = TelegramClient(temp_base, api_id=int(api_id), api_hash=api_hash, **kwargs)
        try:
            await asyncio.wait_for(client.connect(), timeout=20)
            if not await asyncio.wait_for(client.is_user_authorized(), timeout=10):
                await client.disconnect()
                raise RuntimeError('Сессия не авторизована')
            return client, temp_base
        except Exception as e:
            last_error = e
            try:
                await client.disconnect()
            except Exception:
                pass
            if mode == 'proxy':
                logger.warning(
                    "[replies] proxy failed for %s, fallback to direct: %s",
                    account_cfg.get('phone'),
                    e
                )
                continue
            break

    _cleanup_temp_session_files(temp_base)
    raise RuntimeError(f'Не удалось подключить аккаунт: {last_error}')

async def _ensure_client_in_target_chat_async(client, target_chat: str):
    chat_ref = normalize_chat_ref(target_chat)
    if not chat_ref:
        return
    try:
        if '+' in chat_ref or 'joinchat' in chat_ref:
            hash_part = chat_ref
            if 'joinchat/' in hash_part:
                hash_part = hash_part.split('joinchat/')[-1]
            if hash_part.startswith('+'):
                hash_part = hash_part[1:]
            await client(ImportChatInviteRequest(hash_part))
        else:
            entity = await client.get_entity(chat_ref)
            await client(JoinChannelRequest(entity))
    except errors.UserAlreadyParticipantError:
        return
    except Exception as e:
        if 'USER_ALREADY_PARTICIPANT' not in str(e):
            logger.info("[replies] target chat join skipped: %s", e)

async def _warm_reply_entity_from_target_chat_async(client, target_chat: str, user_id: int, limit: int = 1500):
    if not user_id:
        return
    try:
        await _ensure_client_in_target_chat_async(client, target_chat)
        chat_entity = await client.get_entity(target_chat)
        scanned = 0
        async for msg in client.iter_messages(chat_entity, limit=max(50, int(limit))):
            scanned += 1
            sender_id = getattr(msg, 'sender_id', None)
            if sender_id == user_id:
                try:
                    await msg.get_sender()
                except Exception:
                    pass
                break
        logger.info("[replies] warm target chat scanned=%s for user=%s", scanned, user_id)
    except Exception as e:
        logger.info("[replies] warm target chat failed for user %s: %s", user_id, e)

async def _resolve_reply_entity_async(client, contact: dict, target_chat: str):
    uid = None
    try:
        uid = int(contact.get('user_id')) if contact.get('user_id') is not None else None
    except Exception:
        uid = None
    username = _normalize_reply_username(contact.get('username'))
    phone = _normalize_reply_phone(contact.get('phone'))
    access_hash = None
    try:
        access_hash = int(str(contact.get('access_hash') or '').strip())
    except Exception:
        access_hash = None

    errors_seen = []
    if username:
        try:
            return await client.get_input_entity(username)
        except Exception as e:
            errors_seen.append(f"username:{type(e).__name__}:{e}")

    if uid and access_hash:
        try:
            return InputPeerUser(uid, access_hash)
        except Exception as e:
            errors_seen.append(f"id+hash:{type(e).__name__}:{e}")

    if uid:
        try:
            return await client.get_input_entity(PeerUser(uid))
        except Exception as e:
            errors_seen.append(f"peer_user:{type(e).__name__}:{e}")

        try:
            return await client.get_input_entity(uid)
        except Exception as e:
            errors_seen.append(f"id:{type(e).__name__}:{e}")

        await _warm_reply_entity_from_target_chat_async(client, target_chat, uid)
        try:
            return await client.get_input_entity(PeerUser(uid))
        except Exception as e:
            errors_seen.append(f"peer_after_warm:{type(e).__name__}:{e}")

    if phone:
        try:
            import_result = await client(ImportContactsRequest([
                InputPhoneContact(client_id=0, phone=phone, first_name='Reply', last_name='Contact')
            ]))
            if import_result.users:
                return import_result.users[0]
            errors_seen.append('phone_import:empty_result')
        except Exception as e:
            errors_seen.append(f"phone_import:{type(e).__name__}:{e}")

    detail = " | ".join(errors_seen) if errors_seen else "no_identifiers"
    raise RuntimeError(f"Не удалось определить чат контакта: {detail}")

def _trim_reply_history(cursor, tenant_id: int, contact_id: int, keep_limit: int):
    keep = max(50, min(int(keep_limit or 200), 500))
    cursor.execute(
        '''
        DELETE FROM conversations
        WHERE tenant_id = ?
          AND contact_id = ?
          AND id NOT IN (
              SELECT id
              FROM conversations
              WHERE tenant_id = ?
                AND contact_id = ?
              ORDER BY datetime(sent_at) DESC, id DESC
              LIMIT ?
          )
        ''',
        (tenant_id, contact_id, tenant_id, contact_id, keep)
    )

def _load_reply_thread_from_db(tenant_id: int, contact_id: int, limit: int = 120):
    safe_limit = max(20, min(int(limit or 120), 500))
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id, message_id, direction, content, sent_at, COALESCE(is_unread, 0)
            FROM conversations
            WHERE tenant_id = ? AND contact_id = ?
            ORDER BY datetime(sent_at) DESC, id DESC
            LIMIT ?
            ''',
            (tenant_id, contact_id, safe_limit)
        )
        rows = cursor.fetchall()

    rows.reverse()
    messages = []
    for row in rows:
        content = str(row[3] or '')
        messages.append({
            'id': int(row[0]),
            'message_id': int(row[1]) if row[1] is not None else None,
            'direction': str(row[2] or ''),
            'content': content if content else '',
            'sent_at': row[4],
            'is_unread': int(row[5] or 0)
        })
    return messages

async def _sync_reply_thread_async(contact: dict, user: dict, api_id: str, api_hash: str, fetch_limit: int = 120, mark_read: bool = False):
    def _parse_dt(value):
        raw = str(value or '').strip()
        if not raw:
            return None
        candidate = raw
        if candidate.endswith('Z'):
            candidate = f"{candidate[:-1]}+00:00"
        for probe in (candidate, candidate.replace(' ', 'T')):
            try:
                return datetime.fromisoformat(probe)
            except Exception:
                pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(candidate, fmt)
            except Exception:
                pass
        return None

    tenant_id = int(contact['tenant_id'])
    contact_id = int(contact['contact_id'])
    account_phone = str(contact.get('account_used') or '').strip()
    if not account_phone:
        raise RuntimeError('Для контакта не найден аккаунт отправителя')

    account_cfg = _get_reply_account_config(tenant_id, account_phone, user)
    if not account_cfg:
        raise RuntimeError(f'Аккаунт {account_phone} недоступен')

    target_chat = _get_target_chat_for_tenant(tenant_id)
    client = None
    temp_base = None
    inserted = 0
    incoming_seen = False
    fetched = 0
    try:
        client, temp_base = await _connect_reply_client_async(account_cfg, api_id, api_hash)
        target = await _resolve_reply_entity_async(client, contact, target_chat)
        tg_messages = await client.get_messages(target, limit=max(20, min(int(fetch_limit or 120), 300)))

        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            for msg in reversed(list(tg_messages or [])):
                if msg is None:
                    continue
                if getattr(msg, 'action', None) is not None:
                    continue
                fetched += 1
                direction = 'outgoing' if bool(getattr(msg, 'out', False)) else 'incoming'
                if direction == 'incoming':
                    incoming_seen = True
                text = str(getattr(msg, 'message', None) or getattr(msg, 'text', None) or '')
                if not text and getattr(msg, 'media', None) is not None:
                    text = '[media]'
                sent_at = msg.date.isoformat() if getattr(msg, 'date', None) else datetime.now().isoformat()
                message_id = int(msg.id) if getattr(msg, 'id', None) is not None else None
                is_unread = 1 if bool(getattr(msg, 'unread', False)) else 0

                exists = None
                if message_id is not None:
                    cursor.execute(
                        '''
                        SELECT id
                        FROM conversations
                        WHERE tenant_id = ? AND contact_id = ? AND message_id = ?
                        LIMIT 1
                        ''',
                        (tenant_id, contact_id, message_id)
                    )
                    exists = cursor.fetchone()
                if exists:
                    cursor.execute(
                        '''
                        UPDATE conversations
                        SET is_unread = ?,
                            sent_at = COALESCE(NULLIF(sent_at, ''), ?),
                            content = CASE
                                WHEN COALESCE(content, '') = '' THEN ?
                                ELSE content
                            END
                        WHERE id = ?
                        ''',
                        (is_unread, sent_at, text, int(exists[0]))
                    )
                    continue

                # Легаси-склейка: старые исходящие могли быть записаны без message_id.
                # Если нашли такую запись с тем же текстом/направлением — проставляем ей message_id,
                # чтобы не плодить дубли при ручном "Обновить".
                if message_id is not None:
                    cursor.execute(
                        '''
                        SELECT id, sent_at
                        FROM conversations
                        WHERE tenant_id = ?
                          AND contact_id = ?
                          AND message_id IS NULL
                          AND direction = ?
                          AND COALESCE(content, '') = ?
                        ORDER BY id DESC
                        LIMIT 20
                        ''',
                        (tenant_id, contact_id, direction, text)
                    )
                    legacy_rows = cursor.fetchall()
                    if legacy_rows:
                        tg_dt = _parse_dt(sent_at)
                        best_id = None
                        best_delta = None
                        for legacy_id, legacy_sent_at in legacy_rows:
                            if best_id is None:
                                best_id = int(legacy_id)
                            if tg_dt is None:
                                continue
                            legacy_dt = _parse_dt(legacy_sent_at)
                            if legacy_dt is None:
                                continue
                            delta = abs(
                                (
                                    tg_dt.replace(tzinfo=None) - legacy_dt.replace(tzinfo=None)
                                ).total_seconds()
                            )
                            if delta > 7200:
                                continue
                            if best_delta is None or delta < best_delta:
                                best_delta = delta
                                best_id = int(legacy_id)
                        if best_id is not None:
                            cursor.execute(
                                '''
                                UPDATE conversations
                                SET message_id = ?,
                                    sent_at = COALESCE(NULLIF(sent_at, ''), ?),
                                    is_unread = ?
                                WHERE id = ?
                                ''',
                                (message_id, sent_at, is_unread, best_id)
                            )
                            continue

                metadata = None
                if direction == 'outgoing':
                    metadata = json.dumps({'account_used': account_phone}, ensure_ascii=False)

                cursor.execute(
                    '''
                    INSERT INTO conversations
                    (tenant_id, campaign_id, contact_id, message_id, direction, content, sent_at, metadata, is_unread)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        tenant_id,
                        int(contact['campaign_id']),
                        contact_id,
                        message_id,
                        direction,
                        text,
                        sent_at,
                        metadata,
                        is_unread
                    )
                )
                inserted += 1

            if mark_read:
                try:
                    await client.send_read_acknowledge(target)
                except Exception:
                    pass
                cursor.execute(
                    '''
                    UPDATE conversations
                    SET is_unread = 0
                    WHERE tenant_id = ? AND contact_id = ? AND direction = 'incoming'
                    ''',
                    (tenant_id, contact_id)
                )

            if incoming_seen:
                cursor.execute(
                    '''
                    UPDATE outreach_contacts
                    SET status = 'replied',
                        replied_at = COALESCE(replied_at, CURRENT_TIMESTAMP)
                    WHERE tenant_id = ? AND id = ? AND status IN ('new', 'sent', 'ignored')
                    ''',
                    (tenant_id, contact_id)
                )
                # Увеличиваем reply_count кампании только при фактическом переходе
                # статуса контакта в replied.
                if cursor.rowcount and int(cursor.rowcount) > 0:
                    cursor.execute(
                        '''
                        UPDATE outreach_campaigns
                        SET reply_count = reply_count + 1
                        WHERE tenant_id = ? AND id = ?
                        ''',
                        (tenant_id, int(contact['campaign_id']))
                    )

            if not contact.get('account_used'):
                cursor.execute(
                    '''
                    UPDATE outreach_contacts
                    SET account_used = ?
                    WHERE tenant_id = ? AND id = ? AND (account_used IS NULL OR TRIM(account_used) = '')
                    ''',
                    (account_phone, tenant_id, contact_id)
                )

            _trim_reply_history(cursor, tenant_id, contact_id, keep_limit=200)
            conn.commit()

        return {
            'fetched': fetched,
            'inserted': inserted,
            'incoming_seen': incoming_seen,
            'account_used': account_phone
        }
    finally:
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        if temp_base:
            _cleanup_temp_session_files(temp_base)

async def _send_reply_message_async(contact: dict, user: dict, api_id: str, api_hash: str, text: str = '', file_payload: dict = None):
    tenant_id = int(contact['tenant_id'])
    contact_id = int(contact['contact_id'])
    account_phone = str(contact.get('account_used') or '').strip()
    if not account_phone:
        raise RuntimeError('Для контакта не найден аккаунт отправителя')

    account_cfg = _get_reply_account_config(tenant_id, account_phone, user)
    if not account_cfg:
        raise RuntimeError(f'Аккаунт {account_phone} недоступен')

    target_chat = _get_target_chat_for_tenant(tenant_id)
    client = None
    temp_base = None
    temp_upload_path = None
    try:
        client, temp_base = await _connect_reply_client_async(account_cfg, api_id, api_hash)
        target = await _resolve_reply_entity_async(client, contact, target_chat)
        safe_text = str(text or '').strip()
        safe_file = file_payload or None
        file_name = None
        has_file = bool(safe_file and safe_file.get('bytes'))
        if has_file:
            file_name = secure_filename(str(safe_file.get('filename') or 'attachment.bin'))
            suffix = ''
            if '.' in file_name:
                suffix = f".{file_name.rsplit('.', 1)[-1]}"
            with tempfile.NamedTemporaryFile(prefix='reply_upload_', suffix=suffix, delete=False) as tmp_f:
                tmp_f.write(safe_file.get('bytes') or b'')
                temp_upload_path = tmp_f.name
            sent_msg = await client.send_file(
                target,
                temp_upload_path,
                caption=safe_text or None,
                force_document=True
            )
        else:
            sent_msg = await client.send_message(target, safe_text)
        sent_at = sent_msg.date.isoformat() if getattr(sent_msg, 'date', None) else datetime.now().isoformat()
        msg_id = int(sent_msg.id) if getattr(sent_msg, 'id', None) is not None else None
        content_text = safe_text
        if has_file:
            marker = f"[file: {file_name}]"
            content_text = f"{marker}\n{safe_text}" if safe_text else marker
        if not content_text:
            content_text = '[без текста]'
        meta_payload = {'account_used': account_phone, 'source': 'replies_ui'}
        if has_file:
            meta_payload['file_name'] = file_name
            meta_payload['has_file'] = True

        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO conversations
                (tenant_id, campaign_id, contact_id, message_id, direction, content, sent_at, metadata, is_unread)
                VALUES (?, ?, ?, ?, 'outgoing', ?, ?, ?, 1)
                ''',
                (
                    tenant_id,
                    int(contact['campaign_id']),
                    contact_id,
                    msg_id,
                    content_text,
                    sent_at,
                    json.dumps(meta_payload, ensure_ascii=False)
                )
            )
            cursor.execute(
                '''
                INSERT INTO outreach_messages
                (tenant_id, contact_id, account_used, message_text, sent_at, is_incoming)
                VALUES (?, ?, ?, ?, ?, 0)
                ''',
                (tenant_id, contact_id, account_phone, content_text, sent_at)
            )
            cursor.execute(
                '''
                UPDATE outreach_contacts
                SET account_used = COALESCE(NULLIF(TRIM(account_used), ''), ?),
                    last_message = ?,
                    message_sent_at = ?
                WHERE tenant_id = ? AND id = ?
                ''',
                (account_phone, content_text, sent_at, tenant_id, contact_id)
            )
            _trim_reply_history(cursor, tenant_id, contact_id, keep_limit=200)
            conn.commit()

        return {
            'message_id': msg_id,
            'sent_at': sent_at,
            'account_used': account_phone
        }
    finally:
        if temp_upload_path:
            try:
                os.remove(temp_upload_path)
            except Exception:
                pass
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        if temp_base:
            _cleanup_temp_session_files(temp_base)

@app.route('/api/replies', methods=['GET'])
def get_replies_feed():
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    is_admin = is_admin_user(user)

    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    campaign_id = request.args.get('campaign_id', type=int)
    user_id_filter_raw = str(request.args.get('user_id') or '').strip()
    user_id_filter = None
    if user_id_filter_raw:
        if not user_id_filter_raw.isdigit():
            return jsonify({'error': 'user_id должен быть числом'}), 400
        user_id_filter = int(user_id_filter_raw)
    limit = max(1, min(int(limit or 100), 500))
    offset = max(0, int(offset or 0))

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()

        # Кампании, доступные пользователю, для фильтра в UI.
        if is_admin:
            cursor.execute(
                '''
                SELECT id, name
                FROM outreach_campaigns
                WHERE tenant_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                ''',
                (tenant_id,)
            )
        else:
            cursor.execute(
                '''
                SELECT id, name
                FROM outreach_campaigns
                WHERE tenant_id = ?
                  AND created_by_user_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                ''',
                (tenant_id, user_id)
            )
        campaigns = [{'id': int(row[0]), 'name': str(row[1] or '')} for row in cursor.fetchall()]

        where_parts = [
            "oc.tenant_id = ?",
            "oc.status = 'replied'"
        ]
        params = [tenant_id]
        if not is_admin:
            where_parts.append("cp.created_by_user_id = ?")
            params.append(user_id)
        if campaign_id:
            where_parts.append("oc.campaign_id = ?")
            params.append(int(campaign_id))
        if user_id_filter is not None:
            where_parts.append("oc.user_id = ?")
            params.append(int(user_id_filter))
        where_sql = " AND ".join(where_parts)

        cursor.execute(
            f'''
            SELECT COUNT(*)
            FROM outreach_contacts oc
            JOIN outreach_campaigns cp
              ON cp.id = oc.campaign_id
             AND cp.tenant_id = oc.tenant_id
            WHERE {where_sql}
            ''',
            params
        )
        total = int(cursor.fetchone()[0] or 0)

        row_params = [*params, limit, offset]
        cursor.execute(
            f'''
            SELECT
                oc.id AS contact_id,
                oc.campaign_id,
                cp.name AS campaign_name,
                COALESCE(NULLIF(oc.name, ''), NULLIF(oc.username, ''), NULLIF(oc.phone, ''), CAST(oc.user_id AS TEXT), '—') AS contact_name,
                oc.status,
                oc.user_id,
                oc.username,
                COALESCE(
                    NULLIF(TRIM(oc.account_used), ''),
                    (
                        SELECT NULLIF(TRIM(om.account_used), '')
                        FROM outreach_messages om
                        WHERE om.tenant_id = oc.tenant_id
                          AND om.contact_id = oc.id
                          AND COALESCE(om.is_incoming, 0) = 0
                          AND om.account_used IS NOT NULL
                          AND TRIM(om.account_used) != ''
                        ORDER BY datetime(om.sent_at) DESC, om.id DESC
                        LIMIT 1
                    )
                ) AS account_used,
                (
                    SELECT COALESCE(NULLIF(TRIM(cv.content), ''), '[без текста]')
                    FROM conversations cv
                    WHERE cv.tenant_id = oc.tenant_id
                      AND cv.contact_id = oc.id
                      AND cv.direction = 'incoming'
                    ORDER BY datetime(cv.sent_at) ASC, cv.id ASC
                    LIMIT 1
                ) AS first_reply_text,
                (
                    SELECT cv.sent_at
                    FROM conversations cv
                    WHERE cv.tenant_id = oc.tenant_id
                      AND cv.contact_id = oc.id
                      AND cv.direction = 'incoming'
                    ORDER BY datetime(cv.sent_at) ASC, cv.id ASC
                    LIMIT 1
                ) AS first_reply_at,
                (
                    SELECT COALESCE(NULLIF(TRIM(cv.content), ''), '[без текста]')
                    FROM conversations cv
                    WHERE cv.tenant_id = oc.tenant_id
                      AND cv.contact_id = oc.id
                    ORDER BY datetime(cv.sent_at) DESC, cv.id DESC
                    LIMIT 1
                ) AS last_message_text,
                (
                    SELECT cv.sent_at
                    FROM conversations cv
                    WHERE cv.tenant_id = oc.tenant_id
                      AND cv.contact_id = oc.id
                    ORDER BY datetime(cv.sent_at) DESC, cv.id DESC
                    LIMIT 1
                ) AS last_message_at
            FROM outreach_contacts oc
            JOIN outreach_campaigns cp
              ON cp.id = oc.campaign_id
             AND cp.tenant_id = oc.tenant_id
            WHERE {where_sql}
            ORDER BY datetime(
                COALESCE(
                    (
                        SELECT cv.sent_at
                        FROM conversations cv
                        WHERE cv.tenant_id = oc.tenant_id
                          AND cv.contact_id = oc.id
                        ORDER BY datetime(cv.sent_at) DESC, cv.id DESC
                        LIMIT 1
                    ),
                    oc.replied_at,
                    oc.message_sent_at
                )
            ) DESC,
            oc.id DESC
            LIMIT ? OFFSET ?
            ''',
            row_params
        )
        rows = cursor.fetchall()

    account_name_map = {}
    account_phones = sorted({
        str(row[7] or '').strip()
        for row in rows
        if row[7] is not None and str(row[7]).strip()
    })
    if account_phones:
        placeholders = ','.join(['?'] * len(account_phones))
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f'''
                SELECT phone, first_name, last_name, session_name
                FROM account_stats
                WHERE tenant_id = ? AND phone IN ({placeholders})
                ''',
                [tenant_id, *account_phones]
            )
            for phone, first_name, last_name, session_name in cursor.fetchall():
                full_name = " ".join(
                    part for part in (str(first_name or '').strip(), str(last_name or '').strip()) if part
                ).strip()
                account_name_map[str(phone).strip()] = full_name or str(session_name or '').strip() or str(phone).strip()

    items = []
    for row in rows:
        account_used = str(row[7] or '').strip() if row[7] else None
        items.append({
            'contact_id': int(row[0]),
            'campaign_id': int(row[1]),
            'campaign_name': str(row[2] or ''),
            'name': str(row[3] or ''),
            'status': str(row[4] or ''),
            'user_id': int(row[5]) if row[5] is not None else None,
            'username': str(row[6] or '') if row[6] else None,
            'account_used': account_used,
            'account_name': account_name_map.get(account_used or '', account_used),
            'first_reply_text': str(row[8] or '[без текста]'),
            'first_reply_at': row[9],
            'last_message_text': str(row[10] or '[без текста]'),
            'last_message_at': row[11]
        })

    return jsonify({
        'items': items,
        'total': total,
        'limit': limit,
        'offset': offset,
        'campaigns': campaigns,
        'user_id_filter': user_id_filter
    })

@app.route('/api/replies/<int:contact_id>/thread', methods=['GET'])
def get_reply_thread(contact_id: int):
    user = get_current_user() or {}
    contact = _get_replies_contact_for_current_user(contact_id)
    if not contact:
        return jsonify({'error': 'Контакт не найден'}), 404

    safe_limit = max(20, min(int(request.args.get('limit', 120) or 120), 500))
    need_sync = _parse_bool_arg(request.args.get('sync'))
    sync_result = None

    if need_sync:
        api_id, api_hash = get_api_credentials()
        if not api_id or not api_hash:
            return jsonify({'error': 'API ID и API Hash не настроены'}), 400
        try:
            sync_result = run_async_threadsafe(
                _sync_reply_thread_async(
                    contact=contact,
                    user=user,
                    api_id=api_id,
                    api_hash=api_hash,
                    fetch_limit=min(300, safe_limit + 80),
                    mark_read=True
                )
            )
        except Exception as e:
            logger.error("Reply thread sync failed for contact %s: %s", contact_id, e)
            return jsonify({'error': str(e)}), 400
        # Перечитываем контакт (account_used мог обновиться).
        contact = _get_replies_contact_for_current_user(contact_id) or contact

    messages = _load_reply_thread_from_db(contact['tenant_id'], contact['contact_id'], limit=safe_limit)
    return jsonify({
        'contact': {
            'contact_id': int(contact['contact_id']),
            'campaign_id': int(contact['campaign_id']),
            'campaign_name': contact.get('campaign_name') or '',
            'name': contact.get('name') or '—',
            'user_id': contact.get('user_id'),
            'username': contact.get('username'),
            'phone': contact.get('phone'),
            'account_used': contact.get('account_used'),
            'account_name': contact.get('account_name')
        },
        'messages': messages,
        'count': len(messages),
        'synced': bool(sync_result is not None),
        'sync_result': sync_result
    })

@app.route('/api/replies/<int:contact_id>/reload', methods=['POST'])
def reload_reply_thread(contact_id: int):
    user = get_current_user() or {}
    contact = _get_replies_contact_for_current_user(contact_id)
    if not contact:
        return jsonify({'error': 'Контакт не найден'}), 404
    api_id, api_hash = get_api_credentials()
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400
    try:
        sync_result = run_async_threadsafe(
            _sync_reply_thread_async(
                contact=contact,
                user=user,
                api_id=api_id,
                api_hash=api_hash,
                fetch_limit=220,
                mark_read=True
            )
        )
    except Exception as e:
        logger.error("Reply thread manual reload failed for contact %s: %s", contact_id, e)
        return jsonify({'error': str(e)}), 400

    contact = _get_replies_contact_for_current_user(contact_id) or contact
    messages = _load_reply_thread_from_db(contact['tenant_id'], contact['contact_id'], limit=160)
    return jsonify({
        'status': 'reloaded',
        'contact': {
            'contact_id': int(contact['contact_id']),
            'campaign_id': int(contact['campaign_id']),
            'campaign_name': contact.get('campaign_name') or '',
            'name': contact.get('name') or '—',
            'user_id': contact.get('user_id'),
            'username': contact.get('username'),
            'phone': contact.get('phone'),
            'account_used': contact.get('account_used'),
            'account_name': contact.get('account_name')
        },
        'messages': messages,
        'count': len(messages),
        'sync_result': sync_result
    })

@app.route('/api/replies/<int:contact_id>/send', methods=['POST'])
def send_reply_message(contact_id: int):
    user = get_current_user() or {}
    content_type = str(request.content_type or '').lower()
    text = ''
    upload = None
    if 'multipart/form-data' in content_type:
        text = str(request.form.get('text') or '').strip()
        upload = request.files.get('file')
    else:
        payload = request.get_json(silent=True) or {}
        text = str(payload.get('text') or '').strip()

    if len(text) > 4096:
        return jsonify({'error': 'Сообщение слишком длинное (макс. 4096 символов)'}), 400

    file_payload = None
    if upload and upload.filename:
        filename = secure_filename(str(upload.filename or '').strip()) or 'attachment.bin'
        file_bytes = upload.read() or b''
        if not file_bytes:
            return jsonify({'error': 'Файл пустой'}), 400
        if len(file_bytes) > 50 * 1024 * 1024:
            return jsonify({'error': 'Файл слишком большой (макс. 50 МБ)'}), 400
        file_payload = {
            'filename': filename,
            'bytes': file_bytes
        }

    if not text and not file_payload:
        return jsonify({'error': 'Укажите текст сообщения или прикрепите файл'}), 400

    contact = _get_replies_contact_for_current_user(contact_id)
    if not contact:
        return jsonify({'error': 'Контакт не найден'}), 404
    if not contact.get('account_used'):
        return jsonify({'error': 'У контакта не определён аккаунт отправителя'}), 400

    api_id, api_hash = get_api_credentials()
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400

    try:
        sent = run_async_threadsafe(
            _send_reply_message_async(
                contact=contact,
                user=user,
                api_id=api_id,
                api_hash=api_hash,
                text=text,
                file_payload=file_payload
            )
        )
    except Exception as e:
        logger.error("Reply send failed for contact %s: %s", contact_id, e)
        return jsonify({'error': str(e)}), 400

    contact = _get_replies_contact_for_current_user(contact_id) or contact
    messages = _load_reply_thread_from_db(contact['tenant_id'], contact['contact_id'], limit=160)
    return jsonify({
        'status': 'sent',
        'sent': sent,
        'contact': {
            'contact_id': int(contact['contact_id']),
            'campaign_id': int(contact['campaign_id']),
            'campaign_name': contact.get('campaign_name') or '',
            'name': contact.get('name') or '—',
            'user_id': contact.get('user_id'),
            'username': contact.get('username'),
            'phone': contact.get('phone'),
            'account_used': contact.get('account_used'),
            'account_name': contact.get('account_name')
        },
        'messages': messages,
        'count': len(messages)
    })

@app.route('/api/outreach/blacklist', methods=['GET'])
def get_outreach_blacklist():
    tenant_id = get_current_tenant_id()
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, created_at
            FROM outreach_blacklist
            WHERE tenant_id = ?
            ORDER BY datetime(created_at) DESC, user_id DESC
            LIMIT 1000
        ''', (tenant_id,))
        rows = cursor.fetchall()
    return jsonify([{'user_id': row[0], 'created_at': row[1]} for row in rows])

@app.route('/api/bases/blacklist', methods=['GET'])
def get_bases_blacklist():
    return get_outreach_blacklist()

@app.route('/api/outreach/blacklist', methods=['POST'])
def add_to_outreach_blacklist():
    tenant_id = get_current_tenant_id()
    data = request.get_json(silent=True) or {}
    try:
        user_id = int(data.get('user_id'))
    except (TypeError, ValueError):
        return jsonify({'error': 'user_id должен быть числом'}), 400
    if user_id <= 0:
        return jsonify({'error': 'user_id должен быть > 0'}), 400

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO outreach_blacklist (tenant_id, user_id) VALUES (?, ?)', (tenant_id, user_id))
        inserted = cursor.rowcount > 0
        cursor.execute('''
            UPDATE outreach_contacts
            SET status = 'failed',
                notes = 'blacklisted'
            WHERE user_id = ?
              AND tenant_id = ?
              AND status IN ('new', 'sent', 'ignored')
        ''', (user_id, tenant_id))
        affected_contacts = cursor.rowcount
        conn.commit()

    return jsonify({
        'status': 'added' if inserted else 'exists',
        'user_id': user_id,
        'affected_contacts': affected_contacts
    })

@app.route('/api/bases/blacklist', methods=['POST'])
def add_to_bases_blacklist():
    return add_to_outreach_blacklist()

@app.route('/api/outreach/blacklist/<int:user_id>', methods=['DELETE'])
def remove_from_outreach_blacklist(user_id: int):
    tenant_id = get_current_tenant_id()
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM outreach_blacklist WHERE tenant_id = ? AND user_id = ?', (tenant_id, user_id))
        deleted = cursor.rowcount
        cursor.execute('''
            UPDATE outreach_contacts
            SET status = CASE
                    WHEN EXISTS (
                        SELECT 1 FROM conversations c
                        WHERE c.contact_id = outreach_contacts.id
                          AND c.direction = 'outgoing'
                    ) THEN 'ignored'
                    ELSE 'new'
                END,
                notes = NULL
            WHERE user_id = ?
              AND tenant_id = ?
              AND status = 'failed'
              AND notes = 'blacklisted'
        ''', (user_id, tenant_id))
        restored_contacts = cursor.rowcount
        conn.commit()
    return jsonify({
        'status': 'deleted' if deleted else 'not_found',
        'user_id': user_id,
        'restored_contacts': restored_contacts
    })

@app.route('/api/bases/blacklist/<int:user_id>', methods=['DELETE'])
def remove_from_bases_blacklist(user_id: int):
    return remove_from_outreach_blacklist(user_id)

@app.route('/api/outreach/campaign/<int:campaign_id>/start', methods=['POST'])
def start_campaign(campaign_id):
    tenant_id = get_current_tenant_id()
    campaign = get_campaign_for_current_user(campaign_id)
    if not campaign:
        return jsonify({'error': 'Campaign not found'}), 404

    api_id, api_hash = get_api_credentials()
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400
    valid_accounts = filter_valid_outreach_accounts(campaign.get('accounts') or [])
    if not valid_accounts:
        return jsonify({'error': 'У кампании нет активных outreach-аккаунтов'}), 400
    capacity = _calculate_campaign_capacity(tenant_id, valid_accounts, campaign.get('strategy') or {})
    daily_limit_auto = int(capacity.get('safe_new_per_day') or 0)
    current_daily_limit = int(campaign.get('daily_limit') or 0)
    if valid_accounts != (campaign.get('accounts') or []) or current_daily_limit != daily_limit_auto:
        outreach.update_campaign(
            campaign_id=campaign_id,
            name=campaign['name'],
            message_template=campaign['message_template'],
            accounts=valid_accounts,
            daily_limit=daily_limit_auto,
            delay_min=int(campaign.get('delay_min') or 5),
            delay_max=int(campaign.get('delay_max') or 15),
            strategy=campaign.get('strategy') or {'steps': []},
            schedule=campaign.get('schedule'),
            tenant_id=tenant_id,
            base_id=campaign.get('base_id')
        )

    base_id = campaign.get('base_id')
    if not base_id:
        return jsonify({'error': 'Для кампании не выбрана база контактов'}), 400
    if not get_base_for_current_user(int(base_id)):
        return jsonify({'error': 'Выбранная база недоступна'}), 400
    outreach.sync_campaign_contacts_from_base(
        campaign_id=campaign_id,
        base_id=int(base_id),
        tenant_id=tenant_id,
        reset=False
    )

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*)
            FROM outreach_contacts
            WHERE campaign_id = ? AND tenant_id = ? AND status = 'new'
              AND (user_id IS NULL OR user_id NOT IN (SELECT user_id FROM outreach_blacklist WHERE tenant_id = ?))
              AND (user_id IS NOT NULL OR (username IS NOT NULL AND username != '') OR (phone IS NOT NULL AND phone != ''))
        ''', (campaign_id, tenant_id, tenant_id))
        contacts_ready = cursor.fetchone()[0] or 0
    if contacts_ready == 0:
        return jsonify({'error': 'Нет новых контактов с user_id/username/phone для рассылки'}), 400

    try:
        run_async(ensure_outreach_accounts_in_global_target_chat(tenant_id, account_phones=valid_accounts))
    except Exception as e:
        logger.warning(f"Could not ensure global target chat before campaign start #{campaign_id}: {e}")

    # Важно: сначала ставим active, чтобы первый тик планировщика сразу увидел кампанию.
    outreach.update_campaign_status(campaign_id, 'active', tenant_id=tenant_id)
    started_scheduler = ensure_scheduler_running()
    return jsonify({'status': 'started', 'scheduler_started': started_scheduler})

@app.route('/api/outreach/campaign/<int:campaign_id>/pause', methods=['POST'])
def pause_campaign(campaign_id):
    tenant_id = get_current_tenant_id()
    if not get_campaign_for_current_user(campaign_id):
        return jsonify({'error': 'Campaign not found'}), 404
    outreach.update_campaign_status(campaign_id, 'paused', tenant_id=tenant_id)
    if not has_active_campaigns():
        global scheduler, scheduler_thread
        if scheduler:
            scheduler.stop()
            scheduler = None
        scheduler_thread = None
    return jsonify({'status': 'paused'})

@app.route('/api/outreach/campaign/<int:campaign_id>/stop', methods=['POST'])
def stop_campaign(campaign_id):
    tenant_id = get_current_tenant_id()
    if not get_campaign_for_current_user(campaign_id):
        return jsonify({'error': 'Campaign not found'}), 404
    outreach.update_campaign_status(campaign_id, 'stopped', tenant_id=tenant_id)
    if not has_active_campaigns():
        global scheduler, scheduler_thread
        if scheduler:
            scheduler.stop()
            scheduler = None
        scheduler_thread = None
    return jsonify({'status': 'stopped'})

@app.route('/api/outreach/campaign/<int:campaign_id>/readiness', methods=['GET'])
def campaign_readiness(campaign_id):
    tenant_id = get_current_tenant_id()
    campaign = get_campaign_for_current_user(campaign_id)
    if not campaign:
        return jsonify({'error': 'Campaign not found'}), 404

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*),
                   SUM(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END),
                   SUM(CASE WHEN username IS NOT NULL AND username != '' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END)
            FROM outreach_contacts
            WHERE campaign_id = ? AND tenant_id = ?
        ''', (campaign_id, tenant_id))
        contacts_total, contacts_with_user_id, contacts_with_username, contacts_with_phone = cursor.fetchone()
        cursor.execute('''
            SELECT COUNT(*)
            FROM outreach_contacts
            WHERE campaign_id = ? AND tenant_id = ?
              AND user_id IS NOT NULL
              AND user_id IN (SELECT user_id FROM outreach_blacklist WHERE tenant_id = ?)
        ''', (campaign_id, tenant_id, tenant_id))
        blacklisted_contacts = cursor.fetchone()[0] or 0
        cursor.execute('''
            SELECT COUNT(*)
            FROM outreach_contacts
            WHERE campaign_id = ? AND tenant_id = ? AND status = 'new'
              AND (user_id IS NULL OR user_id NOT IN (SELECT user_id FROM outreach_blacklist WHERE tenant_id = ?))
              AND (user_id IS NOT NULL OR (username IS NOT NULL AND username != '') OR (phone IS NOT NULL AND phone != ''))
        ''', (campaign_id, tenant_id, tenant_id))
        new_contacts = cursor.fetchone()[0]
        base_contacts_total = 0
        base_id = int(campaign.get('base_id') or 0)
        if base_id > 0:
            cursor.execute(
                'SELECT COUNT(*) FROM outreach_base_contacts WHERE tenant_id = ? AND base_id = ?',
                (tenant_id, base_id)
            )
            base_contacts_total = cursor.fetchone()[0] or 0

    api_id, api_hash = get_api_credentials()
    has_api = bool(api_id and api_hash)
    valid_accounts = filter_valid_outreach_accounts(campaign.get('accounts') or [])
    capacity = _calculate_campaign_capacity(tenant_id, valid_accounts, campaign.get('strategy') or {})
    has_accounts = bool(valid_accounts)
    has_contacts = bool((contacts_total or 0) > 0 or (base_contacts_total or 0) > 0)
    has_targets = bool((new_contacts or 0) > 0 or (base_contacts_total or 0) > 0)
    scheduler_running = is_scheduler_running()

    now = datetime.now()
    schedule = campaign.get('schedule') or {}
    schedule_open = True
    schedule_reason = 'ok'
    if schedule:
        days = schedule.get('days') or []
        current_day = (now.weekday() + 1) % 7
        if days and current_day not in days:
            schedule_open = False
            schedule_reason = 'outside_days'
        else:
            start_time = schedule.get('start_time')
            end_time = schedule.get('end_time')
            if start_time and end_time:
                try:
                    now_t = now.time()
                    start_t = datetime.strptime(start_time, "%H:%M").time()
                    end_t = datetime.strptime(end_time, "%H:%M").time()
                    if start_t <= end_t:
                        schedule_open = start_t <= now_t <= end_t
                    else:
                        schedule_open = now_t >= start_t or now_t <= end_t
                    if not schedule_open:
                        schedule_reason = 'outside_time_window'
                except ValueError:
                    schedule_reason = 'invalid_time_format'

    return jsonify({
        'campaign_id': campaign_id,
        'campaign_status': campaign.get('status'),
        'scheduler_running': scheduler_running,
        'has_api_credentials': has_api,
        'has_accounts': has_accounts,
        'accounts_selected': len(campaign.get('accounts') or []),
        'accounts_valid': len(valid_accounts),
        'followup_steps': int(capacity.get('followup_steps') or 0),
        'waves_required': int(capacity.get('waves_required') or 1),
        'safe_new_per_day': int(capacity.get('safe_new_per_day') or 0),
        'primary_accounts_per_day': int(capacity.get('primary_accounts_per_day') or 0),
        'shortage_for_rotation': int(capacity.get('shortage_for_rotation') or 0),
        'accounts_to_add_for_next_step': int(capacity.get('accounts_to_add_for_next_step') or 0),
        'contacts_total': contacts_total or 0,
        'contacts_with_user_id': contacts_with_user_id or 0,
        'contacts_with_username': contacts_with_username or 0,
        'contacts_with_phone': contacts_with_phone or 0,
        'contacts_blacklisted': blacklisted_contacts,
        'base_id': campaign.get('base_id'),
        'base_contacts_total': int(base_contacts_total or 0),
        'new_contacts_ready': new_contacts or 0,
        'schedule_open_now': schedule_open,
        'schedule_reason': schedule_reason,
        'ready_to_send_now': all([has_api, has_accounts, has_targets, schedule_open])
    })

@app.route('/api/outreach/campaign/<int:campaign_id>/stats')
def get_campaign_stats(campaign_id):
    """Получение расширенной статистики кампании"""
    tenant_id = get_current_tenant_id()
    if not get_campaign_for_current_user(campaign_id):
        return jsonify({'error': 'Campaign not found'}), 404
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT sent_count, reply_count, meeting_count, total_contacts
                FROM outreach_campaigns WHERE id = ? AND tenant_id = ?
            ''', (campaign_id, tenant_id))
            row = cursor.fetchone()
            
            cursor.execute('''
                SELECT date(sent_at) as day, COUNT(*) as count
                FROM conversations
                WHERE campaign_id = ? AND tenant_id = ? AND direction = 'outgoing'
                GROUP BY date(sent_at)
                ORDER BY day DESC LIMIT 7
            ''', (campaign_id, tenant_id))
            daily = cursor.fetchall()
            
            cursor.execute('''
                SELECT account_used, COUNT(*) as count
                FROM conversations
                WHERE campaign_id = ? AND tenant_id = ? AND direction = 'outgoing'
                GROUP BY account_used
            ''', (campaign_id, tenant_id))
            by_account = cursor.fetchall()
            
            cursor.execute('''
                SELECT 
                    COUNT(CASE WHEN status != 'new' THEN 1 END) as sent,
                    COUNT(CASE WHEN status = 'replied' THEN 1 END) as replied,
                    COUNT(CASE WHEN status = 'interested' THEN 1 END) as interested
                FROM outreach_contacts
                WHERE campaign_id = ? AND tenant_id = ?
            ''', (campaign_id, tenant_id))
            funnel = cursor.fetchone()
            
        return jsonify({
            'sent': row[0] if row else 0,
            'replies': row[1] if row else 0,
            'meetings': row[2] if row else 0,
            'total': row[3] if row else 0,
            'daily': [{'date': d[0], 'count': d[1]} for d in daily],
            'by_account': [{'account': a[0], 'count': a[1]} for a in by_account],
            'funnel': {
                'sent': funnel[0] if funnel else 0,
                'replied': funnel[1] if funnel else 0,
                'interested': funnel[2] if funnel else 0,
                'conversion': round(funnel[1]/funnel[0]*100 if funnel[0] > 0 else 0, 1)
            }
        })
    except Exception as e:
        logger.error(f"Error getting campaign stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/outreach/campaign/<int:campaign_id>/step-report', methods=['GET'])
def campaign_step_report(campaign_id):
    tenant_id = get_current_tenant_id()
    campaign = get_campaign_for_current_user(campaign_id)
    if not campaign:
        return jsonify({'error': 'Campaign not found'}), 404

    steps = [{'step_id': 1, 'title': 'Первое сообщение'}]
    raw_steps = ((campaign.get('strategy') or {}).get('steps') or [])
    for step in sorted(raw_steps, key=lambda s: int(s.get('id', 0) or 0)):
        sid = int(step.get('id', 0) or 0)
        if sid >= 2:
            steps.append({'step_id': sid, 'title': f'Фоллоуап #{sid - 1}'})

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT x.step_id, oc.status, COUNT(*)
            FROM outreach_contacts oc
            JOIN (
                SELECT c.contact_id, c.step_id
                FROM conversations c
                WHERE c.campaign_id = ?
                  AND c.tenant_id = ?
                  AND c.direction = 'outgoing'
                  AND c.id = (
                      SELECT c2.id
                      FROM conversations c2
                      WHERE c2.campaign_id = c.campaign_id
                        AND c2.contact_id = c.contact_id
                        AND c2.direction = 'outgoing'
                        AND c2.tenant_id = c.tenant_id
                      ORDER BY datetime(c2.sent_at) DESC, c2.id DESC
                      LIMIT 1
                  )
            ) x ON x.contact_id = oc.id
            WHERE oc.campaign_id = ?
              AND oc.tenant_id = ?
            GROUP BY x.step_id, oc.status
        ''', (campaign_id, tenant_id, campaign_id, tenant_id))
        grouped = cursor.fetchall()

        cursor.execute('''
            SELECT step_id, COUNT(*)
            FROM conversations
            WHERE campaign_id = ?
              AND tenant_id = ?
              AND direction = 'outgoing'
              AND step_id IS NOT NULL
            GROUP BY step_id
        ''', (campaign_id, tenant_id))
        outgoing_counts = {int(r[0]): int(r[1]) for r in cursor.fetchall() if r[0] is not None}

    by_step = {}
    for step_id, status, count in grouped:
        sid = int(step_id or 0)
        by_step.setdefault(sid, {'sent': 0, 'ignored': 0, 'replied': 0, 'failed': 0})
        if status in by_step[sid]:
            by_step[sid][status] += int(count or 0)

    rows = []
    for step in steps:
        sid = step['step_id']
        stat = by_step.get(sid, {'sent': 0, 'ignored': 0, 'replied': 0, 'failed': 0})
        rows.append({
            'step_id': sid,
            'title': step['title'],
            'outgoing_events': int(outgoing_counts.get(sid, 0)),
            'sent': int(stat['sent']),
            'ignored': int(stat['ignored']),
            'replied': int(stat['replied']),
            'failed': int(stat['failed'])
        })

    return jsonify({'campaign_id': campaign_id, 'rows': rows})

@app.route('/api/outreach/campaign/<int:campaign_id>/step-report/export', methods=['GET'])
def export_campaign_step_report(campaign_id):
    fmt = (request.args.get('format') or 'csv').strip().lower()
    if fmt not in ('csv', 'xlsx'):
        return jsonify({'error': 'format должен быть csv или xlsx'}), 400

    report_resp = campaign_step_report(campaign_id)
    payload = report_resp.get_json(silent=True) or {}
    if payload.get('error'):
        return jsonify(payload), 404
    rows = payload.get('rows') or []

    df = pd.DataFrame(rows, columns=['step_id', 'title', 'outgoing_events', 'sent', 'ignored', 'replied', 'failed'])
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_name = f"campaign_{campaign_id}_step_report_{ts}"

    if fmt == 'xlsx':
        try:
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='step_report')
            out.seek(0)
            return send_file(
                out,
                as_attachment=True,
                download_name=f"{base_name}.xlsx",
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        except Exception as e:
            return jsonify({'error': f'Не удалось сформировать XLSX: {e}'}), 500

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(df.columns.tolist())
    for row in df.itertuples(index=False):
        writer.writerow(list(row))
    mem = io.BytesIO(out.getvalue().encode('utf-8-sig'))
    mem.seek(0)
    return send_file(
        mem,
        as_attachment=True,
        download_name=f"{base_name}.csv",
        mimetype='text/csv'
    )

@app.route('/api/outreach/templates', methods=['GET'])
def get_templates():
    tenant_id = get_current_tenant_id()
    return jsonify(outreach.get_templates(tenant_id=tenant_id))

@app.route('/api/outreach/templates', methods=['POST'])
def save_template():
    tenant_id = get_current_tenant_id()
    data = request.json
    outreach.save_template(data['name'], data['text'], tenant_id=tenant_id)
    return jsonify({'status': 'ok'})

@app.route('/api/outreach/spintax/preview', methods=['POST'])
def preview_spintax():
    data = request.get_json(silent=True) or {}
    text = str(data.get('text') or '')
    try:
        samples = int(data.get('samples', 5))
    except (TypeError, ValueError):
        return jsonify({'error': 'samples должен быть числом'}), 400
    samples = max(1, min(samples, 20))

    try:
        variants = [render_spintax(text) for _ in range(samples)]
    except ValueError as e:
        return jsonify({'error': f'Ошибка спинтакса: {str(e)}'}), 400

    return jsonify({'samples': samples, 'variants': variants})

@app.route('/api/outreach/upload', methods=['POST'])
def upload_contacts():
    tenant_id = get_current_tenant_id()
    if 'file' not in request.files:
        return jsonify({'error': 'No file'}), 400
    
    file = request.files['file']
    campaign_id = request.form.get('campaign_id')
    
    if not campaign_id:
        return jsonify({'error': 'No campaign ID'}), 400
    try:
        campaign_id_int = int(campaign_id)
    except (TypeError, ValueError):
        return jsonify({'error': 'Invalid campaign ID'}), 400
    if not get_campaign_for_current_user(campaign_id_int):
        return jsonify({'error': 'Campaign not found'}), 404
    
    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    
    try:
        validation = outreach.validate_contacts_file(filepath)
        if not validation.get('ok'):
            return jsonify({'error': validation.get('error') or 'Файл не прошёл валидацию'}), 400
        result = outreach.import_contacts(campaign_id_int, filepath, tenant_id=tenant_id)
        result['validation'] = validation
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)


# ===== ЭНДПОИНТЫ ДЛЯ ПЛАНИРОВЩИКА =====

@app.route('/api/outreach/scheduler/start', methods=['POST'])
def start_scheduler():
    """Запуск планировщика рассылок"""
    if is_scheduler_running():
        return jsonify({'error': 'Scheduler already running'}), 400
    ensure_scheduler_running()
    return jsonify({'status': 'started'})

@app.route('/api/outreach/scheduler/stop', methods=['POST'])
def stop_scheduler():
    """Остановка планировщика"""
    global scheduler, scheduler_thread
    
    if scheduler:
        scheduler.stop()
        scheduler = None
    scheduler_thread = None
    
    return jsonify({'status': 'stopped'})

@app.route('/api/outreach/scheduler/status', methods=['GET'])
def scheduler_status():
    return jsonify({'running': is_scheduler_running()})

@app.route('/api/accounts/<path:phone>/update-type', methods=['POST'])
def update_account_type(phone):
    """Обновление типа аккаунта"""
    data = request.json
    account_type = data.get('account_type')
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    
    allowed_types = ['checker', 'outreach']
    if is_admin_user(user):
        allowed_types.append('service')
    if account_type not in allowed_types:
        return jsonify({'error': 'Invalid account type'}), 400
    
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            if is_admin_user(user):
                cursor.execute('''
                    UPDATE account_stats 
                    SET account_type = ? 
                    WHERE tenant_id = ? AND phone = ?
                ''', (account_type, tenant_id, phone))
            else:
                cursor.execute('''
                    UPDATE account_stats
                    SET account_type = ?
                    WHERE tenant_id = ? AND phone = ? AND added_by_user_id = ?
                ''', (account_type, tenant_id, phone, user_id))
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({'error': 'Account not found'}), 404
        joined = None
        if account_type == 'outreach':
            try:
                joined = run_async(ensure_outreach_accounts_in_global_target_chat(tenant_id, account_phones=[phone]))
            except Exception as e:
                logger.warning(f"Failed to ensure global target chat for {phone}: {e}")
        response = {'status': 'updated'}
        if joined is not None:
            response['target_chat_sync'] = joined
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error updating account type: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/accounts/delete', methods=['POST'])
def delete_accounts():
    data = request.get_json(silent=True) or {}
    phones = data.get('phones') or []
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    if not phones:
        return jsonify({'error': 'Не переданы аккаунты для удаления'}), 400

    deleted = []
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()

            placeholders = ",".join("?" for _ in phones)
            cursor.execute(
                f"SELECT phone, session_name FROM account_stats WHERE tenant_id = ? AND phone IN ({placeholders})"
                + ("" if is_admin_user(user) else " AND added_by_user_id = ?"),
                ([tenant_id, *phones] if is_admin_user(user) else [tenant_id, *phones, user_id])
            )
            existing = cursor.fetchall()
            existing_map = {row[0]: row[1] for row in existing}

            # Удаляем ссылки на аккаунты из кампаний.
            if is_admin_user(user):
                cursor.execute("SELECT id, accounts FROM outreach_campaigns WHERE tenant_id = ?", (tenant_id,))
            else:
                cursor.execute("SELECT id, accounts FROM outreach_campaigns WHERE tenant_id = ? AND created_by_user_id = ?", (tenant_id, user_id))
            for campaign_id, accounts_json in cursor.fetchall():
                try:
                    acc = json.loads(accounts_json) if accounts_json else []
                except Exception:
                    acc = []
                new_acc = [a for a in acc if a not in phones]
                if new_acc != acc:
                    cursor.execute(
                        "UPDATE outreach_campaigns SET accounts = ? WHERE tenant_id = ? AND id = ?",
                        (json.dumps(new_acc), tenant_id, campaign_id)
                    )

            for phone in phones:
                session_name = existing_map.get(phone)
                if not session_name:
                    continue

                cursor.execute("DELETE FROM account_fingerprints WHERE tenant_id = ? AND account_phone = ?", (tenant_id, phone))
                cursor.execute("DELETE FROM account_stats WHERE tenant_id = ? AND phone = ?", (tenant_id, phone))

                session_path = os.path.join('sessions', f'{session_name}.session')
                journal_path = f"{session_path}-journal"
                if os.path.exists(session_path):
                    os.remove(session_path)
                if os.path.exists(journal_path):
                    os.remove(journal_path)

                deleted.append(phone)

            conn.commit()
    except Exception as e:
        logger.error(f"Error deleting accounts: {e}")
        return jsonify({'error': str(e)}), 500

    return jsonify({'status': 'deleted', 'deleted': deleted, 'count': len(deleted)})

@app.route('/api/accounts/<path:phone>/outreach-limit', methods=['POST'])
def update_outreach_limit(phone):
    """Обновление суточного лимита для outreach аккаунта"""
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    user_id = int(user.get('id') or 0)
    data = request.get_json(silent=True) or {}
    try:
        outreach_daily_limit = int(data.get('outreach_daily_limit'))
    except (TypeError, ValueError):
        return jsonify({'error': 'outreach_daily_limit должен быть числом'}), 400

    if outreach_daily_limit < 1 or outreach_daily_limit > 500:
        return jsonify({'error': 'outreach_daily_limit должен быть в диапазоне 1..500'}), 400

    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE account_stats
                SET outreach_daily_limit = ?
                WHERE tenant_id = ? AND phone = ? AND account_type = 'outreach'
            ''' + ('' if is_admin_user(user) else ' AND added_by_user_id = ?'),
                (outreach_daily_limit, tenant_id, phone) if is_admin_user(user) else (outreach_daily_limit, tenant_id, phone, user_id))
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({'error': 'Аккаунт не найден или не outreach'}), 404
        return jsonify({'status': 'updated', 'phone': phone, 'outreach_daily_limit': outreach_daily_limit})
    except Exception as e:
        logger.error(f"Error updating outreach limit: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/outreach/campaign/<int:campaign_id>/schedule', methods=['POST'])
def save_campaign_schedule(campaign_id):
    """Сохранение расписания для кампании"""
    data = request.get_json(silent=True) or {}
    tenant_id = get_current_tenant_id()
    campaign = get_campaign_for_current_user(campaign_id)
    if not campaign:
        return jsonify({'error': 'Campaign not found'}), 404
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid schedule payload'}), 400
    followup_steps_count = _count_followup_steps(campaign.get('strategy') or {})
    normalized_schedule = _normalize_campaign_schedule(data, followup_steps_count)

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE outreach_campaigns 
            SET schedule = ? 
            WHERE id = ? AND tenant_id = ?
        ''', (json.dumps(normalized_schedule), campaign_id, tenant_id))
        conn.commit()

    return jsonify({'status': 'saved', 'schedule': normalized_schedule})


@socketio.on('connect')
def handle_connect():
    emit('connected', {'data': 'Connected to server'})

@app.route('/health')
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    acquire_single_process_lock()
    print("=" * 50)
    print("🚀 воронка запущена!")
    print("=" * 50)
    print("📱 Проверка номеров: http://localhost:5001")
    print("📨 Аутрич кампании: http://localhost:5001/outreach")
    print("👥 Управление аккаунтами: http://localhost:5001/accounts")
    print("🌐 Управление прокси: http://localhost:5001/proxies")
    print("=" * 50)
    auto_start_scheduler = os.getenv('AUTO_START_SCHEDULER', '0') == '1'
    if auto_start_scheduler and has_active_campaigns():
        started = ensure_scheduler_running()
        if started:
            print("▶ Планировщик рассылок автозапущен (найдены активные кампании)")
        else:
            print("ℹ Планировщик рассылок уже запущен")
    else:
        print("ℹ Автостарт планировщика отключен (AUTO_START_SCHEDULER=1 чтобы включить)")
    if BILLING_BOT_POLLING_ENABLED:
        if ensure_billing_worker_running():
            print("▶ Billing worker запущен (Telegram Stars)")
        else:
            print("ℹ Billing worker не запущен (проверьте bot token)")
    debug_mode = os.getenv('FLASK_DEBUG', '0') == '1'
    port = int(os.getenv('PORT', '5001'))
    socketio.run(
        app,
        debug=debug_mode,
        host='0.0.0.0',
        port=port,
        allow_unsafe_werkzeug=debug_mode
    )
