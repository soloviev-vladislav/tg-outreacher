from flask import Flask, render_template, request, jsonify, send_file, Response, session, redirect, url_for, g
from flask_socketio import SocketIO, emit
import asyncio
import threading
import os
import json
import sqlite3
import io
import subprocess
from datetime import datetime, date
from werkzeug.utils import secure_filename
from werkzeug.security import check_password_hash, generate_password_hash
import pandas as pd
from telethon import TelegramClient, errors
from telegram_bot import TelegramAccountManager, Database, OutreachManager
import logging
import time
import sys
import atexit
import csv
import uuid

try:
    import fcntl
except Exception:
    fcntl = None

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

# Фиксируем постоянный чат визиток для рассылок (можно переопределить через UI/API).
if not db.get_config('outreach_target_chat'):
    db.set_config('outreach_target_chat', 'aihubco')

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
        cursor.execute("PRAGMA table_info(conversations)")
        conv_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in conv_columns:
            cursor.execute('ALTER TABLE conversations ADD COLUMN tenant_id INTEGER DEFAULT 1')
        cursor.execute("PRAGMA table_info(account_stats)")
        account_columns = [col[1] for col in cursor.fetchall()]
        if 'outreach_daily_limit' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN outreach_daily_limit INTEGER DEFAULT 20')
        if 'tenant_id' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN tenant_id INTEGER DEFAULT 1')
        if 'added_by_user_id' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN added_by_user_id INTEGER')
        cursor.execute("PRAGMA table_info(proxies)")
        proxy_columns = [col[1] for col in cursor.fetchall()]
        if 'tenant_id' not in proxy_columns:
            cursor.execute('ALTER TABLE proxies ADD COLUMN tenant_id INTEGER DEFAULT 1')
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
            'outreach_templates', 'outreach_blacklist', 'scheduler_campaign_locks'
            , 'outreach_bases', 'outreach_base_contacts'
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
                INSERT INTO users (tenant_id, username, password_hash, role, is_active)
                VALUES (?, ?, ?, 'admin', 1)
            ''', (default_tenant_id, admin_username, generate_password_hash(admin_password)))
            logger.warning("Created default admin user for multi-user mode: username='%s'", admin_username)

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_outreach_contacts_campaign_status ON outreach_contacts(campaign_id, status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_contact_direction_sent_at ON conversations(contact_id, direction, sent_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_campaign_direction_sent_at ON conversations(campaign_id, direction, sent_at)')
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
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_account_stats_added_by ON account_stats(tenant_id, added_by_user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_campaigns_created_by ON outreach_campaigns(tenant_id, created_by_user_id)')
        cursor.execute("UPDATE users SET role = 'user' WHERE role = 'viewer'")
        cursor.execute("UPDATE users SET role = 'admin' WHERE role = 'owner'")
        conn.commit()

ensure_db_schema()

# Импортируем новые менеджеры
from proxy_manager import ProxyManager
from fingerprint_manager import FingerprintManager
from outreach_scheduler import OutreachScheduler, render_spintax

proxy_manager = ProxyManager()
fingerprint_manager = FingerprintManager()
DEFAULT_API_ID = os.getenv('TELEGRAM_API_ID', '').strip()
DEFAULT_API_HASH = os.getenv('TELEGRAM_API_HASH', '').strip()
APP_PASSWORD = os.getenv('APP_PASSWORD', '').strip()
AUTH_EXEMPT_PATHS = {
    '/health',
    '/login',
    '/register',
    '/logout',
    '/api/auth/login',
    '/api/auth/me',
    '/api/auth/register'
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
        done = threading.Event()

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

        threading.Thread(target=_runner, daemon=True).start()
        done.wait()
        if result_holder['error']:
            raise result_holder['error']
        return result_holder['result']

    return asyncio.run(coro)

def run_async_threadsafe(coro):
    """Всегда выполняет coroutine в отдельном потоке со своим event loop."""
    result_holder = {'result': None, 'error': None}
    done = threading.Event()

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

    threading.Thread(target=_runner, daemon=True).start()
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
            SELECT id, tenant_id, username, role, is_active
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
        'is_active': bool(row[4])
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

@app.context_processor
def inject_current_user():
    return {'current_user': get_current_user()}

@app.before_request
def enforce_session_auth():
    if _is_auth_exempt_path(request.path):
        return None

    user = get_current_user()
    if user:
        g.current_user = user
        g.tenant_id = int(user['tenant_id'])
        if is_limited_user(user):
            forbidden_prefixes = (
                '/api/auth/users',
                '/api/admin/',
                '/api/proxies',
                '/api/outreach/scheduler/'
            )
            if any(request.path.startswith(p) for p in forbidden_prefixes):
                return jsonify({'error': 'Forbidden'}), 403
        return None

    # Fallback: если ранее использовали APP_PASSWORD, разрешаем basic auth и создаём сессию.
    if APP_PASSWORD:
        auth = request.authorization
        if auth and auth.password == APP_PASSWORD:
            tenant_id = get_default_tenant_id()
            session['user_id'] = 0
            session['tenant_id'] = tenant_id
            g.current_user = {
                'id': 0,
                'tenant_id': tenant_id,
                'username': auth.username or 'basic-auth',
                'role': 'admin',
                'is_active': True
            }
            g.tenant_id = tenant_id
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
    'registered': 0,
    'not_registered': 0,
    'errors': 0,
    'test_checks': 0,
    'accounts': [],
    'start_time': None,
    'estimated_time': None
}

qr_auth_sessions = {}
qr_auth_lock = threading.Lock()

scheduler_thread = None
scheduler = None
last_scheduler_autostart_check_at = 0.0
SCHEDULER_AUTOSTART_CHECK_INTERVAL_SECONDS = 10

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

def has_active_campaigns():
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM outreach_campaigns WHERE status = 'active'")
            return (cursor.fetchone()[0] or 0) > 0
    except Exception as e:
        logger.error(f"Failed to check active campaigns: {e}")
        return False

class TelegramCheckerThread(threading.Thread):
    def __init__(self, api_id, api_hash, target_chat, phones_list, delay=2):
        threading.Thread.__init__(self)
        self.api_id = api_id
        self.api_hash = api_hash
        self.target_chat = target_chat
        self.phones_list = phones_list
        self.delay = delay
        self.loop = None
        self.processed_count = 0
        
    def run(self):
        global checker_status
        try:
            # В окружении gunicorn/eventlet поток может оказаться с уже активным event loop.
            # В этом случае запускаем checker как задачу существующего loop.
            try:
                running_loop = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None

            if running_loop and running_loop.is_running():
                logger.warning("Checker thread: detected active event loop, scheduling run_checker as task")
                running_loop.create_task(self.run_checker())
                return

            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.run_checker())
        except Exception as e:
            logger.exception(f"Checker thread crashed: {e}")
            checker_status['running'] = False
            socketio.emit('error', {'message': f'Ошибка запуска проверки: {str(e)}'})
            socketio.emit('checker_stopped')
        finally:
            if self.loop:
                try:
                    self.loop.close()
                except Exception:
                    pass
        
    async def run_checker(self):
        global checker_status
        
        manager = TelegramAccountManager(self.api_id, self.api_hash, 'sessions')
        
        if not await manager.load_accounts(self.target_chat):
            socketio.emit('error', {'message': 'Не удалось загрузить аккаунты'})
            checker_status['running'] = False
            return
        
        # Защита от дубликатов
        successfully_found = manager.db.get_successfully_found_phones()
        original_count = len(self.phones_list)
        self.phones_list = [p for p in self.phones_list if p not in successfully_found]
        skipped_found = original_count - len(self.phones_list)
        
        if skipped_found > 0:
            logger.info(f"⏭️ Пропущено {skipped_found} номеров - уже были найдены ранее")
            socketio.emit('warning', {
                'message': f'{skipped_found} номеров уже были найдены и будут пропущены'
            })
        
        if not self.phones_list:
            socketio.emit('error', {'message': 'Все номера уже были успешно найдены ранее'})
            checker_status['running'] = False
            return
        
        checker_status['accounts'] = []
        for client, stats in manager.accounts:
            checker_status['accounts'].append({
                'phone': stats.phone,
                'checked_today': stats.checked_today,
                'total_checked': stats.total_checked,
                'can_use': stats.can_use_today()
            })
        
        socketio.emit('accounts_update', checker_status['accounts'])
        
        results = {
            'registered': [],
            'not_registered': [],
            'errors': [],
            'invalid': []
        }
        
        processed = 0
        total = len(self.phones_list)
        checker_status['total'] = total
        checker_status['start_time'] = datetime.now().isoformat()
        test_counter = 0
        
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
                    checker_status['accounts'].append({
                        'phone': stats.phone,
                        'checked_today': stats.checked_today,
                        'total_checked': stats.total_checked,
                        'can_use': stats.can_use_today()
                    })
                socketio.emit('accounts_update', checker_status['accounts'])
                
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
                except:
                    checker_status['estimated_time'] = None
            
            socketio.emit('progress_update', checker_status)
            
            account_data = await manager.get_next_account()
            if not account_data:
                socketio.emit('warning', {'message': 'Нет доступных аккаунтов'})
                break
            
            client, stats = account_data
            
            result = await manager.check_phone(client, stats, phone, is_test)
            
            if result is None:
                results['errors'].append(phone)
                checker_status['errors'] += 1
                socketio.emit('phone_result', {
                    'phone': phone,
                    'status': 'error',
                    'message': 'Ошибка проверки'
                })
            elif 'error' in result:
                results['invalid'].append(phone)
                socketio.emit('phone_result', {
                    'phone': phone,
                    'status': 'invalid',
                    'message': f"Ошибка: {result['error']}"
                })
            elif result['registered']:
                results['registered'].append(result)
                checker_status['registered'] += 1
                
                await manager.send_contact_card(client, self.target_chat, result)
                
                socketio.emit('phone_result', {
                    'phone': phone,
                    'status': 'registered',
                    'is_test': result.get('is_test', False),
                    'user_info': {
                        'user_id': result['user_id'],
                        'username': result.get('username'),
                        'first_name': result.get('first_name'),
                        'last_name': result.get('last_name')
                    }
                })
            else:
                results['not_registered'].append(phone)
                checker_status['not_registered'] += 1
                socketio.emit('phone_result', {
                    'phone': phone,
                    'status': 'not_registered',
                    'is_test': result.get('is_test', False),
                    'message': 'Не зарегистрирован'
                })
            
            await asyncio.sleep(self.delay)
            
            checker_status['accounts'] = []
            for client, stats in manager.accounts:
                checker_status['accounts'].append({
                    'phone': stats.phone,
                    'checked_today': stats.checked_today,
                    'total_checked': stats.total_checked,
                    'can_use': stats.can_use_today()
                })
            socketio.emit('accounts_update', checker_status['accounts'])
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if results['registered']:
            df_reg = pd.DataFrame(results['registered'])
            filename = f'registered_{timestamp}.csv'
            filepath = os.path.join(app.config['RESULTS_FOLDER'], filename)
            df_reg.to_csv(filepath, index=False, encoding='utf-8')
            socketio.emit('file_ready', {'filename': filename, 'type': 'registered'})
        
        if results['not_registered']:
            filename = f'not_registered_{timestamp}.txt'
            filepath = os.path.join(app.config['RESULTS_FOLDER'], filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(results['not_registered']))
            socketio.emit('file_ready', {'filename': filename, 'type': 'not_registered'})
        
        if results['errors']:
            filename = f'errors_{timestamp}.txt'
            filepath = os.path.join(app.config['RESULTS_FOLDER'], filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(results['errors']))
            socketio.emit('file_ready', {'filename': filename, 'type': 'errors'})
        
        socketio.emit('checker_complete', {
            'registered': len(results['registered']),
            'not_registered': len(results['not_registered']),
            'errors': len(results['errors']),
            'invalid': len(results['invalid']),
            'test_checks': checker_status['test_checks']
        })
        
        await manager.close_all()
        checker_status['running'] = False
        socketio.emit('checker_stopped')


# ===== ОСНОВНЫЕ ЭНДПОИНТЫ =====

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/outreach')
def outreach_page():
    return render_template('outreach.html')

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
    return render_template('login.html')

@app.route('/register')
def register_page():
    if get_current_user():
        return redirect(url_for('index'))
    return render_template('register.html')

@app.route('/logout', methods=['GET', 'POST'])
def logout_page():
    session.clear()
    if request.path.startswith('/api/'):
        return jsonify({'status': 'ok'})
    return redirect(url_for('login_page'))

@app.route('/api/auth/login', methods=['POST'])
def api_login():
    data = request.get_json(silent=True) or {}
    username = str(data.get('username') or '').strip()
    password = str(data.get('password') or '')
    if not username or not password:
        return jsonify({'error': 'username and password are required'}), 400

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, tenant_id, username, password_hash, role, is_active
            FROM users
            WHERE username = ?
        ''', (username,))
        row = cursor.fetchone()

    if not row:
        return jsonify({'error': 'Invalid credentials'}), 401
    if not bool(row[5]):
        return jsonify({'error': 'User is inactive'}), 403
    if not check_password_hash(row[3], password):
        return jsonify({'error': 'Invalid credentials'}), 401

    session['user_id'] = int(row[0])
    session['tenant_id'] = int(row[1])
    raw_role = str(row[4] or '').strip().lower()
    role = 'admin' if raw_role in ('admin', 'owner') else 'user'
    return jsonify({
        'status': 'ok',
        'user': {
            'id': int(row[0]),
            'tenant_id': int(row[1]),
            'username': row[2],
            'role': role
        }
    })

@app.route('/api/auth/register', methods=['POST'])
def api_register():
    data = request.get_json(silent=True) or {}
    username = str(data.get('username') or '').strip()
    password = str(data.get('password') or '')

    if not username:
        return jsonify({'error': 'username is required'}), 400
    if len(password) < 6:
        return jsonify({'error': 'password must be at least 6 chars'}), 400
    if ' ' in username:
        return jsonify({'error': 'username must not contain spaces'}), 400

    tenant_id = get_default_tenant_id()
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM users WHERE username = ?', (username,))
        if cursor.fetchone():
            return jsonify({'error': 'Username already exists'}), 409
        cursor.execute('''
            INSERT INTO users (tenant_id, username, password_hash, role, is_active)
            VALUES (?, ?, ?, 'user', 0)
        ''', (tenant_id, username, generate_password_hash(password)))
        conn.commit()

    return jsonify({
        'status': 'pending_approval',
        'message': 'Registration submitted. Wait for admin approval.'
    })

@app.route('/api/auth/me', methods=['GET'])
def api_me():
    user = get_current_user()
    if not user:
        return jsonify({'authenticated': False}), 200
    return jsonify({'authenticated': True, 'user': user})

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
    return jsonify(checker_status)

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
                           f.device_model, f.app_version, f.system_version, f.lang_code
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
                           f.device_model, f.app_version, f.system_version, f.lang_code
                    FROM account_stats a
                    LEFT JOIN account_fingerprints f ON a.phone = f.account_phone
                    WHERE a.tenant_id = ?
                ''' + ('' if is_admin_user(user) else ' AND a.added_by_user_id = ?'), (tenant_id,) if is_admin_user(user) else (tenant_id, user_id))
            
            for row in cursor.fetchall():
                checked_today = row[11] or 0
                last_check_date = row[12]
                if last_check_date != date.today().isoformat():
                    checked_today = 0
                accounts.append({
                    'phone': row[0],
                    'session_name': row[1],
                    'account_type': row[2] or 'checker',
                    'total_checked': row[3] or 0,
                    'test_quality': row[4] or 'unknown',
                    'last_check': row[5],
                    'is_banned': bool(row[6]),
                    'is_frozen': bool(row[7]),
                    'proxy_id': row[8],
                    'test_history': json.loads(row[9]) if row[9] else [],
                    'last_used': row[10],
                    'checked_today': checked_today,
                    'last_check_date': last_check_date,
                    'outreach_daily_limit': row[13] or 20,
                    'fingerprint': {
                        'device_model': row[14],
                        'app_version': row[15],
                        'system_version': row[16],
                        'lang_code': row[17]
                    } if len(row) > 14 and row[14] else None
                })
    except Exception as e:
        logger.error(f"Error in get_accounts_detailed: {e}")
        return jsonify({'error': str(e)}), 500
    
    return jsonify(accounts)

def _register_account_session(tenant_id: int, actor_id: int, phone: str, session_name: str):
    """Регистрирует session в account_stats, если аккаунта ещё нет."""
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT phone, session_name FROM account_stats WHERE tenant_id = ? AND phone = ?', (tenant_id, phone))
        existing = cursor.fetchone()
        if existing:
            return {
                'status': 'exists',
                'phone': existing[0],
                'session_name': existing[1] or session_name,
                'message': 'Аккаунт уже зарегистрирован'
            }

        cursor.execute('''
            INSERT INTO account_stats
            (tenant_id, phone, session_name, account_type, is_banned, is_frozen, checked_today, last_check_date, added_by_user_id)
            VALUES (?, ?, ?, 'checker', 0, 0, 0, ?, ?)
        ''', (tenant_id, phone, session_name, date.today().isoformat(), actor_id if actor_id > 0 else None))
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
    started_ok = False
    for _ in range(30):
        state = _read_qr_worker_state(state_path)
        qr_url = str(state.get('qr_url') or '').strip()
        if qr_url:
            started_ok = True
            break
        if state.get('status') in ('error', 'expired'):
            break
        time.sleep(0.1)

    if not started_ok:
        try:
            os.kill(proc.pid, 15)
        except Exception:
            pass
        state = _read_qr_worker_state(state_path)
        message = state.get('message') or state.get('error') or 'Не удалось получить QR-код'
        return jsonify({'error': message}), 500

    with qr_auth_lock:
        qr_auth_sessions[token] = {
            'status': 'pending',
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
            'message': ''
        }

    return jsonify({
        'status': 'pending',
        'token': token,
        'qr_url': qr_url,
        'session_name': session_name
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
                str(rec.get('session_name') or '')
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
            str(rec.get('session_name') or '')
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
            
            async def get_phone():
                client = None
                try:
                    client = TelegramClient(f'sessions/{session_name}', int(api_id), api_hash)
                    await client.connect()
                    if await client.is_user_authorized():
                        me = await client.get_me()
                        return me.phone
                    return None
                finally:
                    if client:
                        await client.disconnect()
            
            phone = run_async(get_phone())
            
            if phone:
                logger.info(f"Получен номер {phone} для сессии {session_name}")
                
                # Вставляем в БД
                with sqlite3.connect(db.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT OR IGNORE INTO account_stats 
                        (tenant_id, phone, session_name, account_type, is_banned, is_frozen, checked_today, last_check_date, added_by_user_id)
                        VALUES (?, ?, ?, 'checker', 0, 0, 0, ?, ?)
                    ''', (tenant_id, phone, session_name, date.today().isoformat(), actor_id if actor_id > 0 else None))
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
        phone = payload.get('phone') if payload.get('ok') else None

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
                conn.commit()
                return jsonify({
                    'status': 'exists',
                    'phone': phone,
                    'session_name': session_name,
                    'message': 'Аккаунт уже зарегистрирован'
                }), 200

            cursor.execute('''
                INSERT INTO account_stats
                (tenant_id, phone, session_name, account_type, is_banned, is_frozen, checked_today, last_check_date, added_by_user_id)
                VALUES (?, ?, ?, 'checker', 0, 0, 0, ?, ?)
            ''', (tenant_id, phone, session_name, date.today().isoformat(), actor_id if actor_id > 0 else None))
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
    
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            for phone in phones:
                if is_admin_user(user):
                    cursor.execute('''
                        UPDATE account_stats SET proxy_id = ? WHERE tenant_id = ? AND phone = ?
                    ''', (proxy_id, tenant_id, phone))
                else:
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
                if is_admin_user(user):
                    cursor.execute('UPDATE account_stats SET proxy_id = NULL WHERE tenant_id = ? AND phone = ?', (tenant_id, phone))
                else:
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
    return jsonify(form_data)

@app.route('/api/save_config', methods=['POST'])
def save_config():
    data = request.get_json(silent=True) or {}
    tenant_id = get_current_tenant_id()
    api_id, api_hash = get_api_credentials(data)
    target_chat = str(data.get('target_chat', '') or '').strip()
    db.save_form_data(
        api_id=api_id,
        api_hash=api_hash,
        target_chat=target_chat,
        phones_text=data.get('phones_text', ''),
        delay=int(data.get('delay', 2)),
        tenant_id=tenant_id
    )
    db.set_config('api_id', api_id, tenant_id=tenant_id)
    db.set_config('api_hash', api_hash, tenant_id=tenant_id)
    if target_chat:
        db.set_config('outreach_target_chat', target_chat, tenant_id=tenant_id)
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
    api_id, api_hash = get_api_credentials(data)
    
    if not chat_username:
        return jsonify({'error': 'Не указан username чата'}), 400
    
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400
    
    manager = TelegramAccountManager(api_id, api_hash, 'sessions')
    
    if not await manager.load_accounts():
        return jsonify({'error': 'Не удалось загрузить аккаунты'}), 400
    
    results = await manager.ensure_all_accounts_in_chat(chat_username)
    
    await manager.close_all()
    
    return jsonify(results)

@app.route('/api/start', methods=['POST'])
def start_checker():
    global checker_process, checker_status
    
    if checker_status['running']:
        return jsonify({'error': 'Проверка уже запущена'}), 400
    
    data = request.get_json(silent=True) or {}
    tenant_id = get_current_tenant_id()
    api_id, api_hash = get_api_credentials(data)
    target_chat = data.get('target_chat')
    phones_text = data.get('phones')
    delay = int(data.get('delay', 2))
    if not api_id or not api_hash:
        return jsonify({'error': 'API ID и API Hash не настроены'}), 400
    
    # Проверяем обязательные поля
    if not target_chat:
        return jsonify({'error': 'Не указан целевой чат'}), 400
    
    if not phones_text:
        return jsonify({'error': 'Нет номеров для проверки'}), 400
    
    phones_list = [p.strip() for p in phones_text.split('\n') if p.strip()]
    
    if not phones_list:
        return jsonify({'error': 'Нет номеров для проверки'}), 400
    
    db.save_form_data(api_id, api_hash, target_chat, phones_text, delay, tenant_id=tenant_id)
    db.set_config('api_id', api_id, tenant_id=tenant_id)
    db.set_config('api_hash', api_hash, tenant_id=tenant_id)
    db.set_config('outreach_target_chat', target_chat, tenant_id=tenant_id)
    
    checker_status = {
        'running': True,
        'current_phone': '',
        'processed': 0,
        'total': len(phones_list),
        'registered': 0,
        'not_registered': 0,
        'errors': 0,
        'test_checks': 0,
        'accounts': [],
        'start_time': None,
        'estimated_time': None
    }
    
    checker_process = TelegramCheckerThread(
        api_id, api_hash, target_chat, phones_list, delay
    )
    checker_process.start()
    
    return jsonify({'status': 'started', 'total': len(phones_list)})

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
        try:
            if filename.endswith('.csv'):
                df = pd.read_csv(filepath)
                for col in df.columns:
                    if 'phone' in col.lower() or 'tel' in col.lower() or 'номер' in col.lower():
                        phones = df[col].dropna().astype(str).tolist()
                        break
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    phones = [line.strip() for line in f if line.strip()]
        except Exception as e:
            return jsonify({'error': str(e)}), 400
        
        return jsonify({'phones': phones[:100]})


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
        proxies = []
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, ip, port, protocol, username, password, country, is_active, last_used, created_at
                FROM proxies
                WHERE tenant_id = ?
                ORDER BY created_at DESC
            ''', (tenant_id,))
            for row in cursor.fetchall():
                proxy = {
                    'id': row[0], 'ip': row[1], 'port': row[2], 'protocol': row[3],
                    'username': row[4], 'password': row[5], 'country': row[6],
                    'is_active': bool(row[7]), 'last_used': row[8], 'created_at': row[9]
                }
                cursor.execute('SELECT phone FROM account_stats WHERE tenant_id = ? AND proxy_id = ? LIMIT 1', (tenant_id, proxy['id']))
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
    data = request.json
    tenant_id = get_current_tenant_id()
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id FROM proxies
                WHERE tenant_id = ? AND ip = ? AND port = ? AND protocol = ?
            ''', (tenant_id, data['ip'], data['port'], data.get('protocol', 'socks5')))
            if cursor.fetchone():
                return jsonify({'error': 'Proxy already exists'}), 400
            cursor.execute('''
                INSERT INTO proxies (tenant_id, ip, port, protocol, username, password, country, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            ''', (
                tenant_id,
                data['ip'],
                data['port'],
                data.get('protocol', 'socks5'),
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
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE proxies 
                SET ip=?, port=?, protocol=?, username=?, password=?, country=?
                WHERE id=? AND tenant_id = ?
            ''', (
                data['ip'], data['port'], data.get('protocol', 'socks5'),
                data.get('username'), data.get('password'), data.get('country'),
                proxy_id, tenant_id
            ))
            conn.commit()
        return jsonify({'status': 'updated'})
    except Exception as e:
        logger.error(f"Error updating proxy: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/<int:proxy_id>', methods=['DELETE'])
def delete_proxy(proxy_id):
    """Удаление прокси"""
    tenant_id = get_current_tenant_id()
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE account_stats SET proxy_id = NULL WHERE tenant_id = ? AND proxy_id = ?', (tenant_id, proxy_id))
            cursor.execute('DELETE FROM proxies WHERE tenant_id = ? AND id = ?', (tenant_id, proxy_id))
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
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM proxies WHERE tenant_id = ? AND id = ?', (tenant_id, proxy_id))
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
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM proxies WHERE tenant_id = ?', (tenant_id,))
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
                    'SELECT id FROM proxies WHERE tenant_id = ? AND ip = ? AND port = ? AND protocol = ?',
                    (tenant_id, p['ip'], p['port'], p.get('protocol', 'socks5'))
                )
                if cursor.fetchone():
                    skipped += 1
                    continue
                cursor.execute('''
                    INSERT INTO proxies (tenant_id, ip, port, protocol, username, password, country, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                ''', (tenant_id, p['ip'], p['port'], p.get('protocol', 'socks5'), p.get('username'), p.get('password'), p.get('country')))
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
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM proxies WHERE tenant_id = ? AND is_active = FALSE', (tenant_id,))
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

async def _parse_chat_members_with_service(tenant_id: int, chat_ref: str):
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
            ORDER BY id DESC
            LIMIT 1
        ''', (tenant_id,))
        row = cursor.fetchone()
    if not row:
        raise RuntimeError('Нет доступных service-сессий для парсинга')

    session_name = row[1] or row[0]
    session_path = os.path.join('sessions', session_name)
    client = TelegramClient(session_path, int(api_id), api_hash)
    await client.start()
    try:
        entity = await client.get_entity(chat_ref)
        participants = await client.get_participants(entity, aggressive=False)
        contacts = []
        for p in participants:
            username = getattr(p, 'username', None)
            if not username:
                continue
            contacts.append({
                'phone': None,
                'username': username,
                'access_hash': str(getattr(p, 'access_hash', '') or '') or None,
                'name': ' '.join([x for x in [getattr(p, 'first_name', ''), getattr(p, 'last_name', '')] if x]).strip() or None,
                'company': None,
                'position': None,
                'user_id': int(getattr(p, 'id', 0) or 0) or None
            })
        return contacts
    finally:
        await client.disconnect()

@app.route('/api/bases/parse-chat', methods=['POST'])
def parse_chat_to_base():
    tenant_id = get_current_tenant_id()
    user = get_current_user() or {}
    data = request.get_json(silent=True) or {}
    chat_ref = str(data.get('chat') or '').strip()
    base_name = str(data.get('base_name') or '').strip()
    if not chat_ref:
        return jsonify({'error': 'chat is required'}), 400
    if not base_name:
        return jsonify({'error': 'base_name is required'}), 400
    try:
        contacts = run_async(_parse_chat_members_with_service(tenant_id, chat_ref))
        if not contacts:
            return jsonify({'error': 'В чате не найдены участники с username'}), 400
        base_id = outreach.create_base(name=base_name, tenant_id=tenant_id, created_by_user_id=int(user.get('id') or 0))
        result = outreach.add_base_contacts(base_id=base_id, contacts=contacts, tenant_id=tenant_id, source_file=f'chat:{chat_ref}')
        result['base_id'] = base_id
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

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
    schedule = data.get('schedule') or None
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
            daily_limit=int(data.get('dailyLimit', 10)),
            delay_min=int(data.get('delayMin', 5)),
            delay_max=int(data.get('delayMax', 15)),
            strategy=strategy,
            schedule=schedule,
            tenant_id=tenant_id,
            created_by_user_id=actor_id if actor_id > 0 else None,
            base_id=base_id
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({'id': campaign_id})

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

    schedule = data.get('schedule', campaign.get('schedule'))
    strategy = data.get('strategy', campaign.get('strategy') or {'steps': []})
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
        daily_limit = max(1, int(data.get('dailyLimit', campaign.get('daily_limit', 10))))
        delay_min = max(0, int(data.get('delayMin', campaign.get('delay_min', 5))))
        delay_max = max(delay_min, int(data.get('delayMax', campaign.get('delay_max', 15))))
    except (TypeError, ValueError):
        return jsonify({'error': 'Некорректные лимиты/задержка'}), 400

    try:
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
        if int(campaign.get('base_id') or 0) != int(base_id or 0):
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
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({'status': 'updated', 'campaign': outreach.get_campaign(campaign_id, tenant_id=tenant_id)})


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
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    return jsonify(outreach.get_campaign_contacts(campaign_id, limit=limit, offset=offset, tenant_id=tenant_id))

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
    if valid_accounts != (campaign.get('accounts') or []):
        outreach.update_campaign(
            campaign_id=campaign_id,
            name=campaign['name'],
            message_template=campaign['message_template'],
            accounts=valid_accounts,
            daily_limit=int(campaign.get('daily_limit') or 10),
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
        return jsonify({'status': 'updated'})
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
    if not get_campaign_for_current_user(campaign_id):
        return jsonify({'error': 'Campaign not found'}), 404
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid schedule payload'}), 400

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE outreach_campaigns 
            SET schedule = ? 
            WHERE id = ? AND tenant_id = ?
        ''', (json.dumps(data), campaign_id, tenant_id))
        conn.commit()

    return jsonify({'status': 'saved'})


@socketio.on('connect')
def handle_connect():
    emit('connected', {'data': 'Connected to server'})

@app.route('/health')
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    acquire_single_process_lock()
    print("=" * 50)
    print("🚀 Telegram Outreach Platform запущена!")
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
    debug_mode = os.getenv('FLASK_DEBUG', '0') == '1'
    port = int(os.getenv('PORT', '5001'))
    socketio.run(
        app,
        debug=debug_mode,
        host='0.0.0.0',
        port=port,
        allow_unsafe_werkzeug=debug_mode
    )
