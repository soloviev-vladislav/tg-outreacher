from flask import Flask, render_template, request, jsonify, send_file, Response
from flask_socketio import SocketIO, emit
import asyncio
import threading
import os
import json
import sqlite3
import io
from datetime import datetime, date
from werkzeug.utils import secure_filename
import pandas as pd
from telegram_bot import TelegramAccountManager, Database, OutreachManager
import logging
import time
import sys
import atexit
import csv

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

# Fix for asyncio on macOS with Python 3.9+
import asyncio
import selectors

# Configure asyncio for macOS
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
        cursor.execute("PRAGMA table_info(outreach_campaigns)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'schedule' not in columns:
            cursor.execute('ALTER TABLE outreach_campaigns ADD COLUMN schedule TEXT')
        cursor.execute("PRAGMA table_info(outreach_contacts)")
        contact_columns = [col[1] for col in cursor.fetchall()]
        if 'username' not in contact_columns:
            cursor.execute('ALTER TABLE outreach_contacts ADD COLUMN username TEXT')
        if 'access_hash' not in contact_columns:
            cursor.execute('ALTER TABLE outreach_contacts ADD COLUMN access_hash TEXT')
        cursor.execute("PRAGMA table_info(account_stats)")
        account_columns = [col[1] for col in cursor.fetchall()]
        if 'outreach_daily_limit' not in account_columns:
            cursor.execute('ALTER TABLE account_stats ADD COLUMN outreach_daily_limit INTEGER DEFAULT 20')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS outreach_blacklist (
                user_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduler_campaign_locks (
                campaign_id INTEGER PRIMARY KEY,
                owner TEXT NOT NULL,
                locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_outreach_contacts_campaign_status ON outreach_contacts(campaign_id, status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_contact_direction_sent_at ON conversations(contact_id, direction, sent_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_campaign_direction_sent_at ON conversations(campaign_id, direction, sent_at)')
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

if DEFAULT_API_ID and DEFAULT_API_HASH:
    db.set_config('api_id', DEFAULT_API_ID)
    db.set_config('api_hash', DEFAULT_API_HASH)

def run_async(coro):
    """Запуск coroutine в синхронном Flask route."""
    return asyncio.run(coro)

def get_api_credentials(payload=None):
    payload = payload or {}
    api_id = str(payload.get('api_id') or db.get_config('api_id') or DEFAULT_API_ID).strip()
    api_hash = str(payload.get('api_hash') or db.get_config('api_hash') or DEFAULT_API_HASH).strip()
    return api_id, api_hash

def filter_valid_outreach_accounts(selected_accounts):
    selected = [str(a).strip() for a in (selected_accounts or []) if str(a).strip()]
    if not selected:
        return []
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        placeholders = ",".join("?" for _ in selected)
        cursor.execute(
            f'''
            SELECT phone
            FROM account_stats
            WHERE phone IN ({placeholders})
              AND account_type = 'outreach'
              AND COALESCE(is_banned, 0) = 0
              AND COALESCE(is_frozen, 0) = 0
            ''',
            selected
        )
        allowed = {row[0] for row in cursor.fetchall()}
    # сохраняем порядок выбора в UI
    return [phone for phone in selected if phone in allowed]

def _auth_required():
    if not APP_PASSWORD:
        return False
    if request.path.startswith('/static/'):
        return False
    if request.path == '/health':
        return False
    return True

@app.before_request
def enforce_basic_auth():
    if not _auth_required():
        return None
    auth = request.authorization
    if auth and auth.password == APP_PASSWORD:
        return None
    return Response(
        'Authentication required',
        401,
        {'WWW-Authenticate': 'Basic realm="Outreach"'}
    )

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

scheduler_thread = None
scheduler = None

def is_scheduler_running():
    return scheduler_thread is not None and scheduler_thread.is_alive()

def ensure_scheduler_running():
    global scheduler_thread, scheduler
    if is_scheduler_running():
        return False
    scheduler = OutreachScheduler()
    def run_scheduler():
        asyncio.run(scheduler.start())
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    return True

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
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.run_checker())
        self.loop.close()
        
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

@app.route('/accounts')
def accounts_page():
    return render_template('accounts.html')

@app.route('/proxies')
def proxies_page():
    return render_template('proxies.html')


# ===== API ЭНДПОИНТЫ =====

@app.route('/api/status')
def get_status():
    return jsonify(checker_status)

@app.route('/api/accounts')
def get_accounts():
    accounts = []
    sessions_dir = 'sessions'
    if os.path.exists(sessions_dir):
        for file in os.listdir(sessions_dir):
            if file.endswith('.session'):
                accounts.append({
                    'name': file[:-8],
                    'file': file
                })
    return jsonify(accounts)

@app.route('/api/accounts/detailed', methods=['GET'])
def get_accounts_detailed():
    """Получение детальной информации об аккаунтах"""
    accounts = []
    
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
                ''')
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
                ''')
            
            for row in cursor.fetchall():
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
                    'checked_today': row[11] or 0,
                    'last_check_date': row[12],
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
                cursor.execute('SELECT phone FROM account_stats WHERE session_name = ?', (session_name,))
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
            import asyncio
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
            
            # Создаем новый цикл событий для каждой сессии
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            phone = loop.run_until_complete(get_phone())
            loop.close()
            
            if phone:
                logger.info(f"Получен номер {phone} для сессии {session_name}")
                
                # Вставляем в БД
                with sqlite3.connect(db.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT OR IGNORE INTO account_stats 
                        (phone, session_name, account_type, is_banned, is_frozen, checked_today, last_check_date)
                        VALUES (?, ?, 'checker', 0, 0, 0, ?)
                    ''', (phone, session_name, date.today().isoformat()))
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
        import asyncio
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

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        phone = loop.run_until_complete(get_phone())
        loop.close()

        if not phone:
            try:
                os.remove(session_path)
            except Exception:
                pass
            return jsonify({'error': 'Сессия не авторизована или не удалось получить номер'}), 400

        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT phone FROM account_stats WHERE phone = ?', (phone,))
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
                (phone, session_name, account_type, is_banned, is_frozen, checked_today, last_check_date)
                VALUES (?, ?, 'checker', 0, 0, 0, ?)
            ''', (phone, session_name, date.today().isoformat()))
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
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT session_name FROM account_stats WHERE phone = ?', (phone,))
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
                    WHERE phone = ?
                ''', (quality, phone))
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
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT phone FROM account_stats')
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
    data = request.json
    phones = data.get('phones', [])
    proxy_id = data.get('proxy_id')
    
    if not phones or not proxy_id:
        return jsonify({'error': 'Missing data'}), 400
    
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            for phone in phones:
                cursor.execute('''
                    UPDATE account_stats SET proxy_id = ? WHERE phone = ?
                ''', (proxy_id, phone))
            conn.commit()
    except Exception as e:
        logger.error(f"Error assigning proxy: {e}")
        return jsonify({'error': str(e)}), 500
    
    return jsonify({'updated': len(phones)})

@app.route('/api/config', methods=['GET'])
def get_config():
    form_data = db.load_form_data()
    return jsonify(form_data)

@app.route('/api/save_config', methods=['POST'])
def save_config():
    data = request.get_json(silent=True) or {}
    api_id, api_hash = get_api_credentials(data)
    target_chat = str(data.get('target_chat', '') or '').strip()
    db.save_form_data(
        api_id=api_id,
        api_hash=api_hash,
        target_chat=target_chat,
        phones_text=data.get('phones_text', ''),
        delay=int(data.get('delay', 2))
    )
    db.set_config('api_id', api_id)
    db.set_config('api_hash', api_hash)
    if target_chat:
        db.set_config('outreach_target_chat', target_chat)
    return jsonify({'status': 'saved'})

@app.route('/api/history', methods=['GET'])
def get_history():
    limit = request.args.get('limit', 100, type=int)
    history = db.get_history(limit)
    return jsonify(history)

@app.route('/api/phone_stats', methods=['GET'])
def get_phone_stats():
    return jsonify({
        'found': len(db.get_successfully_found_phones()),
        'not_found': len(db.get_phones_never_found()),
        'total': db.get_history_count()
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
    
    db.save_form_data(api_id, api_hash, target_chat, phones_text, delay)
    db.set_config('api_id', api_id)
    db.set_config('api_hash', api_hash)
    db.set_config('outreach_target_chat', target_chat)
    
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
        proxies = proxy_manager.get_proxies(active_only=False)
        
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            for proxy in proxies:
                cursor.execute('''
                    SELECT phone FROM account_stats WHERE proxy_id = ? LIMIT 1
                ''', (proxy['id'],))
                account = cursor.fetchone()
                proxy['assigned_to'] = account[0] if account else None
        
        return jsonify(proxies)
    except Exception as e:
        logger.error(f"Error getting proxies: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies', methods=['POST'])
def add_proxy():
    """Добавление нового прокси"""
    data = request.json
    try:
        proxy_id = proxy_manager.add_proxy(
            ip=data['ip'],
            port=data['port'],
            protocol=data.get('protocol', 'socks5'),
            username=data.get('username'),
            password=data.get('password'),
            country=data.get('country')
        )
        if proxy_id:
            return jsonify({'id': proxy_id, 'status': 'added'})
        return jsonify({'error': 'Proxy already exists'}), 400
    except Exception as e:
        logger.error(f"Error adding proxy: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/<int:proxy_id>', methods=['PUT'])
def update_proxy(proxy_id):
    """Обновление прокси"""
    data = request.json
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE proxies 
                SET ip=?, port=?, protocol=?, username=?, password=?, country=?
                WHERE id=?
            ''', (
                data['ip'], data['port'], data.get('protocol', 'socks5'),
                data.get('username'), data.get('password'), data.get('country'),
                proxy_id
            ))
            conn.commit()
        return jsonify({'status': 'updated'})
    except Exception as e:
        logger.error(f"Error updating proxy: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/<int:proxy_id>', methods=['DELETE'])
def delete_proxy(proxy_id):
    """Удаление прокси"""
    try:
        proxy_manager.delete_proxy(proxy_id)
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
        results = await proxy_manager.test_all_proxies()
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error testing all proxies: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/import', methods=['POST'])
def import_proxies():
    """Импорт прокси из файла"""
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
        results = proxy_manager.add_proxies_bulk(proxies)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error importing proxies: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxies/delete-inactive', methods=['DELETE'])
def delete_inactive_proxies():
    """Удаление всех неактивных прокси"""
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM proxies WHERE is_active = FALSE')
            deleted = cursor.rowcount
            conn.commit()
        return jsonify({'deleted': deleted})
    except Exception as e:
        logger.error(f"Error deleting inactive proxies: {e}")
        return jsonify({'error': str(e)}), 500


# ===== ЭНДПОИНТЫ ДЛЯ АУТРИЧА =====

@app.route('/api/outreach/campaigns', methods=['GET'])
def get_outreach_campaigns():
    return jsonify(outreach.get_campaigns())

@app.route('/api/outreach/campaigns', methods=['POST'])
def create_outreach_campaign():
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
    try:
        campaign_id = outreach.create_campaign(
            name=name,
            message_template=template,
            accounts=accounts,
            daily_limit=int(data.get('dailyLimit', 10)),
            delay_min=int(data.get('delayMin', 5)),
            delay_max=int(data.get('delayMax', 15)),
            strategy=strategy,
            schedule=schedule
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({'id': campaign_id})

@app.route('/api/outreach/campaign/<int:campaign_id>', methods=['PUT'])
def update_outreach_campaign(campaign_id):
    data = request.get_json(silent=True) or {}
    campaign = outreach.get_campaign(campaign_id)
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
    if not accounts:
        return jsonify({'error': 'В кампании должен быть минимум 1 активный outreach-аккаунт'}), 400

    schedule = data.get('schedule', campaign.get('schedule'))
    strategy = data.get('strategy', campaign.get('strategy') or {'steps': []})
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
            schedule=schedule
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({'status': 'updated', 'campaign': outreach.get_campaign(campaign_id)})

@app.route('/api/outreach/campaign/<int:campaign_id>', methods=['GET'])
def get_outreach_campaign(campaign_id):
    return jsonify(outreach.get_campaign(campaign_id))

@app.route('/api/outreach/campaign/<int:campaign_id>/contacts')
def get_campaign_contacts(campaign_id):
    limit = request.args.get('limit', 50, type=int)
    return jsonify(outreach.get_campaign_contacts(campaign_id, limit))

@app.route('/api/outreach/blacklist', methods=['GET'])
def get_outreach_blacklist():
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, created_at
            FROM outreach_blacklist
            ORDER BY datetime(created_at) DESC, user_id DESC
            LIMIT 1000
        ''')
        rows = cursor.fetchall()
    return jsonify([{'user_id': row[0], 'created_at': row[1]} for row in rows])

@app.route('/api/outreach/blacklist', methods=['POST'])
def add_to_outreach_blacklist():
    data = request.get_json(silent=True) or {}
    try:
        user_id = int(data.get('user_id'))
    except (TypeError, ValueError):
        return jsonify({'error': 'user_id должен быть числом'}), 400
    if user_id <= 0:
        return jsonify({'error': 'user_id должен быть > 0'}), 400

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO outreach_blacklist (user_id) VALUES (?)', (user_id,))
        inserted = cursor.rowcount > 0
        cursor.execute('''
            UPDATE outreach_contacts
            SET status = 'failed',
                notes = 'blacklisted'
            WHERE user_id = ?
              AND status IN ('new', 'sent', 'ignored')
        ''', (user_id,))
        affected_contacts = cursor.rowcount
        conn.commit()

    return jsonify({
        'status': 'added' if inserted else 'exists',
        'user_id': user_id,
        'affected_contacts': affected_contacts
    })

@app.route('/api/outreach/blacklist/<int:user_id>', methods=['DELETE'])
def remove_from_outreach_blacklist(user_id: int):
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM outreach_blacklist WHERE user_id = ?', (user_id,))
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
              AND status = 'failed'
              AND notes = 'blacklisted'
        ''', (user_id,))
        restored_contacts = cursor.rowcount
        conn.commit()
    return jsonify({
        'status': 'deleted' if deleted else 'not_found',
        'user_id': user_id,
        'restored_contacts': restored_contacts
    })

@app.route('/api/outreach/campaign/<int:campaign_id>/start', methods=['POST'])
def start_campaign(campaign_id):
    campaign = outreach.get_campaign(campaign_id)
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
            schedule=campaign.get('schedule')
        )

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*)
            FROM outreach_contacts
            WHERE campaign_id = ? AND status = 'new'
              AND (user_id IS NULL OR user_id NOT IN (SELECT user_id FROM outreach_blacklist))
              AND (user_id IS NOT NULL OR (username IS NOT NULL AND username != '') OR (phone IS NOT NULL AND phone != ''))
        ''', (campaign_id,))
        contacts_ready = cursor.fetchone()[0] or 0
    if contacts_ready == 0:
        return jsonify({'error': 'Нет новых контактов с user_id/username/phone для рассылки'}), 400

    # Важно: сначала ставим active, чтобы первый тик планировщика сразу увидел кампанию.
    outreach.update_campaign_status(campaign_id, 'active')
    started_scheduler = ensure_scheduler_running()
    return jsonify({'status': 'started', 'scheduler_started': started_scheduler})

@app.route('/api/outreach/campaign/<int:campaign_id>/pause', methods=['POST'])
def pause_campaign(campaign_id):
    outreach.update_campaign_status(campaign_id, 'paused')
    if not has_active_campaigns():
        global scheduler, scheduler_thread
        if scheduler:
            scheduler.stop()
            scheduler = None
        scheduler_thread = None
    return jsonify({'status': 'paused'})

@app.route('/api/outreach/campaign/<int:campaign_id>/stop', methods=['POST'])
def stop_campaign(campaign_id):
    outreach.update_campaign_status(campaign_id, 'stopped')
    if not has_active_campaigns():
        global scheduler, scheduler_thread
        if scheduler:
            scheduler.stop()
            scheduler = None
        scheduler_thread = None
    return jsonify({'status': 'stopped'})

@app.route('/api/outreach/campaign/<int:campaign_id>/readiness', methods=['GET'])
def campaign_readiness(campaign_id):
    campaign = outreach.get_campaign(campaign_id)
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
            WHERE campaign_id = ?
        ''', (campaign_id,))
        contacts_total, contacts_with_user_id, contacts_with_username, contacts_with_phone = cursor.fetchone()
        cursor.execute('''
            SELECT COUNT(*)
            FROM outreach_contacts
            WHERE campaign_id = ?
              AND user_id IS NOT NULL
              AND user_id IN (SELECT user_id FROM outreach_blacklist)
        ''', (campaign_id,))
        blacklisted_contacts = cursor.fetchone()[0] or 0
        cursor.execute('''
            SELECT COUNT(*)
            FROM outreach_contacts
            WHERE campaign_id = ? AND status = 'new'
              AND (user_id IS NULL OR user_id NOT IN (SELECT user_id FROM outreach_blacklist))
              AND (user_id IS NOT NULL OR (username IS NOT NULL AND username != '') OR (phone IS NOT NULL AND phone != ''))
        ''', (campaign_id,))
        new_contacts = cursor.fetchone()[0]

    api_id, api_hash = get_api_credentials()
    has_api = bool(api_id and api_hash)
    valid_accounts = filter_valid_outreach_accounts(campaign.get('accounts') or [])
    has_accounts = bool(valid_accounts)
    has_contacts = bool((contacts_total or 0) > 0)
    has_targets = bool((new_contacts or 0) > 0)
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
        'new_contacts_ready': new_contacts or 0,
        'schedule_open_now': schedule_open,
        'schedule_reason': schedule_reason,
        'ready_to_send_now': all([has_api, has_accounts, has_targets, schedule_open])
    })

@app.route('/api/outreach/campaign/<int:campaign_id>/stats')
def get_campaign_stats(campaign_id):
    """Получение расширенной статистики кампании"""
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT sent_count, reply_count, meeting_count, total_contacts
                FROM outreach_campaigns WHERE id = ?
            ''', (campaign_id,))
            row = cursor.fetchone()
            
            cursor.execute('''
                SELECT date(sent_at) as day, COUNT(*) as count
                FROM conversations
                WHERE campaign_id = ? AND direction = 'outgoing'
                GROUP BY date(sent_at)
                ORDER BY day DESC LIMIT 7
            ''', (campaign_id,))
            daily = cursor.fetchall()
            
            cursor.execute('''
                SELECT account_used, COUNT(*) as count
                FROM conversations
                WHERE campaign_id = ? AND direction = 'outgoing'
                GROUP BY account_used
            ''', (campaign_id,))
            by_account = cursor.fetchall()
            
            cursor.execute('''
                SELECT 
                    COUNT(CASE WHEN status != 'new' THEN 1 END) as sent,
                    COUNT(CASE WHEN status = 'replied' THEN 1 END) as replied,
                    COUNT(CASE WHEN status = 'interested' THEN 1 END) as interested
                FROM outreach_contacts
                WHERE campaign_id = ?
            ''', (campaign_id,))
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
    campaign = outreach.get_campaign(campaign_id)
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
                  AND c.direction = 'outgoing'
                  AND c.id = (
                      SELECT c2.id
                      FROM conversations c2
                      WHERE c2.campaign_id = c.campaign_id
                        AND c2.contact_id = c.contact_id
                        AND c2.direction = 'outgoing'
                      ORDER BY datetime(c2.sent_at) DESC, c2.id DESC
                      LIMIT 1
                  )
            ) x ON x.contact_id = oc.id
            WHERE oc.campaign_id = ?
            GROUP BY x.step_id, oc.status
        ''', (campaign_id, campaign_id))
        grouped = cursor.fetchall()

        cursor.execute('''
            SELECT step_id, COUNT(*)
            FROM conversations
            WHERE campaign_id = ?
              AND direction = 'outgoing'
              AND step_id IS NOT NULL
            GROUP BY step_id
        ''', (campaign_id,))
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
    return jsonify(outreach.get_templates())

@app.route('/api/outreach/templates', methods=['POST'])
def save_template():
    data = request.json
    outreach.save_template(data['name'], data['text'])
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
    if 'file' not in request.files:
        return jsonify({'error': 'No file'}), 400
    
    file = request.files['file']
    campaign_id = request.form.get('campaign_id')
    
    if not campaign_id:
        return jsonify({'error': 'No campaign ID'}), 400
    
    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    
    try:
        validation = outreach.validate_contacts_file(filepath)
        if not validation.get('ok'):
            return jsonify({'error': validation.get('error') or 'Файл не прошёл валидацию'}), 400
        result = outreach.import_contacts(int(campaign_id), filepath)
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
    
    if account_type not in ['checker', 'outreach']:
        return jsonify({'error': 'Invalid account type'}), 400
    
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE account_stats 
                SET account_type = ? 
                WHERE phone = ?
            ''', (account_type, phone))
            conn.commit()
        return jsonify({'status': 'updated'})
    except Exception as e:
        logger.error(f"Error updating account type: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/accounts/<path:phone>/outreach-limit', methods=['POST'])
def update_outreach_limit(phone):
    """Обновление суточного лимита для outreach аккаунта"""
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
                WHERE phone = ? AND account_type = 'outreach'
            ''', (outreach_daily_limit, phone))
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
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid schedule payload'}), 400

    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE outreach_campaigns 
            SET schedule = ? 
            WHERE id = ?
        ''', (json.dumps(data), campaign_id))
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
