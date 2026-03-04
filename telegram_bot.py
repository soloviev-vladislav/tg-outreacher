import asyncio
import json
import logging
import sqlite3
import os
import shutil
import pandas as pd
from telethon import TelegramClient, errors
from telethon.tl.functions.contacts import ImportContactsRequest, DeleteContactsRequest
from telethon.tl.types import InputPhoneContact, InputMediaContact
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def build_telethon_proxy_config(proxy: Dict) -> Optional[Dict]:
    """Преобразовать прокси из БД в формат Telethon (dict), без URL-строки."""
    if not proxy:
        return None

    protocol = str(proxy.get('protocol') or 'socks5').strip().lower()
    if protocol in ('socks5h', 'socks5'):
        protocol = 'socks5'
    elif protocol in ('socks4a', 'socks4'):
        protocol = 'socks4'
    elif protocol in ('https', 'http'):
        protocol = 'http'
    else:
        logger.warning(f"Unsupported proxy protocol: {proxy.get('protocol')}")
        return None

    ip = str(proxy.get('ip') or '').strip()
    try:
        port = int(proxy.get('port'))
    except (TypeError, ValueError):
        logger.warning(f"Invalid proxy port: {proxy.get('port')}")
        return None

    if not ip:
        logger.warning("Proxy IP is empty")
        return None

    cfg = {
        'proxy_type': protocol,
        'addr': ip,
        'port': port
    }
    if proxy.get('username'):
        cfg['username'] = str(proxy.get('username'))
    if proxy.get('password'):
        cfg['password'] = str(proxy.get('password'))
    if protocol.startswith('socks'):
        cfg['rdns'] = True
    return cfg

# Валидный номер для проверки добавления контакта
VALID_TEST_PHONE = "+79043303474"

class Database:
    """Класс для работы с SQLite базой данных"""
    def __init__(self, db_path='checker.db'):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Инициализация базы данных"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Таблица для истории проверок
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS check_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT NOT NULL,
                    registered BOOLEAN NOT NULL,
                    user_id INTEGER,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    account_used TEXT,
                    check_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_test BOOLEAN DEFAULT FALSE
                )
            ''')
            
            # Таблица для конфигурации
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            
            # Таблица для статистики аккаунтов - ПОЛНАЯ ВЕРСИЯ СО ВСЕМИ КОЛОНКАМИ
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS account_stats (
                    phone TEXT PRIMARY KEY,
                    session_name TEXT,
                    total_checks INTEGER DEFAULT 0,
                    successful_checks INTEGER DEFAULT 0,
                    test_checks INTEGER DEFAULT 0,
                    test_successful INTEGER DEFAULT 0,
                    last_check TIMESTAMP,
                    is_shadow_banned BOOLEAN DEFAULT FALSE,
                    ban_detected_at TIMESTAMP,
                    joined_chats TEXT DEFAULT '[]',
                    is_frozen BOOLEAN DEFAULT FALSE,
                    frozen_detected_at TIMESTAMP,
                    test_quality TEXT DEFAULT 'unknown',
                    last_test_date TIMESTAMP,
                    test_history TEXT DEFAULT '[]',
                    account_type TEXT DEFAULT 'checker',
                    proxy_id INTEGER,
                    fingerprint_id INTEGER,
                    is_banned BOOLEAN DEFAULT FALSE,
                    last_check_date TEXT,
                    checked_today INTEGER DEFAULT 0,
                    outreach_daily_limit INTEGER DEFAULT 20,
                    UNIQUE(phone)
                )
            ''')
            
            # Таблица для сохранения формы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS form_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_id TEXT,
                    api_hash TEXT,
                    target_chat TEXT,
                    phones_text TEXT,
                    delay INTEGER DEFAULT 2,
                    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица прокси
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS proxies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ip TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    protocol TEXT DEFAULT 'socks5',
                    username TEXT,
                    password TEXT,
                    country TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    last_used TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ip, port, protocol)
                )
            ''')
            
            # Таблица цифровых отпечатков
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS account_fingerprints (
                    account_phone TEXT PRIMARY KEY REFERENCES account_stats(phone),
                    device_model TEXT DEFAULT 'Desktop',
                    app_version TEXT DEFAULT '4.16.30',
                    system_version TEXT,
                    lang_code TEXT DEFAULT 'ru',
                    system_lang_code TEXT DEFAULT 'ru',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица для истории переписки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER NOT NULL,
                    contact_id INTEGER NOT NULL,
                    message_id INTEGER,
                    direction TEXT CHECK(direction IN ('outgoing', 'incoming')),
                    content TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_followup BOOLEAN DEFAULT FALSE,
                    step_id INTEGER,
                    metadata TEXT,
                    FOREIGN KEY(campaign_id) REFERENCES outreach_campaigns(id),
                    FOREIGN KEY(contact_id) REFERENCES outreach_contacts(id)
                )
            ''')

            # Локи кампаний для защиты от параллельной обработки несколькими процессами.
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduler_campaign_locks (
                    campaign_id INTEGER PRIMARY KEY,
                    owner TEXT NOT NULL,
                    locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # ===== ТАБЛИЦЫ ДЛЯ АУТРИЧА =====
            
            # Таблица кампаний
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS outreach_campaigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    message_template TEXT NOT NULL,
                    accounts TEXT DEFAULT '[]',
                    daily_limit INTEGER DEFAULT 10,
                    delay_min INTEGER DEFAULT 5,
                    delay_max INTEGER DEFAULT 15,
                    status TEXT DEFAULT 'draft',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    total_contacts INTEGER DEFAULT 0,
                    sent_count INTEGER DEFAULT 0,
                    reply_count INTEGER DEFAULT 0,
                    meeting_count INTEGER DEFAULT 0,
                    strategy TEXT DEFAULT '{"steps": []}',
                    ai_enabled BOOLEAN DEFAULT FALSE,
                    ai_tone TEXT DEFAULT 'professional',
                    schedule TEXT,
                    UNIQUE(name)
                )
            ''')
            
            # Таблица контактов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS outreach_contacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER,
                    phone TEXT,
                    username TEXT,
                    access_hash TEXT,
                    name TEXT,
                    company TEXT,
                    position TEXT,
                    source_file TEXT,
                    status TEXT DEFAULT 'new',
                    message_sent_at TIMESTAMP,
                    replied_at TIMESTAMP,
                    last_message TEXT,
                    notes TEXT,
                    account_used TEXT,
                    user_id INTEGER,
                    UNIQUE(phone, campaign_id),
                    FOREIGN KEY(campaign_id) REFERENCES outreach_campaigns(id)
                )
            ''')
            
            # Таблица сообщений
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS outreach_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_id INTEGER,
                    account_used TEXT,
                    message_text TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_incoming BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY(contact_id) REFERENCES outreach_contacts(id)
                )
            ''')
            
            # Таблица шаблонов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS outreach_templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    template_text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name)
                )
            ''')

            # Глобальный черный список user_id для всех кампаний.
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS outreach_blacklist (
                    user_id INTEGER PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Индексы для производительности рассылок и проверки ответов.
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_outreach_contacts_campaign_status ON outreach_contacts(campaign_id, status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_contact_direction_sent_at ON conversations(contact_id, direction, sent_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_campaign_direction_sent_at ON conversations(campaign_id, direction, sent_at)')

            conn.commit()
            logger.info("База данных успешно инициализирована")
    
    def save_form_data(self, api_id: str, api_hash: str, target_chat: str, phones_text: str, delay: int = 2):
        """Сохранение данных формы"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM form_data')
            cursor.execute('''
                INSERT INTO form_data (api_id, api_hash, target_chat, phones_text, delay)
                VALUES (?, ?, ?, ?, ?)
            ''', (api_id, api_hash, target_chat, phones_text, delay))
            conn.commit()
    
    def load_form_data(self) -> dict:
        """Загрузка сохраненных данных формы"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT api_id, api_hash, target_chat, phones_text, delay 
                FROM form_data ORDER BY saved_at DESC LIMIT 1
            ''')
            result = cursor.fetchone()
            if result:
                return {
                    'api_id': result[0] or '',
                    'api_hash': result[1] or '',
                    'target_chat': result[2] or '',
                    'phones_text': result[3] or '',
                    'delay': result[4] or 2
                }
            return {
                'api_id': '',
                'api_hash': '',
                'target_chat': '',
                'phones_text': '',
                'delay': 2
            }
    
    def save_check(self, phone: str, registered: bool, user_data: dict = None, 
                   account_used: str = None, is_test: bool = False):
        """Сохранение результата проверки"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO check_history 
                (phone, registered, user_id, username, first_name, last_name, account_used, is_test)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                phone, 
                registered,
                user_data.get('user_id') if user_data else None,
                user_data.get('username') if user_data else None,
                user_data.get('first_name') if user_data else None,
                user_data.get('last_name') if user_data else None,
                account_used,
                is_test
            ))
            
            if account_used:
                cursor.execute('''
                    INSERT INTO account_stats (phone, session_name, total_checks, successful_checks, test_checks, test_successful, last_check)
                    VALUES (?, ?, 1, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(phone) DO UPDATE SET
                        total_checks = total_checks + 1,
                        successful_checks = successful_checks + ?,
                        test_checks = test_checks + ?,
                        test_successful = test_successful + ?,
                        last_check = CURRENT_TIMESTAMP
                ''', (
                    account_used,
                    account_used,
                    1 if registered else 0,
                    1 if is_test else 0,
                    1 if is_test and registered else 0,
                    1 if registered else 0,
                    1 if is_test else 0,
                    1 if is_test and registered else 0
                ))
            
            conn.commit()
    
    def get_config(self, key: str, default=None):
        """Получение значения из конфига"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM config WHERE key = ?', (key,))
            result = cursor.fetchone()
            return result[0] if result else default
    
    def set_config(self, key: str, value: str):
        """Сохранение значения в конфиг"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO config (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = ?
            ''', (key, value, value))
            conn.commit()
    
    def get_account_stats(self, account_phone: str) -> dict:
        """Получение статистики аккаунта"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    SELECT phone, session_name, total_checks, successful_checks, 
                           test_checks, test_successful, last_check, is_shadow_banned, 
                           ban_detected_at, joined_chats, is_frozen, frozen_detected_at,
                           test_quality, last_test_date, test_history, account_type, proxy_id,
                           is_banned, last_check_date, checked_today, outreach_daily_limit
                    FROM account_stats WHERE phone = ?
                ''', (account_phone,))
            except sqlite3.OperationalError:
                cursor.execute('''
                    SELECT phone, session_name, total_checks, successful_checks, 
                           test_checks, test_successful, last_check, is_shadow_banned, 
                           ban_detected_at, joined_chats, is_frozen, frozen_detected_at,
                           test_quality, last_test_date, test_history, account_type, proxy_id,
                           is_banned, last_check_date, checked_today, 20 as outreach_daily_limit
                    FROM account_stats WHERE phone = ?
                ''', (account_phone,))
            result = cursor.fetchone()
            if result:
                joined_chats = []
                if len(result) > 9 and result[9]:
                    try:
                        joined_chats = json.loads(result[9])
                    except:
                        joined_chats = []
                
                test_history = []
                if len(result) > 14 and result[14]:
                    try:
                        test_history = json.loads(result[14])
                    except:
                        test_history = []
                
                return {
                    'phone': result[0],
                    'session_name': result[1],
                    'total_checks': result[2],
                    'successful_checks': result[3],
                    'test_checks': result[4],
                    'test_successful': result[5],
                    'last_check': result[6],
                    'is_shadow_banned': bool(result[7]) if len(result) > 7 else False,
                    'ban_detected_at': result[8] if len(result) > 8 else None,
                    'joined_chats': joined_chats,
                    'is_frozen': bool(result[10]) if len(result) > 10 else False,
                    'frozen_detected_at': result[11] if len(result) > 11 else None,
                    'test_quality': result[12] if len(result) > 12 else 'unknown',
                    'last_test_date': result[13] if len(result) > 13 else None,
                    'test_history': test_history,
                    'account_type': result[15] if len(result) > 15 else 'checker',
                    'proxy_id': result[16] if len(result) > 16 else None,
                    'is_banned': bool(result[17]) if len(result) > 17 else False,
                    'last_check_date': result[18] if len(result) > 18 else None,
                    'checked_today': result[19] if len(result) > 19 else 0,
                    'outreach_daily_limit': result[20] if len(result) > 20 else 20
                }
            return None
    
    def mark_account_banned(self, account_phone: str, ban_type: str = "shadow"):
        """Пометить аккаунт как забаненный"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if ban_type == "frozen":
                cursor.execute('''
                    UPDATE account_stats 
                    SET is_frozen = TRUE, frozen_detected_at = CURRENT_TIMESTAMP
                    WHERE phone = ?
                ''', (account_phone,))
                logger.warning(f"❄️ Аккаунт {account_phone} помечен как FROZEN")
            else:
                cursor.execute('''
                    UPDATE account_stats 
                    SET is_banned = TRUE, ban_detected_at = CURRENT_TIMESTAMP
                    WHERE phone = ?
                ''', (account_phone,))
                logger.warning(f"⚠️ Аккаунт {account_phone} помечен как BANNED")
            conn.commit()
    
    def update_account_test_result(self, account_phone: str, quality: str, test_results: dict):
        """Обновление результатов тестирования аккаунта"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('SELECT test_history FROM account_stats WHERE phone = ?', (account_phone,))
            result = cursor.fetchone()
            history = []
            if result and result[0]:
                try:
                    history = json.loads(result[0])
                except:
                    history = []
            
            history.append({
                'date': datetime.now().isoformat(),
                'quality': quality,
                'successes': test_results.get('successes', 0),
                'failures': test_results.get('failures', 0),
                'frozen': test_results.get('frozen', False),
                'details': test_results.get('details', [])
            })
            
            history = history[-10:]
            
            cursor.execute('''
                UPDATE account_stats 
                SET test_quality = ?, last_test_date = CURRENT_TIMESTAMP, test_history = ?
                WHERE phone = ?
            ''', (quality, json.dumps(history), account_phone))
            conn.commit()
    
    def mark_chat_joined(self, account_phone: str, chat_username: str):
        """Отметить что аккаунт вступил в чат"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT joined_chats FROM account_stats WHERE phone = ?', (account_phone,))
            result = cursor.fetchone()
            if result and result[0]:
                joined = json.loads(result[0])
            else:
                joined = []
            
            if chat_username not in joined:
                joined.append(chat_username)
            
            cursor.execute('''
                UPDATE account_stats SET joined_chats = ? WHERE phone = ?
            ''', (json.dumps(joined), account_phone))
            conn.commit()
    
    def delete_frozen_session(self, session_name: str, session_dir: str = 'sessions'):
        """Удаление frozen сессии"""
        try:
            session_path = os.path.join(session_dir, f"{session_name}.session")
            if os.path.exists(session_path):
                os.remove(session_path)
                logger.info(f"🗑️ Удален frozen session файл: {session_name}.session")
            
            journal_path = os.path.join(session_dir, f"{session_name}.session-journal")
            if os.path.exists(journal_path):
                os.remove(journal_path)
                
            return True
        except Exception as e:
            logger.error(f"Ошибка при удалении сессии {session_name}: {e}")
            return False
    
    def is_phone_found_successfully(self, phone: str) -> bool:
        """Проверяет, был ли номер успешно найден"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) FROM check_history 
                WHERE phone = ? AND registered = TRUE
            ''', (phone,))
            count = cursor.fetchone()[0]
            return count > 0

    def get_successfully_found_phones(self) -> List[str]:
        """Возвращает список номеров, которые были успешно найдены"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT phone FROM check_history 
                WHERE registered = TRUE
            ''')
            return [row[0] for row in cursor.fetchall()]

    def get_phones_never_found(self) -> List[str]:
        """Возвращает номера, которые проверялись, но ни разу не были найдены"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT phone FROM check_history 
                WHERE phone NOT IN (
                    SELECT DISTINCT phone FROM check_history WHERE registered = TRUE
                )
            ''')
            return [row[0] for row in cursor.fetchall()]
    
    def get_history_count(self) -> int:
        """Общее количество проверок"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM check_history')
            return cursor.fetchone()[0]
    
    def get_history(self, limit: int = 100) -> List[dict]:
        """Получение истории проверок"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM check_history ORDER BY check_date DESC LIMIT ?
            ''', (limit,))
            results = cursor.fetchall()
            
            history = []
            for row in results:
                history.append({
                    'id': row[0],
                    'phone': row[1],
                    'registered': bool(row[2]),
                    'user_id': row[3],
                    'username': row[4],
                    'first_name': row[5],
                    'last_name': row[6],
                    'account_used': row[7],
                    'check_date': row[8],
                    'is_test': bool(row[9])
                })
            return history


class OutreachManager:
    """Класс для управления аутрич кампаниями"""
    
    def __init__(self, db_path='checker.db'):
        self.db_path = db_path
    
    def create_campaign(self, name: str, message_template: str, accounts: List[str], 
                       daily_limit: int = 10, delay_min: int = 5, delay_max: int = 15,
                       strategy: Optional[dict] = None, schedule: Optional[dict] = None):
        """Создание новой кампании"""
        strategy_json = json.dumps(strategy or {"steps": []})
        schedule_json = json.dumps(schedule) if schedule else None
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO outreach_campaigns 
                (name, message_template, accounts, daily_limit, delay_min, delay_max, strategy, schedule)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (name, message_template, json.dumps(accounts), daily_limit, delay_min, delay_max, strategy_json, schedule_json))
            conn.commit()
            return cursor.lastrowid

    def update_campaign(self, campaign_id: int, name: str, message_template: str, accounts: List[str],
                        daily_limit: int, delay_min: int, delay_max: int,
                        strategy: Optional[dict] = None, schedule: Optional[dict] = None):
        """Обновление основных настроек кампании"""
        strategy_json = json.dumps(strategy or {"steps": []})
        schedule_json = json.dumps(schedule) if schedule else None
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE outreach_campaigns
                SET name = ?, message_template = ?, accounts = ?, daily_limit = ?,
                    delay_min = ?, delay_max = ?, strategy = ?, schedule = ?
                WHERE id = ?
            ''', (
                name,
                message_template,
                json.dumps(accounts),
                daily_limit,
                delay_min,
                delay_max,
                strategy_json,
                schedule_json,
                campaign_id
            ))
            conn.commit()
    
    def get_campaigns(self) -> List[dict]:
        """Получение списка всех кампаний"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            has_schedule = True
            try:
                cursor.execute('''
                    SELECT id, name, message_template, accounts, daily_limit, 
                           delay_min, delay_max, status, created_at, started_at,
                           total_contacts, sent_count, reply_count, meeting_count,
                           strategy, ai_enabled, ai_tone, schedule
                    FROM outreach_campaigns
                    ORDER BY created_at DESC
                ''')
            except sqlite3.OperationalError:
                has_schedule = False
                cursor.execute('''
                    SELECT id, name, message_template, accounts, daily_limit, 
                           delay_min, delay_max, status, created_at, started_at,
                           total_contacts, sent_count, reply_count, meeting_count,
                           strategy, ai_enabled, ai_tone
                    FROM outreach_campaigns
                    ORDER BY created_at DESC
                ''')
            
            campaigns = []
            for row in cursor.fetchall():
                campaigns.append({
                    'id': row[0],
                    'name': row[1],
                    'message_template': row[2],
                    'accounts': json.loads(row[3]),
                    'daily_limit': row[4],
                    'delay_min': row[5],
                    'delay_max': row[6],
                    'status': row[7],
                    'created_at': row[8],
                    'started_at': row[9],
                    'total_contacts': row[10],
                    'sent_count': row[11],
                    'reply_count': row[12],
                    'meeting_count': row[13],
                    'strategy': json.loads(row[14]) if row[14] else {"steps": []},
                    'ai_enabled': bool(row[15]),
                    'ai_tone': row[16],
                    'schedule': json.loads(row[17]) if has_schedule and len(row) > 17 and row[17] else None,
                    'progress': round(row[11]/row[10]*100 if row[10] > 0 else 0)
                })
            return campaigns
    
    def get_campaign(self, campaign_id: int) -> dict:
        """Получение конкретной кампании"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            has_schedule = True
            try:
                cursor.execute('''
                    SELECT id, name, message_template, accounts, daily_limit, 
                           delay_min, delay_max, status, created_at, started_at,
                           total_contacts, sent_count, reply_count, meeting_count,
                           strategy, ai_enabled, ai_tone, schedule
                    FROM outreach_campaigns WHERE id = ?
                ''', (campaign_id,))
            except sqlite3.OperationalError:
                has_schedule = False
                cursor.execute('''
                    SELECT id, name, message_template, accounts, daily_limit, 
                           delay_min, delay_max, status, created_at, started_at,
                           total_contacts, sent_count, reply_count, meeting_count,
                           strategy, ai_enabled, ai_tone
                    FROM outreach_campaigns WHERE id = ?
                ''', (campaign_id,))
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'name': row[1],
                    'message_template': row[2],
                    'accounts': json.loads(row[3]),
                    'daily_limit': row[4],
                    'delay_min': row[5],
                    'delay_max': row[6],
                    'status': row[7],
                    'created_at': row[8],
                    'started_at': row[9],
                    'total_contacts': row[10],
                    'sent_count': row[11],
                    'reply_count': row[12],
                    'meeting_count': row[13],
                    'strategy': json.loads(row[14]) if row[14] else {"steps": []},
                    'ai_enabled': bool(row[15]),
                    'ai_tone': row[16],
                    'schedule': json.loads(row[17]) if has_schedule and len(row) > 17 and row[17] else None
                }
            return None

    @staticmethod
    def _parse_int_cell(value):
        if pd.isna(value):
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if pd.isna(value):
                return None
            return int(value)
        text = str(value).strip().replace(' ', '')
        if not text:
            return None
        if text.endswith('.0'):
            text = text[:-2]
        try:
            return int(text)
        except (TypeError, ValueError):
            try:
                return int(float(text))
            except (TypeError, ValueError):
                return None

    @staticmethod
    def _normalize_phone_cell(value):
        if pd.isna(value):
            return None
        text = str(value).strip()
        if not text:
            return None
        if text.endswith('.0'):
            text = text[:-2]
        text = (
            text.replace(' ', '')
            .replace('-', '')
            .replace('(', '')
            .replace(')', '')
        )
        if text.startswith('00'):
            text = '+' + text[2:]
        if text.startswith('8') and len(text) == 11 and text.isdigit():
            return '+7' + text[1:]
        if text.startswith('7') and len(text) == 11 and text.isdigit():
            return '+' + text
        if text.startswith('+') and text[1:].isdigit():
            return text
        if text.isdigit() and 10 <= len(text) <= 15:
            return '+' + text
        return None

    def _looks_like_phone_number(self, value) -> bool:
        parsed = self._parse_int_cell(value)
        if parsed is None:
            return False
        digits = str(abs(parsed))
        return len(digits) == 11 and digits[0] in ('7', '8')

    def validate_contacts_file(self, file_path: str) -> dict:
        """Серверная валидация файла перед импортом контактов."""
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        if df.empty:
            return {'ok': False, 'error': 'Файл пустой'}

        df.columns = [str(col).lower().strip() for col in df.columns]
        phone_col = next((col for col in df.columns if 'phone' in col or 'tel' in col or 'номер' in col), None)
        user_id_col = next((col for col in df.columns if 'user_id' in col or 'userid' in col or 'telegram_id' in col or 'tg_id' in col), None)
        username_col = next((col for col in df.columns if col in ['username', 'telegram_username', 'tg_username']), None)

        if not phone_col and not user_id_col and not username_col:
            return {'ok': False, 'error': 'Нужна колонка phone, user_id или username'}

        valid_rows = 0
        for _, row in df.iterrows():
            phone = self._normalize_phone_cell(row[phone_col]) if phone_col and pd.notna(row[phone_col]) else None
            user_id = None
            username = None

            if user_id_col and pd.notna(row[user_id_col]):
                raw_user = row[user_id_col]
                if not self._looks_like_phone_number(raw_user):
                    user_id = self._parse_int_cell(raw_user)

            if username_col and pd.notna(row[username_col]):
                username = str(row[username_col]).strip().lstrip('@')
                if username == '':
                    username = None

            if phone or user_id or username:
                valid_rows += 1

        if valid_rows == 0:
            return {'ok': False, 'error': 'В файле нет валидных строк с phone/user_id/username'}

        return {
            'ok': True,
            'rows_total': int(len(df)),
            'rows_valid': int(valid_rows),
            'columns': {
                'phone': phone_col,
                'user_id': user_id_col,
                'username': username_col
            }
        }
    
    def import_contacts(self, campaign_id: int, file_path: str) -> dict:
        """Импорт контактов из CSV/Excel"""
        imported = 0
        skipped = 0
        
        # Определяем тип файла и читаем
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        # Нормализуем названия колонок
        df.columns = [col.lower().strip() for col in df.columns]
        
        logger.info(f"Загружен файл с колонками: {list(df.columns)}")
        
        # Ищем нужные колонки
        phone_col = next((col for col in df.columns if 'phone' in col or 'tel' in col or 'номер' in col), None)
        name_col = next((col for col in df.columns if 'name' in col or 'имя' in col), None)
        company_col = next((col for col in df.columns if 'company' in col or 'компан' in col), None)
        position_col = next((col for col in df.columns if 'position' in col or 'должн' in col or 'role' in col), None)
        user_id_col = next((col for col in df.columns if 'user_id' in col or 'userid' in col or 'telegram_id' in col or 'tg_id' in col), None)
        username_col = next((col for col in df.columns if col in ['username', 'telegram_username', 'tg_username']), None)
        access_hash_col = next((col for col in df.columns if 'access_hash' in col or 'user_hash' in col), None)
        
        # Проверяем наличие обязательных колонок
        if not phone_col and not user_id_col and not username_col:
            raise Exception("Не найдена колонка phone, user_id или username")
        
        logger.info(
            f"Найдены колонки: phone={phone_col}, name={name_col}, company={company_col}, "
            f"position={position_col}, user_id={user_id_col}, username={username_col}, access_hash={access_hash_col}"
        )
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for _, row in df.iterrows():
                # Получаем данные
                phone = None
                user_id = None
                username = None
                access_hash = None
                
                # Колонка phone всегда трактуется как телефон.
                if phone_col and pd.notna(row[phone_col]):
                    phone = self._normalize_phone_cell(row[phone_col])
                
                # Если есть отдельная колонка с user_id, используем её
                if user_id_col and pd.notna(row[user_id_col]):
                    raw_user_val = row[user_id_col]
                    if self._looks_like_phone_number(raw_user_val):
                        if not phone:
                            phone = self._normalize_phone_cell(raw_user_val)
                    else:
                        parsed_user_id = self._parse_int_cell(raw_user_val)
                        if parsed_user_id is not None:
                            user_id = parsed_user_id

                if username_col and pd.notna(row[username_col]):
                    username = str(row[username_col]).strip()
                    if username.startswith('@'):
                        username = username[1:]
                    if username == '':
                        username = None

                if access_hash_col and pd.notna(row[access_hash_col]):
                    parsed_access_hash = self._parse_int_cell(row[access_hash_col])
                    access_hash = str(parsed_access_hash) if parsed_access_hash is not None else None
                
                # Нужен хотя бы один идентификатор
                if not phone and not user_id and not username:
                    logger.warning(f"Строка пропущена: нет ни телефона, ни user_id, ни username: {row.to_dict()}")
                    continue

                if user_id:
                    cursor.execute('SELECT 1 FROM outreach_blacklist WHERE user_id = ?', (user_id,))
                    if cursor.fetchone():
                        skipped += 1
                        logger.info(f"Контакт user_id {user_id} в blacklist, пропускаем")
                        continue
                
                # Получаем остальные поля
                name = row[name_col] if name_col and pd.notna(row[name_col]) else None
                company = row[company_col] if company_col and pd.notna(row[company_col]) else None
                position = row[position_col] if position_col and pd.notna(row[position_col]) else None
                
                # Проверяем, нет ли уже такого контакта (по телефону или user_id)
                if phone:
                    cursor.execute('''
                        SELECT id, status FROM outreach_contacts 
                        WHERE campaign_id = ? AND phone = ?
                    ''', (campaign_id, phone))
                    existing = cursor.fetchone()
                    if existing:
                        if existing[1] == 'failed':
                            cursor.execute('''
                                UPDATE outreach_contacts
                                SET status = 'new', notes = NULL
                                WHERE id = ?
                            ''', (existing[0],))
                            imported += 1
                            logger.info(f"Контакт {phone} был failed, возвращен в new")
                        else:
                            logger.info(f"Контакт с телефоном {phone} уже существует, пропускаем")
                            skipped += 1
                        continue
                
                if user_id:
                    cursor.execute('''
                        SELECT id, status FROM outreach_contacts 
                        WHERE campaign_id = ? AND user_id = ?
                    ''', (campaign_id, user_id))
                    existing = cursor.fetchone()
                    if existing:
                        if existing[1] == 'failed':
                            cursor.execute('''
                                UPDATE outreach_contacts
                                SET status = 'new', notes = NULL
                                WHERE id = ?
                            ''', (existing[0],))
                            imported += 1
                            logger.info(f"Контакт user_id {user_id} был failed, возвращен в new")
                        else:
                            logger.info(f"Контакт с user_id {user_id} уже существует, пропускаем")
                            skipped += 1
                        continue

                if username:
                    cursor.execute('''
                        SELECT id, status FROM outreach_contacts
                        WHERE campaign_id = ? AND username = ?
                    ''', (campaign_id, username))
                    existing = cursor.fetchone()
                    if existing:
                        if existing[1] == 'failed':
                            cursor.execute('''
                                UPDATE outreach_contacts
                                SET status = 'new', notes = NULL
                                WHERE id = ?
                            ''', (existing[0],))
                            imported += 1
                            logger.info(f"Контакт username {username} был failed, возвращен в new")
                        else:
                            logger.info(f"Контакт с username {username} уже существует, пропускаем")
                            skipped += 1
                        continue
                
                # Вставляем новый контакт
                cursor.execute('''
                    INSERT INTO outreach_contacts 
                    (campaign_id, phone, username, access_hash, name, company, position, source_file, status, user_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'new', ?)
                ''', (
                    campaign_id, 
                    phone,
                    username,
                    access_hash,
                    name,
                    company,
                    position,
                    os.path.basename(file_path),
                    user_id
                ))
                imported += 1
                logger.info(f"Добавлен контакт: phone={phone}, user_id={user_id}, username={username}, name={name}")
            
            # Обновляем общее количество контактов в кампании
            cursor.execute('''
                UPDATE outreach_campaigns 
                SET total_contacts = total_contacts + ? 
                WHERE id = ?
            ''', (imported, campaign_id))
            
            conn.commit()
            logger.info(f"Импорт завершен: добавлено {imported}, пропущено {skipped}")
        if imported == 0:
            raise Exception("Импорт не выполнен: нет валидных контактов для добавления (или все в blacklist/дубликаты)")
        return {'imported': imported, 'skipped': skipped}
    
    def get_campaign_contacts(self, campaign_id: int, limit: int = 50, offset: int = 0) -> dict:
        """Получение контактов кампании с пагинацией и агрегированной статистикой."""
        limit = max(1, min(int(limit or 50), 1000))
        offset = max(0, int(offset or 0))
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*)
                FROM outreach_contacts
                WHERE campaign_id = ?
            ''', (campaign_id,))
            total = int(cursor.fetchone()[0] or 0)

            cursor.execute('''
                SELECT status, COUNT(*)
                FROM outreach_contacts
                WHERE campaign_id = ?
                GROUP BY status
            ''', (campaign_id,))
            counts_raw = cursor.fetchall()
            status_counts = {
                'new': 0,
                'sent': 0,
                'ignored': 0,
                'replied': 0,
                'failed': 0
            }
            for status, count in counts_raw:
                key = str(status or '').lower()
                if key in status_counts:
                    status_counts[key] = int(count or 0)

            cursor.execute('''
                SELECT oc.id, oc.phone, oc.name, oc.company, oc.position, oc.status, 
                       oc.message_sent_at, oc.replied_at, oc.last_message, oc.notes, oc.account_used,
                       oc.user_id, oc.username, oc.access_hash,
                       ls.last_step_id
                FROM outreach_contacts oc
                LEFT JOIN (
                    SELECT contact_id, MAX(step_id) as last_step_id
                    FROM conversations
                    WHERE direction = 'outgoing'
                    GROUP BY contact_id
                ) ls ON ls.contact_id = oc.id
                WHERE oc.campaign_id = ?
                ORDER BY 
                    CASE oc.status
                        WHEN 'new' THEN 1
                        WHEN 'sent' THEN 2
                        WHEN 'ignored' THEN 3
                        WHEN 'replied' THEN 3
                        WHEN 'interested' THEN 4
                        ELSE 5
                    END,
                    oc.id DESC
                LIMIT ?
                OFFSET ?
            ''', (campaign_id, limit, offset))
            
            contacts = []
            for row in cursor.fetchall():
                contacts.append({
                    'id': row[0],
                    'phone': row[1] or '—',
                    'name': row[2] or 'Не указано',
                    'company': row[3] or '-',
                    'position': row[4] or '-',
                    'status': row[5],
                    'message_sent_at': row[6],
                    'replied_at': row[7],
                    'last_message': row[8],
                    'notes': row[9],
                    'account_used': row[10],
                    'user_id': row[11],
                    'username': row[12],
                    'access_hash': row[13],
                    'last_step_id': row[14]
                })
            return {
                'items': contacts,
                'total': total,
                'limit': limit,
                'offset': offset,
                'status_counts': status_counts
            }
    
    def get_templates(self) -> List[dict]:
        """Получение списка шаблонов"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, name, template_text FROM outreach_templates ORDER BY name')
            return [{'id': r[0], 'name': r[1], 'text': r[2]} for r in cursor.fetchall()]
    
    def save_template(self, name: str, text: str):
        """Сохранение шаблона"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO outreach_templates (name, template_text)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET template_text = ?
            ''', (name, text, text))
            conn.commit()
    
    def update_campaign_status(self, campaign_id: int, status: str):
        """Обновление статуса кампании"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat() if status == 'active' else None
            cursor.execute('''
                UPDATE outreach_campaigns 
                SET status = ?, started_at = COALESCE(started_at, ?)
                WHERE id = ?
            ''', (status, now, campaign_id))
            conn.commit()


class AccountStats:
    """Класс для хранения статистики использования аккаунта"""
    def __init__(self, session_name):
        self.session_name = session_name
        self.phone = None
        self.checked_today = 0
        self.last_check_date = date.today().isoformat()
        self.total_checked = 0
        self.is_banned = False
        self.ban_reason = None
        self.last_used = None
        self.test_checks_count = 0
        self.test_failures = 0
        self.is_frozen = False
        self.frozen_reason = None
        self.test_quality = "unknown"
        self.test_history = []
        self.last_test_date = None
        self.account_type = "checker"
        self.proxy_id = None
        self.fingerprint_id = None
        self.outreach_daily_limit = 20
        
    def can_use_today(self):
        """Check if account can be used today"""
        if self.is_banned or self.is_frozen:
            return False
        if self.last_check_date != date.today().isoformat():
            self.checked_today = 0
            self.test_checks_count = 0
            self.test_failures = 0
            self.last_check_date = date.today().isoformat()
        limit = 20
        if self.account_type == "outreach":
            try:
                limit = max(1, int(self.outreach_daily_limit or 20))
            except (TypeError, ValueError):
                limit = 20
        return self.checked_today < limit
    
    def increment_checked(self, is_test: bool = False):
        """Increment checked counter"""
        self.checked_today += 1
        self.total_checked += 1
        self.last_used = datetime.now().isoformat()
        if is_test:
            self.test_checks_count += 1
    
    def record_test_failure(self):
        """Записать провал тестовой проверки"""
        self.test_failures += 1
        return self.test_failures >= 2
    
    def record_test_success(self):
        """Записать успешную тестовую проверку"""
        self.test_failures = 0
        
    def mark_frozen(self, reason: str = "Frozen account"):
        """Пометить аккаунт как frozen"""
        self.is_frozen = True
        self.frozen_reason = reason
        self.is_banned = True
        
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'session_name': self.session_name,
            'phone': self.phone,
            'checked_today': self.checked_today,
            'last_check_date': self.last_check_date,
            'total_checked': self.total_checked,
            'is_banned': self.is_banned,
            'ban_reason': self.ban_reason,
            'last_used': self.last_used,
            'test_checks_count': self.test_checks_count,
            'test_failures': self.test_failures,
            'is_frozen': self.is_frozen,
            'frozen_reason': self.frozen_reason,
            'test_quality': self.test_quality,
            'last_test_date': self.last_test_date,
            'test_history': self.test_history,
            'account_type': self.account_type,
            'proxy_id': self.proxy_id,
            'fingerprint_id': self.fingerprint_id,
            'outreach_daily_limit': self.outreach_daily_limit
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create from dictionary"""
        stats = cls(data['session_name'])
        stats.phone = data.get('phone')
        stats.checked_today = data.get('checked_today', 0)
        stats.last_check_date = data.get('last_check_date', date.today().isoformat())
        stats.total_checked = data.get('total_checked', 0)
        stats.is_banned = data.get('is_banned', False)
        stats.ban_reason = data.get('ban_reason')
        stats.last_used = data.get('last_used')
        stats.test_checks_count = data.get('test_checks_count', 0)
        stats.test_failures = data.get('test_failures', 0)
        stats.is_frozen = data.get('is_frozen', False)
        stats.frozen_reason = data.get('frozen_reason')
        stats.test_quality = data.get('test_quality', 'unknown')
        stats.last_test_date = data.get('last_test_date')
        stats.test_history = data.get('test_history', [])
        stats.account_type = data.get('account_type', 'checker')
        stats.proxy_id = data.get('proxy_id')
        stats.fingerprint_id = data.get('fingerprint_id')
        stats.outreach_daily_limit = data.get('outreach_daily_limit', 20)
        return stats


class TelegramAccountManager:
    def __init__(self, api_id=None, api_hash=None, session_dir='sessions', stats_file='account_stats.json'):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_dir = session_dir
        self.stats_file = stats_file
        self.accounts = []
        self.stats_dict = {}
        self.db = Database()
        self.test_phone_index = 0
        self.target_chat = None
        
        if api_id is None:
            self.api_id = self.db.get_config('api_id')
        if api_hash is None:
            self.api_hash = self.db.get_config('api_hash')
    
    def save_api_config(self, api_id: str, api_hash: str):
        """Сохранение API конфигурации"""
        self.api_id = api_id
        self.api_hash = api_hash
        self.db.set_config('api_id', str(api_id))
        self.db.set_config('api_hash', api_hash)
        logger.info("API конфигурация сохранена")
    
    def get_next_test_phone(self) -> str:
        """Получение следующего тестового номера"""
        return VALID_TEST_PHONE
    
    async def test_new_session(self, session_name: str, client) -> Tuple[bool, str, dict]:
        """
        Тестирование новой сессии на валидных номерах
        Возвращает (is_valid: bool, reason: str, details: dict)
        """
        logger.info(f"🔍 Тестирование новой сессии {session_name}...")

        test_phone = VALID_TEST_PHONE
        successes = 0
        failures = 0
        frozen_detected = False
        details = []

        imported_users = []
        try:
            logger.info(f"  Тест 1/1: {test_phone}")

            contact = InputPhoneContact(
                client_id=0,
                phone=test_phone,
                first_name="Test",
                last_name="User"
            )

            result = await client(ImportContactsRequest([contact]))
            imported_users = list(getattr(result, 'users', []) or [])

            if imported_users:
                successes += 1
                logger.info(f"  ✅ Найден: {test_phone}")
                details.append({'phone': test_phone, 'result': 'success'})
            else:
                failures += 1
                logger.warning(f"  ⚠️ Не найден: {test_phone}")
                details.append({'phone': test_phone, 'result': 'not_found'})

        except FloodWaitError as e:
            logger.warning(f"  ⏳ Flood wait {e.seconds}с - ждем")
            details.append({'phone': test_phone, 'result': 'flood_wait', 'seconds': e.seconds})
            await asyncio.sleep(e.seconds)
            failures += 1

        except Exception as e:
            error_str = str(e).lower()
            if "frozen" in error_str or "frozen_method_invalid" in error_str:
                logger.error(f"  ❄️ Сессия FROZEN!")
                frozen_detected = True
                details.append({'phone': test_phone, 'result': 'frozen'})
            elif "phone number invalid" in error_str:
                logger.error(f"  ❌ Неверный номер телефона")
                failures += 1
                details.append({'phone': test_phone, 'result': 'invalid_number'})
            else:
                logger.error(f"  ❌ Ошибка: {e}")
                failures += 1
                details.append({'phone': test_phone, 'result': 'error', 'error': str(e)})
        finally:
            # После теста всегда удаляем добавленный тест-контакт,
            # чтобы следующая проверка снова шла через ImportContactsRequest.
            if imported_users:
                try:
                    await client(DeleteContactsRequest(id=imported_users))
                    logger.info(f"  🗑️ Тест-контакт удален: {test_phone}")
                except Exception as cleanup_error:
                    logger.warning(f"  ⚠️ Не удалось удалить тест-контакт {test_phone}: {cleanup_error}")
        
        test_results = {
            'successes': successes,
            'failures': failures,
            'frozen': frozen_detected,
            'details': details
        }
        
        if frozen_detected:
            return False, "frozen", test_results
        
        if successes >= 1:
            logger.info(f"✅ Сессия {session_name} ВАЛИДНА (найдено {successes}/1)")
            return True, "good", test_results
        else:
            logger.error(f"❌ Сессия {session_name} НЕВАЛИДНА (найдено {successes}/1)")
            return False, "invalid", test_results
    
    def load_stats(self):
        """Load statistics from file"""
        if os.path.exists(self.stats_file):
            try:
                with open(self.stats_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for session_name, stats_data in data.items():
                        self.stats_dict[session_name] = AccountStats.from_dict(stats_data)
                        
                        db_stats = self.db.get_account_stats(stats_data.get('phone'))
                        if db_stats:
                            if db_stats.get('is_frozen'):
                                self.stats_dict[session_name].mark_frozen()
                            elif db_stats.get('is_banned'):
                                self.stats_dict[session_name].is_banned = True
                                self.stats_dict[session_name].ban_reason = "Banned"
                            
                            if db_stats.get('test_quality'):
                                self.stats_dict[session_name].test_quality = db_stats['test_quality']
                            if db_stats.get('last_test_date'):
                                self.stats_dict[session_name].last_test_date = db_stats['last_test_date']
                            if db_stats.get('test_history'):
                                self.stats_dict[session_name].test_history = db_stats['test_history']
                            if db_stats.get('account_type'):
                                self.stats_dict[session_name].account_type = db_stats['account_type']
                            if db_stats.get('proxy_id'):
                                self.stats_dict[session_name].proxy_id = db_stats['proxy_id']
            except Exception as e:
                logger.error(f"Error loading statistics: {e}")
                
    def save_stats(self):
        """Save statistics to file"""
        try:
            data = {}
            for session_name, stats in self.stats_dict.items():
                data[session_name] = stats.to_dict()
            with open(self.stats_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving statistics: {e}")
    
    async def join_chat(self, client, stats, chat_username: str) -> bool:
        """Вступление аккаунта в чат/канал"""
        try:
            db_stats = self.db.get_account_stats(stats.phone)
            if db_stats and chat_username in db_stats.get('joined_chats', []):
                logger.info(f"✅ Аккаунт {stats.phone} уже в чате {chat_username}")
                return True
            
            try:
                if '+' in chat_username or 'joinchat' in chat_username:
                    if 'joinchat/' in chat_username:
                        hash_part = chat_username.split('joinchat/')[-1]
                    else:
                        hash_part = chat_username.replace('+', '')
                    await client(ImportChatInviteRequest(hash_part))
                else:
                    entity = await client.get_entity(chat_username)
                    await client(JoinChannelRequest(entity))
                
                logger.info(f"✅ Аккаунт {stats.phone} успешно вступил в чат {chat_username}")
                self.db.mark_chat_joined(stats.phone, chat_username)
                return True
                
            except UserAlreadyParticipantError:
                logger.info(f"✅ Аккаунт {stats.phone} уже участник чата {chat_username}")
                self.db.mark_chat_joined(stats.phone, chat_username)
                return True
                
            except FloodWaitError as e:
                logger.warning(f"⏳ Flood wait при вступлении в чат {stats.phone}: {e.seconds} сек")
                await asyncio.sleep(e.seconds)
                return False
                
            except Exception as e:
                logger.error(f"❌ Ошибка вступления в чат {stats.phone}: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка в join_chat: {e}")
            return False
    
    async def ensure_all_accounts_in_chat(self, chat_username: str):
        """Проверить и обеспечить вступление всех аккаунтов в чат"""
        if not self.accounts:
            logger.warning("Нет загруженных аккаунтов")
            return
        
        logger.info(f"🔍 Проверяем вступление всех аккаунтов в чат {chat_username}...")
        
        results = {
            'success': 0,
            'already': 0,
            'failed': 0,
            'total': len(self.accounts)
        }
        
        for client, stats in self.accounts:
            db_stats = self.db.get_account_stats(stats.phone)
            if db_stats and chat_username in db_stats.get('joined_chats', []):
                logger.info(f"✅ Аккаунт {stats.phone} уже в чате (по БД)")
                results['already'] += 1
                continue
            
            success = await self.join_chat(client, stats, chat_username)
            if success:
                if db_stats and chat_username in db_stats.get('joined_chats', []):
                    results['already'] += 1
                else:
                    results['success'] += 1
            else:
                results['failed'] += 1
            
            await asyncio.sleep(2)
        
        logger.info(f"📊 Результаты вступления в чат {chat_username}:")
        logger.info(f"   ✅ Успешно: {results['success']}")
        logger.info(f"   🔄 Уже были: {results['already']}")
        logger.info(f"   ❌ Ошибок: {results['failed']}")
        logger.info(f"   📱 Всего аккаунтов: {results['total']}")
        
        return results
    
    async def load_accounts(self, target_chat: str = None):
        """Load all available accounts with proxies and fingerprints"""
        if not self.api_id or not self.api_hash:
            logger.error("API ID и API Hash не настроены")
            return False
            
        Path(self.session_dir).mkdir(exist_ok=True)
        self.load_stats()
        
        from proxy_manager import ProxyManager
        from fingerprint_manager import FingerprintManager
        
        proxy_manager = ProxyManager()
        fingerprint_manager = FingerprintManager()
        
        session_files = []
        for file in os.listdir(self.session_dir):
            if file.endswith('.session'):
                session_name = file[:-8]
                session_files.append(session_name)
        
        if not session_files:
            return False
        
        for session_name in session_files:
            if session_name not in self.stats_dict:
                logger.info(f"🆕 Обнаружена новая сессия: {session_name}")
                
                try:
                    session_path = os.path.join(self.session_dir, session_name)
                    
                    db_account_type = None
                    with sqlite3.connect(self.db.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            SELECT phone, account_type FROM account_stats WHERE session_name = ?
                        ''', (session_name,))
                        row = cursor.fetchone()
                        
                        if row:
                            fingerprint = fingerprint_manager.get_fingerprint(row[0])
                            db_account_type = (row[1] or 'checker')
                        else:
                            fingerprint = None

                    # Автотестим новые сессии только для checker-аккаунтов.
                    if db_account_type != 'checker':
                        logger.info(f"ℹ️ Пропускаем автотест новой сессии {session_name}: account_type={db_account_type or 'unknown'}")
                        continue
                    
                    if not fingerprint:
                        fingerprint = fingerprint_manager.generate_fingerprint('checker')
                    
                    test_client = TelegramClient(
                        session_path, 
                        api_id=int(self.api_id), 
                        api_hash=self.api_hash,
                        device_model=fingerprint['device_model'],
                        app_version=fingerprint['app_version'],
                        lang_code=fingerprint['lang_code'],
                        system_lang_code=fingerprint['system_lang_code']
                    )
                    
                    with sqlite3.connect(self.db.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            SELECT a.proxy_id, p.* FROM account_stats a
                            LEFT JOIN proxies p ON a.proxy_id = p.id
                            WHERE a.session_name = ?
                        ''', (session_name,))
                        proxy_row = cursor.fetchone()
                    
                    if proxy_row and proxy_row[1]:
                        proxy = {
                            'id': proxy_row[1],
                            'ip': proxy_row[2],
                            'port': proxy_row[3],
                            'protocol': proxy_row[4],
                            'username': proxy_row[5],
                            'password': proxy_row[6]
                        }
                        proxy_config = build_telethon_proxy_config(proxy)
                        
                        if proxy_config:
                            test_client = TelegramClient(
                                session_path,
                                api_id=int(self.api_id),
                                api_hash=self.api_hash,
                                device_model=fingerprint['device_model'],
                                app_version=fingerprint['app_version'],
                                lang_code=fingerprint['lang_code'],
                                system_lang_code=fingerprint['system_lang_code'],
                                proxy=proxy_config
                            )
                    
                    await test_client.connect()
                    
                    if not await test_client.is_user_authorized():
                        logger.warning(f"❌ Сессия {session_name} не авторизована")
                        await test_client.disconnect()
                        continue
                    
                    me = await test_client.get_me()
                    phone = me.phone
                    is_valid, reason, test_results = await self.test_new_session(session_name, test_client)
                    await test_client.disconnect()
                    
                    fingerprint_manager.save_fingerprint(phone, fingerprint)
                    
                    stats = AccountStats(session_name)
                    stats.phone = phone
                    
                    with sqlite3.connect(self.db.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            SELECT account_type FROM account_stats WHERE phone = ?
                        ''', (phone,))
                        row = cursor.fetchone()
                        if row:
                            stats.account_type = row[0]
                    
                    if reason == "frozen":
                        logger.error(f"❄️ Сессия {session_name} FROZEN - удаляем")
                        self.db.delete_frozen_session(session_name, self.session_dir)
                        self.db.mark_account_banned(phone, "frozen")
                        
                    elif reason == "invalid":
                        logger.error(f"❌ Сессия {session_name} не прошла тест - удаляем")
                        self.db.delete_frozen_session(session_name, self.session_dir)
                        
                    else:
                        stats.test_quality = reason
                        stats.last_test_date = datetime.now().isoformat()
                        stats.test_history = [test_results]
                        self.stats_dict[session_name] = stats
                        
                        with sqlite3.connect(self.db.db_path) as conn:
                            cursor = conn.cursor()
                            cursor.execute('''
                                INSERT OR REPLACE INTO account_stats 
                                (phone, session_name, account_type, test_quality, last_test_date, test_history)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (phone, session_name, stats.account_type, reason, 
                                  stats.last_test_date, json.dumps([test_results])))
                            conn.commit()
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при тестировании {session_name}: {e}")
                    continue
        
        for session_name in session_files:
            if session_name not in self.stats_dict:
                self.stats_dict[session_name] = AccountStats(session_name)
            
            stats = self.stats_dict[session_name]

            # Для checker-процесса используем только checker-аккаунты.
            db_account_type = None
            try:
                with sqlite3.connect(self.db.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT account_type FROM account_stats WHERE session_name = ?', (session_name,))
                    row = cursor.fetchone()
                    if row and row[0]:
                        db_account_type = row[0]
            except Exception:
                db_account_type = None

            if db_account_type:
                stats.account_type = db_account_type
            if db_account_type != 'checker':
                continue
            
            if stats.is_banned or stats.is_frozen:
                continue
            
            try:
                session_path = os.path.join(self.session_dir, session_name)
                
                fingerprint = None
                if stats.phone:
                    fingerprint = fingerprint_manager.get_fingerprint(stats.phone)
                
                if not fingerprint:
                    fingerprint = fingerprint_manager.generate_fingerprint(stats.account_type)
                    if stats.phone:
                        fingerprint_manager.save_fingerprint(stats.phone, fingerprint)
                
                proxy_config = None
                if stats.proxy_id:
                    proxy = proxy_manager.get_proxy(stats.proxy_id)
                    if proxy and proxy['is_active']:
                        proxy_config = build_telethon_proxy_config(proxy)
                        if proxy_config:
                            proxy_manager.mark_proxy_used(proxy['id'])
                
                client_kwargs = {
                    'device_model': fingerprint['device_model'],
                    'app_version': fingerprint['app_version'],
                    'lang_code': fingerprint['lang_code'],
                    'system_lang_code': fingerprint['system_lang_code']
                }
                
                if proxy_config:
                    client_kwargs['proxy'] = proxy_config
                
                client = TelegramClient(
                    session_path,
                    api_id=int(self.api_id),
                    api_hash=self.api_hash,
                    **client_kwargs
                )
                
                await client.connect()
                
                if not await client.is_user_authorized():
                    continue
                
                me = await client.get_me()
                stats.phone = me.phone
                
                # Обновляем checked_today из БД если есть
                db_stats = self.db.get_account_stats(stats.phone)
                if db_stats:
                    stats.checked_today = db_stats.get('checked_today', 0)
                    stats.last_check_date = db_stats.get('last_check_date', date.today().isoformat())
                    stats.outreach_daily_limit = db_stats.get('outreach_daily_limit', 20)
                
                self.accounts.append((client, stats))
                
            except Exception as e:
                error_str = str(e)
                if "frozen" in error_str.lower():
                    logger.error(f"❄️ Аккаунт {session_name} FROZEN при загрузке")
                    if stats.phone:
                        self.db.mark_account_banned(stats.phone, "frozen")
                    self.db.delete_frozen_session(session_name, self.session_dir)
                else:
                    logger.error(f"Error loading account {session_name}: {e}")
                    stats.is_banned = True
                    stats.ban_reason = str(e)
        
        self.save_stats()
        
        if target_chat and self.accounts:
            await self.ensure_all_accounts_in_chat(target_chat)
        
        return len(self.accounts) > 0
    
    def get_available_accounts(self):
        """Get list of available accounts for today"""
        available = []
        for client, stats in self.accounts:
            if stats.can_use_today():
                available.append((client, stats))
        available.sort(key=lambda x: x[1].checked_today)
        return available
    
    async def get_next_account(self):
        """Get next available account"""
        available = self.get_available_accounts()
        if not available:
            return None
        return available[0]
    
    async def check_phone(self, client, stats, phone_number, is_test: bool = False):
        """Check if phone is registered in Telegram"""
        try:
            if not phone_number.startswith('+'):
                phone_number = '+' + phone_number
            
            contact = InputPhoneContact(
                client_id=0,
                phone=phone_number,
                first_name="Check",
                last_name="User"
            )
            
            result = await client(ImportContactsRequest([contact]))
            
            if result.users:
                user = result.users[0]
                stats.increment_checked(is_test)
                
                if is_test:
                    stats.record_test_success()
                
                user_data = {
                    'user_id': user.id,
                    'username': user.username,
                    'first_name': user.first_name,
                    'last_name': user.last_name
                }
                self.db.save_check(phone_number, True, user_data, stats.phone, is_test)
                self.save_stats()
                
                return {
                    'registered': True,
                    'user_id': user.id,
                    'username': user.username,
                    'first_name': user.first_name,
                    'last_name': user.last_name,
                    'phone': phone_number,
                    'account_used': stats.phone,
                    'is_test': is_test
                }
            else:
                stats.increment_checked(is_test)
                
                if is_test:
                    if stats.record_test_failure():
                        logger.warning(f"⚠️ Аккаунт {stats.phone} возможно в теневом бане (2 теста подряд не найдены)")
                        stats.is_banned = True
                        stats.ban_reason = "Shadow ban detected"
                        self.db.mark_account_banned(stats.phone, "shadow")
                
                self.db.save_check(phone_number, False, None, stats.phone, is_test)
                self.save_stats()
                
                return {
                    'registered': False, 
                    'phone': phone_number,
                    'account_used': stats.phone,
                    'is_test': is_test
                }
                
        except errors.FloodWaitError as e:
            logger.warning(f"Flood wait on {stats.phone}: {e.seconds}s")
            await asyncio.sleep(e.seconds)
            return None
        except errors.rpcerrorlist.PhoneNumberInvalidError:
            return {'registered': False, 'phone': phone_number, 'error': 'invalid_format', 'is_test': is_test}
        except errors.rpcerrorlist.PhoneNumberBannedError:
            return {'registered': False, 'phone': phone_number, 'error': 'banned', 'is_test': is_test}
        except errors.rpcerrorlist.AuthKeyDuplicatedError:
            logger.error(f"Auth key duplicated for {stats.phone}")
            stats.is_banned = True
            stats.ban_reason = "Auth key duplicated"
            self.save_stats()
            return None
        except Exception as e:
            error_str = str(e)
            frozen_markers = (
                "frozen accounts",
                "frozen_method_invalid",
                "method_invalid"
            )
            if any(marker in error_str.lower() for marker in frozen_markers):
                logger.error(f"❄️ Аккаунт {stats.phone} FROZEN при проверке ({error_str})")
                stats.mark_frozen()
                self.db.mark_account_banned(stats.phone, "frozen")
                self.db.delete_frozen_session(stats.session_name, self.session_dir)
                self.save_stats()
                return None
            else:
                logger.error(f"Error checking {phone_number}: {e}")
                return None
    
    async def send_contact_card(self, client, chat_id, user_data):
        """Отправка карточки контакта в чат"""
        try:
            chat_entity = await client.get_entity(chat_id)
            
            phone = user_data['phone']
            first_name = user_data.get('first_name', '') or 'Контакт'
            last_name = user_data.get('last_name', '') or ''
            
            media_contact = InputMediaContact(
                phone_number=phone,
                first_name=first_name,
                last_name=last_name,
                vcard=''
            )
            
            caption = f"📱 **Найден пользователь!**\n\n"
            caption += f"**Телефон:** `{phone}`\n"
            caption += f"**ID:** `{user_data['user_id']}`\n"
            
            if user_data.get('first_name'):
                caption += f"**Имя:** {user_data['first_name']}\n"
            if user_data.get('last_name'):
                caption += f"**Фамилия:** {user_data['last_name']}\n"
            if user_data.get('username'):
                caption += f"**Username:** @{user_data['username']}\n"
            
            if user_data.get('is_test'):
                caption += f"\n🔍 **ТЕСТОВАЯ ПРОВЕРКА**\n"
            
            caption += f"\n✅ Проверено аккаунтом: `{user_data.get('account_used', 'unknown')}`"
            
            await client.send_message(
                entity=chat_entity,
                message=caption,
                file=media_contact,
                parse_mode='markdown'
            )
            
            logger.info(f"✅ Контакт {phone} отправлен в чат {chat_id}" + (" (тест)" if user_data.get('is_test') else ""))
            return True
            
        except FloodWaitError as e:
            logger.warning(f"⏳ Flood wait при отправке контакта: {e.seconds} сек")
            await asyncio.sleep(e.seconds)
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка отправки контакта: {e}")
            return False
    
    async def close_all(self):
        """Close all clients"""
        for client, stats in self.accounts:
            try:
                await client.disconnect()
            except:
                pass
        self.save_stats()
        logger.info("Все аккаунты закрыты, статистика сохранена")
