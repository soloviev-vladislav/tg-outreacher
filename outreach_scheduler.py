import asyncio
import logging
import sqlite3
import json
import random
import os
import uuid
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
from telethon import TelegramClient, errors
from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import InputPhoneContact, InputPeerUser, PeerUser
from telethon.errors import UserAlreadyParticipantError
from telegram_bot import Database, AccountStats
from proxy_manager import ProxyManager
from fingerprint_manager import FingerprintManager

logger = logging.getLogger(__name__)

def build_telethon_proxy_config(proxy: Dict) -> Optional[Dict]:
    """
    Преобразует запись прокси из БД в формат, который ожидает Telethon.
    Строковый URL здесь не используем: часть окружений Telethon его не принимает.
    """
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

def render_spintax(text: str, rng=None, max_replacements: int = 200) -> str:
    """
    Простой рендер спинтакса:
    {a|b|c} -> выбирает один вариант.
    Поддерживает вложенность за счет обработки самых внутренних блоков.
    """
    if text is None:
        return ""
    rendered = str(text)
    if "{" not in rendered and "}" not in rendered:
        return rendered

    rng = rng or random.SystemRandom()
    replacements = 0

    while True:
        if replacements > max_replacements:
            raise ValueError("spintax_too_complex")

        stack = []
        pair_start = None
        pair_end = None
        for i, ch in enumerate(rendered):
            if ch == "{":
                stack.append(i)
            elif ch == "}":
                if not stack:
                    raise ValueError("spintax_unbalanced_braces")
                pair_start = stack.pop()
                pair_end = i
                break

        if pair_start is None:
            if stack:
                raise ValueError("spintax_unbalanced_braces")
            break

        inner = rendered[pair_start + 1:pair_end]
        parts = []
        current = []
        depth = 0
        for ch in inner:
            if ch == "{":
                depth += 1
                current.append(ch)
            elif ch == "}":
                if depth == 0:
                    raise ValueError("spintax_unbalanced_braces")
                depth -= 1
                current.append(ch)
            elif ch == "|" and depth == 0:
                parts.append("".join(current))
                current = []
            else:
                current.append(ch)
        if depth != 0:
            raise ValueError("spintax_unbalanced_braces")
        parts.append("".join(current))

        if len(parts) <= 1:
            # Некорректный блок без вариантов — оставляем как есть.
            choice = inner
        else:
            choice = parts[rng.randrange(len(parts))]

        rendered = rendered[:pair_start] + choice + rendered[pair_end + 1:]
        replacements += 1

    return rendered

class OutreachScheduler:
    """Планировщик рассылок для аутрич кампаний"""
    DEFAULT_TARGET_CHAT = ""
    POLL_INTERVAL_SECONDS = 15
    WARM_PARTICIPANTS_LIMIT = 3000
    WARM_MESSAGES_LIMIT = 800
    CAMPAIGN_LOCK_TTL_MINUTES = 1
    FOLLOWUP_MIN_INTERVAL_SECONDS = 120
    MAX_INITIAL_PER_CYCLE = 1
    MAX_PARALLEL_CAMPAIGNS = 8
    
    def __init__(self, db_path='checker.db', sessions_dir='sessions'):
        self.db = Database(db_path)
        self.sessions_dir = sessions_dir
        self.proxy_manager = ProxyManager(db_path)
        self.fingerprint_manager = FingerprintManager(db_path)
        self.running = False
        self.accounts_cache = {}
        self.rotation_state = {}
        self.target_chat_cache = {}
        self.target_chat_warm_state = {}
        self.contact_card_index = {}
        self.deep_warm_attempted = set()
        self.lock_owner = str(uuid.uuid4())
        self.rng = random.SystemRandom()
        # account_phone -> datetime когда можно отправить следующий фоллоуап
        self.followup_next_allowed_at = {}
        # (tenant_id, campaign_id) -> datetime когда можно отправить следующее первичное
        self.next_initial_send_at = {}
        self.account_send_locks: Dict[tuple, asyncio.Lock] = {}

    @staticmethod
    def _account_cache_key(tenant_id: int, account_phone: str):
        return (int(tenant_id), str(account_phone))

    @staticmethod
    def _campaign_cache_key(tenant_id: int, campaign_id: int):
        return (int(tenant_id), int(campaign_id))

    def _account_lock(self, tenant_id: int, account_phone: str) -> asyncio.Lock:
        key = self._account_cache_key(tenant_id, account_phone)
        lock = self.account_send_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self.account_send_locks[key] = lock
        return lock

    def _read_env_file_value(self, key: str) -> str:
        env_path = '.env'
        if not os.path.exists(env_path):
            return ''
        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    k, v = line.split('=', 1)
                    if k.strip() == key:
                        return v.strip().strip("\"'")
        except Exception:
            return ''
        return ''

    def _resolve_api_credentials(self, tenant_id: int):
        api_id = str(self.db.get_config('api_id', tenant_id=tenant_id) or '').strip()
        api_hash = str(self.db.get_config('api_hash', tenant_id=tenant_id) or '').strip()
        if api_id and api_hash:
            return api_id, api_hash

        api_id = str(os.getenv('TELEGRAM_API_ID', '')).strip()
        api_hash = str(os.getenv('TELEGRAM_API_HASH', '')).strip()
        if api_id and api_hash:
            return api_id, api_hash

        api_id = self._read_env_file_value('TELEGRAM_API_ID')
        api_hash = self._read_env_file_value('TELEGRAM_API_HASH')
        return api_id, api_hash
    
    async def start(self):
        """Запуск планировщика"""
        self.running = True
        logger.info("🚀 Outreach scheduler started")
        
        try:
            while self.running:
                try:
                    await self.process_campaigns()
                    await self.check_replies()
                    await asyncio.sleep(self.POLL_INTERVAL_SECONDS)
                except Exception as e:
                    logger.error(f"Error in scheduler: {e}")
                    await asyncio.sleep(10)
        finally:
            await self.close_cached_clients()
    
    def stop(self):
        """Остановка планировщика"""
        self.running = False
        logger.info("🛑 Outreach scheduler stopped")

    async def close_cached_clients(self):
        """Корректно закрыть подключенные Telegram клиенты."""
        for account_phone, client_data in list(self.accounts_cache.items()):
            client = client_data[0]
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting {account_phone}: {e}")
        self.accounts_cache.clear()
        self.target_chat_warm_state.clear()
        self.contact_card_index.clear()
        self.deep_warm_attempted.clear()
        self.followup_next_allowed_at.clear()
        self.next_initial_send_at.clear()
        self.account_send_locks.clear()

    def _get_followup_min_interval_seconds(self) -> int:
        value = os.getenv('FOLLOWUP_MIN_INTERVAL_SECONDS', '').strip()
        if value:
            try:
                return max(0, int(value))
            except ValueError:
                pass
        return self.FOLLOWUP_MIN_INTERVAL_SECONDS

    @staticmethod
    def _parse_db_datetime(value) -> Optional[datetime]:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            try:
                return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

    def _get_followup_wait_seconds(self, tenant_id: int, account_phone: str) -> int:
        """
        Минимальный интервал между фоллоуапами с одного аккаунта.
        Проверяем сначала in-memory таймер, затем БД (на случай рестарта процесса).
        """
        min_interval = self._get_followup_min_interval_seconds()
        if min_interval <= 0:
            return 0

        now = datetime.now()
        account_key = self._account_cache_key(tenant_id, account_phone)
        cached_next = self.followup_next_allowed_at.get(account_key)
        if cached_next and cached_next > now:
            return int((cached_next - now).total_seconds())

        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT MAX(c.sent_at)
                FROM conversations c
                JOIN outreach_contacts oc ON oc.id = c.contact_id
                WHERE c.direction = 'outgoing'
                  AND COALESCE(c.is_followup, 0) = 1
                  AND c.tenant_id = ?
                  AND oc.tenant_id = ?
                  AND oc.account_used = ?
            ''', (tenant_id, tenant_id, account_phone))
            row = cursor.fetchone()

        last_sent_at = self._parse_db_datetime(row[0] if row else None)
        if not last_sent_at:
            return 0

        next_allowed = last_sent_at + timedelta(seconds=min_interval)
        self.followup_next_allowed_at[account_key] = next_allowed
        if next_allowed <= now:
            return 0
        return int((next_allowed - now).total_seconds())

    def _mark_followup_sent_now(self, tenant_id: int, account_phone: str):
        min_interval = self._get_followup_min_interval_seconds()
        if min_interval <= 0:
            return
        account_key = self._account_cache_key(tenant_id, account_phone)
        self.followup_next_allowed_at[account_key] = datetime.now() + timedelta(seconds=min_interval)

    def _get_target_chat(self, tenant_id: int) -> Optional[str]:
        cached = self.target_chat_cache.get(int(tenant_id))
        if cached is not None:
            return cached

        form_data = self.db.load_form_data(tenant_id=tenant_id)
        target_chat = str(form_data.get('target_chat') or '').strip()
        if not target_chat:
            target_chat = str(self.db.get_config('outreach_target_chat', tenant_id=tenant_id) or '').strip()
        if not target_chat:
            target_chat = str(os.getenv('OUTREACH_TARGET_CHAT', '')).strip()

        self.target_chat_cache[int(tenant_id)] = target_chat or self.DEFAULT_TARGET_CHAT
        return target_chat or None

    @staticmethod
    def _normalize_chat_ref(chat_ref: str) -> str:
        value = (chat_ref or "").strip()
        if not value:
            return value
        lowered = value.lower()
        if lowered.startswith("https://t.me/"):
            value = value.split("https://t.me/", 1)[1]
        elif lowered.startswith("http://t.me/"):
            value = value.split("http://t.me/", 1)[1]
        value = value.strip().strip("/")
        if value.startswith("@"):
            value = value[1:]
        return value

    async def _ensure_account_in_target_chat(self, account_phone: str, client, target_chat: str) -> bool:
        """Если аккаунт не состоит в чате визиток, пытаемся вступить автоматически."""
        chat_ref = self._normalize_chat_ref(target_chat)
        if not chat_ref:
            return False
        try:
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
                logger.info(f"[{account_phone}] joined target chat: {chat_ref}")
                return True
            except UserAlreadyParticipantError:
                return True
            except Exception as e:
                # Возможен кейс, когда вступать не нужно/нельзя, но доступ уже есть.
                try:
                    await client.get_entity(chat_ref)
                    return True
                except Exception:
                    logger.warning(f"[{account_phone}] failed to join target chat {chat_ref}: {e}")
                    return False
        except Exception as e:
            logger.warning(f"[{account_phone}] ensure target chat failed {chat_ref}: {e}")
            return False

    async def _warm_entities_from_target_chat(self, tenant_id: int, account_phone: str, client, use_target_chat: bool = True):
        """
        Прогрев entity-кэша из целевого чата:
        1) get_entity(target_chat)
        2) get_participants для кэша участников
        3) iter_messages и индекс визиток (phone -> user_id)
        """
        account_key = self._account_cache_key(tenant_id, account_phone)
        if self.target_chat_warm_state.get(account_key):
            return

        phone_to_user = {}
        user_to_phone = {}
        phone_to_entity = {}
        user_to_entity = {}
        user_to_input_peer = {}

        async def ingest_contact_cards(chat_entity, limit_messages: int = None):
            nonlocal phone_to_user, user_to_phone, phone_to_entity, user_to_entity, user_to_input_peer
            if limit_messages is None:
                limit_messages = self.WARM_MESSAGES_LIMIT
            async for message in client.iter_messages(chat_entity, limit=limit_messages):
                media = getattr(message, 'media', None)
                if not media:
                    continue
                media_phone = getattr(media, 'phone_number', None)
                media_user_id = self._parse_int(getattr(media, 'user_id', None))
                normalized_phone = self._normalize_phone(media_phone) if media_phone else None
                if not normalized_phone or not media_user_id:
                    continue
                phone_to_user[normalized_phone] = media_user_id
                user_to_phone[media_user_id] = normalized_phone
                # Если в media есть access_hash, сразу сохраняем InputPeer.
                media_access_hash = self._parse_int(getattr(media, 'user_access_hash', None))
                if media_access_hash:
                    user_to_input_peer[media_user_id] = InputPeerUser(media_user_id, media_access_hash)
                try:
                    entity = await client.get_entity(PeerUser(media_user_id))
                    phone_to_entity[normalized_phone] = entity
                    user_to_entity[media_user_id] = entity
                    entity_access_hash = self._parse_int(getattr(entity, 'access_hash', None))
                    if entity_access_hash:
                        user_to_input_peer[media_user_id] = InputPeerUser(media_user_id, entity_access_hash)
                except Exception:
                    try:
                        entity = await client.get_entity(media_user_id)
                        phone_to_entity[normalized_phone] = entity
                        user_to_entity[media_user_id] = entity
                        entity_access_hash = self._parse_int(getattr(entity, 'access_hash', None))
                        if entity_access_hash:
                            user_to_input_peer[media_user_id] = InputPeerUser(media_user_id, entity_access_hash)
                    except Exception:
                        pass

        async def ingest_dialogs_fallback():
            """Прогрев из последних диалогов/групп, если target_chat не задан или нерелевантен."""
            nonlocal phone_to_user, user_to_phone, phone_to_entity, user_to_entity, user_to_input_peer
            try:
                dialogs = await client.get_dialogs(limit=200)
                scanned_dialogs = 0
                scanned_groups = 0
                for dialog in dialogs:
                    if scanned_dialogs >= 80:
                        break
                    scanned_dialogs += 1
                    entity = getattr(dialog, 'entity', None)
                    if not entity:
                        continue

                    is_group_like = bool(getattr(entity, 'megagroup', False) or getattr(entity, 'gigagroup', False))
                    if is_group_like and scanned_groups < 20:
                        try:
                            participants = await client.get_participants(entity, limit=500)
                            scanned_groups += 1
                            for participant in participants:
                                uid = self._parse_int(getattr(participant, 'id', None))
                                if not uid:
                                    continue
                                user_to_entity[uid] = participant
                                participant_phone = self._normalize_phone(getattr(participant, 'phone', None))
                                if participant_phone:
                                    user_to_phone[uid] = participant_phone
                                    phone_to_user[participant_phone] = uid
                                    phone_to_entity[participant_phone] = participant
                                participant_access_hash = self._parse_int(getattr(participant, 'access_hash', None))
                                if participant_access_hash:
                                    user_to_input_peer[uid] = InputPeerUser(uid, participant_access_hash)
                        except Exception:
                            pass

                    try:
                        await ingest_contact_cards(entity, limit_messages=150)
                    except Exception:
                        pass

                logger.info(
                    f"[{account_phone}] fallback dialog warm complete: dialogs={scanned_dialogs}, "
                    f"groups={scanned_groups}, cards={len(phone_to_user)}, users={len(user_to_entity)}"
                )
            except Exception as e:
                logger.warning(f"[{account_phone}] fallback dialogs warm failed: {e}")

        target_chat = self._get_target_chat(tenant_id) if use_target_chat else None
        chat_entity = None
        if target_chat:
            await self._ensure_account_in_target_chat(account_phone, client, target_chat)
            try:
                chat_entity = await client.get_entity(target_chat)
            except Exception as e:
                logger.warning(f"[{account_phone}] target_chat resolve failed ({target_chat}): {e}")
                chat_entity = None

        if chat_entity is not None:
            try:
                participants = await client.get_participants(chat_entity, limit=self.WARM_PARTICIPANTS_LIMIT)
                logger.info(f"[{account_phone}] warmed participants from {target_chat}: {len(participants)}")
                for participant in participants:
                    uid = self._parse_int(getattr(participant, 'id', None))
                    if not uid:
                        continue
                    user_to_entity[uid] = participant
                    participant_phone = self._normalize_phone(getattr(participant, 'phone', None))
                    if participant_phone:
                        user_to_phone[uid] = participant_phone
                        phone_to_user[participant_phone] = uid
                        phone_to_entity[participant_phone] = participant
                    participant_access_hash = self._parse_int(getattr(participant, 'access_hash', None))
                    if participant_access_hash:
                        user_to_input_peer[uid] = InputPeerUser(uid, participant_access_hash)
            except Exception as e:
                logger.warning(f"[{account_phone}] participants warm failed: {e}")

            try:
                await ingest_contact_cards(chat_entity)
            except Exception as e:
                logger.warning(f"[{account_phone}] contact-card scan failed: {e}")

        # Даже если target_chat задан, дополняем кеш из диалогов/групп.
        await ingest_dialogs_fallback()

        self.contact_card_index[account_key] = {
            'phone_to_user': phone_to_user,
            'user_to_phone': user_to_phone,
            'phone_to_entity': phone_to_entity,
            'user_to_entity': user_to_entity,
            'user_to_input_peer': user_to_input_peer
        }
        self.target_chat_warm_state[account_key] = True
        if phone_to_user:
            logger.info(f"[{account_phone}] contact-card index loaded: {len(phone_to_user)}")

    def _within_schedule(self, campaign: Dict) -> bool:
        schedule = campaign.get('schedule') or {}
        if not schedule:
            return True

        now = datetime.now()
        days = schedule.get('days') or []
        if days:
            current_day = (now.weekday() + 1) % 7
            if current_day not in days:
                return False

        start_time = schedule.get('start_time')
        end_time = schedule.get('end_time')
        if not start_time or not end_time:
            return True

        try:
            now_t = now.time()
            start_t = datetime.strptime(start_time, "%H:%M").time()
            end_t = datetime.strptime(end_time, "%H:%M").time()
        except ValueError:
            logger.warning("Invalid campaign schedule time format, skipping time window check")
            return True

        if start_t <= end_t:
            return start_t <= now_t <= end_t
        return now_t >= start_t or now_t <= end_t

    def _remaining_slots(self, campaign: Dict) -> int:
        """
        Лимиты кампании (day/hour) применяются только к первичным сообщениям.
        Фоллоуапы не расходуют эти слоты.
        """
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT COUNT(*) FROM conversations
                WHERE campaign_id = ? AND direction = 'outgoing'
                  AND tenant_id = ?
                  AND COALESCE(is_followup, 0) = 0
                  AND date(sent_at) = date('now')
            ''', (campaign['id'], campaign['tenant_id']))
            sent_today = cursor.fetchone()[0] or 0

            cursor.execute('''
                SELECT COUNT(*) FROM conversations
                WHERE campaign_id = ? AND direction = 'outgoing'
                  AND tenant_id = ?
                  AND COALESCE(is_followup, 0) = 0
                  AND datetime(sent_at) >= datetime('now', '-1 hour')
            ''', (campaign['id'], campaign['tenant_id']))
            sent_last_hour = cursor.fetchone()[0] or 0

        daily_limit = int(campaign.get('daily_limit') or 10)
        hourly_limit = int((campaign.get('schedule') or {}).get('hourly_limit') or daily_limit)

        remaining_daily = max(0, daily_limit - sent_today)
        remaining_hourly = max(0, hourly_limit - sent_last_hour)
        return min(remaining_daily, remaining_hourly)

    def _is_campaign_active(self, campaign_id: int, tenant_id: int) -> bool:
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status FROM outreach_campaigns WHERE id = ? AND tenant_id = ?",
                (campaign_id, tenant_id)
            )
            row = cursor.fetchone()
        return bool(row and row[0] == 'active')
    
    async def process_campaigns(self):
        """Обработка всех активных кампаний"""
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, tenant_id, name, message_template, accounts, daily_limit,
                       delay_min, delay_max, strategy, ai_enabled, ai_tone, schedule
                FROM outreach_campaigns
                WHERE status = 'active'
            ''')
            campaigns = cursor.fetchall()
        
        campaign_payloads = []
        for campaign in campaigns:
            campaign_payloads.append({
                'id': campaign[0],
                'tenant_id': campaign[1],
                'name': campaign[2],
                'message_template': campaign[3],
                'accounts': json.loads(campaign[4]),
                'daily_limit': campaign[5],
                'delay_min': campaign[6],
                'delay_max': campaign[7],
                'strategy': json.loads(campaign[8]) if campaign[8] else {"steps": []},
                'ai_enabled': bool(campaign[9]),
                'ai_tone': campaign[10],
                'schedule': json.loads(campaign[11]) if campaign[11] else None
            })

        if not campaign_payloads:
            return

        try:
            max_parallel = max(1, int(os.getenv('MAX_PARALLEL_CAMPAIGNS', '').strip() or self.MAX_PARALLEL_CAMPAIGNS))
        except Exception:
            max_parallel = self.MAX_PARALLEL_CAMPAIGNS

        sem = asyncio.Semaphore(max_parallel)

        async def _run_one(campaign_payload: Dict):
            async with sem:
                if not self._acquire_campaign_lock(campaign_payload['id'], campaign_payload['tenant_id']):
                    logger.info(f"Skip campaign {campaign_payload['id']} - locked by another scheduler")
                    return
                try:
                    await self.process_campaign(campaign_payload)
                finally:
                    self._release_campaign_lock(campaign_payload['id'], campaign_payload['tenant_id'])

        await asyncio.gather(*[_run_one(payload) for payload in campaign_payloads])

    def _acquire_campaign_lock(self, campaign_id: int, tenant_id: int) -> bool:
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO scheduler_campaign_locks (tenant_id, campaign_id, owner, locked_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (tenant_id, campaign_id, self.lock_owner))
            if cursor.rowcount == 1:
                conn.commit()
                return True

            cursor.execute(
                'SELECT owner FROM scheduler_campaign_locks WHERE tenant_id = ? AND campaign_id = ?',
                (tenant_id, campaign_id)
            )
            row = cursor.fetchone()
            if row and row[0] == self.lock_owner:
                cursor.execute('''
                    UPDATE scheduler_campaign_locks
                    SET locked_at = CURRENT_TIMESTAMP
                    WHERE tenant_id = ? AND campaign_id = ? AND owner = ?
                ''', (tenant_id, campaign_id, self.lock_owner))
                conn.commit()
                return True

            cursor.execute('''
                UPDATE scheduler_campaign_locks
                SET owner = ?, locked_at = CURRENT_TIMESTAMP
                WHERE tenant_id = ?
                  AND campaign_id = ?
                  AND datetime(locked_at) <= datetime('now', ?)
            ''', (self.lock_owner, tenant_id, campaign_id, f'-{self.CAMPAIGN_LOCK_TTL_MINUTES} minutes'))
            conn.commit()
            return cursor.rowcount == 1

    def _release_campaign_lock(self, campaign_id: int, tenant_id: int):
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM scheduler_campaign_locks
                WHERE tenant_id = ? AND campaign_id = ? AND owner = ?
            ''', (tenant_id, campaign_id, self.lock_owner))
            conn.commit()
    
    async def process_campaign(self, campaign: Dict):
        """Обработка конкретной кампании"""
        logger.info(f"Processing campaign: {campaign['name']}")
        tenant_id = int(campaign['tenant_id'])

        blacklisted_blocked = self._apply_blacklist_to_campaign(campaign['id'], tenant_id)

        ignored_marked = self._mark_ignored_contacts(campaign)

        if not self._within_schedule(campaign):
            logger.info(f"Campaign {campaign['id']} skipped by schedule window")
            return

        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()

            # Получаем контакты для фолоуапов
            cursor.execute('''
                SELECT oc.id, oc.phone, oc.name, oc.company, oc.position, oc.user_id, oc.username, oc.access_hash,
                       oc.account_used,
                       MAX(m.sent_at) as last_message
                FROM outreach_contacts oc
                LEFT JOIN conversations m ON oc.id = m.contact_id
                WHERE oc.campaign_id = ? 
                  AND oc.tenant_id = ?
                  AND oc.status = 'ignored'
                  AND (oc.user_id IS NULL OR oc.user_id NOT IN (SELECT user_id FROM outreach_blacklist WHERE tenant_id = ?))
                  AND (oc.user_id IS NOT NULL OR oc.username IS NOT NULL OR oc.phone IS NOT NULL)
                  AND NOT EXISTS (
                      SELECT 1 FROM conversations 
                      WHERE contact_id = oc.id AND direction = 'incoming' AND tenant_id = ?
                  )
                GROUP BY oc.id
                LIMIT 500
            ''', (campaign['id'], tenant_id, tenant_id, tenant_id))
            followup_contacts = cursor.fetchall()
        followup_candidates = len(followup_contacts)
        followup_sent = 0

        # Сначала отправляем фолоуапы (они НЕ учитываются в лимитах отправки),
        # затем первичные сообщения в рамках дневных/часовых лимитов кампании.
        for contact in followup_contacts:
            if not self._is_campaign_active(campaign['id'], tenant_id):
                logger.info(f"Campaign {campaign['id']} paused/stopped during followup batch")
                return
            account_used = contact[8] if len(contact) > 8 else None
            if account_used:
                wait_left = self._get_followup_wait_seconds(tenant_id, account_used)
                if wait_left > 0:
                    continue
            ordered_accounts = self._get_rotated_accounts(
                campaign['id'],
                await self._get_campaign_accounts(campaign, respect_daily_limits=False)
            )
            sent_ok = await self.send_followup(campaign, contact, ordered_accounts)
            if sent_ok:
                followup_sent += 1
                # Важный момент: фоллоуапы не должны задерживать первичные отправки.
                # Поэтому паузу здесь не делаем.

        slots_left = self._remaining_slots(campaign)
        if slots_left <= 0:
            logger.info(f"Campaign {campaign['id']} has no available initial-message slots")
            logger.info(
                f"Campaign {campaign['id']} summary: blacklisted_blocked={blacklisted_blocked}, ignored_marked={ignored_marked}, "
                f"followup={followup_sent}/{followup_candidates}, new=0/0, slots_left={slots_left}"
            )
            return

        campaign_key = self._campaign_cache_key(tenant_id, campaign['id'])
        now_dt = datetime.now()
        next_allowed_dt = self.next_initial_send_at.get(campaign_key)
        if next_allowed_dt and next_allowed_dt > now_dt:
            wait_left = int((next_allowed_dt - now_dt).total_seconds())
            logger.info(f"Campaign {campaign['id']} initial throttle active ({wait_left}s left)")
            logger.info(
                f"Campaign {campaign['id']} summary: blacklisted_blocked={blacklisted_blocked}, ignored_marked={ignored_marked}, "
                f"followup={followup_sent}/{followup_candidates}, new=0/0, slots_left={slots_left}"
            )
            return

        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT oc.id, oc.phone, oc.name, oc.company, oc.position, oc.user_id, oc.username, oc.access_hash
                FROM outreach_contacts oc
                WHERE oc.campaign_id = ? AND oc.tenant_id = ? AND oc.status = 'new'
                  AND (oc.user_id IS NULL OR oc.user_id NOT IN (SELECT user_id FROM outreach_blacklist WHERE tenant_id = ?))
                  AND (oc.user_id IS NOT NULL OR oc.phone IS NOT NULL OR oc.username IS NOT NULL)
                LIMIT ?
            ''', (campaign['id'], tenant_id, tenant_id, slots_left))
            new_contacts = cursor.fetchall()
        new_candidates = len(new_contacts)
        new_sent = 0

        sent_this_cycle = 0
        for contact in new_contacts:
            if not self._is_campaign_active(campaign['id'], tenant_id):
                logger.info(f"Campaign {campaign['id']} paused/stopped during new batch")
                return
            if slots_left <= 0:
                break
            if sent_this_cycle >= self.MAX_INITIAL_PER_CYCLE:
                break
            ordered_accounts = self._get_rotated_accounts(
                campaign['id'],
                await self._get_campaign_accounts(campaign)
            )
            sent_ok = await self.send_message(campaign, contact, ordered_accounts)
            if sent_ok:
                slots_left -= 1
                new_sent += 1
                sent_this_cycle += 1
                delay_seconds = self._pick_delay_seconds(campaign)
                if delay_seconds > 0:
                    self.next_initial_send_at[campaign_key] = datetime.now() + timedelta(seconds=delay_seconds)
                else:
                    self.next_initial_send_at[campaign_key] = datetime.now()

        logger.info(
            f"Campaign {campaign['id']} summary: blacklisted_blocked={blacklisted_blocked}, ignored_marked={ignored_marked}, "
            f"followup={followup_sent}/{followup_candidates}, new={new_sent}/{new_candidates}, slots_left={slots_left}"
        )

    def _apply_blacklist_to_campaign(self, campaign_id: int, tenant_id: int) -> int:
        """Остановить дальнейшую рассылку по контактам из blacklist."""
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE outreach_contacts
                SET status = 'failed',
                    notes = 'blacklisted'
                WHERE campaign_id = ?
                  AND tenant_id = ?
                  AND user_id IS NOT NULL
                  AND user_id IN (SELECT user_id FROM outreach_blacklist WHERE tenant_id = ?)
                  AND status IN ('new', 'sent', 'ignored')
            ''', (campaign_id, tenant_id, tenant_id))
            conn.commit()
            return cursor.rowcount

    def _is_blacklisted_user(self, tenant_id: int, user_id) -> bool:
        uid = self._parse_int(user_id)
        if not uid:
            return False
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM outreach_blacklist WHERE tenant_id = ? AND user_id = ?', (tenant_id, uid))
            return cursor.fetchone() is not None

    @staticmethod
    def _is_spam_limit_error(exc: Exception) -> bool:
        """Определяет ошибки антиспама/лимитов, при которых нужен прогрев через @SpamBot."""
        if isinstance(exc, errors.FloodWaitError):
            return True
        name = type(exc).__name__.lower()
        text = str(exc).lower()
        spam_markers = (
            'peerflood',
            'peer_flood',
            'flood',
            'too many requests',
            'retry after',
            'a wait of'
        )
        return any(marker in name or marker in text for marker in spam_markers)

    async def _run_spambot_recovery(self, account_phone: str, client) -> bool:
        """
        Пытается снять ограничение отправки:
        1) отправляет /start в @SpamBot
        2) ждёт 90 секунд
        """
        try:
            await client.send_message('@SpamBot', '/start')
            logger.warning(f"[{account_phone}] spam-limit detected, sent /start to @SpamBot, waiting 90s before retry")
            await asyncio.sleep(90)
            return True
        except Exception as e:
            logger.error(f"[{account_phone}] failed to run SpamBot recovery: {e}")
            return False

    def _mark_ignored_contacts(self, campaign: Dict):
        """
        Игнор = N часов без входящего ответа после последнего исходящего.
        N задается в настройках кампании: schedule.ignore_after_hours.
        """
        campaign_id = campaign['id']
        tenant_id = int(campaign['tenant_id'])
        schedule = campaign.get('schedule') or {}
        try:
            ignore_after_hours = max(1, int(schedule.get('ignore_after_hours') or 24))
        except (TypeError, ValueError):
            ignore_after_hours = 24
        threshold_modifier = f'-{ignore_after_hours} hours'

        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE outreach_contacts
                SET status = 'ignored'
                WHERE campaign_id = ?
                  AND tenant_id = ?
                  AND status = 'sent'
                  AND (user_id IS NULL OR user_id NOT IN (SELECT user_id FROM outreach_blacklist WHERE tenant_id = ?))
                  AND EXISTS (
                      SELECT 1
                      FROM conversations c_out
                      WHERE c_out.contact_id = outreach_contacts.id
                        AND c_out.tenant_id = outreach_contacts.tenant_id
                        AND c_out.direction = 'outgoing'
                      GROUP BY c_out.contact_id
                      HAVING MAX(datetime(c_out.sent_at)) <= datetime('now', ?)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM conversations c_in
                      WHERE c_in.contact_id = outreach_contacts.id
                        AND c_in.tenant_id = outreach_contacts.tenant_id
                        AND c_in.direction = 'incoming'
                  )
            ''', (campaign_id, tenant_id, tenant_id, threshold_modifier))
            conn.commit()
            return cursor.rowcount

    def _persist_account_usage(self, stats: AccountStats):
        tenant_id = self._parse_int(getattr(stats, 'tenant_id', None))
        if not stats or not stats.phone or not tenant_id:
            return
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE account_stats
                SET checked_today = ?, last_check_date = ?, last_check = CURRENT_TIMESTAMP
                WHERE tenant_id = ? AND phone = ?
            ''', (stats.checked_today, stats.last_check_date, tenant_id, stats.phone))
            conn.commit()

    def _transition_contact_status(
        self,
        contact_id: int,
        tenant_id: int,
        new_status: str,
        allowed_from: Optional[List[str]] = None,
        extra_updates: Optional[Dict[str, object]] = None
    ) -> bool:
        allowed_from = allowed_from or []
        extra_updates = extra_updates or {}

        set_parts = ["status = ?"]
        params: List[object] = [new_status]
        for key, value in extra_updates.items():
            set_parts.append(f"{key} = ?")
            params.append(value)

        where = "id = ? AND tenant_id = ?"
        params.append(contact_id)
        params.append(tenant_id)
        if allowed_from:
            placeholders = ",".join("?" for _ in allowed_from)
            where += f" AND status IN ({placeholders})"
            params.extend(allowed_from)

        query = f"UPDATE outreach_contacts SET {', '.join(set_parts)} WHERE {where}"
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount == 1

    def _render_campaign_message(self, template: str, *, name=None, company=None, position=None, phone=None) -> str:
        message = str(template or "")
        message = message.replace('{name}', name or '')
        message = message.replace('{company}', company or '')
        message = message.replace('{position}', position or '')
        message = message.replace('{phone}', phone or '')
        return render_spintax(message, rng=self.rng)
    
    async def get_account_client(self, tenant_id: int, account_phone: str):
        """Получение клиента для аккаунта (с прокси и fingerprint)"""
        account_key = self._account_cache_key(tenant_id, account_phone)
        if account_key in self.accounts_cache:
            client, stats = self.accounts_cache[account_key]
            if client.is_connected():
                return client, stats
        
        api_id, api_hash = self._resolve_api_credentials(tenant_id)
        
        if not api_id or not api_hash:
            logger.error("API credentials not configured")
            return None, None
        
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT session_name, proxy_id, account_type
                FROM account_stats
                WHERE tenant_id = ? AND phone = ?
            ''', (tenant_id, account_phone))
            row = cursor.fetchone()
            if not row:
                return None, None
            
            session_name = row[0]
            proxy_id = row[1]
            account_type = row[2]
        
        fingerprint = self.fingerprint_manager.get_fingerprint(account_phone)
        if not fingerprint:
            fingerprint = self.fingerprint_manager.generate_fingerprint(account_type)
            self.fingerprint_manager.save_fingerprint(account_phone, fingerprint)
        
        proxy_config = None
        if proxy_id:
            proxy = self.proxy_manager.get_proxy(proxy_id)
            if proxy and proxy['is_active']:
                proxy_config = build_telethon_proxy_config(proxy)
        
        client_kwargs = {
            'device_model': fingerprint['device_model'],
            'app_version': fingerprint['app_version'],
            'lang_code': fingerprint['lang_code'],
            'system_lang_code': fingerprint['system_lang_code']
        }
        
        client = None
        last_connect_error = None
        for attempt_mode in ('proxy', 'direct'):
            kwargs = dict(client_kwargs)
            if attempt_mode == 'proxy':
                if not proxy_config:
                    continue
                kwargs['proxy'] = proxy_config
            try:
                client = TelegramClient(
                    f"{self.sessions_dir}/{session_name}",
                    api_id=int(api_id),
                    api_hash=api_hash,
                    **kwargs
                )
                await client.start()
                if not await client.is_user_authorized():
                    await client.disconnect()
                    return None, None
                if attempt_mode == 'proxy':
                    self.proxy_manager.mark_proxy_used(proxy_id)
                else:
                    if proxy_config:
                        logger.warning(f"[{account_phone}] proxy fallback: connected without proxy")
                break
            except Exception as e:
                last_connect_error = e
                if client:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                if attempt_mode == 'proxy':
                    logger.warning(f"[{account_phone}] proxy connect failed, fallback to direct: {e}")
                    client = None
                    continue
                return None, None

        if not client:
            if last_connect_error:
                logger.error(f"[{account_phone}] failed to connect account client: {last_connect_error}")
            return None, None

        # Прогреваем кэш диалогов для лучшего резолва user_id.
        try:
            await client.get_dialogs(limit=1000)
        except Exception:
            pass

        # Дополнительный прогрев из целевого чата (где лежат визитки/участники).
        try:
            # На этапе подключения не форсим target_chat: для user_id-баз это не нужно.
            await self._warm_entities_from_target_chat(tenant_id, account_phone, client, use_target_chat=False)
        except Exception as e:
            logger.warning(f"[{account_phone}] target chat warm error: {e}")
        
        stats = AccountStats(session_name)
        stats.tenant_id = tenant_id
        stats.phone = account_phone
        stats.account_type = account_type
        stats.proxy_id = proxy_id
        db_stats = None
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT checked_today, last_check_date, outreach_daily_limit
                FROM account_stats
                WHERE tenant_id = ? AND phone = ?
            ''', (tenant_id, account_phone))
            row = cursor.fetchone()
            if row:
                db_stats = {
                    'checked_today': row[0],
                    'last_check_date': row[1],
                    'outreach_daily_limit': row[2]
                }
        if db_stats:
            stats.checked_today = db_stats.get('checked_today', 0)
            stats.last_check_date = db_stats.get('last_check_date', date.today().isoformat())
            stats.outreach_daily_limit = db_stats.get('outreach_daily_limit', 20)
        
        self.accounts_cache[account_key] = (client, stats)
        return client, stats

    async def _get_campaign_accounts(self, campaign: Dict, respect_daily_limits: bool = True):
        """
        Доступные аккаунты кампании: только выбранные outreach, в порядке выбора.
        respect_daily_limits=False используется для фоллоуапов, чтобы не расходовать/не ограничивать их дневным лимитом.
        """
        selected = campaign.get('accounts') or []
        if not selected:
            return []
        tenant_id = int(campaign['tenant_id'])

        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            placeholders = ",".join("?" for _ in selected)
            cursor.execute(
                f'''
                SELECT phone, account_type, COALESCE(is_banned, 0), COALESCE(is_frozen, 0)
                FROM account_stats
                WHERE phone IN ({placeholders})
                  AND tenant_id = ?
                ''',
                [*selected, tenant_id]
            )
            rows = cursor.fetchall()

        meta_by_phone = {row[0]: row for row in rows}
        available = []
        for phone in selected:
            row = meta_by_phone.get(phone)
            if not row:
                continue
            _, account_type, is_banned, is_frozen = row
            if account_type != 'outreach' or is_banned or is_frozen:
                continue
            client, stats = await self.get_account_client(tenant_id, phone)
            if not client or not stats:
                continue
            # Если наступил новый день, can_use_today() обнулит счётчики только в памяти.
            # Сразу сохраняем это в БД, чтобы UI показывал корректный today.
            before_date = stats.last_check_date
            before_checked = stats.checked_today
            can_use = stats.can_use_today()
            if stats.last_check_date != before_date or stats.checked_today != before_checked:
                self._persist_account_usage(stats)

            if respect_daily_limits and not can_use:
                continue
            available.append((phone, client, stats))
        return available

    def _get_rotated_accounts(self, campaign_id: int, accounts: List[tuple]) -> List[tuple]:
        if not accounts:
            return []
        offset = self.rotation_state.get(campaign_id, 0) % len(accounts)
        rotated = accounts[offset:] + accounts[:offset]
        self.rotation_state[campaign_id] = (offset + 1) % len(accounts)
        return rotated

    @staticmethod
    def _pick_delay_seconds(campaign: Dict) -> int:
        try:
            delay_min = max(0, int(campaign.get('delay_min') or 0))
        except (TypeError, ValueError):
            delay_min = 0
        try:
            delay_max = max(delay_min, int(campaign.get('delay_max') or delay_min))
        except (TypeError, ValueError):
            delay_max = delay_min
        return random.randint(delay_min * 60, delay_max * 60)

    @staticmethod
    def _normalize_username(username: Optional[str]) -> Optional[str]:
        if not username:
            return None
        normalized = str(username).strip()
        if not normalized:
            return None
        return normalized if normalized.startswith('@') else f"@{normalized}"

    @staticmethod
    def _normalize_phone(phone: Optional[str]) -> Optional[str]:
        if not phone:
            return None
        normalized = str(phone).strip()
        if not normalized:
            return None
        if not normalized.startswith('+'):
            normalized = '+' + normalized
        return normalized

    @staticmethod
    def _parse_int(value) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        text = str(value).strip()
        if not text:
            return None
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return None

    def _derive_phone_from_user_id_like_value(self, user_id) -> Optional[str]:
        """Фолбек для старых кривых импортов: телефон сохранен в поле user_id."""
        uid = self._parse_int(user_id)
        if not uid:
            return None
        digits = str(abs(uid))
        if len(digits) == 11 and digits[0] in ('7', '8'):
            if digits[0] == '8':
                return '+7' + digits[1:]
            return '+' + digits
        return None

    def _user_id_looks_like_phone(self, user_id) -> bool:
        uid = self._parse_int(user_id)
        if not uid:
            return False
        digits = str(abs(uid))
        return len(digits) == 11 and digits[0] in ('7', '8')

    def _contact_from_card_index(self, tenant_id: int, account_phone: str, user_id=None, phone=None):
        account_key = self._account_cache_key(tenant_id, account_phone)
        idx = self.contact_card_index.get(account_key) or {}
        phone_to_user = idx.get('phone_to_user') or {}
        user_to_phone = idx.get('user_to_phone') or {}
        phone_to_entity = idx.get('phone_to_entity') or {}
        user_to_entity = idx.get('user_to_entity') or {}

        normalized_phone = self._normalize_phone(phone) if phone else None
        uid = self._parse_int(user_id)

        if normalized_phone and normalized_phone in phone_to_user:
            mapped_uid = phone_to_user[normalized_phone]
            entity = phone_to_entity.get(normalized_phone) or user_to_entity.get(mapped_uid)
            return normalized_phone, mapped_uid, entity
        if uid and uid in user_to_phone:
            mapped_phone = user_to_phone[uid]
            entity = user_to_entity.get(uid) or phone_to_entity.get(mapped_phone)
            return mapped_phone, uid, entity
        entity = None
        if normalized_phone and normalized_phone in phone_to_entity:
            entity = phone_to_entity[normalized_phone]
        elif uid and uid in user_to_entity:
            entity = user_to_entity[uid]
        return normalized_phone, uid, entity

    async def _deep_warm_target_chat_for_user(
        self,
        tenant_id: int,
        account_phone: str,
        client,
        user_id: int,
        max_messages: int = 50000
    ) -> bool:
        """
        Глубокий прогрев только при фейле отправки по user_id:
        сканирует историю target_chat и собирает contact-card индекс глубже,
        как при ручном вызове /chat_history перед /send.
        """
        uid = self._parse_int(user_id)
        if not uid:
            return False

        target_chat = self._get_target_chat(tenant_id)
        if not target_chat:
            return False
        await self._ensure_account_in_target_chat(account_phone, client, target_chat)

        account_key = self._account_cache_key(tenant_id, account_phone)
        idx = self.contact_card_index.setdefault(account_key, {
            'phone_to_user': {},
            'user_to_phone': {},
            'phone_to_entity': {},
            'user_to_entity': {},
            'user_to_input_peer': {}
        })
        phone_to_user = idx['phone_to_user']
        user_to_phone = idx['user_to_phone']
        phone_to_entity = idx['phone_to_entity']
        user_to_entity = idx['user_to_entity']
        user_to_input_peer = idx['user_to_input_peer']

        if uid in user_to_entity or uid in user_to_phone or uid in user_to_input_peer:
            return True

        try:
            chat_entity = await client.get_entity(target_chat)
        except Exception as e:
            logger.warning(f"[{account_phone}] deep warm target_chat resolve failed ({target_chat}): {e}")
            return False

        found = False
        scanned = 0
        try:
            async for message in client.iter_messages(chat_entity, limit=max_messages):
                scanned += 1
                media = getattr(message, 'media', None)
                if not media:
                    continue
                media_phone = getattr(media, 'phone_number', None)
                media_user_id = self._parse_int(getattr(media, 'user_id', None))
                normalized_phone = self._normalize_phone(media_phone) if media_phone else None
                if not normalized_phone or not media_user_id:
                    continue

                phone_to_user[normalized_phone] = media_user_id
                user_to_phone[media_user_id] = normalized_phone

                media_access_hash = self._parse_int(getattr(media, 'user_access_hash', None))
                if media_access_hash:
                    user_to_input_peer[media_user_id] = InputPeerUser(media_user_id, media_access_hash)

                try:
                    entity = await client.get_entity(PeerUser(media_user_id))
                    phone_to_entity[normalized_phone] = entity
                    user_to_entity[media_user_id] = entity
                    entity_access_hash = self._parse_int(getattr(entity, 'access_hash', None))
                    if entity_access_hash:
                        user_to_input_peer[media_user_id] = InputPeerUser(media_user_id, entity_access_hash)
                except Exception:
                    pass

                if media_user_id == uid:
                    found = True
                    break
        except Exception as e:
            logger.warning(f"[{account_phone}] deep warm scan failed for user {uid}: {e}")

        logger.info(
            f"[{account_phone}] deep warm finished for user {uid}: found={found}, scanned={scanned}, cards={len(phone_to_user)}"
        )
        return found

    async def _prime_cache_by_reading_target_chat_history(
        self,
        tenant_id: int,
        account_phone: str,
        client,
        user_id: int,
        max_messages: int = 50000
    ) -> bool:
        """
        Прямой аналог ручного /chat_history перед /send:
        просто читаем историю target_chat, чтобы Telethon прогрел entity cache.
        """
        uid = self._parse_int(user_id)
        if not uid:
            return False

        target_chat = self._get_target_chat(tenant_id)
        if not target_chat:
            logger.warning(f"[{account_phone}] target_chat is empty, cannot prime history cache")
            return False
        await self._ensure_account_in_target_chat(account_phone, client, target_chat)

        try:
            chat_entity = await client.get_entity(target_chat)
        except Exception as e:
            logger.warning(f"[{account_phone}] history prime target_chat resolve failed ({target_chat}): {e}")
            return False

        scanned = 0
        seen_uid = False
        try:
            async for message in client.iter_messages(chat_entity, limit=max_messages):
                scanned += 1
                msg_sender = self._parse_int(getattr(message, 'sender_id', None))
                if msg_sender == uid:
                    seen_uid = True
                    try:
                        await message.get_sender()
                    except Exception:
                        pass
                    break
        except Exception as e:
            logger.warning(f"[{account_phone}] history prime scan failed for user {uid}: {e}")
            return False

        if not seen_uid:
            logger.info(f"[{account_phone}] history prime scanned={scanned}, user {uid} not seen as sender")

        try:
            await client.get_input_entity(PeerUser(uid))
            logger.info(f"[{account_phone}] history prime resolved input entity for user {uid}, scanned={scanned}")
            return True
        except Exception as e:
            logger.info(
                f"[{account_phone}] history prime did not resolve user {uid} after scanned={scanned}: {type(e).__name__}: {e}"
            )
            return False

    async def _send_with_target_chat_retry(
        self,
        *,
        tenant_id: int,
        account_phone: str,
        client,
        message: str,
        user_id=None,
        username=None,
        phone=None,
        name=None,
        allow_target_chat_retry: bool = True
    ) -> str:
        phone_candidate = phone or self._derive_phone_from_user_id_like_value(user_id)
        phone_candidate, user_id_candidate, _ = self._contact_from_card_index(
            tenant_id,
            account_phone,
            user_id=user_id,
            phone=phone_candidate
        )

        try:
            return await self._send_like_gateway(
                client,
                message,
                user_id=user_id_candidate,
                username=username,
                phone=phone_candidate,
                name=name
            )
        except Exception as first_error:
            uid = self._parse_int(user_id_candidate or user_id)
            if not uid or self._user_id_looks_like_phone(uid):
                raise first_error
            if not allow_target_chat_retry:
                raise first_error

            deep_key = (tenant_id, account_phone, uid)
            if deep_key in self.deep_warm_attempted:
                logger.info(f"[{account_phone}] deep warm already attempted for user {uid}, re-raising first error")
                raise first_error
            self.deep_warm_attempted.add(deep_key)

            found = await self._deep_warm_target_chat_for_user(tenant_id, account_phone, client, uid, max_messages=50000)
            if not found:
                logger.info(f"[{account_phone}] card-based deep warm did not find user {uid}, trying history prime")
                primed = await self._prime_cache_by_reading_target_chat_history(tenant_id, account_phone, client, uid, max_messages=50000)
                if not primed:
                    logger.info(f"[{account_phone}] history prime failed for user {uid}, re-raising first error")
                    raise first_error

            phone_retry, uid_retry, _ = self._contact_from_card_index(
                tenant_id,
                account_phone,
                user_id=uid,
                phone=phone_candidate
            )
            return await self._send_like_gateway(
                client,
                message,
                user_id=uid_retry,
                username=username,
                phone=phone_retry,
                name=name
            )

    async def _resolve_contact_entity(
        self,
        client,
        *,
        user_id=None,
        username=None,
        access_hash=None,
        phone=None,
        allow_import_phone: bool = True
    ):
        """Резолв entity для отправки в порядке надёжности."""
        uid = self._parse_int(user_id)
        ahash = self._parse_int(access_hash)
        uname = self._normalize_username(username)
        phone_normalized = self._normalize_phone(phone)
        errors_seen = []
        uid_is_phone = self._user_id_looks_like_phone(uid)

        if uname:
            try:
                return await client.get_input_entity(uname), "username"
            except Exception as e:
                errors_seen.append(f"username:{type(e).__name__}:{e}")

        if uid and ahash and not uid_is_phone:
            try:
                return InputPeerUser(uid, ahash), "id+hash"
            except Exception as e:
                errors_seen.append(f"id+hash:{type(e).__name__}:{e}")

        if uid and not uid_is_phone:
            try:
                return await client.get_input_entity(PeerUser(uid)), "peer_user_cache"
            except Exception as e:
                errors_seen.append(f"peer_user_cache:{type(e).__name__}:{e}")

            try:
                return await client.get_input_entity(uid), "id_cache"
            except Exception as e:
                errors_seen.append(f"id_cache:{type(e).__name__}:{e}")

        if allow_import_phone and phone_normalized:
            try:
                import_result = await client(ImportContactsRequest([
                    InputPhoneContact(0, phone_normalized, "Contact", "Imported")
                ]))
                if import_result.users:
                    return import_result.users[0], "phone_import"
                errors_seen.append("phone_import:empty_result")
            except Exception as e:
                errors_seen.append(f"phone_import:{type(e).__name__}:{e}")

        detail = " | ".join(errors_seen) if errors_seen else "no_identifiers"
        raise RuntimeError(f"entity_resolve_failed:{detail}")

    async def _send_like_gateway(self, client, message: str, *, user_id=None, username=None, phone=None, name=None):
        """
        Поведение как в рабочем /send:
        1) direct send_message(chat_id/text) по username/id/phone
        2) fallback для phone: ImportContactsRequest -> send_message(user)
        """
        errors_seen = []

        uname = self._normalize_username(username)
        uid = self._parse_int(user_id)
        phone_norm = self._normalize_phone(phone)

        if uname:
            try:
                await client.send_message(uname, message)
                return "send_direct_username"
            except Exception as e:
                errors_seen.append(f"direct_username:{type(e).__name__}:{e}")

        if uid and not self._user_id_looks_like_phone(uid):
            try:
                await client.send_message(uid, message)
                return "send_direct_id"
            except Exception as e:
                errors_seen.append(f"direct_id:{type(e).__name__}:{e}")

        if phone_norm:
            # В некоторых сессиях прямой send по phone работает, если контакт уже в адресной книге.
            try:
                await client.send_message(phone_norm, message)
                return "send_direct_phone"
            except Exception as e:
                errors_seen.append(f"direct_phone:{type(e).__name__}:{e}")

            try:
                import_result = await client(ImportContactsRequest([
                    InputPhoneContact(0, phone_norm, name or "Contact", "")
                ]))
                if import_result.users:
                    await client.send_message(import_result.users[0], message)
                    return "import_phone_send"
                errors_seen.append("import_phone:empty_result")
            except Exception as e:
                errors_seen.append(f"import_phone:{type(e).__name__}:{e}")

        detail = " | ".join(errors_seen) if errors_seen else "no_identifiers"
        raise RuntimeError(f"send_like_gateway_failed:{detail}")
    
    async def send_message(self, campaign: Dict, contact: tuple, ordered_accounts: Optional[List[tuple]] = None):
        """Отправка сообщения контакту по Telegram ID"""
        tenant_id = int(campaign['tenant_id'])
        # contact: (id, phone, name, company, position, user_id, username, access_hash)
        if len(contact) >= 8:
            contact_id, phone, name, company, position, user_id, username, access_hash = contact
        else:
            contact_id, phone, name, company, position, user_id = contact
            username = None
            access_hash = None
        
        if not user_id and not username and not phone:
            logger.warning(f"Contact {contact_id} has no user_id/username/phone, skipping")
            return False
        if self._is_blacklisted_user(tenant_id, user_id):
            self._transition_contact_status(
                contact_id=contact_id,
                tenant_id=tenant_id,
                new_status='failed',
                allowed_from=['new', 'sent', 'ignored'],
                extra_updates={'notes': 'blacklisted'}
            )
            return False
        
        available_accounts = ordered_accounts if ordered_accounts is not None else await self._get_campaign_accounts(campaign)
        
        if not available_accounts:
            logger.warning(f"No available accounts for campaign {campaign['name']}")
            return False
        
        # Персонализируем сообщение
        message = self._render_campaign_message(
            campaign['message_template'],
            name=name,
            company=company,
            position=position,
            phone=phone
        )
        
        last_error = None
        attempt_errors = []

        for account_phone, client, stats in available_accounts:
            spam_recovery_used = False
            while True:
                try:
                    async with self._account_lock(tenant_id, account_phone):
                        use_target_chat = not (self._parse_int(user_id) and not username and not phone)
                        await self._warm_entities_from_target_chat(tenant_id, account_phone, client, use_target_chat=use_target_chat)
                        route = await self._send_with_target_chat_retry(
                            tenant_id=tenant_id,
                            account_phone=account_phone,
                            client=client,
                            message=message,
                            user_id=user_id,
                            username=username,
                            phone=phone,
                            name=name,
                            allow_target_chat_retry=use_target_chat
                        )

                    # Первичные сообщения расходуют лимит аккаунта outreach.
                    stats.increment_checked(is_test=False)
                    self._persist_account_usage(stats)

                    status_changed = self._transition_contact_status(
                        contact_id=contact_id,
                        tenant_id=tenant_id,
                        new_status='sent',
                        allowed_from=['new'],
                        extra_updates={
                            'message_sent_at': datetime.now().isoformat(),
                            'account_used': account_phone,
                            'last_message': message,
                            'notes': None
                        }
                    )
                    if not status_changed:
                        logger.warning(f"Contact {contact_id} status changed concurrently, skip duplicate initial send record")
                        return False

                    with sqlite3.connect(self.db.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO conversations 
                            (tenant_id, campaign_id, contact_id, direction, content, step_id)
                            VALUES (?, ?, ?, 'outgoing', ?, 1)
                        ''', (tenant_id, campaign['id'], contact_id, message))

                        cursor.execute('''
                            UPDATE outreach_campaigns 
                            SET sent_count = sent_count + 1
                            WHERE id = ? AND tenant_id = ?
                        ''', (campaign['id'], tenant_id))

                        conn.commit()

                    logger.info(f"✅ Message sent to user {user_id} using {account_phone} via {route}")
                    return True

                except Exception as e:
                    if self._is_spam_limit_error(e) and not spam_recovery_used:
                        recovered = await self._run_spambot_recovery(account_phone, client)
                        spam_recovery_used = True
                        if recovered:
                            logger.info(f"[{account_phone}] retrying send after SpamBot recovery")
                            continue
                    last_error = e
                    attempt_errors.append(f"{account_phone}:{type(e).__name__}:{e}")
                    logger.error(f"Error sending message to user {user_id} via {account_phone}: {e}")
                    break

        notes_text = str(last_error)[:500] if last_error else 'unknown_error'
        if attempt_errors:
            notes_text = ' | '.join(attempt_errors)[:500]
        self._transition_contact_status(
            contact_id=contact_id,
            tenant_id=tenant_id,
            new_status='failed',
            allowed_from=['new'],
            extra_updates={'notes': notes_text}
        )
        logger.error(f"Failed to deliver contact {contact_id} (user_id={user_id}) with all available accounts")
        return False
    
    async def send_followup(self, campaign: Dict, contact: tuple, ordered_accounts: Optional[List[tuple]] = None):
        """Отправка фолоуапа"""
        tenant_id = int(campaign['tenant_id'])
        # contact: (id, phone, name, company, position, user_id, username, access_hash, account_used, last_message)
        contact_id, phone, name, company, position, user_id, username, access_hash, account_used, last_message = contact
        
        if not user_id and not username and not phone:
            logger.warning(f"Contact {contact_id} has no user_id/username/phone, skipping followup")
            return False
        if self._is_blacklisted_user(tenant_id, user_id):
            self._transition_contact_status(
                contact_id=contact_id,
                tenant_id=tenant_id,
                new_status='failed',
                allowed_from=['new', 'sent', 'ignored'],
                extra_updates={'notes': 'blacklisted'}
            )
            return False
        
        strategy = campaign.get('strategy', {'steps': []})
        raw_steps = strategy.get('steps', [])
        steps = []
        for step in raw_steps:
            content = (step.get('content') or '').strip() if isinstance(step, dict) else ''
            if not content:
                continue
            sid = self._parse_int(step.get('id')) if isinstance(step, dict) else None
            if not sid:
                continue
            # Step 1 — это первое сообщение кампании; фоллоуапы начинаются с 2.
            if sid <= 1:
                continue
            steps.append({
                'id': sid,
                'content': content
            })
        steps.sort(key=lambda s: s['id'])
        
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT step_id FROM conversations
                WHERE tenant_id = ? AND contact_id = ? AND direction = 'outgoing'
                ORDER BY sent_at DESC LIMIT 1
            ''', (tenant_id, contact_id))
            row = cursor.fetchone()
            current_step = row[0] if row else 0
        
        # Берем следующий шаг по порядку после последнего отправленного outgoing.
        next_step = next((step for step in steps if step['id'] > current_step), None)
        
        if not next_step:
            logger.info(f"Followup skipped for contact {contact_id}: no next followup step configured after step {current_step}")
            return False
        
        message = self._render_campaign_message(
            next_step['content'],
            name=name,
            company=company,
            position=position,
            phone=phone
        )
        
        available_accounts = (
            ordered_accounts
            if ordered_accounts is not None
            else await self._get_campaign_accounts(campaign, respect_daily_limits=False)
        )
        # Фоллоуап должен идти строго с того же аккаунта, который отправлял первичное сообщение.
        if account_used:
            available_accounts = [a for a in available_accounts if a[0] == account_used]
        else:
            logger.warning(f"Followup skipped for contact {contact_id}: no account_used from initial message")
            return False

        if not available_accounts:
            logger.warning(
                f"No available account for followup contact {contact_id}: required account {account_used}"
            )
            return False

        wait_left = self._get_followup_wait_seconds(tenant_id, account_used)
        if wait_left > 0:
            logger.info(
                f"Followup throttled for account {account_used}: wait {wait_left}s"
            )
            return False

        last_error = None
        for account_phone, client, stats in available_accounts:
            if not client:
                continue
            spam_recovery_used = False
            while True:
                try:
                    async with self._account_lock(tenant_id, account_phone):
                        use_target_chat = not (self._parse_int(user_id) and not username and not phone)
                        await self._warm_entities_from_target_chat(tenant_id, account_phone, client, use_target_chat=use_target_chat)
                        route = await self._send_with_target_chat_retry(
                            tenant_id=tenant_id,
                            account_phone=account_phone,
                            client=client,
                            message=message,
                            user_id=user_id,
                            username=username,
                            phone=phone,
                            name=name,
                            allow_target_chat_retry=use_target_chat
                        )
                    # Фоллоуапы не должны расходовать лимиты аккаунта outreach.
                    self._persist_account_usage(stats)

                    status_changed = self._transition_contact_status(
                        contact_id=contact_id,
                        tenant_id=tenant_id,
                        new_status='sent',
                        allowed_from=['ignored'],
                        extra_updates={
                            'last_message': message,
                            'message_sent_at': datetime.now().isoformat()
                        }
                    )
                    if not status_changed:
                        logger.warning(f"Contact {contact_id} status changed concurrently, skip duplicate followup record")
                        return False
                    
                    with sqlite3.connect(self.db.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO conversations 
                            (tenant_id, campaign_id, contact_id, direction, content, step_id, is_followup)
                            VALUES (?, ?, ?, 'outgoing', ?, ?, TRUE)
                        ''', (tenant_id, campaign['id'], contact_id, message, next_step['id']))
                        
                        conn.commit()

                    self._mark_followup_sent_now(tenant_id, account_phone)
                    
                    logger.info(f"✅ Followup sent to user {user_id} via {route} using {account_phone}")
                    return True
                except Exception as e:
                    if self._is_spam_limit_error(e) and not spam_recovery_used:
                        recovered = await self._run_spambot_recovery(account_phone, client)
                        spam_recovery_used = True
                        if recovered:
                            logger.info(f"[{account_phone}] retrying followup after SpamBot recovery")
                            continue
                    last_error = e
                    logger.error(f"Error sending followup to user {user_id} via {account_phone}: {e}")
                    break

        if last_error:
            self._transition_contact_status(
                contact_id=contact_id,
                tenant_id=tenant_id,
                new_status='failed',
                allowed_from=['ignored'],
                extra_updates={'notes': str(last_error)[:500]}
            )
            logger.error(f"Followup failed for contact {contact_id}: {last_error}")
        return False
    
    async def check_replies(self):
        """Проверка ответов на сообщения"""
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT c.tenant_id, c.campaign_id, c.contact_id, oc.account_used, oc.user_id
                FROM conversations c
                JOIN outreach_contacts oc ON c.contact_id = oc.id
                WHERE c.direction = 'outgoing'
                  AND oc.tenant_id = c.tenant_id
                  AND oc.status IN ('sent', 'ignored')
                  AND (oc.user_id IS NOT NULL OR oc.username IS NOT NULL OR oc.phone IS NOT NULL)
                  AND c.sent_at > datetime('now', '-7 days')
                ORDER BY c.sent_at DESC
            ''')
            sent_messages = cursor.fetchall()
        
        for tenant_id, campaign_id, contact_id, account_phone, user_id in sent_messages:
            try:
                client, stats = await self.get_account_client(int(tenant_id), account_phone)
                if not client:
                    continue

                with sqlite3.connect(self.db.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT phone, username, access_hash
                        FROM outreach_contacts
                        WHERE tenant_id = ? AND id = ?
                    ''', (tenant_id, contact_id))
                    row = cursor.fetchone()
                    phone = row[0] if row else None
                    username = row[1] if row else None
                    access_hash = row[2] if row else None

                target, _ = await self._resolve_contact_entity(
                    client,
                    user_id=user_id,
                    username=username,
                    access_hash=access_hash,
                    phone=phone,
                    allow_import_phone=False
                )

                async for message in client.iter_messages(target, limit=5):
                    if message.out:
                        continue
                    
                    with sqlite3.connect(self.db.db_path) as conn:
                        cursor = conn.cursor()
                        
                        cursor.execute('''
                            SELECT id FROM conversations 
                            WHERE tenant_id = ? AND contact_id = ? AND message_id = ?
                        ''', (tenant_id, contact_id, message.id))
                        
                        if not cursor.fetchone():
                            cursor.execute('''
                                INSERT INTO conversations
                                (tenant_id, campaign_id, contact_id, message_id, direction, content)
                                VALUES (?, ?, ?, ?, 'incoming', ?)
                            ''', (tenant_id, campaign_id, contact_id, message.id, message.text))
                            
                            cursor.execute('''
                                UPDATE outreach_contacts
                                SET status = 'replied', replied_at = CURRENT_TIMESTAMP
                                WHERE tenant_id = ? AND id = ? AND status IN ('sent', 'ignored')
                            ''', (tenant_id, contact_id))
                            
                            cursor.execute('''
                                UPDATE outreach_campaigns
                                SET reply_count = reply_count + 1
                                WHERE tenant_id = ? AND id = ?
                            ''', (tenant_id, campaign_id))
                            
                            conn.commit()
                            
                            logger.info(f"💬 Reply received from user {user_id}")
                    
                    break
                    
            except Exception as e:
                logger.error(f"Error checking replies for contact {contact_id}: {e}")
