import sqlite3
import logging
import random
from typing import List, Dict, Optional
from datetime import datetime
import aiohttp
import asyncio

logger = logging.getLogger(__name__)

class ProxyManager:
    """Менеджер для работы с прокси"""
    
    def __init__(self, db_path='checker.db'):
        self.db_path = db_path
    
    def add_proxy(self, ip: str, port: int, protocol: str = 'socks5', 
                  username: str = None, password: str = None, 
                  country: str = None) -> int:
        """Добавление нового прокси"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO proxies (ip, port, protocol, username, password, country)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (ip, port, protocol, username, password, country))
                conn.commit()
                proxy_id = cursor.lastrowid
                logger.info(f"✅ Добавлен прокси {ip}:{port} ({protocol})")
                return proxy_id
            except sqlite3.IntegrityError:
                logger.warning(f"⚠️ Прокси {ip}:{port} уже существует")
                return None
    
    def add_proxies_bulk(self, proxies: List[Dict]) -> Dict:
        """Массовое добавление прокси"""
        results = {'added': 0, 'skipped': 0, 'errors': 0}
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for proxy in proxies:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO proxies 
                        (ip, port, protocol, username, password, country)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        proxy['ip'], 
                        proxy['port'], 
                        proxy.get('protocol', 'socks5'),
                        proxy.get('username'),
                        proxy.get('password'),
                        proxy.get('country')
                    ))
                    if cursor.rowcount > 0:
                        results['added'] += 1
                    else:
                        results['skipped'] += 1
                except Exception as e:
                    logger.error(f"Ошибка при добавлении прокси {proxy}: {e}")
                    results['errors'] += 1
            conn.commit()
        
        return results
    
    def get_proxies(self, active_only: bool = True) -> List[Dict]:
        """Получение списка прокси"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            query = 'SELECT * FROM proxies'
            if active_only:
                query += ' WHERE is_active = TRUE'
            query += ' ORDER BY last_used NULLS FIRST'
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            proxies = []
            for row in rows:
                proxies.append({
                    'id': row[0],
                    'ip': row[1],
                    'port': row[2],
                    'protocol': row[3],
                    'username': row[4],
                    'password': row[5],
                    'country': row[6],
                    'is_active': bool(row[7]),
                    'last_used': row[8],
                    'created_at': row[9]
                })
            return proxies
    
    def get_proxy(self, proxy_id: int) -> Optional[Dict]:
        """Получение прокси по ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM proxies WHERE id = ?', (proxy_id,))
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'ip': row[1],
                    'port': row[2],
                    'protocol': row[3],
                    'username': row[4],
                    'password': row[5],
                    'country': row[6],
                    'is_active': bool(row[7]),
                    'last_used': row[8],
                    'created_at': row[9]
                }
            return None
    
    def get_random_proxy(self) -> Optional[Dict]:
        """Получение случайного активного прокси"""
        proxies = self.get_proxies(active_only=True)
        return random.choice(proxies) if proxies else None
    
    def update_proxy_status(self, proxy_id: int, is_active: bool):
        """Обновление статуса прокси"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE proxies SET is_active = ? WHERE id = ?
            ''', (is_active, proxy_id))
            conn.commit()
    
    def mark_proxy_used(self, proxy_id: int):
        """Отметить использование прокси"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE proxies SET last_used = CURRENT_TIMESTAMP WHERE id = ?
            ''', (proxy_id,))
            conn.commit()
    
    def delete_proxy(self, proxy_id: int):
        """Удаление прокси"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM proxies WHERE id = ?', (proxy_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    async def test_proxy(self, proxy_id: int) -> bool:
        """Тестирование работоспособности прокси"""
        proxy = self.get_proxy(proxy_id)
        if not proxy:
            return False
        
        try:
            proxy_url = f"{proxy['protocol']}://"
            if proxy.get('username') and proxy.get('password'):
                proxy_url += f"{proxy['username']}:{proxy['password']}@"
            proxy_url += f"{proxy['ip']}:{proxy['port']}"
            
            try:
                from aiohttp_socks import ProxyConnector
                connector = ProxyConnector.from_url(proxy_url)
            except ImportError:
                logger.warning("aiohttp_socks not installed, using regular connector")
                connector = aiohttp.TCPConnector()
            
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get('http://httpbin.org/ip', timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        logger.info(f"✅ Прокси {proxy['ip']} работает. Внешний IP: {data.get('origin')}")
                        self.update_proxy_status(proxy_id, True)
                        return True
                    else:
                        logger.warning(f"⚠️ Прокси {proxy['ip']} ответил кодом {resp.status}")
                        self.update_proxy_status(proxy_id, False)
                        return False
        except Exception as e:
            logger.error(f"❌ Прокси {proxy['ip']} не работает: {e}")
            self.update_proxy_status(proxy_id, False)
            return False
    
    async def test_all_proxies(self) -> Dict:
        """Тестирование всех прокси"""
        proxies = self.get_proxies(active_only=False)
        results = {'tested': 0, 'working': 0, 'failed': 0}
        
        for proxy in proxies:
            if await self.test_proxy(proxy['id']):
                results['working'] += 1
            else:
                results['failed'] += 1
            results['tested'] += 1
            await asyncio.sleep(1)
        
        return results