import random
import sqlite3
import logging
from datetime import datetime
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

class FingerprintManager:
    """Менеджер для создания и управления цифровыми отпечатками аккаунтов"""
    
    DEVICE_MODELS = [
        "Desktop",
        "iPhone14,3",
        "iPhone15,2",
        "SM-S918B",
        "SM-G998B",
        "Pixel 7 Pro",
        "Xiaomi 13 Pro",
        "OnePlus 11",
        "MacBookPro18,3",
        "Windows 10",
        "Windows 11"
    ]
    
    APP_VERSIONS = [
        "4.16.30",
        "4.15.0",
        "4.14.0",
        "4.13.1",
        "4.12.0",
        "4.11.0",
        "4.10.0"
    ]
    
    SYSTEM_VERSIONS = {
        "Desktop": ["Windows 10", "Windows 11", "macOS 13", "macOS 14", "Linux"],
        "iPhone": ["iOS 16.4", "iOS 16.5", "iOS 17.0", "iOS 17.1"],
        "Android": ["Android 13", "Android 14", "Android 12"]
    }
    
    LANG_CODES = ["ru", "en", "uk", "be", "kk"]
    
    def __init__(self, db_path='checker.db'):
        self.db_path = db_path
    
    def generate_fingerprint(self, account_type: str = "outreach") -> Dict:
        """Генерация случайного цифрового отпечатка"""
        
        if account_type == "outreach":
            device_model = random.choice([d for d in self.DEVICE_MODELS if "Desktop" not in d] + ["Desktop"] * 3)
        else:
            device_model = random.choice(self.DEVICE_MODELS)
        
        if "iPhone" in device_model:
            system_version = random.choice(self.SYSTEM_VERSIONS["iPhone"])
        elif any(x in device_model for x in ["SM-", "Pixel", "Xiaomi", "OnePlus"]):
            system_version = random.choice(self.SYSTEM_VERSIONS["Android"])
        else:
            system_version = random.choice(self.SYSTEM_VERSIONS["Desktop"])
        
        fingerprint = {
            'device_model': device_model,
            'app_version': random.choice(self.APP_VERSIONS),
            'system_version': system_version,
            'lang_code': random.choice(self.LANG_CODES),
            'system_lang_code': random.choice(self.LANG_CODES)
        }
        
        return fingerprint
    
    def save_fingerprint(self, account_phone: str, fingerprint: Dict) -> int:
        """Сохранение отпечатка для аккаунта"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO account_fingerprints 
                (account_phone, device_model, app_version, system_version, lang_code, system_lang_code)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                account_phone,
                fingerprint['device_model'],
                fingerprint['app_version'],
                fingerprint['system_version'],
                fingerprint['lang_code'],
                fingerprint['system_lang_code']
            ))
            conn.commit()
            logger.info(f"✅ Отпечаток сохранен для аккаунта {account_phone}")
            return cursor.lastrowid
    
    def get_fingerprint(self, account_phone: str) -> Optional[Dict]:
        """Получение отпечатка для аккаунта"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT device_model, app_version, system_version, lang_code, system_lang_code, last_updated
                FROM account_fingerprints WHERE account_phone = ?
            ''', (account_phone,))
            row = cursor.fetchone()
            if row:
                return {
                    'device_model': row[0],
                    'app_version': row[1],
                    'system_version': row[2],
                    'lang_code': row[3],
                    'system_lang_code': row[4],
                    'last_updated': row[5]
                }
            return None
    
    def assign_fingerprint_to_account(self, account_phone: str, account_type: str = "outreach"):
        """Генерирует и назначает отпечаток аккаунту"""
        fingerprint = self.generate_fingerprint(account_type)
        self.save_fingerprint(account_phone, fingerprint)
        return fingerprint