import base64
import html
import os
import re
import threading
import time
import sqlite3
import unicodedata
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set
from urllib.parse import quote

import requests
from flask import Flask, Response, request
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from easynews_client import (
    EasynewsClient,
    EasynewsError,
    SearchItem,
    _active_endpoint_label,
    paginate_enabled,
    max_pages,
    _SEARCH_ATTEMPT_TIMEOUT,
)
from query_replace import parse_rules as _parse_query_replace, apply_rules as _apply_query_replace


APP = Flask(__name__)
_CLIENT: Optional[EasynewsClient] = None
_CLIENT_LOCK = threading.Lock()
_CLIENT_LOGIN_TTL = 600  # seconds
_CLIENT_LAST_LOGIN: float = 0.0


def _load_dotenv():
    path = os.path.join(os.getcwd(), ".env")
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    os.environ.setdefault(k, v)
    except Exception:
        pass


_load_dotenv()

# Setup logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("easynews_indexer")

API_KEY = os.environ.get("NEWZNAB_APIKEY")
if not API_KEY or API_KEY.strip() == "":
    logger.error("CRITICAL SECURITY ERROR: NEWZNAB_APIKEY is not configured! All API endpoints will return 401 Unauthorized.")
else:
    logger.info(f"API key authentication active (key length: {len(API_KEY)})")

EZ_USER = os.environ.get("EASYNEWS_USER")
EZ_PASS = os.environ.get("EASYNEWS_PASS")
CACHE_TTL = int(os.environ.get("CACHE_TTL", os.environ.get("EASYNEWS_CACHE_TTL", "300")))
CACHE_MAXSIZE = int(os.environ.get("CACHE_MAXSIZE", "500"))
ALLOW_PASSWORDED = os.environ.get("ALLOW_PASSWORDED", "true").strip().lower() in ("1", "true", "yes")
DISCARD_LOW_QUALITY = os.environ.get("DISCARD_LOW_QUALITY", "true").strip().lower() in ("1", "true", "yes")
EXCLUDE_REGEX_STR = os.environ.get("EASYNEWS_EXCLUDE_REGEX", "").strip()

# ── Additional feature flags (ported from Lystad93/Easynews_as_indexer_x) ──────
# Skip season-pack queries (S05 with no episode) — Easynews rarely carries real
# season packs, so this stops them from polluting season searches.
IGNORE_SEASON_PACKS = os.environ.get("IGNORE_SEASON_PACKS", "").strip().lower() in ("1", "true", "yes")

# Skip title-matching filters entirely (let Sonarr/Radarr do their own matching).
DISABLE_RESULT_FILTERS = os.environ.get("EASYNEWS_DISABLE_FILTERS", "").strip().lower() in ("1", "true", "yes")

# On dedup, keep the newest re-post instead of the first (relevance-ranked) entry.
DEDUP_KEEP_NEWEST = os.environ.get("EASYNEWS_DEDUP_KEEP_NEWEST", "").strip().lower() in ("1", "true", "yes")

# Keep password-flagged results (flag is often a false positive on VIDEO results).
ALLOW_PASSWORD = os.environ.get("EASYNEWS_ALLOW_PASSWORD", "").strip().lower() in ("1", "true", "yes")

# Drop connector stopwords from the outbound query (recall fix for titles with
# 'and/of/the' that Easynews AND-matches but releases omit).
STRIP_STOPWORDS = os.environ.get("EASYNEWS_STRIP_STOPWORDS", "true").strip().lower() not in ("0", "false", "no", "off")

# Per-title query rewrite rules (e.g. "norsemen => Vikingane").
# Configured via EASYNEWS_QUERY_REPLACE env var (simple or JSON format).
QUERY_REPLACE = _parse_query_replace(os.getenv("EASYNEWS_QUERY_REPLACE", ""))

# Fold Norwegian æ/ø/å to ASCII digraphs (ae/oe/aa) in both the outbound query
# and the title filters — scene releases routinely ASCII-fold these.
TRANSLITERATE_NORWEGIAN = os.environ.get("EASYNEWS_TRANSLITERATE_NORWEGIAN", "").strip().lower() in ("1", "true", "yes")

_NORWEGIAN_TRANSLITERATION = {
    ord("æ"): "ae", ord("Æ"): "Ae",
    ord("ø"): "oe", ord("Ø"): "Oe",
    ord("å"): "aa", ord("Å"): "Aa",
}


def _transliterate_norwegian(text: str) -> str:
    """Fold Norwegian æ/ø/å to their conventional ASCII digraphs (ae/oe/aa)."""
    if not text:
        return text
    return text.translate(_NORWEGIAN_TRANSLITERATION)


# Extra metadata attrs (subtitle/audio/codecs/bitrate/group/password).
# AIOStreams reads 'subs' and 'language' attrs for language filtering.
META_SUBS = os.environ.get("EASYNEWS_META_SUBS", "true").strip().lower() not in ("0", "false", "no", "off")
META_AUDIO = os.environ.get("EASYNEWS_META_AUDIO", "true").strip().lower() not in ("0", "false", "no", "off")
META_CODECS = os.environ.get("EASYNEWS_META_CODECS", "true").strip().lower() not in ("0", "false", "no", "off")
META_BITRATE = os.environ.get("EASYNEWS_META_BITRATE", "true").strip().lower() not in ("0", "false", "no", "off")
META_GROUP = os.environ.get("EASYNEWS_META_GROUP", "true").strip().lower() not in ("0", "false", "no", "off")
META_PASSWORD = os.environ.get("EASYNEWS_META_PASSWORD", "true").strip().lower() not in ("0", "false", "no", "off")


def _format_bitrate_mbps(raw: Any) -> Optional[str]:
    """Easynews's `bps` (raw bits/sec) → '12.72 Mbps'."""
    try:
        bps = int(raw)
    except (TypeError, ValueError):
        return None
    if bps <= 0:
        return None
    return f"{bps / 1_000_000:.2f} Mbps"


# Extra search terms (comma-separated). For each term the bridge also runs
# '<query> <term>' alongside the bare query and merges the results. Easynews
# AND-matches the term, so a language tag like 'nordic' surfaces releases that
# the bare relevance ranking buries deep. Example: EASYNEWS_EXTRA_TERMS=nordic
EXTRA_TERMS = [
    term.strip()
    for term in os.environ.get("EASYNEWS_EXTRA_TERMS", "").split(",")
    if term.strip()
]

# Run EXTRA_TERMS only as part of the 0-result fallback, not on every search.
EXTRA_TERMS_FALLBACK_ONLY = os.environ.get("EASYNEWS_EXTRA_TERMS_FALLBACK_ONLY", "").strip().lower() in ("1", "true", "yes")

# Restrict results to releases whose subtitle tracks include at least one of
# these language codes (e.g. "nor" for Norwegian). Global default; overridable
# per-request with &subs= on the API URL. Empty = no restriction.
REQUIRE_SUBS_DEFAULT = [
    v.strip().lower()
    for v in os.environ.get("EASYNEWS_REQUIRE_SUBS", "").split(",")
    if v.strip()
]

# 0-result fallback: fire alternate spelling / alias queries when the primary
# search returns nothing. Never wastes requests when the primary found something.
FALLBACK_SEARCH = os.environ.get("EASYNEWS_FALLBACK_SEARCH", "").strip().lower() in ("1", "true", "yes")
FALLBACK_TRANSLITERATE = os.environ.get("EASYNEWS_FALLBACK_TRANSLITERATE", "true").strip().lower() not in ("0", "false", "no", "off")
FALLBACK_ALT_TITLES = os.environ.get("EASYNEWS_FALLBACK_ALT_TITLES", "true").strip().lower() not in ("0", "false", "no", "off")

# ── Language canonicalisation (ISO 639-2 variant folding) ─────────────────────
# Fold ISO 639-2/B, 2-letter, and dialect codes to a single canonical token so
# REQUIRE_SUBS / &subs= filters match regardless of which code is used.
_LANG_GROUPS = (
    ("nor", ("no", "nb", "nn", "nob", "nno", "nor")),  # Norwegian (+Bokmål/Nynorsk)
    ("eng", ("en", "eng")), ("swe", ("sv", "swe")), ("dan", ("da", "dan")),
    ("fin", ("fi", "fin")), ("isl", ("is", "ice", "isl")), ("ara", ("ar", "ara")),
    ("ger", ("de", "ger", "deu")), ("fre", ("fr", "fre", "fra")),
    ("spa", ("es", "spa")), ("ita", ("it", "ita")), ("dut", ("nl", "dut", "nld")),
    ("por", ("pt", "por")), ("rus", ("ru", "rus")), ("pol", ("pl", "pol")),
    ("cze", ("cs", "cze", "ces")), ("gre", ("el", "gre", "ell")),
    ("rum", ("ro", "rum", "ron")), ("slo", ("sk", "slo", "slk")),
    ("chi", ("zh", "chi", "zho")), ("hrv", ("hr", "hrv")), ("hun", ("hu", "hun")),
    ("jpn", ("ja", "jpn")), ("kor", ("ko", "kor")), ("tur", ("tr", "tur")),
    ("ukr", ("uk", "ukr")), ("vie", ("vi", "vie")), ("tha", ("th", "tha")),
)
_LANG_CANON: Dict[str, str] = {
    alias: canon for canon, aliases in _LANG_GROUPS for alias in aliases
}


def _canon_langs(codes: List[str]) -> Set[str]:
    """Fold a list of language codes to canonical tokens."""
    out: Set[str] = set()
    for c in codes:
        c = (c or "").strip().lower()
        if c:
            out.add(_LANG_CANON.get(c, c))
    return out


def _join_langs(value: Any) -> Optional[str]:
    """Normalise a language field (list or comma string) to a clean comma-joined string."""
    if not value:
        return None
    if isinstance(value, str):
        parts = [p.strip().lower() for p in value.split(",")]
    elif isinstance(value, (list, tuple)):
        parts = [str(p).strip().lower() for p in value]
    else:
        return None
    seen: List[str] = []
    for p in parts:
        if p:
            canon = _LANG_CANON.get(p, p)
            if canon not in seen:
                seen.append(canon)
    return ",".join(sorted(seen)) if seen else None


# ── Accent-strip helpers for 0-result fallback transliteration ─────────────────
_VAR_DIGRAPH = {
    ord("æ"): "ae", ord("Æ"): "Ae", ord("ø"): "oe", ord("Ø"): "Oe",
    ord("å"): "aa", ord("Å"): "Aa", ord("ä"): "ae", ord("Ä"): "Ae",
    ord("ö"): "oe", ord("Ö"): "Oe", ord("ü"): "ue", ord("Ü"): "Ue",
    ord("ß"): "ss",
}
_VAR_BAREVOWEL = {
    ord("æ"): "ae", ord("Æ"): "Ae", ord("ø"): "o", ord("Ø"): "O",
    ord("å"): "a", ord("Å"): "A", ord("ä"): "a", ord("Ä"): "A",
    ord("ö"): "o", ord("Ö"): "O", ord("ü"): "u", ord("Ü"): "U",
    ord("ß"): "ss",
}
_VAR_PRESTRIP = {
    ord("ø"): "o", ord("Ø"): "O", ord("å"): "a", ord("Å"): "A",
    ord("æ"): "ae", ord("Æ"): "Ae", ord("ß"): "ss", ord("đ"): "d", ord("Đ"): "D",
    ord("ł"): "l", ord("Ł"): "L",
}


def _strip_accents(text: str) -> str:
    """Fold any accented/diacritic title to plain ASCII (José→Jose, Köln→Koln)."""
    pre = text.translate(_VAR_PRESTRIP)
    nfkd = unicodedata.normalize("NFKD", pre)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def _spelling_variants(title: str) -> List[str]:
    """Up to three alternate ASCII spellings: digraph (ø→oe), bare-vowel (ø→o),
    and full accent-strip. Empty for already-ASCII titles."""
    if not title:
        return []
    out: List[str] = []
    for conv in (title.translate(_VAR_DIGRAPH), title.translate(_VAR_BAREVOWEL), _strip_accents(title)):
        conv = conv.strip()
        if conv and conv != title and conv not in out:
            out.append(conv)
    return out

_TRASH_REJECTION_RE = re.compile(
    r'\b(?:'
    r'cam|camrip|hdcam|hd-cam|'
    r'telesync|telecine|tc|ts|hdts|hd-ts|'
    r'scr|screener|dvdscr|dvdscreener|bdscr|'
    r'3d|sbs|ou|h-sbs|h-ou|half-sbs|half-ou|hsbs|hou|halfsbs|halfou|'
    r'hc|korsub|korean(?:\.|\s|_)*sub'
    r')\b',
    re.IGNORECASE
)

_CUSTOM_EXCLUDE_RE = None
if EXCLUDE_REGEX_STR:
    try:
        _CUSTOM_EXCLUDE_RE = re.compile(EXCLUDE_REGEX_STR, re.IGNORECASE)
        logger.info(f"Custom exclusion regex active: {EXCLUDE_REGEX_STR}")
    except Exception as e:
        logger.error(f"Failed to compile custom exclusion regex '{EXCLUDE_REGEX_STR}': {e}")

class SearchCache:
    """Thread-safe LRU cache with TTL expiry and bounded maxsize to prevent unbounded memory growth."""

    def __init__(self, ttl: int = 300, maxsize: int = 500):
        self.ttl = ttl
        self.maxsize = maxsize
        self.cache: OrderedDict = OrderedDict()
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            if key in self.cache:
                val, expiry = self.cache[key]
                if time.time() < expiry:
                    # Move to end (most recently used)
                    self.cache.move_to_end(key)
                    return val
                else:
                    del self.cache[key]
            return None

    def set(self, key: str, value: Any):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = (value, time.time() + self.ttl)
            # Evict oldest entry if over capacity
            while len(self.cache) > self.maxsize:
                self.cache.popitem(last=False)

    def purge_expired(self):
        """Remove all expired entries. Called periodically by background thread."""
        now = time.time()
        with self.lock:
            expired = [k for k, (_, exp) in self.cache.items() if now >= exp]
            for k in expired:
                del self.cache[k]
        return len(expired)

    def __len__(self):
        with self.lock:
            return len(self.cache)


def _cache_cleanup_worker():
    """Background thread: purge expired entries from all caches every 60 seconds."""
    while True:
        time.sleep(60)
        try:
            n1 = _SEARCH_CACHE.purge_expired()
            n2 = _NZB_CACHE.purge_expired()
            if n1 or n2:
                logger.debug(f"Cache cleanup: evicted {n1} search + {n2} NZB expired entries")
        except Exception as e:
            logger.warning(f"Cache cleanup error: {e}")


_SEARCH_CACHE = SearchCache(ttl=CACHE_TTL, maxsize=CACHE_MAXSIZE)
_NZB_CACHE = SearchCache(ttl=3600, maxsize=200)

_cache_cleanup_thread = threading.Thread(target=_cache_cleanup_worker, daemon=True, name="cache-cleanup")
_cache_cleanup_thread.start()


class DeobfuscationCache:
    def __init__(self, db_path="indexer_cache.db"):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS deobfuscated_releases (
                        hash TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        imdb_id TEXT,
                        year INTEGER,
                        category INTEGER,
                        detected_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_imdb ON deobfuscated_releases(imdb_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_title ON deobfuscated_releases(title)")
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize deobfuscation cache DB: {e}")

    def add_release(self, release_hash, title, imdb_id=None, year=None, category=None):
        if not release_hash or not title:
            return
        try:
            # Normalize hash for lookup consistency
            release_hash = release_hash.strip().lower()
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO deobfuscated_releases (hash, title, imdb_id, year, category, detected_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (release_hash, title, imdb_id, year, category))
                conn.commit()
                logger.info(f"Cached deobfuscated release: {release_hash} -> {title} (IMDb: {imdb_id})")
        except Exception as e:
            logger.error(f"Failed to add release to deobfuscation cache: {e}")

    def get_by_imdb(self, imdb_id):
        if not imdb_id:
            return []
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT hash, title, category FROM deobfuscated_releases WHERE imdb_id = ?", (imdb_id,))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Failed to query deobfuscation cache by IMDb: {e}")
            return []

    def get_by_title(self, title_query):
        if not title_query:
            return []
        try:
            # Clean query by replacing spaces and other separators with % wildcards
            cleaned_query = "%" + "%".join(re.split(r'[\s\.\-_]+', title_query.strip())) + "%"
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT hash, title, category FROM deobfuscated_releases WHERE title LIKE ?", (cleaned_query,))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Failed to query deobfuscation cache by title: {e}")
            return []

    def get_by_hash(self, release_hash):
        if not release_hash:
            return None
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT title, category FROM deobfuscated_releases WHERE hash = ?", (release_hash.strip().lower(),))
                row = cursor.fetchone()
                if row:
                    return row
        except Exception as e:
            logger.error(f"Failed to query deobfuscation cache by hash: {e}")
        return None

_DEOBFUSCATION_CACHE = DeobfuscationCache()


def extract_release_name_from_nfo(text: str) -> Optional[str]:
    # Match standard release name formats: e.g. Name.Name.S01E01.1080p.WEB.H264-Group or movie name
    pattern = r'\b[a-zA-Z0-9_\.\-]+\b(?:s\d{2}e\d{2}|19\d{2}|20\d{2})\b[a-zA-Z0-9_\.\-]+\b'
    matches = re.findall(pattern, text, re.IGNORECASE)
    for m in matches:
        if re.search(r'\b(?:720p|1080p|2160p|4k|uhd|bluray|web-?dl|webrip|x264|x265|hevc)\b', m, re.IGNORECASE):
            cleaned = m.strip(' .-_')
            if len(cleaned) > 10:
                return cleaned
    return None


def extract_imdb_id(text: str) -> Optional[str]:
    m = re.search(r'tt\d{7,8}', text)
    return m.group(0) if m else None


_IMDB_TITLE_CACHE = SearchCache(ttl=86400, maxsize=1000)

def resolve_imdb_title(imdb_id: str, is_movie: bool = True) -> Optional[str]:
    cached = _IMDB_TITLE_CACHE.get(imdb_id)
    if cached:
        return cached

    types = ["movie", "series"] if is_movie else ["series", "movie"]
    for t in types:
        url = f"https://v3-cinemeta.stremio.com/meta/{t}/{imdb_id}.json"
        try:
            r = requests.get(url, timeout=3.0)
            if r.status_code == 200:
                data = r.json()
                meta = data.get("meta", {})
                title = meta.get("name")
                if title:
                    _IMDB_TITLE_CACHE.set(imdb_id, title)
                    return title
        except Exception as e:
            logger.error(f"Failed to resolve IMDb ID {imdb_id} via Cinemeta ({t}): {e}")
    return None

_NFO_THREAD_POOL = ThreadPoolExecutor(max_workers=5)

def resolve_nfo_background(base_prefix, nfo_item, json_data, category_id):
    def worker():
        try:
            dl_farm = json_data.get("dlFarm")
            dl_port = json_data.get("dlPort")
            down_url = json_data.get("downURL") or "https://members.easynews.com/dl"
            file_path = f"{nfo_item['hash']}{nfo_item['ext']}/{nfo_item['filename']}{nfo_item['ext']}"
            url = f"{down_url}/{dl_farm}/{dl_port}/{file_path}"
            
            logger.info(f"Background NFO de-obfuscation: {url}")
            c = client()
            r_nfo = c.s.get(url, timeout=5.0)
            if r_nfo.status_code == 200:
                nfo_text = r_nfo.content.decode("utf-8", errors="ignore")
                parsed_name = extract_release_name_from_nfo(nfo_text)
                imdb_id = extract_imdb_id(nfo_text)
                
                if parsed_name:
                    title = parsed_name
                    if not title.lower().endswith((".mkv", ".mp4", ".avi", ".ts", ".mov")):
                        title = f"{title}.mkv"
                    _DEOBFUSCATION_CACHE.add_release(base_prefix, title, imdb_id=imdb_id, category=category_id)
                elif imdb_id:
                    resolved_title = resolve_imdb_title(imdb_id, is_movie=(category_id == CATEGORY_MOVIES))
                    if resolved_title:
                        title = f"{resolved_title}.mkv"
                        _DEOBFUSCATION_CACHE.add_release(base_prefix, title, imdb_id=imdb_id, category=category_id)
        except Exception as e:
            logger.error(f"Failed background NFO de-obfuscation for hash {base_prefix}: {e}")
            
    _NFO_THREAD_POOL.submit(worker)


_CUSTOM_TITLES: Dict[str, List[str]] = {}
_LAST_CUSTOM_TITLES_PATH: Optional[str] = None
_LAST_CUSTOM_TITLES_MTIME: float = 0.0
_CUSTOM_TITLES_LOCK = threading.Lock()

def load_custom_titles() -> Dict[str, List[str]]:
    global _CUSTOM_TITLES, _LAST_CUSTOM_TITLES_PATH, _LAST_CUSTOM_TITLES_MTIME
    paths = [
        os.path.join(os.getcwd(), "custom-titles.json"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "custom-titles.json"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "custom-titles.json"),
        "/app/custom-titles.json",
        "/custom-titles.json"
    ]
    for p in paths:
        if os.path.exists(p):
            try:
                mtime = os.path.getmtime(p)
                with _CUSTOM_TITLES_LOCK:
                    if _LAST_CUSTOM_TITLES_PATH == p and _LAST_CUSTOM_TITLES_MTIME == mtime:
                        return _CUSTOM_TITLES
                    with open(p, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, dict):
                            _CUSTOM_TITLES = data
                            _LAST_CUSTOM_TITLES_PATH = p
                            _LAST_CUSTOM_TITLES_MTIME = mtime
                            logger.info(f"Loaded {len(_CUSTOM_TITLES)} custom titles from {p}")
                            # Seed deobfuscation cache with custom-titles.json mappings
                            for title, mappings in data.items():
                                if isinstance(mappings, list):
                                    for mapping in mappings:
                                        if isinstance(mapping, dict) and "search" in mapping and "replace_title" in mapping:
                                            r_title = mapping["replace_title"]
                                            r_search = mapping["search"]
                                            _DEOBFUSCATION_CACHE.add_release(r_search, r_title)
                            return data
            except Exception as e:
                logger.error(f"Failed to parse custom titles from {p}: {e}")
    logger.info("No custom titles file found")
    return {}

load_custom_titles()



def require_apikey() -> bool:
    if not API_KEY or API_KEY.strip() == "":
        return False
    key = request.args.get("apikey") or request.headers.get("X-Api-Key")
    return key == API_KEY


_CLIENT_REFRESHING = False  # guard: only one background refresh at a time


def _refresh_login_async() -> None:
    """Re-login in the background so a search is never blocked on a slow login.
    HTTP Basic Auth stays on the session, so searches keep working with the
    existing session while this runs."""
    global _CLIENT_LAST_LOGIN, _CLIENT_REFRESHING
    try:
        _CLIENT.login()  # type: ignore[union-attr]
        with _CLIENT_LOCK:
            _CLIENT_LAST_LOGIN = time.time()
        logger.info("Background session refresh succeeded.")
    except EasynewsError as e:
        with _CLIENT_LOCK:
            _CLIENT_LAST_LOGIN = time.time() - (_CLIENT_LOGIN_TTL - 60)
        logger.warning(
            "Background session refresh failed: %s. "
            "Keeping existing session (HTTP Basic Auth) — searches still work. "
            "Will retry in ~60s.", e,
        )
    finally:
        with _CLIENT_LOCK:
            _CLIENT_REFRESHING = False


def client() -> EasynewsClient:
    if not EZ_USER or not EZ_PASS:
        raise RuntimeError("Set EASYNEWS_USER and EASYNEWS_PASS environment variables")
    global _CLIENT, _CLIENT_LAST_LOGIN, _CLIENT_REFRESHING
    with _CLIENT_LOCK:
        now = time.time()
        if _CLIENT is None:
            logger.info("Starting up: logging in to Easynews for the first time...")
            _CLIENT = EasynewsClient(EZ_USER, EZ_PASS)
            _CLIENT.login()
            _CLIENT_LAST_LOGIN = now
            _CLIENT.start_keepalive()
            logger.info("Startup login succeeded. Indexer is ready (endpoint: %s).", _active_endpoint_label())
            return _CLIENT
        # Periodic refresh runs in the background so the current request is
        # never blocked on the login round-trip.
        if now - _CLIENT_LAST_LOGIN > _CLIENT_LOGIN_TTL and not _CLIENT_REFRESHING:
            age_mins = int((now - _CLIENT_LAST_LOGIN) / 60)
            logger.info(
                "Session is %d min old (TTL=%ds). Refreshing login in background...",
                age_mins, _CLIENT_LOGIN_TTL,
            )
            _CLIENT_REFRESHING = True
            _CLIENT_LAST_LOGIN = now  # push forward so we don't spawn multiple threads
            threading.Thread(target=_refresh_login_async, daemon=True, name="session-refresh").start()
        return _CLIENT


def invalidate_client() -> None:
    global _CLIENT
    with _CLIENT_LOCK:
        logger.info("Invalidating cached EasynewsClient session.")
        _CLIENT = None


def xml_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def encode_id(item: dict) -> str:
    # Pack info needed to build NZB for a single selection and preserve title for filename
    payload = {
        "hash": item.get("hash"),
        "filename": item.get("filename"),
        "ext": item.get("ext"),
        "sig": item.get("sig"),
        "title": item.get("title"),
    }
    if item.get("is_archive"):
        payload["is_archive"] = True
        payload["archive_prefix"] = item.get("archive_prefix")
        payload["poster"] = item.get("poster")
    if item.get("sample"):
        payload["sample"] = True
    raw = (
        base64.urlsafe_b64encode(json.dumps(payload, ensure_ascii=False).encode())
        .decode()
        .rstrip("=")
    )
    return raw


def decode_id(enc: str) -> dict:
    pad = "=" * (-len(enc) % 4)
    raw = base64.urlsafe_b64decode(enc + pad).decode()
    return json.loads(raw)


def to_search_item(d: dict) -> SearchItem:
    return SearchItem(
        id=None,
        hash=d["hash"],
        filename=d["filename"],
        ext=d["ext"],
        sig=d.get("sig"),
        type="VIDEO",
        raw={},
    )


_ARCHIVE_SUFFIX_RE = re.compile(
    r"(?:\.part\d+|\.r\d+|\.vol\d+(?:[+\-_]\d+)?|\.nfo|\.sfv|\.par2|\.par|\.releaseinfo|\.rar|\.nzb|\.bad|\.queued|\.part)$",
    re.IGNORECASE
)

def get_release_prefix(filename: str) -> str:
    base = filename.strip().rstrip(".")
    prev = ""
    while base != prev:
        prev = base
        base = _ARCHIVE_SUFFIX_RE.sub("", base)
        base = base.rstrip(".")
    return base


_TITLE_PARENS_RE = re.compile(r"\(([^()]*)\)")


def _normalize_title(raw: str) -> str:
    text = html.unescape(raw or "").strip()
    if not text:
        return text
    matches = _TITLE_PARENS_RE.findall(text)
    for candidate in reversed(matches):
        cleaned = candidate.strip()
        if cleaned:
            return cleaned
    return text


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(int(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            try:
                return datetime.fromtimestamp(int(text), tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                dt = datetime.strptime(text.replace("Z", "+0000"), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except ValueError:
                continue
    return None


_ALLOWED_VIDEO_EXTENSIONS = {
    ".mkv",
    ".mp4",
    ".m4v",
    ".avi",
    ".ts",
    ".mov",
    ".wmv",
    ".mpg",
    ".mpeg",
    ".flv",
    ".webm",
}


def _has_actual_data_files(items: List[dict]) -> bool:
    for it in items:
        ext = (it.get("ext") or "").lower()
        if ext in _ALLOWED_VIDEO_EXTENSIONS:
            return True
        if ext == ".nzb":
            return True
        if ext in (".rar", ".zip", ".7z"):
            return True
        if re.search(r'\.r\d+$', ext):
            return True
    return False


def _is_archive_complete(items: List[dict]) -> bool:
    part_numbers = set()
    has_parts = False

    for it in items:
        fn = it.get("filename") or ""
        ext = it.get("ext") or ""
        fullname = f"{fn}{ext}"

        # 1. Check for .partXX.rar or .partXX
        part_match = re.search(r'\.part(?P<num>\d+)\b', fullname, re.IGNORECASE)
        if part_match:
            part_numbers.add(int(part_match.group("num")))
            has_parts = True
            continue

        # 2. Check for .rXX or .zXX extensions
        ext_lower = ext.lower()
        rz_match = re.match(r'^\.[rz](?P<num>\d+)$', ext_lower)
        if rz_match:
            part_numbers.add(int(rz_match.group("num")))
            has_parts = True
            continue

        # 3. Check for numeric extensions like .001, .01
        num_match = re.match(r'^\.(?P<num>\d+)$', ext_lower)
        if num_match:
            part_numbers.add(int(num_match.group("num")))
            has_parts = True
            continue

    if not has_parts:
        return True

    if not part_numbers:
        return True

    min_part = min(part_numbers)
    max_part = max(part_numbers)

    if min_part > 1:
        return False

    for p in range(min_part, max_part + 1):
        if p not in part_numbers:
            return False

    return True



_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "of",
    "in",
    "for",
    "on",
}

_MIN_DURATION_SECONDS = int(os.environ.get("MIN_DURATION_SECONDS", os.environ.get("EASYNEWS_MIN_DURATION", "360")))
_TOKEN_SPLIT_RE = re.compile(r"[^\w]+", re.UNICODE)
_QUALITY_RE = re.compile(r"(2160|1440|1080|720|480|360)\s*(p|i)?", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(19[2-9]\d|20[0-2]\d)\b")
_SEASON_EP_RE = re.compile(
    r"(?:s(?P<season>\d{1,2})[\s\.\-_]*e(?P<episode>\d{1,2})|"
    r"(?<!\d)(?P<season2>\d{1,2})x(?P<episode2>\d{1,2})(?!\d)|"
    r"\b(?:season|seizoen|saison|staffel|temporada|series)[\s\.\-_]*(?P<season3>\d{1,2})\b|"
    r"\bep(?:isode)?[\s\.\-_]*(?P<episode3>\d{1,2})\b)",
    re.IGNORECASE,
)
# Anime detection patterns
_ANIME_BRACKET_GROUP_RE = re.compile(r"^\[([^\]]+)\]", re.IGNORECASE)

# Known fansub groups for anime detection
_KNOWN_FANSUB_GROUPS = {
    "subsplease",
    "erai-raws",
    "horriblesubs",
    "judas",
    "gjm",
    "commiesubs",
    "commie",
    "animekaizoku",
    "anime time",
    "asenshi",
    "damedesuyo",
    "gg",
    "fff",
    "underwater",
    "ember",
    "kametsu",
    "kawaiika",
    "mezashite",
    "reinforce",
    "senritsu",
    "vivid",
    "coalgirls",
    "utw",
    "thora",
    "ohys-raws",
    "leopard-raws",
    "asw",
    "mtbb",
    "anime-time",
}

for _grp in os.environ.get("ADDITIONAL_FANSUB_GROUPS", os.environ.get("ANIME_FANSUB_GROUPS", "")).split(","):
    if _grp.strip():
        _KNOWN_FANSUB_GROUPS.add(_grp.strip().lower())
_SANITIZE_SYMBOLS_RE = re.compile(r"[\.\-_:\s]+")
_NON_ALNUM_RE = re.compile(r"[^\w\sÀ-ÿ]")

# Newznab category constants
CATEGORY_MOVIES = 2000
CATEGORY_MOVIES_HD = 2030
CATEGORY_MOVIES_UHD = 2040
CATEGORY_TV = 5000
CATEGORY_TV_HD = 5030
CATEGORY_TV_UHD = 5040
CATEGORY_ANIME = 5070  # Anime as TV subcategory
CATEGORY_OTHER = 7000


def _parse_duration_seconds(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        if raw <= 0:
            return None
        return int(raw)
    text = str(raw).strip().lower()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    total = 0
    matched = False
    for label, multiplier in (("h", 3600), ("m", 60), ("s", 1)):
        for part in re.findall(rf"(\d+)\s*{label}", text):
            total += int(part) * multiplier
            matched = True
    if matched:
        return total
    if ":" in text:
        try:
            pieces = [int(p) for p in text.split(":")]
            if len(pieces) == 3:
                h, m, s = pieces
            elif len(pieces) == 2:
                h = 0
                m, s = pieces
            else:
                return None
            return h * 3600 + m * 60 + s
        except ValueError:
            return None
    return None


def _as_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _tokenize(text: str) -> List[str]:
    if not text:
        return []
    normalized = _TOKEN_SPLIT_RE.sub(" ", text.lower())
    tokens = [
        tok for tok in normalized.split() if len(tok) > 1 and tok not in _STOPWORDS
    ]
    return tokens


def _is_token_subset(token_set: Set[str], title_tokens: Set[str]) -> bool:
    if not title_tokens:
        return False
    for q_tok in token_set:
        if q_tok in title_tokens:
            continue
        # Season token fallback match (e.g. s02 matches s02e03)
        if re.match(r'^s\d{1,2}$', q_tok):
            found_se_match = False
            for t_tok in title_tokens:
                if t_tok.startswith(q_tok) and re.match(r'^s\d{1,2}e\d{1,4}', t_tok):
                    found_se_match = True
                    break
            if found_se_match:
                continue
        return False
    return True


def _sanitize_phrase(text: str) -> str:
    if not text:
        return ""
    working = text.replace("&", " and ")
    working = _SANITIZE_SYMBOLS_RE.sub(" ", working)
    working = _NON_ALNUM_RE.sub("", working)
    return working.lower().strip()


def _is_flagged_item(item: Any, ext: str, duration_seconds: Optional[int]) -> bool:
    passwd = False
    virus = False
    file_type = ""
    if isinstance(item, dict):
        passwd = bool(item.get("passwd") or item.get("password"))
        virus = bool(item.get("virus"))
        file_type = str(item.get("type") or item.get("file_type") or "").upper()
    if (passwd and not ALLOW_PASSWORDED) or virus:
        return True
    if file_type and file_type != "VIDEO":
        return True
    if ext and ext.lower() not in _ALLOWED_VIDEO_EXTENSIONS:
        return True
    if duration_seconds is not None and duration_seconds < _MIN_DURATION_SECONDS:
        return True
    return False


def _format_duration(seconds: Optional[int]) -> Optional[str]:
    if seconds is None:
        return None
    if seconds <= 0:
        return None
    td = timedelta(seconds=seconds)
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{secs:02}"


def _extract_quality(*texts: Optional[str]) -> Optional[str]:
    for text in texts:
        if not text:
            continue
        lowered = text.lower()
        if "4k" in lowered:
            return "2160p"
        match = _QUALITY_RE.search(lowered)
        if match:
            value = match.group(1)
            suffix = match.group(2) or "p"
            return f"{value}{suffix.lower()}"
        if "uhd" in lowered:
            return "2160p"
        if "fhd" in lowered:
            return "1080p"
    return None


def _build_thumbnail_url(
    base: Optional[str], hash_id: Optional[str], slug: Optional[str]
) -> Optional[str]:
    if not base or not hash_id:
        return None
    base = base.rstrip("/") + "/"
    prefix = hash_id[:3]
    safe_slug = quote((slug or hash_id).replace("/", "_"))
    return f"{base}{prefix}/pr-{hash_id}.jpg/th-{safe_slug}.jpg"


def _extract_release_markers(
    text: str, quality_hint: Optional[str] = None
) -> Dict[str, Optional[Any]]:
    info: Dict[str, Optional[Any]] = {}
    if not text:
        return info

    # Check for S01E01-E04 or 01x01-04 range first
    range_match = re.search(
        r'\bs(?P<season>\d{1,2})[\s\.\-_]*e(?P<start>\d{1,2})[\s\.\-_]*(?:-|to|e)[\s\.\-_]*e?(?P<end>\d{1,2})\b',
        text,
        re.IGNORECASE
    )
    if not range_match:
        range_match = re.search(
            r'\b(?P<season>\d{1,2})x(?P<start>\d{1,2})[\s\.\-_]*(?:-|to)[\s\.\-_]*(?P<end>\d{1,2})\b',
            text,
            re.IGNORECASE
        )

    if range_match:
        season = int(range_match.group("season"))
        start = int(range_match.group("start"))
        end = int(range_match.group("end"))
        if start < end and (end - start) <= 24:
            info["season"] = season
            info["episode"] = start
            info["episodes"] = list(range(start, end + 1))

    if "season" not in info:
        season_match = _SEASON_EP_RE.search(text)
        if season_match:
            season = season_match.group("season") or season_match.group("season2") or season_match.group("season3")
            episode = season_match.group("episode") or season_match.group("episode2") or season_match.group("episode3")
            if season:
                info["season"] = int(season)
            if episode:
                info["episode"] = int(episode)
        else:
            # Season-only fallback (e.g. S02 or Staffel 2)
            season_only_match = re.search(
                r'\b(?:S|season|seizoen|saison|staffel|temporada|series)[\s\.\-_]*(?P<season>\d{1,2})\b',
                text,
                re.IGNORECASE
            )
            if season_only_match:
                info["season"] = int(season_only_match.group("season"))
    year_match = _YEAR_RE.search(text)
    if year_match:
        info["year"] = int(year_match.group(0))
    quality = quality_hint or _extract_quality(text)
    if quality:
        info["quality"] = quality
    return info


def _detect_anime(title: str) -> bool:
    """
    Detect anime releases using fansub indicators.

    Requirements (all must be true):
    1. Bracketed release group at start: [Group]
    2. Group must be in known fansub whitelist
    3. Episode-only numbering (- 01, Ep 01, Episode 01)
    4. NO traditional TV patterns (S01E01, 1x02)

    Returns: True if anime, False otherwise
    """
    # Guard: Exclude if traditional TV patterns exist
    if _SEASON_EP_RE.search(title):
        return False

    bracket_match = _ANIME_BRACKET_GROUP_RE.search(title)
    if not bracket_match:
        return False

    # This prevents [BBC], [PBS], [REPACK] from being detected as anime
    group_name = bracket_match.group(1).strip().lower()
    if group_name not in _KNOWN_FANSUB_GROUPS:
        return False

    # Remove bracketed group to avoid false matches
    title_without_group = title[bracket_match.end() :].strip()

    # Episode patterns: "- 01", "Ep 01", "Episode 01", "- 01v2", "-1090."
    # Support up to 4 digits for long-running anime (e.g., One Piece episode 1045)
    episode_patterns = [
        r"[\s\-_]+\d{1,4}(?:\s*v\d+)?[\s\-_\.\(\[]",  # " - 1090 " or "-1045." or "- 01v2 -"
        r"[\s\-_]Ep?\.?\s*\d{1,4}",  # "- Ep01" or " E1045"
        r"[\s\-_]Episode\s*\d{1,4}",  # "- Episode 01" or "- Episode 1045"
    ]

    has_episode = any(
        re.search(pattern, title_without_group, re.IGNORECASE)
        for pattern in episode_patterns
    )

    return has_episode


def _detect_category(title: str, metadata: Dict[str, Optional[Any]]) -> int:
    """
    Detect Newznab category based on filename and extracted metadata.

    Detection logic:
    1. Anime: bracketed fansub groups + episode-only patterns (PRIORITY)
    2. TV shows: presence of season/episode patterns (SxxExx or xxyy)
    3. Movies: presence of year, absence of TV patterns
    4. Resolution subcategories: 720p+ = HD, 2160p/4K/UHD = UHD (TV/Movies only)
    5. Default to generic categories if uncertain

    Args:
        title: The filename/title to analyze
        metadata: Dict with season, episode, year, quality keys

    Returns:
        Newznab category ID (int)
    """
    # Check for anime FIRST (priority detection)
    if _detect_anime(title):
        return CATEGORY_ANIME  # 5070 - No quality subcategories

    season = metadata.get("season")
    episode = metadata.get("episode")
    quality = metadata.get("quality")
    year = metadata.get("year")

    quality_lower = (quality or "").lower()
    is_uhd = False
    is_hd = False

    if quality_lower:
        # UHD: 2160p or higher, or contains 4k/uhd keywords
        if "2160" in quality_lower or "4k" in quality_lower or "uhd" in quality_lower:
            is_uhd = True
        # HD: 720p or 1080p
        elif "720" in quality_lower or "1080" in quality_lower:
            is_hd = True

    has_tv_pattern = season is not None or episode is not None

    if not has_tv_pattern:
        if _SEASON_EP_RE.search(title):
            has_tv_pattern = True

    if has_tv_pattern:
        if is_uhd:
            return CATEGORY_TV_UHD  # 5040
        elif is_hd:
            return CATEGORY_TV_HD  # 5030
        else:
            return CATEGORY_TV  # 5000

    # Movies typically have a year but no season/episode
    if year or (not has_tv_pattern):
        if is_uhd:
            return CATEGORY_MOVIES_UHD  # 2040
        elif is_hd:
            return CATEGORY_MOVIES_HD  # 2030
        else:
            return CATEGORY_MOVIES  # 2000

    # Default fallback to generic Movies
    return CATEGORY_MOVIES  # 2000


def parse_torrent_title(title: str) -> dict:
    name, ext = os.path.splitext(title)
    
    # Find year (between 1920 and 2030)
    year_match = _YEAR_RE.search(name)
    year = int(year_match.group(1)) if year_match else None
    
    # Find resolution
    res_match = re.search(r'\b(2160p|1440p|1080p|720p|480p|360p|4k|uhd|fhd|hd)\b', name, re.IGNORECASE)
    resolution = res_match.group(1).lower() if res_match else None
    
    # Find season/episode
    se_match = _SEASON_EP_RE.search(name)
    if not se_match:
        se_match = re.search(r'\bS(?P<season>\d{1,2})\b', name, re.IGNORECASE)
    
    # Common source/codec/other keywords:
    keywords = [
        r'\bbluray\b', r'\bweb-?dl\b', r'\bwebrip\b', r'\bbrrip\b', r'\bdvdrip\b',
        r'\bhdr(10)?\b', r'\bdv\b', r'\bhevc\b', r'\bx264\b', r'\bx265\b', r'\bh264\b',
        r'\bh265\b', r'\bremux\b', r'\bmulti\b', r'\bdual-?audio\b', r'\bsubbed\b',
        r'\bdts(-hd)?\b', r'\batmos\b', r'\bdd5\b', r'\bac3\b'
    ]
    
    earliest_idx = len(name)
    
    if year_match:
        earliest_idx = min(earliest_idx, year_match.start())
    if se_match:
        earliest_idx = min(earliest_idx, se_match.start())
    if res_match:
        earliest_idx = min(earliest_idx, res_match.start())
        
    for kw in keywords:
        kw_match = re.search(kw, name, re.IGNORECASE)
        if kw_match:
            earliest_idx = min(earliest_idx, kw_match.start())
            
    clean_title = name[:earliest_idx].strip(' .-_()[]{}')
    if not clean_title:
        clean_title = name
        
    return {
        "title": clean_title,
        "year": year,
        "resolution": resolution
    }

def sanitize_title(title: str) -> str:
    if not title:
        return ""
    
    replacements = {
        'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss',
        'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue'
    }
    result = title
    for k, v in replacements.items():
        result = result.replace(k, v)
        
    result = result.replace('&', 'and')
    result = re.sub(r'[\.\-_:\s]+', ' ', result)
    result = re.sub(r'[\[\]\(\){}]', ' ', result)
    result = re.sub(r'[^\w\sÀ-ÿ]', '', result)
    return result.lower().strip()

def matches_title(title: str, query: str, strict: bool) -> bool:
    if not query:
        return True
    sanitized_query = sanitize_title(query)
    sanitized_title = sanitize_title(title)
    
    # Season/episode pattern inside sanitized title/query
    season_episode_pattern = r's\d+\s*e\d+|season\s*\d+|ep(?:isode)?\s*\d+|s\d+'
    
    query_parts = re.split(season_episode_pattern, sanitized_query, flags=re.IGNORECASE)
    main_query_part = query_parts[0].strip()
    
    if strict:
        has_season_episode_pattern = bool(re.search(season_episode_pattern, sanitized_query, re.IGNORECASE))
        
        if has_season_episode_pattern:
            title_words = sanitized_title.split()
            main_query_words = main_query_part.split()
            
            if sanitized_title == main_query_part:
                return True
                
            se_match = re.search(season_episode_pattern, sanitized_title, re.IGNORECASE)
            if se_match:
                title_before_se = sanitized_title.split(se_match.group(0))[0].strip()
                if title_before_se == main_query_part:
                    return True
                    
                title_without_year = re.sub(_YEAR_RE, '', title_before_se).strip()
                if title_without_year == main_query_part:
                    return True
                    
                title_words_without_year = title_without_year.split()
                if len(title_words_without_year) > len(main_query_words):
                    noise_words = {"remastered", "extended", "uncut", "director", "cut", "special", "edition", "imax"}
                    extra_words = set(title_words_without_year) - set(main_query_words)
                    if not extra_words.issubset(noise_words):
                        return False
            else:
                return False
                
            se_match = re.search(season_episode_pattern, sanitized_title, re.IGNORECASE)
            title_before_se = sanitized_title.split(se_match.group(0))[0].strip() if se_match else sanitized_title
            title_without_year = re.sub(_YEAR_RE, '', title_before_se).strip()
            title_words_without_year = title_without_year.split()
            
            is_exact_word_match = True
            for i, word in enumerate(main_query_words):
                if i >= len(title_words_without_year) or title_words_without_year[i] != word:
                    is_exact_word_match = False
                    break
            return is_exact_word_match
            
        parsed = parse_torrent_title(title)
        parsed_title = parsed.get("title")
        year = parsed.get("year")
        
        if parsed_title:
            sanitized_parsed_title = sanitize_title(parsed_title)
            parsed_title_words = sanitized_parsed_title.split()
            query_words = sanitized_query.split()
            
            if sanitized_parsed_title == sanitized_query:
                return True
                
            if year:
                title_without_year = sanitized_parsed_title.replace(str(year), '').strip()
                if title_without_year == sanitized_query:
                    return True
                    
            query_year_match = re.search(_YEAR_RE, sanitized_query)
            if query_year_match and year:
                query_year = query_year_match.group(1)
                query_without_year = sanitized_query.replace(query_year, '').strip()
                title_without_year = sanitized_parsed_title.replace(str(year), '').strip()
                if query_without_year == title_without_year and str(year) == query_year:
                    return True
                    
                noise_words = {"remastered", "extended", "uncut", "director", "cut", "special", "edition", "imax"}
                title_words_without_year = title_without_year.split()
                query_words_without_year = query_without_year.split()
                extra_words = set(title_words_without_year) - set(query_words_without_year)
                if extra_words.issubset(noise_words) and all(w in title_words_without_year for w in query_words_without_year):
                    return True
            
            parsed_title_without_year = re.sub(_YEAR_RE, '', sanitized_parsed_title).strip()
            parsed_title_words_without_year = parsed_title_without_year.split()
            noise_words = {"remastered", "extended", "uncut", "director", "cut", "special", "edition", "imax"}
            extra_words = set(parsed_title_words_without_year) - set(query_words)
            if not extra_words.issubset(noise_words):
                if len(parsed_title_words_without_year) > len(query_words):
                    return False
        return False
        
    has_season_episode_pattern = bool(re.search(season_episode_pattern, sanitized_query, re.IGNORECASE))
    
    if has_season_episode_pattern:
        se_match = re.search(season_episode_pattern, sanitized_query, re.IGNORECASE)
        if se_match:
            pattern = se_match.group(0).lower()
            if pattern not in sanitized_title:
                return False
                
            name_words = [
                word for word in re.sub(season_episode_pattern, ' ', sanitized_query, flags=re.IGNORECASE).split()
                if len(word) > 2
            ]
            if len(name_words) == 0:
                return True
                
            matching_name_words = sum(1 for word in name_words if word in sanitized_title)
            name_ratio = matching_name_words / len(name_words)
            return name_ratio >= 0.7
            
    query_words = sanitized_query.split()
    all_words_match = True
    for word in query_words:
        if len(word) <= 2:
            continue
        if word not in sanitized_title:
            all_words_match = False
            break
            
    if len(query_words) > 1 and not strict:
        matching_words = sum(1 for word in query_words if len(word) > 2 and word in sanitized_title)
        significant_words = sum(1 for word in query_words if len(word) > 2)
        if significant_words > 0:
            match_ratio = matching_words / significant_words
            return match_ratio >= 0.7
            
    return all_words_match
def clean_subject(subject: str) -> str:
    if not subject:
        return ""
    import html
    s = html.unescape(subject).strip()
    
    # 0. Check if there is a parenthesized string containing movie/tv markers
    # If so, extract the content inside parentheses as the primary title candidate
    parentheses_match = re.search(r'\(([^()]*?\b(?:s\d+|19\d{2}|20\d{2}|720p|1080p|2160p|uhd|bluray|webrip|web-dl|h264|x264|h265|x265|hevc|dd5\.1)\b[^()]*?)\)', s, flags=re.IGNORECASE)
    if parentheses_match:
        inside = parentheses_match.group(1).strip()
        # Clean tool/usenet indicators first
        inside_clean = re.sub(r'\b(?:AutoUnRAR|yenc)\b', ' ', inside, flags=re.IGNORECASE).strip()
        inside_clean = re.sub(r'\s+', ' ', inside_clean).strip(' .-_:()[]{}')
        # Then strip archive/info extensions from the end
        inside_clean = re.sub(r'\.(?:nfo|rar|part\d+|r\d+|sfv|par2|releaseinfo|nzb|zip|bad|queued)$', '', inside_clean, flags=re.IGNORECASE)
        inside_clean = inside_clean.strip(' .-_:()[]{}')
        if len(inside_clean) > 8:
            return inside_clean

    # Strip parenthesized archive filenames if we did not extract them as the candidate
    s = re.sub(r'\([^\)]*?\.(?:rar|r\d+|mp4|mkv|zip|par2|nzb|nfo|sfv)\)', ' ', s, flags=re.IGNORECASE)
    
    # 1. Strip quoted filename strings (e.g. - "filename.r01")
    s = re.sub(r'\".*?\"', ' ', s)
    s = re.sub(r'\'.*?\'', ' ', s)
    
    # 2. Strip yEnc indicators
    s = re.sub(r'\byenc\b', ' ', s, flags=re.IGNORECASE)
    
    # 3. Strip part / segment progress indicators like [01/50], (1/50), [1 of 50], [50]
    s = re.sub(r'[\(\[\{]\s*\d+\s*(?:/|of)\s*\d+\s*[\)\]\}]', ' ', s, flags=re.IGNORECASE)
    s = re.sub(r'[\(\[\{]\s*\d{1,3}\s*[\)\]\}]', ' ', s)
    
    # 4. Strip post prefix ID brackets at start (e.g. [12345] or [#12345])
    s = re.sub(r'^\[#?\d+\]', ' ', s)
    
    # 5. Clean up multiple spaces, hyphens, dots and trailing punctuation
    s = re.sub(r'\s+', ' ', s)
    s = s.strip(' .-_:()[]{}')
    return s



def filter_and_map(
    json_data: dict,
    min_bytes: int,
    query_tokens: Optional[List[str]] = None,
    query_meta: Optional[Dict[str, Optional[Any]]] = None,
    strict_phrase: Optional[str] = None,
    strict_match: bool = False,
    custom_titles: Optional[dict] = None,
    allow_password_archives: bool = False,
) -> List[dict]:

    token_set: Set[str] = set(query_tokens or [])
    thumb_base = json_data.get("thumbURL") or json_data.get("thumbUrl")
    
    replacements = {}
    if custom_titles and strict_phrase:
        parsed_strict = parse_torrent_title(strict_phrase)
        clean_strict = parsed_strict.get("title", "").lower()
        for key, vals in custom_titles.items():
            if key.lower() == clean_strict:
                if not isinstance(vals, list):
                    vals = [vals]
                for val in vals:
                    if isinstance(val, dict) and "search" in val and "replace_title" in val:
                        replacements[val["search"].lower()] = val["replace_title"]

    # Load database-based deobfuscated replacements
    db_replacements = []
    if strict_phrase:
        db_replacements.extend(_DEOBFUSCATION_CACHE.get_by_title(strict_phrase))
    q_imdb = query_meta.get("imdb_id") if query_meta else None
    if q_imdb:
        db_replacements.extend(_DEOBFUSCATION_CACHE.get_by_imdb(q_imdb))
    for db_hash, db_title, db_cat in db_replacements:
        replacements[db_hash.lower()] = db_title
    
    # 1. Parse all items into groups by base_prefix
    groups = {}
    for it in json_data.get("data", []):
        hash_id: Optional[str] = None
        subject: Optional[str] = None
        filename_no_ext: Optional[str] = None
        ext: Optional[str] = None
        size: Any = 0
        poster: Optional[str] = None
        posted_raw: Any = None
        sig: Optional[str] = None
        display_fn: Optional[str] = None
        extension_field: Optional[str] = None
        duration_raw: Any = None
        fullres: Optional[str] = None

        if isinstance(it, list):
            if len(it) >= 12:
                hash_id = it[0]
                subject = it[6]
                filename_no_ext = it[10]
                ext = it[11]
            if len(it) > 7:
                poster = it[7]
            if len(it) > 8:
                posted_raw = it[8]
            if len(it) > 14:
                duration_raw = it[14]
        elif isinstance(it, dict):
            hash_id = it.get("hash") or it.get("0") or it.get("id")
            subject = it.get("subject") or it.get("6")
            filename_no_ext = it.get("filename") or it.get("10")
            ext = it.get("ext") or it.get("11")
            size = it.get("size", 0)
            poster = it.get("poster") or it.get("7")
            posted_raw = it.get("timestamp") or it.get("ts") or it.get("dtime") or it.get("date") or it.get("12")
            sig = it.get("sig")
            display_fn = it.get("fn") or it.get("filename")
            extension_field = it.get("extension") or it.get("ext")
            duration_raw = it.get("14") or it.get("duration") or it.get("len")
            fullres = it.get("fullres") or it.get("resolution")

        filename_no_ext = filename_no_ext or display_fn or ""
        ext = ext or extension_field or ""

        if not hash_id or not ext:
            continue

        if not isinstance(size, int):
            try:
                size = int(size)
            except Exception:
                size = 0

        fullname = f"{filename_no_ext}{ext}" if ext else filename_no_ext
        base = get_release_prefix(fullname)

        parsed = {
            "hash": hash_id,
            "filename": filename_no_ext,
            "ext": ext,
            "size": size,
            "poster": poster,
            "posted": posted_raw,
            "sig": sig,
            "display_fn": display_fn,
            "duration_raw": duration_raw,
            "fullres": fullres,
            "subject": subject,
            "raw_item": it,
        }

        poster_str = (poster or "").strip().lower()
        group_key = (base.lower(), poster_str)
        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(parsed)

    out: List[dict] = []
    
    # 2. Process each group
    for (base_lower, poster_lower), items in groups.items():
        first_it = items[0]
        base_prefix = get_release_prefix(f"{first_it['filename']}{first_it['ext']}")
        # Check if the group contains a raw video file
        video_items = [it for it in items if it["ext"].lower() in _ALLOWED_VIDEO_EXTENSIONS]
        
        if video_items:
            # Group contains raw video file(s) - process them individually
            for it in video_items:
                size = it["size"]
                if size < min_bytes:
                    continue
                
                duration_seconds = _parse_duration_seconds(it["duration_raw"])
                if _is_flagged_item(it["raw_item"], it["ext"], duration_seconds):
                    continue

                title: Optional[str] = None
                base_lower = base_prefix.lower()
                if base_lower in replacements:
                    title = replacements[base_lower]
                else:
                    subj_lower = (it["subject"] or "").lower()
                    fn_lower_check = (it["filename"] or "").lower()
                    matched_replacement = False
                    for s_key, r_title in replacements.items():
                        if s_key in subj_lower or s_key in fn_lower_check:
                            title = r_title
                            matched_replacement = True
                            break
                    if not matched_replacement:
                        display_fn = it["display_fn"]
                        if display_fn:
                            cleaned = display_fn.strip()
                            if cleaned:
                                normalized = cleaned.replace(" - ", "-")
                                parts = [segment for segment in normalized.split(" ") if segment]
                                sanitized = ".".join(parts)
                                ext_component = it["ext"] or ""
                                if ext_component and not ext_component.startswith("."):
                                    ext_component = f".{ext_component}"
                                title = f"{sanitized}{ext_component}" if ext_component else sanitized

                        if not title:
                            fallback = it["subject"] or f"{it['filename']}{it['ext']}"
                            title = _normalize_title(fallback)

                # Filter out sample files unless 'sample' was explicitly searched for
                fn_lower = it["filename"].lower() if it["filename"] else ""
                subj_lower = (it["subject"] or "").lower()
                if "sample" in fn_lower or "sample" in subj_lower:
                    if "sample" not in token_set:
                        continue

                quality = _extract_quality(title, it["fullres"])
                title_meta = _extract_release_markers(title, quality)
                if not quality and title_meta.get("quality"):
                    quality = title_meta.get("quality")

                # Discard low-quality / unwanted release types (TRaSH Guides style)
                if DISCARD_LOW_QUALITY:
                    trash_match = _TRASH_REJECTION_RE.search(title)
                    if trash_match:
                        matched_word = trash_match.group(0).lower()
                        if not any(t in matched_word or matched_word in t for t in token_set):
                            continue
                if _CUSTOM_EXCLUDE_RE:
                    custom_match = _CUSTOM_EXCLUDE_RE.search(title)
                    if custom_match:
                        matched_word = custom_match.group(0).lower()
                        if not any(t in matched_word or matched_word in t for t in token_set):
                            continue

                if strict_match and not matches_title(title, strict_phrase, strict=True):
                    continue

                if query_meta:
                    q_year = query_meta.get("year")
                    q_season = query_meta.get("season")
                    q_episode = query_meta.get("episode")
                    q_quality = query_meta.get("quality")
                    t_year = title_meta.get("year")
                    t_season = title_meta.get("season")
                    t_episode = title_meta.get("episode")
                    t_quality = quality or title_meta.get("quality")
                    if q_year and t_year and q_year != t_year:
                        continue
                    if q_season and t_season and q_season != t_season:
                        continue
                    if q_episode and t_episode and q_episode != t_episode:
                        t_episodes = title_meta.get("episodes")
                        if t_episodes and q_episode in t_episodes:
                            pass
                        else:
                            continue
                    if q_quality and t_quality and q_quality.lower() != t_quality.lower():
                        continue

                if token_set:
                    title_tokens = set(_tokenize(title))
                    if not _is_token_subset(token_set, title_tokens):
                        continue

                duration_formatted = _format_duration(duration_seconds)
                thumbnail_url = _build_thumbnail_url(thumb_base, it["hash"], it["filename"])
                year = title_meta.get("year")
                category_id = _detect_category(title, title_meta)

                out.append(
                    {
                        "hash": it["hash"],
                        "filename": it["filename"],
                        "ext": it["ext"],
                        "sig": it["sig"],
                        "size": size,
                        "title": title,
                        "poster": it["poster"],
                        "posted": it["posted"],
                        "duration": duration_seconds,
                        "duration_hms": duration_formatted,
                        "quality": quality,
                        "thumbnail": thumbnail_url,
                        "year": year,
                        "season": title_meta.get("season"),
                        "episode": title_meta.get("episode"),
                        "category": category_id,
                        "is_archive": False,
                        "raw_item": it["raw_item"],
                    }
                )
        else:
            # Group does NOT contain raw video file(s) - treat as a combined archive release!
            # Check if this group actually contains archive files (RAR, PAR2, NFO, SFV, NZB, etc.)
            has_archive_files = any(
                _ARCHIVE_SUFFIX_RE.search(f"{it['filename']}{it['ext']}") or it["ext"].lower() in {".rar", ".par2", ".sfv", ".nfo", ".nzb"}
                for it in items
            )
            if not has_archive_files:
                continue

            # Ensure the group contains actual data files (RARs, ZIPs, or NZBs) and is not just metadata
            if not _has_actual_data_files(items):
                continue

            # Ensure the archive is complete (no missing parts/volumes)
            if not _is_archive_complete(items):
                logger.warning(f"Discarding incomplete archive release group for prefix: {base_prefix}")
                continue


            total_size = sum(it["size"] for it in items)
            first_it = items[0]

            # Choose the best title between the base prefix and the subject title.
            # We prefer whichever one contains clean release markers and is not an obfuscated hash.
            def has_release_markers(t: str) -> bool:
                if not t:
                    return False
                markers = [
                    r'\bS\d+E\d+\b', # S01E01
                    r'\bS\d+\b',     # S01
                    r'\bE\d+\b',     # E01
                    r'\b19\d{2}\b',  # Year
                    r'\b20\d{2}\b',  # Year
                    r'\b(?:720p|1080p|2160p|4k|uhd|bluray|webrip|web-dl|h264|x264|h265|x265|hevc|dd5\.1|ddp5\.1|atmos)\b'
                ]
                pattern = '|'.join(markers)
                return bool(re.search(pattern, t, flags=re.IGNORECASE))

            title = None
            base_lower = base_prefix.lower()
            if base_lower in replacements:
                title = replacements[base_lower]
            else:
                subj_lower = (first_it.get("subject") or "").lower()
                for s_key, r_title in replacements.items():
                    if s_key in subj_lower:
                        title = r_title
                        break

            if not title:
                is_base_hash = bool(re.match(r'^[a-fA-F0-9]{32,40}$', base_prefix) or (len(base_prefix) >= 16 and '.' not in base_prefix and ' ' not in base_prefix and '-' not in base_prefix))
                base_has = has_release_markers(base_prefix)
                
                if base_has and not is_base_hash:
                    title = base_prefix
                else:
                    subject_title = clean_subject(first_it.get("subject"))
                    sub_has = has_release_markers(subject_title)
                    is_sub_hash = bool(re.match(r'^[a-fA-F0-9]{32,40}$', subject_title) or (len(subject_title) >= 16 and '.' not in subject_title and ' ' not in subject_title and '-' not in subject_title))
                    
                    if sub_has and not is_sub_hash:
                        title = subject_title
                    else:
                        title = base_prefix

            if not title.lower().endswith((".mkv", ".mp4", ".avi", ".ts", ".mov")):
                title = f"{title}.mkv"

            # Filter out sample files unless 'sample' was explicitly searched for
            title_lower = title.lower()
            if "sample" in title_lower:
                if "sample" not in token_set:
                    continue

            quality = _extract_quality(title, first_it["fullres"])
            title_meta = _extract_release_markers(title, quality)
            if not quality and title_meta.get("quality"):
                quality = title_meta.get("quality")

            category_id = _detect_category(title, title_meta)

            # If the determined title is still an obfuscated hash, try to resolve it
            is_obfuscated_title = bool(re.match(r'^[a-fA-F0-9]{32,40}$', title.split(".")[0]) or (len(title.split(".")[0]) >= 16 and '.' not in title.split(".")[0] and ' ' not in title.split(".")[0] and '-' not in title.split(".")[0]))
            if is_obfuscated_title:
                cached_res = _DEOBFUSCATION_CACHE.get_by_hash(base_prefix)
                if cached_res:
                    cached_title, cached_cat = cached_res
                    title = cached_title
                    if not title.lower().endswith((".mkv", ".mp4", ".avi", ".ts", ".mov")):
                        title = f"{title}.mkv"
                    quality = _extract_quality(title, first_it["fullres"])
                    title_meta = _extract_release_markers(title, quality)
                    if not quality and title_meta.get("quality"):
                        quality = title_meta.get("quality")
                    category_id = cached_cat if cached_cat else _detect_category(title, title_meta)
                    is_obfuscated_title = False
                    logger.info(f"Resolved obfuscated release via hash cache: {base_prefix} -> {title}")
                else:
                    nfo_item = None
                    for it in items:
                        if it["ext"].lower() == ".nfo":
                            nfo_item = it
                            break
                    if nfo_item:
                        resolve_nfo_background(base_prefix, nfo_item, json_data, category_id)

            # If size is small (< 50MB) because we only matched metadata files,
            # set a realistic simulated size based on category so Sonarr/Radarr doesn't reject it
            if total_size < 50 * 1024 * 1024:
                if category_id == CATEGORY_MOVIES:
                    total_size = 4500 * 1024 * 1024 # 4.5 GB
                else:
                    total_size = 1500 * 1024 * 1024 # 1.5 GB

            if total_size < min_bytes:
                continue

            raw_it = first_it["raw_item"]
            passwd = False
            virus = False
            if isinstance(raw_it, dict):
                passwd = bool(raw_it.get("passwd") or raw_it.get("password"))
                virus = bool(raw_it.get("virus"))
            # Discard archive releases if password-protected (unless allow_password_archives is enabled)
            if (passwd and not (allow_password_archives and ALLOW_PASSWORDED)) or virus:
                continue

            # Discard low-quality / unwanted release types (TRaSH Guides style)
            if DISCARD_LOW_QUALITY:
                trash_match = _TRASH_REJECTION_RE.search(title)
                if trash_match:
                    matched_word = trash_match.group(0).lower()
                    if not any(t in matched_word or matched_word in t for t in token_set):
                        continue
            if _CUSTOM_EXCLUDE_RE:
                custom_match = _CUSTOM_EXCLUDE_RE.search(title)
                if custom_match:
                    matched_word = custom_match.group(0).lower()
                    if not any(t in matched_word or matched_word in t for t in token_set):
                        continue

            if strict_match and not matches_title(title, strict_phrase, strict=True):
                continue

            if query_meta:
                q_year = query_meta.get("year")
                q_season = query_meta.get("season")
                q_episode = query_meta.get("episode")
                q_quality = query_meta.get("quality")
                t_year = title_meta.get("year")
                t_season = title_meta.get("season")
                t_episode = title_meta.get("episode")
                t_quality = quality or title_meta.get("quality")
                if q_year and t_year and q_year != t_year:
                    continue
                if q_season and t_season and q_season != t_season:
                    continue
                if q_episode and t_episode and q_episode != t_episode:
                    t_episodes = title_meta.get("episodes")
                    if t_episodes and q_episode in t_episodes:
                        pass
                    else:
                        continue
                if q_quality and t_quality and q_quality.lower() != t_quality.lower():
                    continue

            if token_set:
                title_tokens = set(_tokenize(title))
                if not _is_token_subset(token_set, title_tokens):
                    continue

            thumbnail_url = _build_thumbnail_url(thumb_base, first_it["hash"], first_it["filename"])
            year = title_meta.get("year")
            category_id = _detect_category(title, title_meta)

            out.append(
                {
                    "hash": first_it["hash"],
                    "filename": first_it["filename"],
                    "ext": first_it["ext"],
                    "sig": first_it["sig"],
                    "size": total_size,
                    "title": title,
                    "poster": first_it["poster"],
                    "posted": first_it["posted"],
                    "duration": None,
                    "duration_hms": None,
                    "quality": quality,
                    "thumbnail": thumbnail_url,
                    "year": year,
                    "season": title_meta.get("season"),
                    "episode": title_meta.get("episode"),
                    "category": category_id,
                    "is_archive": True,
                    "archive_prefix": base_prefix,
                    "raw_item": first_it["raw_item"],
                }
            )

    return out

def _category_matches(detected_cat: int, requested_cats: Set[int]) -> bool:
    if not requested_cats:
        return True
    if detected_cat in requested_cats:
        return True
    # Check parent categories (Movies: 2000-2999, TV: 5000-5999)
    for req in requested_cats:
        if req == 2000 and 2000 <= detected_cat < 3000:
            return True
        if req == 5000 and 5000 <= detected_cat < 6000:
            return True
    return False

def map_query_with_custom_titles(q: str, custom_titles: dict) -> List[str]:
    parsed = parse_torrent_title(q)
    clean_title = parsed.get("title", "")
    if not clean_title:
        return [q]
        
    matched_key = None
    for key in custom_titles.keys():
        if key.lower() == clean_title.lower():
            matched_key = key
            break
            
    if not matched_key:
        return [q]
        
    mapped_values = custom_titles[matched_key]
    if not isinstance(mapped_values, list):
        mapped_values = [mapped_values]
        
    queries = []
    # Reconstruct query using the mapped values
    idx = q.lower().find(clean_title.lower())
    suffix = q[idx + len(clean_title):] if idx != -1 else ""
    
    for val in mapped_values:
        if isinstance(val, dict):
            # If it's a dict, use the search term directly without suffix (obfuscated mapping)
            search_term = val.get("search", "")
            if search_term and search_term not in queries:
                queries.append(search_term)
        else:
            queries.append(f"{val}{suffix}")
        
    if q not in queries:
        queries.append(q)
        
    return queries
@APP.route("/health")
def health():
    try:
        c = client()
        return Response("OK", status=200, mimetype="text/plain")
    except Exception as e:
        return Response(f"Unhealthy: {e}", status=500, mimetype="text/plain")


@APP.route("/")
@APP.route("/api")
@APP.route("/api/")
@APP.route("/api/api")
@APP.route("/api/api/")
@APP.route("/api/v2")
@APP.route("/api/v2/")
@APP.route("/v2")
@APP.route("/v2/")
@APP.route("/v2/api")
@APP.route("/v2/api/")
def api():
    load_custom_titles()


    if not require_apikey():
        return Response("Unauthorized", status=401)

    t = request.args.get("t", "caps")

    allow_password_archives = False
    if (
        request.args.get("allow_password_archives") in ("1", "true", "yes")
        or request.args.get("allow_passwd_archives") in ("1", "true", "yes")
        or request.args.get("v") == "2"
        or request.path.startswith(("/api/v2", "/v2/api"))
    ):
        allow_password_archives = True

    if t == "caps":

        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            "<caps>"
            '<server version="0.1" title="Easynews Bridge"/>'
            '<limits max="100" default="100"/>'
            '<registration available="no" open="no"/>'
            "<searching>"
            '<search available="yes" supportedParams="q"/>'
            '<movie-search available="yes" supportedParams="q,year"/>'
            '<tv-search available="yes" supportedParams="q,season,ep"/>'
            "</searching>"
            "<categories>"
            '<category id="2000" name="Movies">'
            '<subcat id="2030" name="Movies/HD"/>'
            '<subcat id="2040" name="Movies/UHD"/>'
            "</category>"
            '<category id="5000" name="TV">'
            '<subcat id="5030" name="TV/HD"/>'
            '<subcat id="5040" name="TV/UHD"/>'
            '<subcat id="5070" name="TV/Anime"/>'
            "</category>"
            '<category id="7000" name="Other"/>'
            "</categories>"
            "</caps>"
        )
        return Response(xml, mimetype="application/xml")

    if t in ("search", "movie", "tvsearch"):
        base_query = (request.args.get("q") or "").strip()
        cat_param = request.args.get("cat") or ""
        
        # Parse IMDb ID parameter
        imdb_param = request.args.get("imdbid") or request.args.get("imdb")
        if imdb_param:
            imdb_param = imdb_param.strip()
            if not imdb_param.startswith("tt"):
                imdb_param = f"tt{imdb_param}"
            
            # Resolve movie/series title from Cinemeta if query is empty
            if not base_query:
                is_movie_search = (t == "movie") or ("2000" in cat_param)
                resolved_title = resolve_imdb_title(imdb_param, is_movie=is_movie_search)
                if resolved_title:
                    base_query = resolved_title
                    logger.info(f"Resolved query title '{base_query}' from IMDb parameter '{imdb_param}'")

        season_param = request.args.get("season") or request.args.get("seasonnum")
        episode_param = (
            request.args.get("ep")
            or request.args.get("epnum")
            or request.args.get("episode")
        )
        year_param = request.args.get("year") or request.args.get("yr")
        season_int = _as_int(season_param)
        episode_int = _as_int(episode_param)
        year_int = _as_int(year_param)

        search_components: List[str] = []
        if base_query:
            search_components.append(base_query)

        if t == "movie":
            if year_int and str(year_int) not in base_query:
                search_components.append(str(year_int))
        elif t == "tvsearch":
            if season_int is not None and episode_int is not None:
                search_components.append(f"S{season_int:02}E{episode_int:02}")
            elif season_int is not None:
                search_components.append(f"S{season_int:02}")
            if year_int and str(year_int) not in base_query:
                search_components.append(str(year_int))

        search_label = " ".join(part for part in search_components if part).strip()
        raw_query = search_label or base_query
        q = raw_query.strip()
        fallback_query = False
        if (
            not q or q.lower() == "test"
        ):  # allow Prowlarr validation calls to receive data
            # Check if TV/Anime categories are requested
            tv_categories = {"5000", "5030", "5040"}
            anime_categories = {"5070"}
            requested_categories = set(cat_param.split(",")) if cat_param else set()
            wants_tv = t == "tvsearch" or bool(requested_categories & tv_categories)
            wants_anime = bool(requested_categories & anime_categories)
            # Use appropriate fallback query
            if wants_anime:
                q = "one piece"  # Anime fallback
            elif wants_tv:
                q = "breaking bad"  # TV fallback
            else:
                q = "matrix"  # Movie fallback
            fallback_query = True
        query_tokens = _tokenize(raw_query)
        query_meta = _extract_release_markers(raw_query)
        if year_int:
            query_meta["year"] = year_int
        if season_int is not None:
            query_meta["season"] = season_int
        if episode_int is not None:
            query_meta["episode"] = episode_int
        if imdb_param:
            query_meta["imdb_id"] = imdb_param
        strict_param = request.args.get("strict")
        strict_requested = t in {"movie", "tvsearch"}
        if strict_param is not None:
            strict_requested = strict_param.strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        strict_phrase = raw_query
        limit = int(request.args.get("limit", "100"))
        offset = int(request.args.get("offset", "0"))
        min_size_param = request.args.get("minsize")
        min_size_mb = 100
        if min_size_param:
            try:
                min_size_mb = max(100, int(min_size_param))
            except ValueError:
                min_size_mb = 100
        min_bytes = min_size_mb * 1024 * 1024
        requested_cats = set()
        if cat_param:
            for c_str in cat_param.split(","):
                try:
                    requested_cats.add(int(c_str.strip()))
                except ValueError:
                    pass

        logger.info(f"API Search - q: {q}, t: {t}, cat: {cat_param}, requested_cats: {requested_cats}")

        if fallback_query:
            # Check if TV/Anime categories are requested
            tv_categories = {5000, 5030, 5040}
            anime_categories = {5070}
            wants_tv = t == "tvsearch" or bool(requested_cats & tv_categories)
            wants_anime = bool(requested_cats & anime_categories)

            if wants_anime:
                # Anime-appropriate fallback
                items = [
                    {
                        "hash": "SAMPLEHASH_ANIME123",
                        "filename": "sample.anime.series.01.720p.mkv",
                        "ext": ".mkv",
                        "sig": None,
                        "size": 350 * 1024 * 1024,
                        "title": "[SampleSubs] Sample Anime Series - 01 [720p]",
                        "sample": True,
                        "poster": "sample@example.com",
                        "posted": int(time.time()),
                        "category": CATEGORY_ANIME,
                    }
                ]
            elif wants_tv:
                # TV-appropriate fallback for Sonarr
                items = [
                    {
                        "hash": "SAMPLEHASH_TV123456",
                        "filename": "sample.tv.show.s01e01.1080p.mkv",
                        "ext": ".mkv",
                        "sig": None,
                        "size": 800 * 1024 * 1024,
                        "title": "Sample TV Show S01E01 1080p",
                        "sample": True,
                        "poster": "sample@example.com",
                        "posted": int(time.time()),
                        "category": CATEGORY_TV_HD,
                    }
                ]
            else:
                # Movie fallback for Radarr
                items = [
                    {
                        "hash": "SAMPLEHASH1234567890",
                        "filename": "sample.matrix.clip",
                        "ext": ".mkv",
                        "sig": None,
                        "size": 700 * 1024 * 1024,
                        "title": "Sample Matrix Clip",
                        "sample": True,
                        "poster": "sample@example.com",
                        "posted": int(time.time()),
                        "category": CATEGORY_MOVIES,
                    }
                ]
        else:
            # Apply per-title query rewrite rules (e.g. "norsemen => Vikingane")
            if QUERY_REPLACE:
                rewritten = _apply_query_replace(q, QUERY_REPLACE)
                if rewritten != q:
                    logger.info("Query rewritten: %r → %r", q, rewritten)
                    q = rewritten

            # Apply Norwegian/accent transliteration if enabled
            if TRANSLITERATE_NORWEGIAN:
                q = _transliterate_norwegian(q)

            # Custom title expansion
            queries_to_run = map_query_with_custom_titles(q, _CUSTOM_TITLES)
            
            # Query Deobfuscation Cache by title
            db_results = _DEOBFUSCATION_CACHE.get_by_title(q)
            for db_hash, db_title, db_cat in db_results:
                if db_hash not in queries_to_run:
                    queries_to_run.append(db_hash)
                    
            # Add any hashes found by IMDb query
            if imdb_param:
                for db_hash, db_title, db_cat in _DEOBFUSCATION_CACHE.get_by_imdb(imdb_param):
                    if db_hash not in queries_to_run:
                        queries_to_run.append(db_hash)
            
            # Add EXTRA_TERMS fan-out queries (language boosters like 'nordic')
            if EXTRA_TERMS and not EXTRA_TERMS_FALLBACK_ONLY:
                for term in EXTRA_TERMS:
                    extra_q = f"{q} {term}"
                    if extra_q not in queries_to_run:
                        queries_to_run.append(extra_q)

            all_results_data = {"data": []}
            seen_hashes = set()
            # Run query expansions concurrently in parallel
            queries_to_fetch = []
            for search_q in queries_to_run:
                cached_data = _SEARCH_CACHE.get(search_q)
                if cached_data is not None:
                    if cached_data and "data" in cached_data:
                        if not all_results_data.get("thumbURL") and cached_data.get("thumbURL"):
                            all_results_data["thumbURL"] = cached_data.get("thumbURL")
                        if not all_results_data.get("thumbUrl") and cached_data.get("thumbUrl"):
                            all_results_data["thumbUrl"] = cached_data.get("thumbUrl")
                        for it in cached_data["data"]:
                            hash_id = None
                            if isinstance(it, list) and len(it) >= 12:
                                hash_id = it[0]
                            elif isinstance(it, dict):
                                hash_id = it.get("hash") or it.get("0") or it.get("id")
                            if hash_id and hash_id not in seen_hashes:
                                seen_hashes.add(hash_id)
                                all_results_data["data"].append(it)
                else:
                    queries_to_fetch.append(search_q)

            if queries_to_fetch:
                def fetch_query(q):
                    try:
                        c = client()
                        data = c.search(
                            query=q,
                            file_type=None,
                            per_page=1000,
                            sort_field="dtime",
                            sort_dir="-",
                        )
                        # Fetch additional pages if saturated to retrieve older/other seasons/episodes
                        returned = data.get("returned", 0) if data else 0
                        if data and returned >= 950:
                            max_pages = int(os.environ.get("EASYNEWS_MAX_PAGES", "5"))
                            current_page = 1
                            last_page_returned = returned
                            while last_page_returned >= 950 and current_page < max_pages:
                                current_page += 1
                                try:
                                    logger.info(f"Query '{q}' returned {last_page_returned} results (saturated). Fetching page {current_page}...")
                                    page_data = c.search(
                                        query=q,
                                        file_type=None,
                                        page=current_page,
                                        per_page=1000,
                                        sort_field="dtime",
                                        sort_dir="-",
                                    )
                                    if page_data and "data" in page_data:
                                        page_len = len(page_data["data"])
                                        data["data"] = data.get("data", []) + page_data["data"]
                                        data["returned"] = data.get("returned", 0) + page_len
                                        last_page_returned = page_len
                                    else:
                                        break
                                except Exception as e2:
                                    logger.error(f"Failed to fetch page {current_page} for query '{q}': {e2}")
                                    break
                        _SEARCH_CACHE.set(q, data)
                        return q, data
                    except Exception as e:
                        logger.error(f"Search failed for query '{q}': {e}")
                        if isinstance(e, EasynewsError) and ("Unauthorized" in str(e) or "login" in str(e).lower() or "redirect" in str(e).lower()):
                            invalidate_client()
                        return q, None

                with ThreadPoolExecutor(max_workers=min(5, len(queries_to_fetch))) as executor:
                    futures = {executor.submit(fetch_query, q): q for q in queries_to_fetch}
                    for future in as_completed(futures):
                        q, cached_data = future.result()
                        if cached_data and "data" in cached_data:
                            if not all_results_data.get("thumbURL") and cached_data.get("thumbURL"):
                                all_results_data["thumbURL"] = cached_data.get("thumbURL")
                            if not all_results_data.get("thumbUrl") and cached_data.get("thumbUrl"):
                                all_results_data["thumbUrl"] = cached_data.get("thumbUrl")
                            for it in cached_data["data"]:
                                hash_id = None
                                if isinstance(it, list) and len(it) >= 12:
                                    hash_id = it[0]
                                elif isinstance(it, dict):
                                    hash_id = it.get("hash") or it.get("0") or it.get("id")
                                if hash_id and hash_id not in seen_hashes:
                                    seen_hashes.add(hash_id)
                                    all_results_data["data"].append(it)
                            
            items = filter_and_map(
                all_results_data,
                min_bytes=min_bytes,
                query_tokens=query_tokens,
                query_meta=query_meta,
                strict_phrase=strict_phrase,
                strict_match=strict_requested,
                custom_titles=_CUSTOM_TITLES,
                allow_password_archives=allow_password_archives,
            )

            # 0-result fallback: try spelling variants + alias queries
            if not items and FALLBACK_SEARCH and not fallback_query:
                fallback_queries: List[str] = []
                if FALLBACK_TRANSLITERATE:
                    fallback_queries.extend(_spelling_variants(base_query))
                if FALLBACK_ALT_TITLES:
                    custom_titles_simple = {
                        k: ([v if isinstance(v, str) else v.get("search", "") for v in vals] if isinstance(vals, list) else [str(vals)])
                        for k, vals in _CUSTOM_TITLES.items()
                    }
                    for k, aliases in custom_titles_simple.items():
                        kl = k.lower()
                        ql = base_query.lower()
                        if kl == ql or kl in ql or ql in kl:
                            for a in aliases:
                                if a and a.lower() != ql and a not in fallback_queries:
                                    fallback_queries.append(a)
                if EXTRA_TERMS and EXTRA_TERMS_FALLBACK_ONLY:
                    for term in EXTRA_TERMS:
                        extra_q = f"{base_query} {term}"
                        if extra_q not in fallback_queries:
                            fallback_queries.append(extra_q)
                if fallback_queries:
                    logger.info("Primary search returned 0 results. Trying %d fallback queries: %s", len(fallback_queries), fallback_queries[:4])
                    fallback_data: dict = {"data": []}
                    fallback_seen: Set[str] = set()
                    def _do_fallback(fq: str):
                        try:
                            c = client()
                            d = c.search(query=fq, file_type=None, per_page=100, sort_field="dtime", sort_dir="-")
                            return fq, d
                        except Exception as e:
                            logger.debug("Fallback query '%s' failed: %s", fq, e)
                            return fq, None
                    with ThreadPoolExecutor(max_workers=min(4, len(fallback_queries))) as ex:
                        for _fq, _fd in ex.map(lambda fq: _do_fallback(fq), fallback_queries):
                            if _fd and "data" in _fd:
                                if not fallback_data.get("thumbURL") and _fd.get("thumbURL"):
                                    fallback_data["thumbURL"] = _fd["thumbURL"]
                                for it in _fd["data"]:
                                    hid = it[0] if isinstance(it, list) and len(it) >= 1 else (it.get("hash") or it.get("0") if isinstance(it, dict) else None)
                                    if hid and hid not in fallback_seen:
                                        fallback_seen.add(hid)
                                        fallback_data["data"].append(it)
                    if fallback_data["data"]:
                        items = filter_and_map(
                            fallback_data,
                            min_bytes=min_bytes,
                            query_tokens=query_tokens,
                            query_meta=query_meta,
                            strict_phrase=strict_phrase,
                            strict_match=strict_requested,
                            custom_titles=_CUSTOM_TITLES,
                            allow_password_archives=allow_password_archives,
                        )

            # Apply REQUIRE_SUBS filter (global default, overridable per-request)
            require_subs_param = request.args.get("subs", "").strip().lower()
            require_subs = [
                v.strip().lower() for v in require_subs_param.split(",") if v.strip()
            ] if require_subs_param else REQUIRE_SUBS_DEFAULT
            if require_subs:
                want_langs = _canon_langs(require_subs)
                def _has_subs(it: dict) -> bool:
                    raw = it.get("raw_item") if isinstance(it, dict) else None
                    if not isinstance(raw, dict):
                        return True  # can't check → let it through
                    slangs = raw.get("slangs") or raw.get("subtitle_tracks") or []
                    if isinstance(slangs, str):
                        slangs = [s.strip() for s in slangs.split(",") if s.strip()]
                    return bool(_canon_langs(slangs) & want_langs)
                items = [it for it in items if _has_subs(it)]


        # Filter by requested category
        if requested_cats:
            items = [it for it in items if _category_matches(it.get("category", CATEGORY_MOVIES), requested_cats)]

        # Sort items by size descending (largest first)
        items = sorted(items, key=lambda x: x.get("size", 0), reverse=True)

        # Trim by limit (handles fallback and real queries)
        items = items[offset : offset + limit]

        display_q = raw_query if raw_query else q
        chan_title = f"Results for {display_q}"
        now_dt = datetime.now(timezone.utc)
        channel_pub = now_dt.strftime("%a, %d %b %Y %H:%M:%S %z")

        header = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">'
            "<channel>"
            f"<title>{xml_escape(chan_title)}</title>"
            f"<description>{xml_escape(chan_title)}</description>"
            f"<link>{request.url_root.rstrip('/')}/api</link>"
            f"<pubDate>{channel_pub}</pubDate>"
        )

        body_parts: List[str] = []
        for it in items:
            enc_id = encode_id(it)
            title = xml_escape(it["title"]) if it["title"] else "Untitled"
            link = f"{request.url_root.rstrip('/')}/api?t=get&id={enc_id}&apikey={request.args.get('apikey')}"
            safe_link = xml_escape(link)
            size = it["size"]
            guid = enc_id
            poster = it.get("poster")
            posted_dt = _coerce_datetime(it.get("posted")) or now_dt
            posted_str = posted_dt.strftime("%a, %d %b %Y %H:%M:%S %z")
            posted_epoch = str(int(posted_dt.timestamp()))
            duration_hms = it.get("duration_hms")
            quality = it.get("quality")
            thumb = it.get("thumbnail")
            year = it.get("year")
            season = it.get("season")
            episode = it.get("episode")

            category_id = it.get("category", CATEGORY_MOVIES)

            attr_parts = [
                f'<newznab:attr name="size" value="{size}"/>',
                f'<newznab:attr name="category" value="{category_id}"/>',
                f'<newznab:attr name="usenetdate" value="{posted_str}"/>',
                f'<newznab:attr name="posted" value="{posted_epoch}"/>',
            ]
            if poster:
                attr_parts.append(
                    f'<newznab:attr name="poster" value="{xml_escape(poster)}"/>'
                )
            if quality:
                attr_parts.append(
                    f'<newznab:attr name="quality" value="{xml_escape(quality)}"/>'
                )
            if duration_hms:
                attr_parts.append(
                    f'<newznab:attr name="duration" value="{duration_hms}"/>'
                )
            if thumb:
                attr_parts.append(
                    f'<newznab:attr name="thumb" value="{xml_escape(thumb)}"/>'
                )
            if year:
                attr_parts.append(f'<newznab:attr name="year" value="{year}"/>')
            if season:
                attr_parts.append(f'<newznab:attr name="season" value="{season}"/>')
            if episode:
                attr_parts.append(f'<newznab:attr name="episode" value="{episode}"/>')

            # ── Rich metadata attrs from Easynews raw item ──────────────────
            raw_it = it.get("raw_item") if isinstance(it, dict) else None
            if isinstance(raw_it, dict):
                if META_SUBS:
                    slangs = raw_it.get("slangs") or raw_it.get("subtitle_tracks") or []
                    subs_str = _join_langs(slangs)
                    if subs_str:
                        attr_parts.append(f'<newznab:attr name="subs" value="{xml_escape(subs_str)}"/>')
                if META_AUDIO:
                    alangs = raw_it.get("alangs") or raw_it.get("audio_tracks") or []
                    audio_str = _join_langs(alangs)
                    if audio_str:
                        attr_parts.append(f'<newznab:attr name="language" value="{xml_escape(audio_str)}"/>')
                if META_CODECS:
                    vcodec = raw_it.get("vcodec") or raw_it.get("video_codec")
                    acodec = raw_it.get("acodec") or raw_it.get("audio_codec")
                    if vcodec:
                        attr_parts.append(f'<newznab:attr name="videocodec" value="{xml_escape(str(vcodec))}"/>')
                    if acodec:
                        attr_parts.append(f'<newznab:attr name="audiocodec" value="{xml_escape(str(acodec))}"/>')
                if META_BITRATE:
                    bps_str = _format_bitrate_mbps(raw_it.get("bps"))
                    if bps_str:
                        attr_parts.append(f'<newznab:attr name="bitrate" value="{xml_escape(bps_str)}"/>')
                if META_GROUP:
                    grp = raw_it.get("group") or raw_it.get("newsgroup")
                    if grp:
                        attr_parts.append(f'<newznab:attr name="group" value="{xml_escape(str(grp))}"/>')
                if META_PASSWORD:
                    is_passwd = bool(raw_it.get("passwd") or raw_it.get("password"))
                    attr_parts.append(f'<newznab:attr name="password" value="{"1" if is_passwd else "0"}"/>')

            attr_xml = "".join(attr_parts)
            item_xml = (
                f"<item>"
                f"<title>{title}</title>"
                f'<guid isPermaLink="false">{guid}</guid>'
                f"<link>{safe_link}</link>"
                f"<category>{category_id}</category>"
                f"<pubDate>{posted_str}</pubDate>"
                f"{attr_xml}"
                f'<enclosure url="{safe_link}" length="{size}" type="application/x-nzb"/>'
                f"</item>"
            )
            body_parts.append(item_xml)

        footer = "</channel></rss>"
        xml = header + "".join(body_parts) + footer
        return Response(xml, mimetype="application/rss+xml")

    if t in ("get", "getnzb"):
        enc_id = request.args.get("id")
        if not enc_id:
            return Response("Missing id", status=400)
        d = decode_id(enc_id)
        cached_nzb = _NZB_CACHE.get(enc_id)
        if cached_nzb is not None:
            logger.info("Serving NZB from in-memory cache")
            content, filename = cached_nzb
            resp = Response(content, mimetype="application/x-nzb")
            resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
            return resp
        if d.get("sample"):
            title = d.get("title", "Sample Item")
            safe_title = "sample"
            nzb_content = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">'
                '<file subject="Sample Matrix Clip" date="0" poster="sample@example.com">'
                "<groups><group>alt.binaries.sample</group></groups>"
                '<segments><segment bytes="1024" number="1">sample</segment></segments>'
                "</file></nzb>"
            ).encode("utf-8")
            resp = Response(nzb_content, mimetype="application/x-nzb")
            resp.headers["Content-Disposition"] = (
                f'attachment; filename="{safe_title}.nzb"'
            )
            return resp
        if d.get("is_archive"):
            # Grouped archive release - fetch all parts belonging to the prefix
            try:
                c = client()
                prefix = d.get("archive_prefix")
                res = c.search(
                    query=prefix,
                    file_type=None,
                    per_page=1000,
                )
                parts_data = res.get("data", [])
                
                # Filter files to only include the ones belonging to the exact release.
                # If poster is available, filter strictly by poster. Otherwise, fall back
                # to case-sensitive prefix matching.
                req_fn = d.get("filename") or ""
                req_ex = d.get("ext") or ""
                expected_prefix = get_release_prefix(f"{req_fn}{req_ex}")
                req_poster = d.get("poster")
                req_poster_lower = req_poster.strip().lower() if req_poster else None
                
                search_items = []
                for it in parts_data:
                    h, fn, ex, sg, poster = None, None, None, None, None
                    if isinstance(it, list) and len(it) >= 12:
                        h = it[0]
                        poster = it[7] if len(it) > 7 else None
                        fn = it[10]
                        ex = it[11]
                        sg = it[19] if len(it) > 19 else None
                    elif isinstance(it, dict):
                        h = it.get("0") or it.get("hash")
                        poster = it.get("poster")
                        fn = it.get("10") or it.get("filename")
                        ex = it.get("11") or it.get("ext")
                        sg = it.get("sig")
                    
                    if h and fn and ex:
                        if req_poster_lower:
                            it_poster_lower = poster.strip().lower() if poster else ""
                            if it_poster_lower != req_poster_lower:
                                continue
                        else:
                            it_prefix = get_release_prefix(f"{fn}{ex}")
                            if it_prefix != expected_prefix:
                                continue
                        
                        search_items.append(SearchItem(id=None, hash=h, filename=fn, ext=ex, sig=sg, type="DOCUMENT", raw={}))
                
                if not search_items:
                    si = to_search_item(d)
                    search_items = [si]
                    
                # Look for a pre-existing uploaded .nzb file in the search results matching this release prefix and poster.
                # If one exists, download it directly and serve it.
                nzb_item = None
                for it in search_items:
                    if it.ext.lower() == ".nzb":
                        nzb_item = it
                        break
                
                if nzb_item:
                    dl_farm = res.get("dlFarm")
                    dl_port = res.get("dlPort")
                    down_url = res.get("downURL") or "https://members.easynews.com/dl"
                    file_path = f"{nzb_item.hash}{nzb_item.ext}/{nzb_item.filename}{nzb_item.ext}"
                    url = f"{down_url}/{dl_farm}/{dl_port}/{file_path}"
                    
                    logger.info(f"Downloading pre-existing NZB file directly from Easynews: {url}")
                    r_dl = c.s.get(url, timeout=30)
                    r_dl.raise_for_status()
                    
                    # Normalize empty NZB date fields if necessary
                    content = r_dl.content.replace(b'date=""', b'date="0"')
                    title = d.get("title") or (nzb_item.filename + nzb_item.ext)
                    safe_title = "".join(ch for ch in title if ch.isalnum() or ch in (" ", "-", "_", "."))[:200].strip() or "download"
                    
                    resp = Response(content, mimetype="application/x-nzb")
                    resp.headers["Content-Disposition"] = f'attachment; filename="{safe_title}.nzb"'
                    _NZB_CACHE.set(enc_id, (content, f"{safe_title}.nzb"))
                    return resp
                    
                payload = c.build_nzb_payload(search_items, name=d.get("title"))
                url = "https://members.easynews.com/2.0/api/dl-nzb"
                r = c.s.post(url, data=payload, timeout=60)
                r.raise_for_status()
            except EasynewsError as e:
                logger.exception("EasynewsError during archive NZB generation")
                if "Unauthorized" in str(e) or "login" in str(e).lower() or "redirect" in str(e).lower():
                    invalidate_client()
                return Response(f"Upstream error: {e}", status=502)
            except requests.exceptions.RequestException as e:
                logger.exception("Network error during archive NZB generation")
                invalidate_client()
                return Response(f"Upstream network error: {e}", status=502)
        else:
            # Standard single video release
            si = to_search_item(d)
            try:
                c = client()
                payload = c.build_nzb_payload([si], name=d.get("title"))
                url = "https://members.easynews.com/2.0/api/dl-nzb"
                r = c.s.post(url, data=payload, timeout=60)
                r.raise_for_status()

                # Self-healing logic for expired search signatures
                is_empty_nzb = len(r.content) < 500 or b'<file' not in r.content
                if is_empty_nzb:
                    logger.warning("Upstream returned an empty NZB (likely due to expired signature). Attempting to fetch a fresh signature...")
                    search_term = d.get("filename") or ""
                    if search_term:
                        fresh_search = c.search(query=search_term, file_type="VIDEO")
                        fresh_items = fresh_search.get("data", [])
                        found_fresh = False
                        for fit in fresh_items:
                            fh = None
                            fsig = None
                            if isinstance(fit, list) and len(fit) >= 12:
                                fh = fit[0]
                                fsig = fit[19] if len(fit) > 19 else None
                            elif isinstance(fit, dict):
                                fh = fit.get("hash") or fit.get("0") or fit.get("id")
                                fsig = fit.get("sig") or fit.get("19")
                            
                            if fh and fh == d.get("hash") and fsig:
                                logger.info(f"Found match with fresh signature: {fsig}")
                                d["sig"] = fsig
                                fresh_si = to_search_item(d)
                                fresh_payload = c.build_nzb_payload([fresh_si], name=d.get("title"))
                                r_fresh = c.s.post(url, data=fresh_payload, timeout=60)
                                if r_fresh.status_code == 200 and len(r_fresh.content) >= 500 and b'<file' in r_fresh.content:
                                    r = r_fresh
                                    found_fresh = True
                                    logger.info("Successfully regenerated standard NZB using refreshed signature.")
                                    break
                        if not found_fresh:
                            logger.error("Could not locate the file with a valid fresh signature on Easynews.")
            except EasynewsError as e:
                logger.exception("EasynewsError during standard NZB generation")
                if "Unauthorized" in str(e) or "login" in str(e).lower() or "redirect" in str(e).lower():
                    invalidate_client()
                return Response(f"Upstream error: {e}", status=502)
            except requests.exceptions.RequestException as e:
                logger.exception("Network error during standard NZB generation")
                invalidate_client()
                return Response(f"Upstream network error: {e}", status=502)
        if r.status_code != 200:
            return Response(f"Upstream error {r.status_code}", status=502)
        # Name file as title.nzb
        title = d.get("title") or (d.get("filename", "download") + d.get("ext", ""))
        safe_title = (
            "".join(ch for ch in title if ch.isalnum() or ch in (" ", "-", "_", "."))[
                :200
            ].strip()
            or "download"
        )
        resp = Response(r.content, mimetype="application/x-nzb")
        resp.headers["Content-Disposition"] = f'attachment; filename="{safe_title}.nzb"'
        _NZB_CACHE.set(enc_id, (r.content, f"{safe_title}.nzb"))
        return resp

    return Response("Unsupported 't' parameter", status=400)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    APP.run(host="0.0.0.0", port=port)
