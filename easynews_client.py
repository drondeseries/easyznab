"""
Easynews API-like client (unofficial) to perform searches and download NZB files.

This client mimics the webapp behavior by calling:
- GET /2.0/search/solr-search/ (or /3.0/api/search, see EASYNEWS_SEARCH_API)
  for search results (JSON)
- POST /2.0/api/dl-nzb to create/download NZB for selected items (always 2.0)

Authentication is HTTP Basic Auth set on the requests session (self.s.auth) —
every request carries it. login() only primes and validates the session; a
stale/failed cookie refresh does not stop searches from working.
You'll need a valid Easynews account. Use responsibly and per Easynews TOS.
"""

from __future__ import annotations

import base64
import logging
import os
import queue
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, TypeVar

import requests
from requests.exceptions import ConnectionError, ReadTimeout, RequestException

# ──────────────────────────────────────────────────────────────────────────────
# Base URL + search endpoint are overridable via .env so you can A/B-test
# endpoints with no rebuild (just `docker compose ... up -d`):
#   EASYNEWS_BASE_URL          host (default https://members.easynews.com)
#   EASYNEWS_SEARCH_API        "2.0" (solr-search, proven/default) | "3.0" (newer JSON api)
#   EASYNEWS_SEARCH_URL_TEMPLATE  full override; wins over SEARCH_API. Supports
#                              {base} {query} {page} {per_page} placeholders.
#   EASYNEWS_RESULTS_KEY       top-level JSON key holding result rows (default "data")
#   EASYNEWS_LOG_LATENCY       "true" → log endpoint + per-request latency at INFO
# ──────────────────────────────────────────────────────────────────────────────
EASYNEWS_BASE = os.environ.get(
    "EASYNEWS_BASE_URL", "https://members.easynews.com"
).rstrip("/")
_SEARCH_API = os.environ.get("EASYNEWS_SEARCH_API", "2.0").strip()
_SEARCH_URL_TEMPLATE = os.environ.get("EASYNEWS_SEARCH_URL_TEMPLATE", "").strip()
_RESULTS_KEY = (os.environ.get("EASYNEWS_RESULTS_KEY", "data").strip() or "data")
_LOG_LATENCY = os.environ.get("EASYNEWS_LOG_LATENCY", "").strip().lower() in (
    "1", "true", "yes", "on",
)

# ──────────────────────────────────────────────────────────────────────────────
# Optional multi-page fetch. OFF by default so the latency budget is untouched.
#   EASYNEWS_PAGINATE   "true" → also fetch pages 2..N after the first
#   EASYNEWS_MAX_PAGES  how many pages total (default 1 = first page only)
# ──────────────────────────────────────────────────────────────────────────────
_PAGINATE = os.environ.get("EASYNEWS_PAGINATE", "").strip().lower() in (
    "1", "true", "yes", "on",
)
try:
    _MAX_PAGES = max(1, int(os.environ.get("EASYNEWS_MAX_PAGES", "1")))
except ValueError:
    _MAX_PAGES = 1


def paginate_enabled() -> bool:
    return _PAGINATE and _MAX_PAGES > 1


def max_pages() -> int:
    return _MAX_PAGES


# ──────────────────────────────────────────────────────────────────────────────
# Sorting (env-configurable, multi-level). Easynews ranks by up to three sort
# keys. With none of these set, only the primary key the caller passes is
# emitted. Set EASYNEWS_SORT_1 to override the primary field, and SORT_2/SORT_3
# to add tie-breakers.
#   field values: relevance | dsize (size) | dtime (date posted) | dsubject
#   direction:    "-" = descending (default) | "+" = ascending
# A good "biggest, then most-relevant, then newest" order is:
#   EASYNEWS_SORT_1=dsize  EASYNEWS_SORT_2=relevance  EASYNEWS_SORT_3=dtime
# ──────────────────────────────────────────────────────────────────────────────
_SORT_1 = os.environ.get("EASYNEWS_SORT_1", "").strip()
_SORT_1_DIR = os.environ.get("EASYNEWS_SORT_1_DIR", "").strip()
_SORT_2 = os.environ.get("EASYNEWS_SORT_2", "").strip()
_SORT_2_DIR = os.environ.get("EASYNEWS_SORT_2_DIR", "-").strip() or "-"
_SORT_3 = os.environ.get("EASYNEWS_SORT_3", "").strip()
_SORT_3_DIR = os.environ.get("EASYNEWS_SORT_3_DIR", "-").strip() or "-"

# ──────────────────────────────────────────────────────────────────────────────
# Advanced search (env-configurable). When ON, the 2.0 solr endpoint switches
# to its "/advanced" variant (st=adv), which supports server-side filtering:
#   spamf  – drop Easynews-flagged spam before it ever reaches our filters
#   fex    – a file-extension whitelist (only return these video containers)
# This trims junk upstream (fewer rows to map/dedup). PROVEN on the 2.0 endpoint.
# On 3.0 the same params are sent but Easynews may ignore them.
# ──────────────────────────────────────────────────────────────────────────────
_ADVANCED_SEARCH = os.environ.get("EASYNEWS_ADVANCED_SEARCH", "").strip().lower() in (
    "1", "true", "yes", "on",
)
# Spam filter (advanced only). Defaults ON when advanced search is enabled.
_SPAM_FILTER = os.environ.get(
    "EASYNEWS_SPAM_FILTER", "true" if _ADVANCED_SEARCH else "false"
).strip().lower() in ("1", "true", "yes", "on")
# File-extension whitelist (advanced only). Comma-separated, no leading dots.
# Empty = don't send fex (let Easynews return any video container).
_FILE_EXTENSIONS = os.environ.get(
    "EASYNEWS_FILE_EXTENSIONS",
    "m4v,3gp,mov,divx,xvid,wmv,avi,mpg,mpeg,mp4,mkv,avc,flv,webm",
).strip()


def _sort_params(sort_field: Optional[str], sort_dir: str) -> Dict[str, str]:
    """Build s1/s2/s3 sort params. Env vars override; with none set, the single
    caller-supplied sort_field/sort_dir becomes the primary (and only) key."""
    params: Dict[str, str] = {}
    s1 = _SORT_1 or (sort_field or "")
    s1d = _SORT_1_DIR or sort_dir or "-"
    if s1:
        params["s1"] = s1
        params["s1d"] = s1d
    if _SORT_2:
        params["s2"] = _SORT_2
        params["s2d"] = _SORT_2_DIR
    if _SORT_3:
        params["s3"] = _SORT_3
        params["s3d"] = _SORT_3_DIR
    return params


def _apply_advanced(params: Dict[str, str]) -> None:
    """Add the advanced-search params (st=adv + spam filter + extension
    whitelist) in place. No-op unless EASYNEWS_ADVANCED_SEARCH is on."""
    if not _ADVANCED_SEARCH:
        return
    params["st"] = "adv"
    params["gx"] = "1"
    params["sS"] = "3"
    if _SPAM_FILTER:
        params["spamf"] = "1"
    if _FILE_EXTENSIONS:
        params["fex"] = _FILE_EXTENSIONS


def _active_endpoint_label() -> str:
    if _SEARCH_URL_TEMPLATE:
        base = "custom-template"
    else:
        base = f"api {_SEARCH_API}"
    return f"{base}+adv" if _ADVANCED_SEARCH else base


def _normalize_response(payload: Any) -> Any:
    """Map a non-default results key onto ``data`` so the rest of the code,
    which always reads ``payload['data']``, works regardless of endpoint."""
    if (
        _RESULTS_KEY != "data"
        and isinstance(payload, dict)
        and _RESULTS_KEY in payload
        and "data" not in payload
    ):
        payload = dict(payload)
        payload["data"] = payload.get(_RESULTS_KEY) or []
    return payload


# ──────────────────────────────────────────────────────────────────────────────
# Timeouts
# ──────────────────────────────────────────────────────────────────────────────
_LOGIN_TIMEOUT = 15
_SEARCH_TIMEOUT = 30
_DOWNLOAD_TIMEOUT = 60

# ──────────────────────────────────────────────────────────────────────────────
# Latency-bounded search tuning (overridable via env). Defaults keep the whole
# response comfortably under NZBHydra's 4s indexer timeout while hedging away a
# slow/hung Easynews response so we don't hand back a spurious "0 results".
#   budget        – hard wall-clock cap for the whole search call
#   hedge_after   – if the in-flight request is slower than this, fire a fresh
#                   parallel one and take whichever returns real data first
#   attempt_timeout – read timeout for the single-shot fan-out requests (extra
#                   pages, extra terms, fallback); hedged attempts instead use
#                   the remaining budget as their read timeout
# ──────────────────────────────────────────────────────────────────────────────
_SEARCH_BUDGET = float(os.environ.get("SEARCH_BUDGET_SECONDS", "3.3"))
_SEARCH_HEDGE_AFTER = float(os.environ.get("SEARCH_HEDGE_AFTER_SECONDS", "1.2"))
_SEARCH_ATTEMPT_TIMEOUT = float(os.environ.get("SEARCH_ATTEMPT_TIMEOUT_SECONDS", "2.5"))

# ──────────────────────────────────────────────────────────────────────────────
# Keepalive (overridable via env). A background thread holds a warm TLS
# connection open during idle gaps so the next real search skips the cold
# handshake. Toggle the whole thing off with EASYNEWS_KEEPALIVE=false (e.g. to
# minimise idle account activity); a search will simply pay the handshake cost.
#   enabled  – master on/off switch (default on)
#   interval – how often the background thread wakes to maybe ping
#   idle     – only ping after this many seconds of no real search traffic
# ──────────────────────────────────────────────────────────────────────────────
_KEEPALIVE_ENABLED = os.environ.get("EASYNEWS_KEEPALIVE", "true").strip().lower() in {
    "1", "true", "yes", "on",
}
_KEEPALIVE_INTERVAL = float(os.environ.get("EASYNEWS_KEEPALIVE_INTERVAL_SECONDS", "45"))
_KEEPALIVE_IDLE = float(os.environ.get("EASYNEWS_KEEPALIVE_IDLE_SECONDS", "40"))

# Trust a successful HTTP-200-with-no-data as a genuine "0 results" and return
# immediately, instead of retrying it.
_TRUST_EMPTY = os.environ.get("SEARCH_TRUST_EMPTY", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Limit concurrent search requests to Easynews globally to prevent hitting rate limits
_SEARCH_SEMAPHORE = threading.Semaphore(4)


def _plain_error(e: Exception) -> str:
    """Turn a requests exception into a short, non-technical description."""
    if isinstance(e, ReadTimeout):
        return "Easynews did not respond in time (read timeout)"
    if isinstance(e, ConnectionError):
        msg = str(e)
        if "RemoteDisconnected" in msg or "Connection aborted" in msg:
            return "Easynews closed the connection without sending a response"
        if "Failed to establish" in msg or "Connection refused" in msg:
            return "Could not reach Easynews (connection refused or DNS failure)"
        return f"Network connection error: {msg[:120]}"
    return f"{type(e).__name__}: {str(e)[:120]}"


class EasynewsError(Exception):
    pass


@dataclass
class SearchItem:
    id: Optional[str]
    hash: str
    filename: str
    ext: str
    sig: Optional[str]
    type: str
    raw: Dict[str, Any]

    @property
    def value_token(self) -> str:
        """
        Build the value string Easynews expects for checkbox selections:
        format: "{hash}|{b64(filename)}:{b64(ext)}"
        As seen in members.js createNZB -> it reads from input[checkbox].value
        """
        fn_b64 = base64.b64encode(self.filename.encode()).decode().replace("=", "")
        ext_b64 = base64.b64encode(self.ext.encode()).decode().replace("=", "")
        return f"{self.hash}|{fn_b64}:{ext_b64}"


def _retry(
    fn: Callable[[], T],
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retryable_exceptions: tuple = (RequestException,),
) -> T:
    """Exponential backoff + jitter retry wrapper."""
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except retryable_exceptions as exc:  # type: ignore[misc]
            last_exc = exc
            if attempt < max_retries:
                delay = min(
                    base_delay * (2 ** attempt) + random.uniform(0, base_delay),
                    max_delay,
                )
                logger.debug("Retry %d/%d after %.1fs: %s", attempt + 1, max_retries, delay, _plain_error(exc))
                time.sleep(delay)
    raise last_exc  # type: ignore[misc]


class EasynewsClient:
    def __init__(
        self, username: str, password: str, session: Optional[requests.Session] = None
    ):
        self.username = username
        self.password = password
        self.s = session or requests.Session()
        # Increase pool size to support concurrent requests safely
        adapter = requests.adapters.HTTPAdapter(pool_connections=25, pool_maxsize=25)
        self.s.mount("https://", adapter)
        self.s.mount("http://", adapter)
        # Default headers
        self.s.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) EasynewsClient/1.0",
                "Accept": "application/json, text/javascript, */*; q=0.9",
            }
        )
        # Use HTTP Basic Auth for endpoints that support it
        self.s.auth = (self.username, self.password)
        self._last_search_ts: float = 0.0
        self._keepalive_thread: Optional[threading.Thread] = None

    def login(self) -> None:
        """
        Prime session and validate credentials using a quick authenticated call.
        This relies on HTTP Basic Auth configured on the session.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Step 1: Prime session with basic auth
                r1 = self.s.get(f"{EASYNEWS_BASE}/2.0/", timeout=_LOGIN_TIMEOUT)
                if r1.status_code in (401, 403):
                    raise EasynewsError("Unauthorized; check username/password")
                if r1.status_code != 200:
                    raise EasynewsError(f"Login failed: Easynews returned status code {r1.status_code}")

                # Step 2: Query a lightweight dummy solr search to initialize the Solr session cookies
                prime_url = f"{EASYNEWS_BASE}/2.0/search/solr-search/?fly=2&gps=primemysolrsession12345&sb=1&pno=1&pby=1&u=1&chxu=1&chxgx=1&st=basic&s1=dtime&s1d=-&sS=3&vv=1&fty%5B%5D=VIDEO"
                r2 = self.s.get(prime_url, timeout=_LOGIN_TIMEOUT)
                if r2.status_code != 200:
                    raise EasynewsError(f"Solr session priming failed: status code {r2.status_code}")

                logger.info("EasynewsClient login and Solr session priming succeeded (endpoint: %s).", _active_endpoint_label())
                return
            except (RequestException, EasynewsError) as e:
                logger.warning(f"Login attempt {attempt + 1}/{max_retries} failed: {_plain_error(e)}")
                if attempt == max_retries - 1:
                    raise EasynewsError(f"Network error during Easynews login: {_plain_error(e)}") from e
                time.sleep(1.0)

    def start_keepalive(self) -> None:
        """Start the background keepalive thread (idempotent)."""
        if not _KEEPALIVE_ENABLED:
            return
        if self._keepalive_thread and self._keepalive_thread.is_alive():
            return
        t = threading.Thread(target=self._keepalive_loop, daemon=True, name="easynews-keepalive")
        t.start()
        self._keepalive_thread = t
        logger.debug("Keepalive thread started (interval=%.0fs, idle_threshold=%.0fs).", _KEEPALIVE_INTERVAL, _KEEPALIVE_IDLE)

    def _keepalive_loop(self) -> None:
        """Ping Easynews after long idle periods to keep the TLS connection warm."""
        while True:
            time.sleep(_KEEPALIVE_INTERVAL)
            idle = time.monotonic() - self._last_search_ts
            if idle < _KEEPALIVE_IDLE:
                continue
            try:
                url = f"{EASYNEWS_BASE}/2.0/search/solr-search/?fly=2&gps=keepalive&sb=1&pno=1&pby=1&u=1&st=basic&s1=dtime&s1d=-&sS=3&vv=1&fty%5B%5D=VIDEO"
                self.s.get(url, timeout=10)
                logger.debug("Keepalive ping sent.")
            except Exception:
                pass  # Keepalive failures are non-fatal

    def _build_search_url(
        self,
        query: str,
        file_type: Optional[str],
        page: int,
        per_page: int,
        sort_field: Optional[str],
        sort_dir: str,
        safe_off: int,
    ) -> str:
        """Build the full search URL for the configured endpoint."""
        if _SEARCH_URL_TEMPLATE:
            return _SEARCH_URL_TEMPLATE.format(
                base=EASYNEWS_BASE,
                query=requests.utils.quote(query),
                page=page,
                per_page=per_page,
            )

        sort_p = _sort_params(sort_field, sort_dir)

        if _SEARCH_API == "3.0":
            params: Dict[str, str] = {
                "q": query,
                "page": str(page),
                "pageSize": str(per_page),
                "safeO": str(safe_off),
            }
            params.update(sort_p)
            _apply_advanced(params)
            if file_type:
                params["fty"] = file_type
            qs = "&".join(f"{k}={requests.utils.quote(str(v))}" for k, v in params.items())
            return f"{EASYNEWS_BASE}/3.0/api/search?{qs}"

        # Default: 2.0 solr-search
        params = {
            "fly": "2",
            "sb": "1",
            "pno": str(page),
            "pby": str(per_page),
            "u": "1",
            "chxu": "1",
            "chxgx": "1",
            "st": "basic",
            "gps": query,
            "vv": "1",
            "safeO": str(safe_off),
        }
        params.update(sort_p)
        _apply_advanced(params)

        url = f"{EASYNEWS_BASE}/2.0/search/solr-search/"
        qs = "&".join(f"{k}={requests.utils.quote(str(v))}" for k, v in params.items())
        if file_type:
            qs += f"&fty%5B%5D={requests.utils.quote(file_type)}"
        return f"{url}?{qs}"

    def _do_search(self, full_url: str, timeout: float) -> Dict[str, Any]:
        """Execute a single search HTTP request and return parsed JSON."""
        t0 = time.monotonic()
        with _SEARCH_SEMAPHORE:
            r = self.s.get(full_url, timeout=timeout)
        latency = time.monotonic() - t0
        if _LOG_LATENCY:
            logger.info("[%s] search latency=%.2fs status=%d", _active_endpoint_label(), latency, r.status_code)

        r.raise_for_status()
        content_stripped = r.text.lstrip()
        if content_stripped.startswith("<"):
            raise EasynewsError("Easynews returned an HTML page (possibly login redirect) instead of search results.")
        try:
            payload = r.json()
        except ValueError as e:
            raise EasynewsError(f"Failed to parse search response as JSON: {e}. Content: {r.text[:200]}") from e
        return _normalize_response(payload)

    def search(
        self,
        query: str,
        file_type: Optional[str] = None,
        page: int = 1,
        per_page: int = 50,
        sort_field: Optional[str] = "dtime",
        sort_dir: str = "-",
        safe_off: int = 0,
    ) -> Dict[str, Any]:
        """
        Call the Easynews search endpoint with latency hedging.
        Returns the raw JSON dict, including data and pagination fields.

        A "hedge" is fired if the primary request is still in-flight after
        _SEARCH_HEDGE_AFTER seconds — the first response that contains real
        data wins. This prevents a single slow Easynews PoP from causing
        NZBHydra timeouts.
        """
        self._last_search_ts = time.monotonic()
        full_url = self._build_search_url(query, file_type, page, per_page, sort_field, sort_dir, safe_off)

        deadline = time.monotonic() + _SEARCH_BUDGET
        result_q: "queue.Queue[Any]" = queue.Queue()

        def _attempt(timeout: float) -> None:
            try:
                data = self._do_search(full_url, timeout=timeout)
                result_q.put(data)
            except Exception as exc:
                result_q.put(exc)

        # Fire primary request
        primary = threading.Thread(target=_attempt, args=(_SEARCH_ATTEMPT_TIMEOUT,), daemon=True)
        primary.start()

        # Wait up to hedge_after for primary; fire hedge if still waiting
        hedge: Optional[threading.Thread] = None
        try:
            result = result_q.get(timeout=_SEARCH_HEDGE_AFTER)
            if isinstance(result, Exception):
                raise result
            if result.get("data"):
                return result
            # Empty result — trust it immediately if configured to do so
            if _TRUST_EMPTY:
                return result
            # Otherwise fall through to retry
        except queue.Empty:
            # Primary is slow — fire a hedge
            remaining = max(0.5, deadline - time.monotonic())
            hedge = threading.Thread(target=_attempt, args=(remaining,), daemon=True)
            hedge.start()

        # Wait for whichever response arrives first with real data
        attempts_left = 2
        while attempts_left > 0 and time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                result = result_q.get(timeout=remaining)
                attempts_left -= 1
                if isinstance(result, Exception):
                    last_exc = result
                    continue
                if result.get("data"):
                    return result
                if _TRUST_EMPTY:
                    return result
            except queue.Empty:
                break

        # All attempts exhausted — fall back to a plain blocking retry
        max_retries = 3
        last_error: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                timeout = max(1.0, deadline - time.monotonic())
                return self._do_search(full_url, timeout=min(timeout, _SEARCH_ATTEMPT_TIMEOUT))
            except (RequestException, EasynewsError) as e:
                last_error = e
                logger.warning("Search attempt %d/%d failed for query '%s': %s", attempt + 1, max_retries, query, _plain_error(e))
                if attempt < max_retries - 1:
                    time.sleep(1.0)

        logger.error("All search attempts failed for query '%s'", query)
        if last_error:
            if isinstance(last_error, EasynewsError):
                raise last_error
            raise EasynewsError(f"Search request failed: {_plain_error(last_error)}") from last_error
        raise EasynewsError(f"Search failed for query '{query}' (budget exhausted)")

    @staticmethod
    def _collect_items(json_data: Dict[str, Any]) -> List[SearchItem]:
        items: List[SearchItem] = []
        for it in json_data.get("data", []):
            hash_id = ""
            filename_no_ext = ""
            ext = ""
            sig: Optional[str] = None
            typ = ""
            item_id: Optional[str] = None

            if isinstance(it, list):
                if len(it) >= 12:
                    hash_id = it[0]
                    filename_no_ext = it[10]
                    ext = it[11]
            elif isinstance(it, dict):
                if "0" in it:
                    hash_id = it.get("0", "")
                if "10" in it:
                    filename_no_ext = it.get("10", "")
                if "11" in it:
                    ext = it.get("11", "")
                sig = it.get("sig")
                typ = it.get("type", "")
                item_id = it.get("id")

            if not hash_id or not ext:
                continue

            items.append(
                SearchItem(
                    id=item_id,
                    hash=hash_id,
                    filename=filename_no_ext,
                    ext=ext,
                    sig=sig,
                    type=typ,
                    raw=it if isinstance(it, dict) else {},
                )
            )
        return items

    def build_nzb_payload(
        self,
        items: List[SearchItem],
        name: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Build the form-encoded payload expected by /2.0/api/dl-nzb.
        Emulates createNZB() from members.js which submits hidden inputs of the checked items.
        Keys look like "{index}&sig={sig}" and value is value_token.
        We'll just use sequential indexes starting at 0.
        """
        data: Dict[str, str] = {"autoNZB": "1"}
        for idx, it in enumerate(items):
            key = str(idx)
            if it.sig:
                key = f"{idx}&sig={it.sig}"
            data[key] = it.value_token
        if name:
            data["nameZipQ0"] = name
        return data

    def download_nzb(self, payload: Dict[str, str], out_path: str) -> str:
        url = f"{EASYNEWS_BASE}/2.0/api/dl-nzb"
        try:
            r = self.s.post(url, data=payload, stream=True, timeout=_DOWNLOAD_TIMEOUT)
        except RequestException as e:
            logger.exception("NZB download request failed")
            raise EasynewsError(f"NZB download request failed: {_plain_error(e)}") from e
        if r.status_code != 200:
            raise EasynewsError(f"NZB creation failed: HTTP {r.status_code}")

        content = r.content.replace(
            b'date=""', b'date="0"'
        )  # normalize empty NZB date fields
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "wb") as f:
            f.write(content)
        return out_path

    def search_and_nzb(
        self,
        query: str,
        file_type: str = "VIDEO",
        max_items: int = 5,
        nzb_name: Optional[str] = None,
        out_path: str = "download.nzb",
    ) -> str:
        data = self.search(query=query, file_type=file_type)
        items = self._collect_items(data)
        if not items:
            raise EasynewsError("No results found for query")
        sel = items[:max_items]
        payload = self.build_nzb_payload(sel, name=nzb_name)
        return self.download_nzb(payload, out_path)


__all__ = [
    "EasynewsClient",
    "EasynewsError",
    "SearchItem",
    "_active_endpoint_label",
    "paginate_enabled",
    "max_pages",
    "_SEARCH_ATTEMPT_TIMEOUT",
]
