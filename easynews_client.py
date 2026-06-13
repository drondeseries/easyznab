"""
Easynews API-like client (unofficial) to perform searches and download NZB files.

This client mimics the webapp behavior by calling:
- GET /2.0/search/solr-search for search results (JSON)
- POST /2.0/api/dl-nzb to create/download NZB for selected items

Authentication is cookie-based via username/password POST to the login endpoint.
You'll need a valid Easynews account. Use responsibly and per Easynews TOS.
"""

from __future__ import annotations

import base64
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
import threading
from requests.exceptions import RequestException


EASYNEWS_BASE = "https://members.easynews.com"

_LOGIN_TIMEOUT = 30
_SEARCH_TIMEOUT = 30
_DOWNLOAD_TIMEOUT = 60

logger = logging.getLogger(__name__)

# Limit concurrent search requests to Easynews globally to prevent hitting rate limits
_SEARCH_SEMAPHORE = threading.Semaphore(2)


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
                
                # Succeeded
                logger.info("EasynewsClient login and Solr session priming succeeded.")
                return
            except (RequestException, EasynewsError) as e:
                logger.warning(f"Login attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt == max_retries - 1:
                    logger.exception("All login attempts failed")
                    raise EasynewsError(f"Network error during Easynews login: {e}") from e
                time.sleep(1.0)

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
        Call the same Solr-backed endpoint used by the site.
        Returns the raw JSON dict, including data and pagination fields.
        """
        params = {
            # Backend selector, 1 = solr-search
            "fly": "2",
            "sb": "1",
            "pno": str(page),
            "pby": str(per_page),
            "u": "1",
            "chxu": "1",
            "chxgx": "1",
            "st": "basic",
            "gps": query,
            "vv": "1",  # for VIDEO hover/preview data
            "safeO": str(safe_off),
        }
        if sort_field:
            params["s1"] = sort_field
            params["s1d"] = sort_dir

        url = f"{EASYNEWS_BASE}/2.0/search/solr-search/"
        query_params = "&".join([f"{k}={requests.utils.quote(v)}" for k, v in params.items()])
        if file_type:
            query_params += f"&fty%5B%5D={requests.utils.quote(file_type)}"
        full_url = f"{url}?{query_params}"

        max_retries = 3
        last_error = None
        for attempt in range(max_retries):
            try:
                with _SEARCH_SEMAPHORE:
                    r = self.s.get(full_url, timeout=_SEARCH_TIMEOUT)
                r.raise_for_status()
                
                # Check if response is HTML page (implies login redirect / failure)
                content_stripped = r.text.lstrip()
                if content_stripped.startswith("<"):
                    raise EasynewsError("Easynews returned an HTML page (possibly login redirect) instead of search results.")
                    
                try:
                    return r.json()
                except ValueError as e:
                    raise EasynewsError(f"Failed to parse search response as JSON: {e}. Content: {r.text[:200]}") from e
            except (RequestException, EasynewsError) as e:
                last_error = e
                logger.warning(f"Search attempt {attempt + 1}/{max_retries} failed for query '{query}': {e}")
                if attempt < max_retries - 1:
                    time.sleep(1.0)
        
        # If we get here, all attempts failed
        logger.error(f"All search attempts failed for query '{query}'")
        if isinstance(last_error, EasynewsError):
            raise last_error
        raise EasynewsError(f"Search request failed: {last_error}") from last_error

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
                # Some APIs may return dict with numeric keys as strings
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
                # Skip malformed entries
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
        # The site posts to /2.0/api/dl-nzb and returns the NZB file content (application/x-nzb or xml)
        return data

    def download_nzb(self, payload: Dict[str, str], out_path: str) -> str:
        url = f"{EASYNEWS_BASE}/2.0/api/dl-nzb"
        try:
            r = self.s.post(url, data=payload, stream=True, timeout=_DOWNLOAD_TIMEOUT)
        except RequestException as e:
            logger.exception("NZB download request failed")
            raise EasynewsError(f"NZB download request failed: {e}") from e
        if r.status_code != 200:
            raise EasynewsError(f"NZB creation failed: HTTP {r.status_code}")

        content_type = r.headers.get("Content-Type", "")
        if "xml" not in content_type and "nzb" not in content_type:
            # Sometimes returns text/html with a redirect page; still try to save
            pass

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


__all__ = ["EasynewsClient", "EasynewsError", "SearchItem"]
