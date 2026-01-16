#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BRAK / BRAV (bravsearch.bea-brak.de) scraper — NO Selenium
==========================================================

This scraper speaks PrimeFaces/JSF AJAX directly, including the correct
`jakarta.faces.partial.execute` handling for the BRAK registry.
"""
from __future__ import annotations

import argparse
import csv
import logging
import random
import re
import requests
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from itertools import cycle
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, build_opener, HTTPCookieProcessor
from urllib.error import URLError
import http.cookiejar
import xml.etree.ElementTree as ET

log = logging.getLogger("brak_scraper")

HOST = "https://bravsearch.bea-brak.de"
CTX = "/bravsearch"
INDEX_URL = f"{HOST}{CTX}/index.xhtml"

# --- Proxy & Header Setup ---
API_KEY = "ih0pzluqjthtlixlztt2xoxkyo3m08lrzchn3viv"  # Webshare API key

HEADERS_LIST = [
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/16.0 Safari/605.1.15"
        ),
        "Accept-Language": "en-US,en;q=0.8",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/16.0 Mobile/15E148 Safari/604.1"
        ),
        "Accept-Language": "en-US,en;q=0.7",
    },
]


def get_proxies(api_key: str) -> List[Dict[str, str]]:
    """Retrieve proxies from the Webshare API."""
    try:
        url = "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=100"
        resp = requests.get(url, headers={"Authorization": f"Token {api_key}"}, timeout=30)
        resp.raise_for_status()
        
        proxies = []
        for entry in resp.json().get("results", []):
            user, pwd = entry["username"], entry["password"]
            addr, port = entry["proxy_address"], entry["port"]
            proxy_url = f"http://{user}:{pwd}@{addr}:{port}"
            proxies.append({"http": proxy_url, "https": proxy_url})
        
        if proxies:
            log.info(f"Loaded {len(proxies)} proxies from Webshare API")
            return proxies
        else:
            log.warning("No proxies returned from Webshare API, using direct connection")
            return [{}]  # Empty dict = no proxy
    except Exception as e:
        log.warning(f"Failed to load proxies from Webshare: {e}, using direct connection")
        return [{}]


proxy_list = get_proxies(API_KEY)
proxy_cycle = cycle(proxy_list)
header_cycle = cycle(HEADERS_LIST)


def _sleep(base: float, jitter: float = 0.15) -> None:
    if base <= 0:
        return
    time.sleep(base + random.random() * jitter)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _is_jsf_partial(text: str) -> bool:
    t = (text or "").lstrip()
    return "<partial-response" in t[:4000]


def _parse_partial_response(xml_text: str) -> Tuple[Dict[str, str], Optional[str]]:
    updates: Dict[str, str] = {}
    viewstate: Optional[str] = None

    root = ET.fromstring(xml_text)
    for update in root.iter():
        if update.tag.endswith("update"):
            update_id = update.attrib.get("id", "")
            payload = update.text or ""
            updates[update_id] = payload
            if "ViewState" in update_id and payload:
                viewstate = payload.strip()

    return updates, viewstate


def _extract_viewstate_any(html_or_xml: str) -> str:
    if _is_jsf_partial(html_or_xml):
        updates, viewstate = _parse_partial_response(html_or_xml)
        if viewstate:
            return viewstate
        raise RuntimeError(
            "Partial-response had no ViewState update. Keys="
            f"{', '.join(sorted(updates.keys()))}"
        )

    for pattern in (
        r'name="jakarta.faces.ViewState"\s+value="([^"]+)"',
        r'name="javax.faces.ViewState"\s+value="([^"]+)"',
        r'id="[^"]*ViewState[^"]*"\s+value="([^"]+)"',
    ):
        match = re.search(pattern, html_or_xml, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    raise RuntimeError("Could not find (jakarta|javax).faces.ViewState in HTML")


class _SelectParser(HTMLParser):
    def __init__(self, select_id: str) -> None:
        super().__init__()
        self.select_id = select_id
        self.in_select = False
        self.in_option = False
        self.current_value = ""
        self.current_label: List[str] = []
        self.options: Dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_map = {key: (value or "") for key, value in attrs}
        if tag == "select" and attr_map.get("id") == self.select_id:
            self.in_select = True
        elif self.in_select and tag == "option":
            self.in_option = True
            self.current_value = attr_map.get("value", "").strip()
            self.current_label = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "select" and self.in_select:
            self.in_select = False
        elif tag == "option" and self.in_option:
            label = _clean("".join(self.current_label))
            if label and self.current_value:
                self.options[label] = self.current_value
            self.in_option = False
            self.current_value = ""
            self.current_label = []

    def handle_data(self, data: str) -> None:
        if self.in_option:
            self.current_label.append(data)


def _extract_select_options(html: str, select_id: str) -> Dict[str, str]:
    parser = _SelectParser(select_id)
    parser.feed(html)
    if not parser.options:
        raise RuntimeError(f"Could not find <select id='{select_id}'>")
    return parser.options


def _extract_total_results(results_html: str) -> int:
    text = _clean(re.sub(r"<[^>]+>", " ", results_html))
    for pattern in (
        r"Number of result entries:\s*([0-9][0-9\.]*)",
        r"Anzahl der Treffer:\s*([0-9][0-9\.]*)",
        r"Entries\s+\d+\s*-\s*\d+\s+of\s+([0-9][0-9\.]*)",
    ):
        match = re.search(pattern, text)
        if match:
            return int(match.group(1).replace(".", ""))
    raise RuntimeError("Could not parse total result count from results HTML")


def _extract_datagrid_id(results_html: str) -> str:
    match = re.search(r'PrimeFaces\.cw\("DataGrid","[^"]+",\{id:"([^"]+)"', results_html)
    if not match:
        raise RuntimeError("Could not find PrimeFaces DataGrid init id in results HTML")
    return match.group(1)


def _extract_updateDataResult_source(results_html: str) -> str:
    match = re.search(
        r'updateDataResult\s*=\s*function\(\)\s*\{return PrimeFaces\.ab\(\{s:"([^"]+)"',
        results_html,
    )
    if not match:
        raise RuntimeError("Could not find updateDataResult() source id in results HTML")
    return match.group(1)


class _ResultCardParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.cards: List[Dict[str, Iterable[str]]] = []
        self._in_card = False
        self._depth = 0
        self._in_header = False
        self._in_li = False
        self._header_parts: List[str] = []
        self._current_li: List[str] = []
        self._lis: List[str] = []
        self._current_info_id: Optional[str] = None

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_map = {key: (value or "") for key, value in attrs}
        classes = {token.strip() for token in attr_map.get("class", "").split()}
        if tag == "div" and "resultCard" in classes:
            self._in_card = True
            self._depth = 1
            self._header_parts = []
            self._lis = []
            self._current_info_id = None
            return

        if not self._in_card:
            return

        self._depth += 1
        
        # Extract info button ID from commandlink
        if tag == "a" and "resultCardDetailLink" in classes:
            self._current_info_id = attr_map.get("id", "")
        
        # Check both div and span for resultCardHeader
        if (tag in ("div", "span")) and "resultCardHeader" in classes:
            self._in_header = True
        if tag == "li":
            self._in_li = True
            self._current_li = []

    def handle_endtag(self, tag: str) -> None:
        if not self._in_card:
            return

        if (tag in ("div", "span")) and self._in_header:
            self._in_header = False
        if tag == "li" and self._in_li:
            text = _clean("".join(self._current_li))
            if text:
                self._lis.append(text)
            self._in_li = False
            self._current_li = []

        self._depth -= 1
        if self._depth <= 0:
            header = _clean("".join(self._header_parts))
            card_data: Dict[str, Iterable[str]] = {"header": header, "lis": list(self._lis)}
            if self._current_info_id:
                card_data["info_id"] = self._current_info_id
            self.cards.append(card_data)
            self._in_card = False
            self._depth = 0

    def handle_data(self, data: str) -> None:
        if self._in_header:
            self._header_parts.append(data)
        elif self._in_li:
            self._current_li.append(data)


def _parse_cards(results_html: str, bar_label: str) -> List[Dict[str, str]]:
    parser = _ResultCardParser()
    parser.feed(results_html)
    out: List[Dict[str, str]] = []

    for card in parser.cards:
        name = _clean(card.get("header", ""))
        lis = [item for item in card.get("lis", []) if item]

        professional_title = lis[0] if lis else ""
        rest = lis[1:]

        office = ""
        street = ""
        zip_code = ""
        city = ""
        zip_city_raw = ""

        if rest:
            if re.match(r"^\d{5}\s+.+$", rest[-1]):
                zip_city_raw = rest[-1]
                match = re.match(r"^(\d{5})\s+(.+)$", zip_city_raw)
                if match:
                    zip_code, city = match.group(1), match.group(2).strip()
                rest = rest[:-1]

            if len(rest) == 1:
                street = rest[0]
            elif len(rest) >= 2:
                street = rest[-1]
                office = " | ".join(rest[:-1]).strip()

        card_dict: Dict[str, str] = {
            "bar": bar_label,
            "name": name,
            "professional_title": professional_title,
            "office": office,
            "street": street,
            "zip": zip_code,
            "city": city,
            "zip_city_raw": zip_city_raw,
        }
        
        # Include info_id if available for detail fetching
        if "info_id" in card:
            card_dict["info_id"] = card.get("info_id", "")
        
        out.append(card_dict)

    return out


def _write_csv(path: str, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not out_path.exists()

    with out_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


@dataclass
class SearchIds:
    form_id: str
    viewstate: str
    lang_name: Optional[str]
    bar_name: str
    search_button_id: str
    bar_options: Dict[str, str]


@dataclass
class ResultIds:
    form_id: str
    viewstate: str
    datagrid_id: str
    update_data_result_source: str


class BRAVScraper:
    def __init__(
        self,
        timeout: float = 30.0,
        sleep_s: float = 0.25,
        debug_dir: Optional[str] = None,
        max_retries: int = 3,
    ) -> None:
        self.timeout = timeout
        self.sleep_s = sleep_s
        self.max_retries = max_retries
        self.debug_dir = Path(debug_dir) if debug_dir else None
        if self.debug_dir:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
        self._dbg = 0

        self.cookie_jar = http.cookiejar.CookieJar()
        self.opener = build_opener(HTTPCookieProcessor(self.cookie_jar))
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120 Safari/537.36"
            ),
            "Accept-Language": "en,de;q=0.8",
            "Connection": "keep-alive",
        }
        
        # Proxy and header rotation tracking
        self.request_count = 0
        self.rotate_every = 20  # Rotate headers every 20 requests
        self.current_headers = dict(self.headers)
        self.current_proxy = {}  # Start with direct connection (empty dict)
        
        # Create requests session for proxy and header rotation support
        self.session = requests.Session()
        self.session.cookies = self.cookie_jar

    def _save(self, name: str, content: str, ext: str) -> None:
        if not self.debug_dir:
            return
        self._dbg += 1
        path = self.debug_dir / f"{self._dbg:04d}_{name}.{ext}"
        path.write_text(content, encoding="utf-8", errors="replace")

    def _request(self, method: str, url: str, *, headers=None, data=None) -> str:
        last_error: Optional[Exception] = None
        
        # Rotate headers every N requests
        self.request_count += 1
        if self.request_count % self.rotate_every == 0:
            self.current_headers = dict(next(header_cycle))
            log.debug(f"Rotating to new User-Agent (request #{self.request_count})")
        
        # Rotate proxies every 60 requests (3x header rotation)
        if self.request_count % (self.rotate_every * 3) == 0:
            self.current_proxy = next(proxy_cycle)
            if self.current_proxy:
                log.debug(f"Rotating to new proxy (request #{self.request_count})")
        
        for attempt in range(1, self.max_retries + 1):
            try:
                req_headers = dict(self.headers)
                # Merge current rotated headers
                req_headers.update(self.current_headers)
                if headers:
                    req_headers.update(headers)
                
                payload = None
                if data is not None:
                    payload = urlencode(data).encode("utf-8")
                
                # Use requests session with proxy rotation
                resp = self.session.request(
                    method=method,
                    url=url,
                    data=payload,
                    headers=req_headers,
                    proxies=self.current_proxy,  # Use proxy for all requests (direct if empty dict)
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                return resp.text
                
            except (requests.RequestException, OSError) as exc:
                last_error = exc
                _sleep(0.4 * attempt)
        
        raise RuntimeError(f"HTTP {method} failed after {self.max_retries} retries: {last_error}")

    def get_search_page(self) -> str:
        candidates = [
            f"{HOST}/",
            f"{HOST}{CTX}/",
            f"{HOST}{CTX}/index.brak",
            f"{HOST}{CTX}/index.xhtml",
        ]

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": INDEX_URL,
        }

        last_err = None
        for url in candidates:
            cache_buster = "" if "?" in url else f"?_cb={random.randint(1, 10_000_000)}"
            candidate = url + cache_buster
            log.debug("GET candidate search URL: %s", candidate)
            text = self._request("GET", candidate, headers=headers)
            self._save("search_candidate", text, "xml" if _is_jsf_partial(text) else "html")

            if _is_jsf_partial(text):
                last_err = f"Got partial-response XML from GET {candidate}"
                continue

            if 'id="searchForm"' not in text:
                last_err = f"No searchForm in GET {candidate}"
                continue

            try:
                _extract_viewstate_any(text)
            except Exception as exc:
                last_err = f"ViewState missing in GET {candidate}: {exc}"
                continue

            return text

        raise RuntimeError(
            "Could not obtain the real search page from any candidate URL. "
            f"Last error: {last_err}. Use --debug-dir to inspect responses."
        )

    def parse_search_ids(self, html: str) -> SearchIds:
        viewstate = _extract_viewstate_any(html)

        lang_match = re.search(r'name="([^"]*ddLanguage_input)"', html)
        lang_name = lang_match.group(1) if lang_match else None

        bar_match = re.search(r'name="([^"]*ddRAKammer_input)"', html)
        if not bar_match:
            raise RuntimeError("Could not find bar dropdown (ddRAKammer_input)")
        bar_name = bar_match.group(1)

        bar_options = _extract_select_options(html, "searchForm:ddRAKammer_input")

        search_button_id = None
        for match in re.finditer(r'id="([^"]+)"[^>]*onclick="([^"]+)"', html):
            button_id, onclick = match.group(1), match.group(2)
            if "PrimeFaces.ab" in onclick and 'u:&quot;mainPageContent&quot;' in onclick:
                search_button_id = button_id
                break
        if not search_button_id:
            raise RuntimeError("Could not locate search command button for mainPageContent update.")

        return SearchIds(
            form_id="searchForm",
            viewstate=viewstate,
            lang_name=lang_name,
            bar_name=bar_name,
            search_button_id=search_button_id,
            bar_options=bar_options,
        )

    def ajax(
        self,
        *,
        form_id: str,
        viewstate: str,
        source_id: str,
        execute: Optional[str],
        render: Optional[str],
        extra: Optional[Dict[str, str]] = None,
        behavior_event: Optional[str] = None,
    ) -> Tuple[Dict[str, str], str]:
        headers = {
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/xml, text/xml, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": HOST,
            "Referer": INDEX_URL,
        }

        data: Dict[str, str] = {
            form_id: form_id,
            "jakarta.faces.partial.ajax": "true",
            "jakarta.faces.source": source_id,
            "jakarta.faces.partial.execute": execute if execute is not None else source_id,
        }

        if render is not None:
            data["jakarta.faces.partial.render"] = render

        if behavior_event:
            data["jakarta.faces.behavior.event"] = behavior_event
            data["jakarta.faces.partial.event"] = behavior_event

        data[source_id] = source_id
        data["jakarta.faces.ViewState"] = viewstate

        if extra:
            data.update({key: str(value) for key, value in extra.items()})

        response = self._request("POST", INDEX_URL, headers=headers, data=data)
        self._save("ajax", response, "xml")

        # Validate response is actual JSF partial-response before parsing
        if not response or not response.strip():
            raise RuntimeError(
                "AJAX returned empty response. This usually indicates a server error or session issue."
            )
        
        if not _is_jsf_partial(response):
            snippet = response[:500] if len(response) > 500 else response
            raise RuntimeError(
                f"AJAX returned non-XML response (expected JSF partial-response). "
                f"First 500 chars: {snippet}"
            )

        updates, new_viewstate = _parse_partial_response(response)
        return updates, (new_viewstate or viewstate)

    def ajax_search_by_bar(self, bar_value: str, language: str = "en") -> Tuple[str, str]:
        search_html = self.get_search_page()
        sids = self.parse_search_ids(search_html)

        extra = {sids.bar_name: bar_value}
        if sids.lang_name:
            extra[sids.lang_name] = language

        updates, viewstate = self.ajax(
            form_id=sids.form_id,
            viewstate=sids.viewstate,
            source_id=sids.search_button_id,
            execute=sids.form_id,
            render="mainPageContent",
            extra=extra,
            behavior_event="click",
        )

        fragment = updates.get("mainPageContent") or ""
        if not fragment:
            keys = ", ".join(sorted(updates.keys()))
            raise RuntimeError(
                "AJAX search returned no mainPageContent update. "
                f"Update keys were: {keys}."
            )

        return fragment, viewstate

    def parse_result_ids(self, results_html: str, viewstate: str) -> ResultIds:
        return ResultIds(
            form_id="resultForm",
            viewstate=viewstate,
            datagrid_id=_extract_datagrid_id(results_html),
            update_data_result_source=_extract_updateDataResult_source(results_html),
        )

    def fetch_page(self, ids: ResultIds, first: int, rows: int) -> Tuple[str, str]:
        datagrid_id = ids.datagrid_id

        extra_page = {
            f"{datagrid_id}_pagination": "true",
            f"{datagrid_id}_first": str(first),
            f"{datagrid_id}_rows": str(rows),
            f"{datagrid_id}_page": str(first // rows if rows else 0),
            f"{datagrid_id}_encodeFeature": "true",
        }
        _updates, viewstate = self.ajax(
            form_id=ids.form_id,
            viewstate=ids.viewstate,
            source_id=datagrid_id,
            execute=datagrid_id,
            render=None,
            extra=extra_page,
            behavior_event="page",
        )

        updates, new_viewstate = self.ajax(
            form_id=ids.form_id,
            viewstate=viewstate,
            source_id=ids.update_data_result_source,
            execute=ids.form_id,
            render=ids.form_id,
            extra=extra_page,
        )

        html = updates.get(ids.form_id) or ""
        if not html:
            keys = ", ".join(sorted(updates.keys()))
            # If we're getting searchForm back instead of resultForm, the view has expired
            if "searchForm" in keys:
                raise RuntimeError(
                    f"JSF view expired (server returned searchForm instead of resultForm). "
                    f"This can happen after many pagination requests. Session needs to be refreshed."
                )
            raise RuntimeError(
                "updateDataResult returned no resultForm HTML update. "
                f"Update keys: {keys}."
            )

        return html, new_viewstate

    def fetch_details(self, info_button_id: str, viewstate: str, form_id: str = "resultForm") -> Tuple[str, str]:
        """Fetch detail page for a lawyer by clicking the info button."""
        render_target = "resultDetailForm"
        
        updates, new_viewstate = self.ajax(
            form_id=form_id,
            viewstate=viewstate,
            source_id=info_button_id,
            execute=None,
            render=render_target,
        )
        
        detail_html = updates.get(render_target) or ""
        if not detail_html:
            keys = ", ".join(sorted(updates.keys()))
            raise RuntimeError(
                f"Detail fetch returned no {render_target} update. "
                f"Update keys: {keys}."
            )
        
        return detail_html, new_viewstate


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )




def _clean(s: str) -> str:
    """Strip whitespace and normalize."""
    return re.sub(r'\s+', ' ', s.strip())


def _extract_details(detail_html: str) -> Dict[str, str]:
    """Extract detail fields from detail page HTML using row-based parsing."""
    details: Dict[str, str] = {}
    label_map = {
        "email": "email",
        "mobile_phone": "mobile_phone",
        "telephone": "telephone",
        "telefax": "telefax",
        "date_of_admission": "date_of_admission",
        "date_of_first_admission": "date_of_first_admission",
        "bar_membership": "bar_membership",
        "professional_title": "detail_professional_title",
        "form_of_address": "form_of_address",
        "first_name_last_name": "first_name_last_name",
        "law_office": "detail_office",
        "office_address": "detail_street",
        "internet_address": "internet_address",
        "bea_safeid": "bea_safe_id",
        "interest_for_getting_appointed_by_court_as_defence_counsel": "court_appointment_interest",
    }
    
    # Split by cssRow divs - simpler pattern that works for all rows including the last one
    row_pattern = r'<div class="cssRow">(.*?)(?=<div class="cssRow"|$)'
    
    for row_match in re.finditer(row_pattern, detail_html, re.DOTALL | re.IGNORECASE):
        row_html = row_match.group(1)
        
        # Extract label
        label_match = re.search(r'<label[^>]*>([^<]*?)</label>', row_html, re.DOTALL | re.IGNORECASE)
        if not label_match:
            continue
        
        label_text = _clean(label_match.group(1)).rstrip(":")
        if not label_text:
            continue
        
        # Extract value - look for text in cssColResultDetail* divs
        value_match = re.search(r'cssColResultDetail(?:Text|TextGroup)[^>]*>(.*?)(?=</div)', row_html, re.DOTALL | re.IGNORECASE)
        if not value_match:
            continue
        
        value_html = value_match.group(1)
        # Remove HTML tags to get plain text, preserving some spacing for multi-line values
        value_text = re.sub(r'<[^>]+>', '\n', value_html)
        value_text = '\n'.join(line.strip() for line in value_text.split('\n') if line.strip())
        value_text = _clean(value_text.replace('\n', ' '))
        
        if value_text and value_text != "No Information":
            # Normalize label for lookup (remove hyphens and special chars, lowercase, replace spaces with underscores)
            normalized = label_text.lower().replace('-', '').replace(' ', '_')
            key = label_map.get(normalized, normalized)
            if key not in details:  # Don't overwrite if already found
                details[key] = value_text
    
    return details


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Scrape BRAK/BRAV lawyer registry (PrimeFaces/JSF, no Selenium)")
    parser.add_argument("--out", default="lawyers.csv", help="Output CSV path")
    parser.add_argument("--rows", type=int, default=800, help="Rows per page request (server may clamp)")
    parser.add_argument("--sleep", type=float, default=0.25, help="Base sleep between requests (seconds)")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout (seconds)")
    parser.add_argument("--bars", default="", help="Comma-separated bar labels. Empty = all")
    parser.add_argument("--max-records", type=int, default=0, help="Stop after N total records (0 = no limit)")
    parser.add_argument("--loglevel", default="INFO", help="DEBUG, INFO, WARNING, ...")
    parser.add_argument("--debug-dir", default="", help="Write raw HTML/XML responses here")

    args, _unknown = parser.parse_known_args(argv)

    setup_logging(args.loglevel)

    scraper = BRAVScraper(timeout=args.timeout, sleep_s=args.sleep, debug_dir=args.debug_dir or None)

    search_html = scraper.get_search_page()
    sids = scraper.parse_search_ids(search_html)

    all_bars = list(sids.bar_options.keys())
    if args.bars.strip():
        wanted = [b.strip() for b in args.bars.split(",") if b.strip()]
        wanted = [b for b in wanted if b in sids.bar_options]
        if not wanted:
            raise RuntimeError(
                "No valid bars selected. Use exact dropdown labels. "
                f"Example bars: {', '.join(all_bars[:10])} ..."
            )
    else:
        wanted = all_bars

    fieldnames = [
        "bar", "name", "professional_title", "office", "street", "zip", "city", "zip_city_raw",
        "email", "mobile_phone", "telephone", "telefax",
        "date_of_admission", "date_of_first_admission", "bar_membership", "bea_safe_id",
        "form_of_address", "first_name_last_name", "detail_professional_title",
        "detail_office", "detail_street", "internet_address", "court_appointment_interest"
    ]

    total_written = 0

    for bar_label in wanted:
        bar_value = sids.bar_options[bar_label]
        log.info("=== BAR: %s ===", bar_label)

        results_html, viewstate = scraper.ajax_search_by_bar(bar_value=bar_value, language="en")
        ids = scraper.parse_result_ids(results_html, viewstate=viewstate)

        total = _extract_total_results(results_html)
        log.info("Total entries for %s: %d", bar_label, total)

        first = 0
        while first < total:
            try:
                page_html, new_viewstate = scraper.fetch_page(ids, first=first, rows=args.rows)
                ids.viewstate = new_viewstate
            except RuntimeError as e:
                if "JSF view expired" in str(e):
                    log.warning(f"JSF view expired at offset {first}. Refreshing search session...")
                    results_html, viewstate = scraper.ajax_search_by_bar(bar_value=bar_value, language="en")
                    ids = scraper.parse_result_ids(results_html, viewstate=viewstate)
                    # Retry this page
                    try:
                        page_html, new_viewstate = scraper.fetch_page(ids, first=first, rows=args.rows)
                        ids.viewstate = new_viewstate
                    except Exception as retry_error:
                        log.error(f"Failed to fetch page after refresh: {retry_error}")
                        raise
                else:
                    raise

            cards = _parse_cards(page_html, bar_label=bar_label)
            
            # Fetch details for each card to extract additional fields
            for card in cards:
                info_id = card.get("info_id")
                if info_id:
                    try:
                        detail_html, new_viewstate = scraper.fetch_details(
                            info_button_id=info_id, 
                            viewstate=ids.viewstate
                        )
                        ids.viewstate = new_viewstate
                        details = _extract_details(detail_html)
                        card.update(details)
                        _sleep(scraper.sleep_s)
                    except Exception as e:
                        log.warning(f"Failed to fetch details for {card.get('name')}: {e}")
            
            _write_csv(args.out, cards, fieldnames)

            total_written += len(cards)
            log.info(
                "Bar %s: wrote %d records (offset %d). Total written=%d",
                bar_label,
                len(cards),
                first,
                total_written,
            )

            if args.max_records and total_written >= args.max_records:
                log.warning("Reached --max-records=%d. Stopping.", args.max_records)
                return 0

            first += args.rows
            _sleep(args.sleep)

    log.info("Done. Total written=%d -> %s", total_written, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
