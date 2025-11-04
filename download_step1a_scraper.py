from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import duckdb
import pyarrow as pa
import requests
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

# Configuration

# Site endpoints
BASE = "https://consultacontratos.ocpr.gov.pr"
HOME = BASE + "/"
SEARCH_URL = BASE + "/contract/search"

PAGE_LENGTH = 10000

# Browser settings
HEADLESS = True
PAGE_LOAD_TIMEOUT = 20.0  # Seconds

# Request pacing and retries
REQUEST_DELAY = 0.25      # Delay in seconds
MAX_RETRIES   = 4
RETRY_BACKOFF = 2.0       # Exponential backoff multiplier

# DataTables payload structure captured from DevTools
COLUMNS_PAYLOAD = [
    {"data": None,               "name": "", "searchable": False, "orderable": False, "search": {"value": "", "regex": False}},
    {"data": "ContractNumber",   "name": "", "searchable": True,  "orderable": True,  "search": {"value": "", "regex": False}},
    {"data": "Contractors",      "name": "", "searchable": True,  "orderable": True,  "search": {"value": "", "regex": False}},
    {"data": "DateOfGrant",      "name": "", "searchable": True,  "orderable": True,  "search": {"value": "", "regex": False}},
    {"data": "EffectiveDateFrom","name": "", "searchable": True,  "orderable": True,  "search": {"value": "", "regex": False}},
    {"data": "EffectiveDateTo",  "name": "", "searchable": True,  "orderable": True,  "search": {"value": "", "regex": False}},
    {"data": "AmountToPay",      "name": "", "searchable": True,  "orderable": True,  "search": {"value": "", "regex": False}},
    {"data": "Service",          "name": "", "searchable": True,  "orderable": True,  "search": {"value": "", "regex": False}},
    {"data": "EntityId",         "name": "", "searchable": True,  "orderable": True,  "search": {"value": "", "regex": False}},
    {"data": "CancellationDate", "name": "", "searchable": True,  "orderable": True,  "search": {"value": "", "regex": False}},
    {"data": None,               "name": "", "searchable": False, "orderable": False, "search": {"value": "", "regex": False}},
]
ORDER_PAYLOAD = [{"column": 3, "dir": "asc"}]  # order by DateOfGrant asc

# Output location
OUTPUT_DIR = Path("download/1a_contracts_raw")


def start_browser_get_token(headless: bool = True, timeout: float = 20.0) -> Dict[str, Any]:
    """Start Selenium, visit home page and return cookies dict + CSRF token + user agent."""
    options = Options()
    options.headless = headless
    service = Service(GeckoDriverManager().install())
    driver = webdriver.Firefox(service=service, options=options)
    wait = WebDriverWait(driver, timeout)
    try:
        driver.get(HOME)
        wait.until(lambda d: d.execute_script("return document.readyState === 'complete'"))

        # Try hidden input first
        token = None
        try:
            el = driver.find_element(By.NAME, "__RequestVerificationToken")
            token = el.get_attribute("value")
        except Exception:
            token = None

        # Fallback: cookie
        if not token:
            for c in driver.get_cookies():
                if c.get("name") == "__RequestVerificationToken":
                    token = c.get("value")
                    break

        # Cookies → dict
        cookies = {c["name"]: c["value"] for c in driver.get_cookies()}

        # User-Agent from browser
        ua = driver.execute_script("return navigator.userAgent;")

        return {"driver": driver, "token": token, "cookies": cookies, "user_agent": ua}
    except Exception:
        driver.quit()
        raise


def update_session_from_driver(session: requests.Session, driver_info: Dict[str, Any]) -> None:
    """Copy cookies and set default headers (including CSRF token) into requests.Session."""
    for name, val in driver_info["cookies"].items():
        session.cookies.set(name, val, domain="consultacontratos.ocpr.gov.pr", path="/")

    headers = {
        "User-Agent": driver_info.get("user_agent") or "python-requests",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": HOME,
        "Origin": BASE,
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json; charset=utf-8",
    }
    session.headers.update(headers)
    token = driver_info.get("token")
    if token:
        session.headers["__RequestVerificationToken"] = token


def build_payload(draw: int, start: int, length: int, date_from: str, date_to: str) -> Dict[str, Any]:
    """Construct the JSON payload for a single page request."""
    return {
        "draw": draw,
        "columns": COLUMNS_PAYLOAD,
        "order": ORDER_PAYLOAD,
        "start": start,
        "length": length,
        "search": {"value": "", "regex": False},
        "EntityId": None,
        "ContractNumber": None,
        "ContractorName": None,
        "DateOfGrantFrom": date_from,
        "DateOfGrantTo": date_to,
        "EffectiveDateFrom": None,
        "EffectiveDateTo": None,
        "AmountFrom": None,
        "AmountTo": None,
        "ServiceGroupId": None,
        "ServiceId": None,
        "FundId": None,
        "ContractingFormId": None,
        "PCONumber": None,
    }


def try_post_page(session: requests.Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST payload and return JSON dict; raise on non-200."""
    r = session.post(SEARCH_URL, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"POST returned HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure complex fields are JSON-encoded for Parquet compatibility."""
    processed: List[Dict[str, Any]] = []
    for row in rows:
        if not row:
            continue
        data = dict(row)
        contractors = data.get("Contractors")
        if contractors is not None and not isinstance(contractors, (str, int, float, bool)):
            try:
                data["Contractors"] = json.dumps(contractors, ensure_ascii=False)
            except Exception:
                data["Contractors"] = str(contractors)
        processed.append(data)
    return processed


def _build_date(ddmm: str, year: int) -> str:
    """Return dd/mm/yyyy string ensuring zero-padded day/month."""
    parts = ddmm.strip().split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Expected DD/MM format, got {ddmm!r}")
    dd, mm = (part.zfill(2) for part in parts)
    return f"{dd}/{mm}/{year}"


def run_t(ddmm: str, year_chosen: int, date_to_ddmm: Optional[str] = None):
    print("Launching browser to fetch cookies and CSRF token...")
    driver_info = start_browser_get_token(headless=HEADLESS, timeout=PAGE_LOAD_TIMEOUT)
    driver = driver_info.pop("driver")  # keep to refresh token/cookies if needed

    session = requests.Session()
    update_session_from_driver(session, driver_info)

    collected_rows: List[Dict[str, Any]] = []
    observed_keys: Set[str] = set()
    date_from = _build_date(ddmm, year_chosen)
    date_to = _build_date(date_to_ddmm or "31/12", year_chosen)

    try:
        # First page to discover totals
        draw = 1
        start = 0
        length = PAGE_LENGTH
        payload = build_payload(draw=draw, start=start, length=length, date_from=date_from, date_to=date_to)

        print("Fetching first page (to discover total count)...")
        resp_json = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp_json = try_post_page(session, payload)
                break
            except Exception as e:
                print(f"First page request failed (attempt {attempt}): {e}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_BACKOFF ** (attempt - 1))

        records_total = resp_json.get("recordsTotal") or 0
        records_filtered = resp_json.get("recordsFiltered") or records_total
        print(f"recordsTotal={records_total}, recordsFiltered={records_filtered}")

        total_pages = max(1, math.ceil(records_filtered / float(length)))
        print(f"Using length={length}; total_pages (approx) = {total_pages}")

        first_rows = resp_json.get("data", [])
        print("First page rows:", len(first_rows))
        normalized_first_rows = normalize_rows(first_rows)
        collected_rows.extend(normalized_first_rows)
        for item in normalized_first_rows:
            observed_keys.update(item.keys())

        # Remaining pages
        for page_idx in range(1, total_pages):
            start = page_idx * length
            draw += 1
            payload = build_payload(draw=draw, start=start, length=length, date_from=date_from, date_to=date_to)

            attempt = 0
            while True:
                attempt += 1
                try:
                    resp_json = try_post_page(session, payload)
                    data_rows = resp_json.get("data", [])
                    print(f"Page {page_idx+1}/{total_pages} start={start}: got {len(data_rows)} rows")
                    normalized_rows = normalize_rows(data_rows)
                    collected_rows.extend(normalized_rows)
                    for item in normalized_rows:
                        observed_keys.update(item.keys())
                    break
                except Exception as e:
                    print(f"Error fetching page start={start} (attempt {attempt}): {e}")
                    if attempt >= MAX_RETRIES:
                        print("Max retries reached for this page — aborting.")
                        raise
                    # Refresh cookies/token via Selenium if needed
                    print("Refreshing cookies and CSRF token via Selenium and retrying...")
                    try:
                        driver.get(HOME)
                        WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState === 'complete'"))
                        # pull fresh token/cookies
                        try:
                            el = driver.find_element(By.NAME, "__RequestVerificationToken")
                            new_token = el.get_attribute("value")
                        except Exception:
                            new_token = None
                        new_cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
                        driver_info_update = {"cookies": new_cookies, "token": new_token, "user_agent": driver.execute_script("return navigator.userAgent;")}
                        update_session_from_driver(session, driver_info_update)
                    except Exception as e2:
                        print("Failed to refresh token/cookies:", e2)
                    time.sleep(RETRY_BACKOFF ** (attempt - 1))

            time.sleep(REQUEST_DELAY)
            if len(data_rows) < length:
                print("Last page shorter than length; reached end of results.")
                break

        parquet_path = OUTPUT_DIR / f"contracts_{year_chosen}.parquet"
        print(f"All pages fetched. Writing Parquet: {parquet_path}")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        if collected_rows:
            table = pa.Table.from_pylist(collected_rows)
        else:
            fallback_keys = [col["data"] for col in COLUMNS_PAYLOAD if isinstance(col["data"], str) and col["data"]]
            if not observed_keys:
                observed_keys.update(fallback_keys)
            if not observed_keys:
                observed_keys.add("__empty__")
            sorted_keys = sorted(observed_keys)
            arrays = [pa.array([], type=pa.string()) for _ in sorted_keys]
            table = pa.Table.from_arrays(arrays, names=sorted_keys)

        con = duckdb.connect()
        try:
            con.register("contracts_results", table)
            con.execute(f"COPY contracts_results TO '{parquet_path.as_posix()}' (FORMAT PARQUET)")
            con.unregister("contracts_results")
        finally:
            con.close()
        print("Wrote Parquet:", parquet_path)
    finally:
        try:
            driver.quit()
        except Exception:
            pass



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download contracts for a given year.")
    parser.add_argument("year", type=int, help="Year to download contracts for (e.g. 2024)")
    parser.add_argument(
        "--date-from",
        default="01/01",
        help="DD/MM start date within the selected year (default: 01/01).",
    )
    parser.add_argument(
        "--date-to",
        default=None,
        help="DD/MM end date within the selected year (default: 31/12).",
    )
    args = parser.parse_args()
    run_t(args.date_from, args.year, args.date_to)
