#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task 2:
- Read 'Input' and 'Output' columns from your main tracker (Google Sheets).
- For each row, open the child sheet from 'Output', locate the 'Total Views' column,
  sum numeric values (skipping bottom SUM(...) totals), and map back to the Input SID.
- Update Notion child databases (under parent pages) with:
  - Total Views (number)
  - Performance Score (Views per $100) = (Total Views / Cost) * 100 if Cost > 0
  - Clears Performance Score when Cost missing/zero to avoid stale data.

Optimizations and reliability:
- Rate limiter wraps ALL Google Sheets calls (open_by_key, get, batch_get, col_values, worksheet selection).
- Exponential backoff + long cooldown after 429 errors.
- One read per child sheet (uses FORMULA to detect SUM).
- Batch get for tracker Input/Output columns (one request).
- Optional fixed column/row to skip header scanning.

Env highlights (see your .env):
- NOTION_TOKEN, NOTION_DATABASE_ID, NOTION_CHILD_DB_TITLE
- NOTION_PROP_SPREADSHEET, NOTION_PROP_CAMPAIGN, NOTION_PROP_COST
- (optional) NOTION_PROP_TOTAL_VIEWS, NOTION_PROP_PERF_SCORE
- GOOGLE_SERVICE_ACCOUNT_FILE, TIKTOK_TRACKER_SPREADSHEET_ID, MAIN_SHEET_NAME
- TRACKER_INPUT_COLUMN_HEADER, TRACKER_OUTPUT_COLUMN_HEADER (or OUTPUT_COL_HEADER_NAME)
- CHILD_OUTPUT_TAB_NAME (optional), CHILD_VIEWS_HEADER, CHILD_VIEWS_FIXED_COL_LETTER, CHILD_HEADER_SCAN_ROWS
- CHILD_ASSUME_FIXED_COLUMN, CHILD_FIXED_HEADER_ROW
- RPM_LIMIT, MAX_RETRIES, RETRY_BASE_DELAY, COOLDOWN_ON_429, DRY_RUN, LOG_LEVEL
"""

import os
import re
import time
import random
import logging
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from notion_client import Client as NotionClient
import gspread
from gspread.exceptions import APIError

# -------------------- Utils --------------------

def env_get(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = v.strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1].strip()
    return v

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())

GSHEET_ID_RE = re.compile(r"/spreadsheets/d/([A-Za-z0-9_-]+)")
URL_RE = re.compile(r'https?://[^\s")]+')
GID_RE = re.compile(r"[?&#]gid=(\d+)")

def extract_first_url(cell_value: Optional[str]) -> Optional[str]:
    if not cell_value:
        return None
    m = URL_RE.search(str(cell_value))
    return m.group(0) if m else None

def extract_sid_and_gid(url_or_id: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    if not url_or_id:
        return None, None
    s = str(url_or_id).strip()
    # raw id
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", s) and "://" not in s:
        return s, None
    m = GSHEET_ID_RE.search(s)
    sid = m.group(1) if m else None
    gid = None
    mg = GID_RE.search(s)
    if mg and mg.group(1).isdigit():
        gid = int(mg.group(1))
    return sid, gid

def to_number(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            return float(val)
        except Exception:
            return None
    s = str(val).strip().replace(",", "")
    if s == "" or s.lower() in {"nan", "none", "-"}:
        return None
    try:
        return float(s)
    except Exception:
        return None

def col_to_a1(idx: int) -> str:
    res = ""
    while idx:
        idx, r = divmod(idx - 1, 26)
        res = chr(65 + r) + res
    return res

def contains_views_word(text: str) -> bool:
    # avoid false positives like "Preview"/"Review"
    tokens = re.findall(r"[a-z]+", (text or "").lower())
    return any(t in {"view", "views"} for t in tokens)

# -------------------- Load env --------------------

load_dotenv()

# Notion config
NOTION_TOKEN = env_get("NOTION_TOKEN")
PARENT_DB_ID = env_get("NOTION_DATABASE_ID")
CHILD_DB_TITLE = env_get("NOTION_CHILD_DB_TITLE", "Campaigns Summary")

# Notion property names
PROP_CAMPAIGN = env_get("NOTION_PROP_CAMPAIGN", "Campaign")
PROP_SPREADSHEET = env_get("NOTION_PROP_SPREADSHEET", "Spreadsheet")
PROP_COST = env_get("NOTION_PROP_COST", "Cost")
PROP_TOTAL_VIEWS = env_get("NOTION_PROP_TOTAL_VIEWS", "Total Views")
PROP_PERF_SCORE = env_get("NOTION_PROP_PERF_SCORE", "Performance Score (Views per $100)")

# Google config
GOOGLE_SERVICE_ACCOUNT_FILE = env_get("GOOGLE_SERVICE_ACCOUNT_FILE")
TRACKER_SHEET_ID = env_get("TIKTOK_TRACKER_SPREADSHEET_ID")
MAIN_SHEET_NAME = env_get("MAIN_SHEET_NAME", "").strip()  # blank = first tab

# Tracker headers
INPUT_HEADER = env_get("TRACKER_INPUT_COLUMN_HEADER", "Input")
# Prefer TASK2 header; fallback to legacy var OUTPUT_COL_HEADER_NAME; then "Output"
OUTPUT_HEADER = env_get("TRACKER_OUTPUT_COLUMN_HEADER", env_get("OUTPUT_COL_HEADER_NAME", "Output"))

# Child output sheet format
CHILD_OUTPUT_TAB_NAME = env_get("CHILD_OUTPUT_TAB_NAME", "")  # prefer this tab name if set
CHILD_VIEWS_HEADER = env_get("CHILD_VIEWS_HEADER", "Total Views")
CHILD_VIEWS_FIXED_COL_LETTER = env_get("CHILD_VIEWS_FIXED_COL_LETTER", "D")  # fast path
CHILD_HEADER_SCAN_ROWS = int(env_get("CHILD_HEADER_SCAN_ROWS", "10"))

# Behavior
DRY_RUN = env_get("DRY_RUN", "false").lower() in {"1", "true", "yes", "y", "on"}
LOG_LEVEL = env_get("LOG_LEVEL", "INFO").upper()

# Rate limiting / retries
RPM_LIMIT = int(env_get("RPM_LIMIT", "30"))  # be conservative; Sheets cap is 60/min/user
MAX_RETRIES = int(env_get("MAX_RETRIES", "6"))
RETRY_BASE_DELAY = float(env_get("RETRY_BASE_DELAY", "1.0"))
COOLDOWN_ON_429 = float(env_get("COOLDOWN_ON_429", "65.0"))  # long cooldown after rate limit

# Header scan skip (assume fixed col/row)
CHILD_ASSUME_FIXED_COLUMN = env_get("CHILD_ASSUME_FIXED_COLUMN", "true").lower() in {"1", "true", "yes", "y", "on"}
CHILD_FIXED_HEADER_ROW = int(env_get("CHILD_FIXED_HEADER_ROW", "1"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -------------------- Rate-limited gspread helpers --------------------

class _RateLimiter:
    def __init__(self, rpm: int):
        self.min_interval = 60.0 / max(1, rpm)
        self._last = 0.0
    def wait(self):
        now = time.time()
        delta = now - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.time()
    def reset(self):
        self._last = 0.0

_rate = _RateLimiter(RPM_LIMIT)

def _is_rate_limit_error(e: Exception) -> bool:
    s = str(e)
    return "429" in s or "RATE_LIMIT" in s or "RESOURCE_EXHAUSTED" in s

def _backoff_delay(attempt: int) -> float:
    return min(20.0, RETRY_BASE_DELAY * (2 ** attempt)) * (1 + 0.1 * random.random())

def _cooldown():
    logging.warning(f"Sheets 429 — cooling down {COOLDOWN_ON_429:.0f}s to reset quota window")
    time.sleep(COOLDOWN_ON_429)
    _rate.reset()

def gc_open_by_key_rl(gc: gspread.Client, key: str) -> gspread.Spreadsheet:
    for attempt in range(MAX_RETRIES):
        try:
            _rate.wait()
            return gc.open_by_key(key)
        except APIError as e:
            if _is_rate_limit_error(e):
                if attempt >= MAX_RETRIES - 2:
                    _cooldown()
                else:
                    delay = _backoff_delay(attempt)
                    logging.warning(f"Sheets 429 (open_by_key) — backing off {delay:.1f}s (attempt {attempt+1}/{MAX_RETRIES})")
                    time.sleep(delay)
                continue
            raise

def ss_get_worksheet_by_id_rl(spreadsheet: gspread.Spreadsheet, gid: int) -> Optional[gspread.Worksheet]:
    for attempt in range(MAX_RETRIES):
        try:
            _rate.wait()
            return spreadsheet.get_worksheet_by_id(gid)
        except APIError as e:
            if _is_rate_limit_error(e):
                if attempt >= MAX_RETRIES - 2:
                    _cooldown()
                else:
                    delay = _backoff_delay(attempt)
                    logging.warning(f"Sheets 429 (get_worksheet_by_id) — backoff {delay:.1f}s (attempt {attempt+1}/{MAX_RETRIES})")
                    time.sleep(delay)
                continue
            raise
        except Exception as e:
            # not typically rate-limited, but handle defensively
            if _is_rate_limit_error(e):
                if attempt >= MAX_RETRIES - 2:
                    _cooldown()
                else:
                    delay = _backoff_delay(attempt)
                    logging.warning(f"Sheets 429 (get_worksheet_by_id-ex) — backoff {delay:.1f}s")
                    time.sleep(delay)
                continue
            raise
    return None

def ss_worksheet_by_name_rl(spreadsheet: gspread.Spreadsheet, name: str) -> Optional[gspread.Worksheet]:
    for attempt in range(MAX_RETRIES):
        try:
            _rate.wait()
            return spreadsheet.worksheet(name)
        except APIError as e:
            if _is_rate_limit_error(e):
                if attempt >= MAX_RETRIES - 2:
                    _cooldown()
                else:
                    delay = _backoff_delay(attempt)
                    logging.warning(f"Sheets 429 (worksheet by name) — backoff {delay:.1f}s")
                    time.sleep(delay)
                continue
            # if not found, propagate
            raise
        except Exception as e:
            if _is_rate_limit_error(e):
                if attempt >= MAX_RETRIES - 2:
                    _cooldown()
                else:
                    delay = _backoff_delay(attempt)
                    logging.warning(f"Sheets 429 (worksheet by name-ex) — backoff {delay:.1f}s")
                    time.sleep(delay)
                continue
            raise

def ss_first_worksheet_rl(spreadsheet: gspread.Spreadsheet) -> gspread.Worksheet:
    for attempt in range(MAX_RETRIES):
        try:
            _rate.wait()
            return spreadsheet.get_worksheet(0)
        except APIError as e:
            if _is_rate_limit_error(e):
                if attempt >= MAX_RETRIES - 2:
                    _cooldown()
                else:
                    delay = _backoff_delay(attempt)
                    logging.warning(f"Sheets 429 (get_worksheet 0) — backoff {delay:.1f}s")
                    time.sleep(delay)
                continue
            raise

def ws_get(ws: gspread.Worksheet, range_name: str, value_render_option: Optional[str] = None):
    for attempt in range(MAX_RETRIES):
        try:
            _rate.wait()
            if value_render_option:
                return ws.get(range_name, value_render_option=value_render_option)
            else:
                return ws.get(range_name)
        except APIError as e:
            if _is_rate_limit_error(e):
                if attempt >= MAX_RETRIES - 2:
                    _cooldown()
                else:
                    delay = _backoff_delay(attempt)
                    logging.warning(f"Sheets 429 — backing off {delay:.1f}s (attempt {attempt+1}/{MAX_RETRIES})")
                    time.sleep(delay)
                continue
            raise

def ws_batch_get(ws: gspread.Worksheet, ranges: List[str], value_render_option: Optional[str] = None):
    for attempt in range(MAX_RETRIES):
        try:
            _rate.wait()
            return ws.batch_get(ranges, value_render_option=value_render_option)
        except APIError as e:
            if _is_rate_limit_error(e):
                if attempt >= MAX_RETRIES - 2:
                    _cooldown()
                else:
                    delay = _backoff_delay(attempt)
                    logging.warning(f"Sheets 429 (batch) — backing off {delay:.1f}s (attempt {attempt+1}/{MAX_RETRIES})")
                    time.sleep(delay)
                continue
            raise

def ws_col_values_rl(ws: gspread.Worksheet, col: int) -> List[str]:
    for attempt in range(MAX_RETRIES):
        try:
            _rate.wait()
            return ws.col_values(col)
        except APIError as e:
            if _is_rate_limit_error(e):
                if attempt >= MAX_RETRIES - 2:
                    _cooldown()
                else:
                    delay = _backoff_delay(attempt)
                    logging.warning(f"Sheets 429 (col_values) — backoff {delay:.1f}s (attempt {attempt+1}/{MAX_RETRIES})")
                    time.sleep(delay)
                continue
            raise

# -------------------- Notion helpers --------------------

def get_prop_safely(page: dict, name: str) -> Optional[dict]:
    props = page.get("properties", {})
    if name in props:
        return props[name]
    lowered = {k.lower(): k for k in props.keys()}
    if name.lower() in lowered:
        return props[lowered[name.lower()]]
    return None

def get_text_property(page: dict, name: str) -> Optional[str]:
    p = get_prop_safely(page, name)
    if not p:
        return None
    t = p.get("type")
    if t == "title":
        arr = p.get("title") or []
        return "".join([r.get("plain_text","") for r in arr]).strip() or None
    if t == "rich_text":
        arr = p.get("rich_text") or []
        return "".join([r.get("plain_text","") for r in arr]).strip() or None
    if t == "select":
        sel = p.get("select"); return sel.get("name") if sel else None
    if t == "status":
        st = p.get("status"); return st.get("name") if st else None
    if t == "url":
        return p.get("url")
    return None

def get_number_property(page: dict, name: str) -> Optional[float]:
    p = get_prop_safely(page, name)
    if not p:
        return None
    if p.get("type") == "number":
        return p.get("number")
    txt = get_text_property(page, name)
    return to_number(txt) if txt is not None else None

def get_url_property(page: dict, name: str) -> Optional[str]:
    p = get_prop_safely(page, name)
    if not p:
        return None
    if p.get("type") == "url":
        return p.get("url")
    return get_text_property(page, name)

def ensure_number_props_on_db(notion: NotionClient, db_id: str):
    db = notion.databases.retrieve(database_id=db_id)
    props = db.get("properties", {})
    to_create = {}
    if PROP_TOTAL_VIEWS not in props and PROP_TOTAL_VIEWS.title() not in props:
        to_create[PROP_TOTAL_VIEWS] = {"number": {}}
    if PROP_PERF_SCORE not in props and PROP_PERF_SCORE.title() not in props:
        to_create[PROP_PERF_SCORE] = {"number": {}}
    if to_create:
        logging.info(f"Adding properties on DB {db_id[-6:]}: {list(to_create.keys())}")
        if not DRY_RUN:
            notion.databases.update(database_id=db_id, properties=to_create)

def iter_parent_pages(notion: NotionClient, database_id: str):
    start = None
    while True:
        resp = notion.databases.query(database_id=database_id, start_cursor=start)
        for p in resp.get("results", []):
            yield p
        if not resp.get("has_more"):
            break
        start = resp.get("next_cursor")

def iter_child_databases(notion: NotionClient, page_id: str, title_filter: Optional[str]) -> List[dict]:
    out = []
    start=None
    while True:
        resp = notion.blocks.children.list(block_id=page_id, start_cursor=start)
        for b in resp.get("results", []):
            if b.get("type") == "child_database":
                title = b["child_database"].get("title")
                if not title_filter or title == title_filter:
                    out.append({"id": b["id"], "title": title})
        if not resp.get("has_more"):
            break
        start = resp.get("next_cursor")
    return out

def iter_db_rows(notion: NotionClient, db_id: str):
    start=None
    while True:
        resp = notion.databases.query(database_id=db_id, start_cursor=start)
        for p in resp.get("results", []):
            yield p
        if not resp.get("has_more"):
            break
        start = resp.get("next_cursor")

# -------------------- Google helpers --------------------

def open_gspread_client() -> gspread.Client:
    if GOOGLE_SERVICE_ACCOUNT_FILE and os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        return gspread.service_account(filename=GOOGLE_SERVICE_ACCOUNT_FILE)
    raise FileNotFoundError("Provide GOOGLE_SERVICE_ACCOUNT_FILE in .env.")

def open_tracker_ws(gc: gspread.Client, tracker_id: str, tab_name: str) -> gspread.Worksheet:
    sh = gc_open_by_key_rl(gc, tracker_id)
    if tab_name:
        try:
            ws = ss_worksheet_by_name_rl(sh, tab_name)
            if ws:
                return ws
            logging.warning(f"Tab '{tab_name}' not found, using first tab.")
        except Exception:
            logging.warning(f"Tab '{tab_name}' not found, using first tab.")
    return ss_first_worksheet_rl(sh)

def get_header_map(ws: gspread.Worksheet) -> Dict[str, int]:
    try:
        rows = ws_get(ws, "1:1")
        headers = rows[0] if rows else []
    except Exception:
        headers = ws.row_values(1)
    return {norm(h): i+1 for i, h in enumerate(headers)}

# -------------------- Child sheet reading --------------------

def pick_child_ws(child: gspread.Spreadsheet, gid: Optional[int], prefer_name: Optional[str]) -> gspread.Worksheet:
    if gid is not None:
        ws = ss_get_worksheet_by_id_rl(child, gid)
        if ws:
            return ws
    if prefer_name:
        try:
            ws = ss_worksheet_by_name_rl(child, prefer_name)
            if ws:
                return ws
        except Exception:
            pass
    return ss_first_worksheet_rl(child)

def find_views_header(ws: gspread.Worksheet, scan_rows: int) -> Tuple[Optional[int], Optional[int]]:
    try:
        rows = ws_get(ws, f"1:{scan_rows}")
    except Exception:
        rows = [ws.row_values(r) for r in range(1, scan_rows+1)]
    target = norm(CHILD_VIEWS_HEADER)
    for r_idx, row in enumerate(rows, start=1):
        for c_idx, h in enumerate(row, start=1):
            if norm(h) == target or contains_views_word(h or ""):
                return r_idx, c_idx
    return None, None

def sum_views_in_child(gc: gspread.Client, out_sid: str, gid: Optional[int]) -> Optional[float]:
    try:
        child = gc_open_by_key_rl(gc, out_sid)
    except Exception as e:
        logging.warning(f"Cannot open child sheet {out_sid}: {e}")
        return None
    ws = pick_child_ws(child, gid, CHILD_OUTPUT_TAB_NAME or None)

    # Determine column and header row
    if CHILD_ASSUME_FIXED_COLUMN and CHILD_VIEWS_FIXED_COL_LETTER:
        vcol = ord(CHILD_VIEWS_FIXED_COL_LETTER.upper()) - 64
        hrow = CHILD_FIXED_HEADER_ROW
    else:
        hrow, vcol = find_views_header(ws, CHILD_HEADER_SCAN_ROWS)
        if not vcol:
            logging.warning(f"No '{CHILD_VIEWS_HEADER}' header found in {child.title}/{ws.title}")
            return None

    # Data starts below the header
    start = (hrow or 1) + 1
    col_letter = col_to_a1(vcol)

    # Single request: use FORMULA to detect bottom totals (=SUM(...))
    try:
        vals = ws_get(ws, f"{col_letter}{start}:{col_letter}", value_render_option="FORMULA")
    except Exception:
        # Fallback with rate-limited col_values
        vals = ws_col_values_rl(ws, vcol)[start-1:]

    # Normalize to 2D
    if vals and isinstance(vals[0], str):
        vals = [[x] for x in vals]

    total = 0.0
    for row in vals:
        cell = row[0] if row else None
        if isinstance(cell, str) and cell.strip().startswith("="):
            # skip totals like =SUM(...)
            if re.search(r"\bSUM\s*KATEX_INLINE_OPEN", cell, re.IGNORECASE):
                continue
            # other formulas: ignore
            continue
        n = to_number(cell)
        if n is not None:
            total += float(n)

    return total

# -------------------- Build totals from main tracker (Input->Output) --------------------

def build_totals_from_tracker_output(gc: gspread.Client) -> Dict[str, float]:
    ws = open_tracker_ws(gc, TRACKER_SHEET_ID, MAIN_SHEET_NAME)
    hmap = get_header_map(ws)

    in_idx = hmap.get(norm(INPUT_HEADER))
    out_idx = hmap.get(norm(OUTPUT_HEADER))
    if not in_idx or not out_idx:
        raise RuntimeError(f"Missing tracker headers. Need '{INPUT_HEADER}' and '{OUTPUT_HEADER}' in row 1.")

    in_letter = col_to_a1(in_idx)
    out_letter = col_to_a1(out_idx)

    # One batch request for Input and Output
    try:
        in_cells, out_cells = ws_batch_get(ws, [f"{in_letter}2:{in_letter}", f"{out_letter}2:{out_letter}"], value_render_option="FORMULA")
    except Exception:
        # Fallback (rare)
        in_vals = ws_col_values_rl(ws, in_idx)[1:]
        out_vals = ws_col_values_rl(ws, out_idx)[1:]
        in_cells = [[v] for v in in_vals]
        out_cells = [[v] for v in out_vals]

    totals: Dict[str, float] = {}
    child_total_cache: Dict[Tuple[str, Optional[int]], Optional[float]] = {}

    nrows = max(len(in_cells), len(out_cells))
    for i in range(nrows):
        in_raw = in_cells[i][0] if i < len(in_cells) and in_cells[i] else ""
        out_raw = out_cells[i][0] if i < len(out_cells) and out_cells[i] else ""
        in_url = extract_first_url(in_raw) or in_raw
        out_url = extract_first_url(out_raw) or out_raw
        in_sid, _ = extract_sid_and_gid(in_url)
        out_sid, gid = extract_sid_and_gid(out_url)
        if not in_sid or not out_sid:
            continue

        key = (out_sid, gid)
        if key not in child_total_cache:
            child_total_cache[key] = sum_views_in_child(gc, out_sid, gid)
        total = child_total_cache.get(key)
        if total is None:
            continue

        # Aggregate in case the same Input SID appears more than once
        totals[in_sid] = totals.get(in_sid, 0.0) + float(total)

    logging.info(f"Built totals from main tracker Output column: {len(totals)} campaigns.")
    return totals

# -------------------- Task 2: Update Notion --------------------

def task2_update(notion: NotionClient, gc: gspread.Client):
    totals = build_totals_from_tracker_output(gc)
    if not totals:
        logging.warning("No totals found from tracker Output links; nothing to update.")
        return

    processed = updated = 0

    for parent in iter_parent_pages(notion, PARENT_DB_ID):
        for child in iter_child_databases(notion, parent.get("id"), CHILD_DB_TITLE):
            ensure_number_props_on_db(notion, child["id"])
            for page in iter_db_rows(notion, child["id"]):
                spreadsheet = get_url_property(page, PROP_SPREADSHEET)
                sid, _ = extract_sid_and_gid(spreadsheet)
                if not sid:
                    continue

                total_views = totals.get(sid)
                if total_views is None:
                    processed += 1
                    continue

                cost = get_number_property(page, PROP_COST)
                perf = (float(total_views) / float(cost)) * 100.0 if (cost is not None and cost > 0) else None

                props = {PROP_TOTAL_VIEWS: {"number": float(total_views)}}
                # Clear score when cost missing/zero to avoid stale values
                props[PROP_PERF_SCORE] = {"number": float(perf)} if perf is not None else {"number": None}

                logging.info(f"Notion update | {get_text_property(page, PROP_CAMPAIGN)} | Views={total_views} | Cost={cost} | Score={perf}")
                if not DRY_RUN:
                    notion.pages.update(page_id=page["id"], properties=props)
                updated += 1
                processed += 1

    logging.info(f"Task2: processed={processed}, Notion updated={updated}{' (DRY RUN)' if DRY_RUN else ''}")

# -------------------- Main --------------------

def main():
    if not NOTION_TOKEN or not PARENT_DB_ID:
        raise RuntimeError("Missing NOTION_TOKEN or NOTION_DATABASE_ID")
    if not GOOGLE_SERVICE_ACCOUNT_FILE or not TRACKER_SHEET_ID:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_FILE or TIKTOK_TRACKER_SPREADSHEET_ID")

    notion = NotionClient(auth=NOTION_TOKEN)
    gc = open_gspread_client()

    logging.info(f"DRY_RUN={DRY_RUN} | RPM_LIMIT={RPM_LIMIT} | Reading Output column '{OUTPUT_HEADER}' from '{MAIN_SHEET_NAME or 'first tab'}'")
    task2_update(notion, gc)
    logging.info("All done.")

if __name__ == "__main__":
    main()