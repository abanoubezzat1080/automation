import os
import re
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from notion_client import Client as NotionClient
import gspread

# -------------------- Utils --------------------

def env_get(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = v.strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1].strip()
    return v

def norm_header(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def today_str(fmt: str = "%Y-%m-%d") -> str:
    return datetime.now().strftime(fmt)

GSHEET_ID_RE = re.compile(r"/spreadsheets/d/([A-Za-z0-9_-]+)")
URL_RE = re.compile(r'https?://[^\s")]+')

def extract_first_url(cell_value: Optional[str]) -> Optional[str]:
    if not cell_value:
        return None
    m = URL_RE.search(str(cell_value))
    return m.group(0) if m else None

def extract_spreadsheet_id(url_or_id: Optional[str]) -> Optional[str]:
    if not url_or_id:
        return None
    s = (url_or_id or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", s) and "://" not in s:
        return s
    m = GSHEET_ID_RE.search(s)
    return m.group(1) if m else None

def canonical_gsheet_url(url_or_id: str) -> str:
    sid = extract_spreadsheet_id(url_or_id) or (url_or_id or "").strip()
    return f"https://docs.google.com/spreadsheets/d/{sid}" if sid else (url_or_id or "").strip()

def col_to_a1(col_idx: int) -> str:
    result = ""
    while col_idx:
        col_idx, rem = divmod(col_idx - 1, 26)
        result = chr(rem + 65) + result
    return result

def letter_to_col_idx(letter: str) -> int:
    s = (letter or "").strip().upper()
    if not s or not s.isalpha():
        return 0
    n = 0
    for ch in s:
        n = n * 26 + (ord(ch) - 64)
    return n

def last_used_row(ws: gspread.Worksheet, anchor_letter: str = "A") -> int:
    idx = letter_to_col_idx(anchor_letter)
    vals = ws.col_values(idx)
    return len(vals)

# -------------------- Load env --------------------

load_dotenv()

# Notion
NOTION_TOKEN = env_get("NOTION_TOKEN")
PARENT_DB_ID = env_get("NOTION_DATABASE_ID")
CHILD_DB_TITLE = env_get("NOTION_CHILD_DB_TITLE", "Campaigns Summary")

# Notion properties
PROP_CAMPAIGN = env_get("NOTION_PROP_CAMPAIGN", "Campaign")
PROP_SPREADSHEET = env_get("NOTION_PROP_SPREADSHEET", "Spreadsheet")

# Google
GOOGLE_SERVICE_ACCOUNT_FILE = env_get("GOOGLE_SERVICE_ACCOUNT_FILE")

# Your tracker (hard-coded fallback to the sheet you shared)
TRACKER_SHEET_ID = env_get("TIKTOK_TRACKER_SPREADSHEET_ID", "1outpRjn5_g1iCIJr0JIzkUiXZa1i0SSoogHw0blNcaE")
MAIN_SHEET_NAME = env_get("MAIN_SHEET_NAME", "")  # leave blank to use gid=0
MAIN_SHEET_GID = int(env_get("MAIN_SHEET_GID", "0"))  # we will select tab by gid=0

# Tracker headers (as in your screenshot)
INPUT_COL_HEADER_NAME = env_get("TRACKER_INPUT_COLUMN_HEADER", "Input")
CAMPAIGN_TITLE_COL_HEADER_NAME = env_get("TRACKER_CAMPAIGN_TITLE_COLUMN_HEADER", "Campaign Title")
UPDATED_BY_SCRIPT_COL_HEADER_NAME = env_get("TRACKER_UPDATED_BY_SCRIPT_COLUMN_HEADER", "updated by script")

# Columns to fill by default on append (like your view)
DEFAULT_ACTIVE_VALUE = env_get("DEFAULT_ACTIVE_VALUE", "Yes")             # column B "Active"
DEFAULT_TYPE_VALUE = env_get("DEFAULT_TYPE_VALUE", "Standard")            # column D "Type Of Campaign"
DATE_COL_HEADER_NAME = env_get("DATE_COL_HEADER_NAME", "Start Date")      # column F "Start Date"
DATE_WRITE_FORMAT = env_get("DATE_WRITE_FORMAT", "%Y-%m-%d")
OUTPUT_COL_HEADER_NAME = env_get("OUTPUT_COL_HEADER_NAME", "Output")      # column C "Output" -> empty

# Behavior
UPDATE_EXISTING_TIMESTAMPS = env_get("UPDATE_EXISTING_TIMESTAMPS", "true").lower() in {"1","true","yes","y","on"}
LOG_LEVEL = env_get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")

# -------------------- Notion helpers --------------------

def get_prop_safely(page: dict, name: str) -> Optional[dict]:
    props = page.get("properties", {})
    if name in props:
        return props[name]
    lowered = {k.lower(): k for k in props.keys()}
    if name.lower() in lowered:
        return props[lowered[name.lower()]]
    return None

def get_any_property(page: dict, name: str) -> Optional[str]:
    prop = get_prop_safely(page, name)
    if not prop:
        return None
    t = prop.get("type")
    if t == "title":
        arr = prop.get("title") or []
        return "".join([r.get("plain_text","") for r in arr]).strip() or None
    if t == "rich_text":
        arr = prop.get("rich_text") or []
        return "".join([r.get("plain_text","") for r in arr]).strip() or None
    if t == "url":
        return prop.get("url") or None
    if t == "select":
        sel = prop.get("select")
        return sel.get("name") if sel else None
    if t == "status":
        st = prop.get("status")
        return st.get("name") if st else None
    if t == "number":
        n = prop.get("number")
        return str(n) if n is not None else None
    if t == "checkbox":
        return "Yes" if prop.get("checkbox") else "No"
    if t == "date":
        d = prop.get("date")
        if d and d.get("start"):
            return d["start"]
    return None

def iter_parent_pages(notion: NotionClient, database_id: str):
    start = None
    while True:
        resp = notion.databases.query(database_id=database_id, start_cursor=start)
        for p in resp.get("results", []): yield p
        if not resp.get("has_more"): break
        start = resp.get("next_cursor")

def iter_child_databases(notion: NotionClient, page_id: str, title_filter: Optional[str]) -> List[dict]:
    out = []
    start = None
    while True:
        resp = notion.blocks.children.list(block_id=page_id, start_cursor=start)
        for b in resp.get("results", []):
            if b.get("type") == "child_database":
                title = b["child_database"].get("title")
                if not title_filter or title == title_filter:
                    out.append({"id": b["id"], "title": title})
        if not resp.get("has_more"): break
        start = resp.get("next_cursor")
    return out

def iter_db_rows(notion: NotionClient, database_id: str):
    start = None
    while True:
        resp = notion.databases.query(database_id=database_id, start_cursor=start)
        for p in resp.get("results", []): yield p
        if not resp.get("has_more"): break
        start = resp.get("next_cursor")

def collect_campaigns(notion: NotionClient) -> List[dict]:
    items: List[dict] = []
    for song_page in iter_parent_pages(notion, PARENT_DB_ID):
        for child_db in iter_child_databases(notion, song_page.get("id"), title_filter=CHILD_DB_TITLE):
            for camp_page in iter_db_rows(notion, child_db["id"]):
                items.append({"page": camp_page})
    return items

# -------------------- Google Sheets helpers --------------------

def open_gspread_client() -> gspread.Client:
    if GOOGLE_SERVICE_ACCOUNT_FILE and os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        return gspread.service_account(filename=GOOGLE_SERVICE_ACCOUNT_FILE)
    raise FileNotFoundError("Provide GOOGLE_SERVICE_ACCOUNT_FILE in .env.")

def find_worksheet_by_gid(sh: gspread.Spreadsheet, gid: int) -> gspread.Worksheet:
    for ws in sh.worksheets():
        if ws.id == gid:
            return ws
    # fallback
    return sh.get_worksheet(0)

def find_main_worksheet(sh: gspread.Spreadsheet, prefer_title: str, gid: int) -> gspread.Worksheet:
    if prefer_title:
        try: return sh.worksheet(prefer_title)
        except Exception: pass
    # prefer gid
    return find_worksheet_by_gid(sh, gid)

def get_header_map(ws: gspread.Worksheet) -> Tuple[List[str], Dict[str, int]]:
    headers = ws.row_values(1)
    return headers, {norm_header(h): i+1 for i,h in enumerate(headers)}

def build_existing_input_index(ws: gspread.Worksheet, input_col_idx: int) -> Dict[str, int]:
    col_letter = col_to_a1(input_col_idx)
    try:
        cells = ws.get(f"{col_letter}2:{col_letter}", value_render_option="FORMULA")
        column = [(i+2, (cells[i][0] if i < len(cells) and cells[i] else "")) for i in range(len(cells))]
    except Exception:
        values = ws.col_values(input_col_idx)
        column = [(i+2, v) for i, v in enumerate(values[1:])]
    idx: Dict[str, int] = {}
    for r_idx, raw in column:
        url = extract_first_url(raw) or raw
        sid = extract_spreadsheet_id(url)
        if sid and sid not in idx:
            idx[sid] = r_idx
    return idx

# -------------------- Task 1 (append full row) --------------------

def task1_sync_tracker(tracker_ws: gspread.Worksheet,
                       headers: List[str],
                       header_map: Dict[str,int],
                       items: List[dict]) -> Tuple[int, int, Optional[int], Optional[int]]:
    """
    Append new Notion rows as full rows:
      - Input (canonical URL)
      - Campaign Title
      - Active (Yes)
      - Type Of Campaign (Standard)
      - Output (empty)
      - Start Date (today)
      - updated by script (if column exists)
    Dedupe by Input (Spreadsheet ID).
    """
    input_col_idx = header_map.get(norm_header(INPUT_COL_HEADER_NAME))
    if not input_col_idx:
        raise RuntimeError(f"Tracker Input column '{INPUT_COL_HEADER_NAME}' not found.")
    updated_col_idx = header_map.get(norm_header(UPDATED_BY_SCRIPT_COL_HEADER_NAME))

    existing_index = build_existing_input_index(tracker_ws, input_col_idx)

    rows_to_append: List[List[str]] = []
    ts_updates: List[Tuple[int, str]] = []

    for obj in items:
        page = obj["page"]

        spreadsheet_val = get_any_property(page, PROP_SPREADSHEET)
        sid = extract_spreadsheet_id(spreadsheet_val) if spreadsheet_val else None
        if not sid:
            continue

        if sid in existing_index:
            if UPDATE_EXISTING_TIMESTAMPS and updated_col_idx:
                ts_updates.append((existing_index[sid], now_utc_str()))
            continue

        # Build row by sheet headers
        assign: Dict[str, str] = {}

        # Input
        assign[norm_header(INPUT_COL_HEADER_NAME)] = canonical_gsheet_url(spreadsheet_val)

        # Campaign Title
        campaign_name = get_any_property(page, PROP_CAMPAIGN) or ""
        if header_map.get(norm_header(CAMPAIGN_TITLE_COL_HEADER_NAME)):
            assign[norm_header(CAMPAIGN_TITLE_COL_HEADER_NAME)] = campaign_name

        # Active default
        assign[norm_header("Active")] = DEFAULT_ACTIVE_VALUE

        # Output empty
        assign[norm_header(OUTPUT_COL_HEADER_NAME)] = ""

        # Type Of Campaign default
        assign[norm_header("Type Of Campaign")] = DEFAULT_TYPE_VALUE

        # Start Date default
        if DATE_COL_HEADER_NAME:
            assign[norm_header(DATE_COL_HEADER_NAME)] = today_str(DATE_WRITE_FORMAT)

        # updated by script
        if updated_col_idx:
            assign[norm_header(UPDATED_BY_SCRIPT_COL_HEADER_NAME)] = now_utc_str()

        # Align to existing headers
        row = [assign.get(norm_header(h), "") for h in headers]
        rows_to_append.append(row)

        existing_index[sid] = -1  # prevent duplicates in this run

    new_rows = 0
    start_row = None
    end_row = None
    if rows_to_append:
        start_row = last_used_row(tracker_ws, "A") + 1
        end_row = start_row + len(rows_to_append) - 1
        logging.info(f"Task1: appending {len(rows_to_append)} rows to '{tracker_ws.title}' starting at row {start_row}.")
        tracker_ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        new_rows = len(rows_to_append)

    ts_updated = 0
    if ts_updates and updated_col_idx:
        col_letter = col_to_a1(updated_col_idx)
        data = [{"range": f"{col_letter}{r}", "values": [[ts]]} for r, ts in ts_updates if r > 0]
        if data:
            try:
                tracker_ws.batch_update(data, value_input_option="USER_ENTERED")
            except TypeError:
                tracker_ws.batch_update(data)
            ts_updated = len(data)

    return new_rows, ts_updated, start_row, end_row

# -------------------- Main --------------------

def main():
    if not NOTION_TOKEN or not PARENT_DB_ID:
        raise RuntimeError("Missing NOTION_TOKEN or NOTION_DATABASE_ID in .env")
    if not GOOGLE_SERVICE_ACCOUNT_FILE:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_FILE in .env")

    notion = NotionClient(auth=NOTION_TOKEN)
    gc = open_gspread_client()

    tracker = gc.open_by_key(TRACKER_SHEET_ID)
    ws = find_main_worksheet(tracker, MAIN_SHEET_NAME, MAIN_SHEET_GID)

    headers, header_map = get_header_map(ws)

    logging.info(f"Writing to spreadsheet: '{tracker.title}' (id={tracker.id})")
    logging.info(f"Tab: '{ws.title}' (gid={ws.id})")
    logging.info(f"Open tab URL: https://docs.google.com/spreadsheets/d/{tracker.id}/edit#gid={ws.id}")

    items = collect_campaigns(notion)
    logging.info(f"Found {len(items)} campaigns in Notion child databases.")

    new_rows, ts_updated, start_row, end_row = task1_sync_tracker(ws, headers, header_map, items)
    logging.info(f"Task1: new rows appended={new_rows}, timestamps updated={ts_updated}")
    if new_rows and start_row:
        logging.info(f"Jump to first appended row: https://docs.google.com/spreadsheets/d/{tracker.id}/edit#gid={ws.id}&range=A{start_row}")
        if end_row and end_row > start_row:
            logging.info(f"Appended range: rows {start_row}..{end_row}")

    logging.info("Done. If you don't see rows: turn off filters, unhide rows, and open the 'Open tab URL' above.")

if __name__ == "__main__":
    main()