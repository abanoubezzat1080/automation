#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from dotenv import load_dotenv
from notion_client import Client
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


SPREADSHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


@dataclass
class SyncConfig:
    notion_token: str
    notion_database_id: str
    service_account_file: str
    tracker_spreadsheet_id: str
    output_sheet_name: str = "output"
    links_sheet_name: Optional[str] = None
    links_column: str = "A"
    dry_run: bool = True
    log_level: str = "INFO"


class NotionSheetsSync:
    def __init__(self, config: SyncConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.notion = Client(auth=config.notion_token)
        creds = Credentials.from_service_account_file(
            config.service_account_file, scopes=[SPREADSHEETS_SCOPE]
        )
        self.sheets = build("sheets", "v4", credentials=creds)

    # -------------------------- Public API ---------------------------
    def run(self) -> Dict[str, Any]:
        self._ensure_db_metrics_properties()

        pages = self._get_all_database_pages(self.config.notion_database_id)
        self.logger.info("Loaded %d Notion pages", len(pages))

        # 1) Push spreadsheet links to tracker, deduped
        spreadsheet_urls = [self._extract_spreadsheet_url_from_page(p) for p in pages]
        spreadsheet_urls = [u for u in spreadsheet_urls if u]
        self.logger.info("Found %d spreadsheet URLs in Notion", len(spreadsheet_urls))

        added_links = self._sync_links_to_tracker(spreadsheet_urls)

        # 2) Compute views and update metrics in Notion
        views_by_sheet_id, global_views = self._read_output_views_map()

        updated_pages: List[Tuple[str, int, float]] = []
        for page in pages:
            page_id = page.get("id")
            budget = self._extract_budget_from_page(page) or 0.0
            sheet_url = self._extract_spreadsheet_url_from_page(page)
            per_page_views = None

            if sheet_url:
                sheet_id = extract_sheet_id(sheet_url)
                if sheet_id and sheet_id in views_by_sheet_id:
                    per_page_views = views_by_sheet_id[sheet_id]

            total_views_for_page = (
                per_page_views if per_page_views is not None else global_views
            )

            perf_score = (
                (total_views_for_page / budget * 100.0) if budget and budget > 0 else 0.0
            )

            self._update_page_metrics(page_id, total_views_for_page, perf_score)
            updated_pages.append((page_id, total_views_for_page, perf_score))

        return {
            "pages_processed": len(pages),
            "links_found": len(spreadsheet_urls),
            "links_added": len(added_links),
            "global_views": global_views,
            "updated_pages": updated_pages,
        }

    # --------------------- Notion: Database schema -------------------
    def _ensure_db_metrics_properties(self) -> None:
        db = self.notion.databases.retrieve(database_id=self.config.notion_database_id)
        props = db.get("properties", {})
        need_total = "Total views" not in props
        need_perf = "Performance score (views per $100)" not in props
        if not (need_total or need_perf):
            return

        patch: Dict[str, Any] = {"properties": {}}
        if need_total:
            patch["properties"]["Total views"] = {"number": {}}
        if need_perf:
            patch["properties"]["Performance score (views per $100)"] = {"number": {}}

        self.logger.info("Ensuring Notion database has metrics properties: %s", list(patch["properties"].keys()))
        if not self.config.dry_run:
            self.notion.databases.update(
                database_id=self.config.notion_database_id, **patch
            )

    # --------------------- Notion: Read database ---------------------
    def _get_all_database_pages(self, database_id: str) -> List[Dict[str, Any]]:
        pages: List[Dict[str, Any]] = []
        start_cursor: Optional[str] = None
        while True:
            resp = self.notion.databases.query(
                database_id=database_id, start_cursor=start_cursor
            )
            pages.extend(resp.get("results", []))
            if not resp.get("has_more"):
                break
            start_cursor = resp.get("next_cursor")
        return pages

    def _extract_spreadsheet_url_from_page(self, page: Dict[str, Any]) -> Optional[str]:
        props = page.get("properties", {})
        # Prefer a property literally named "Spreadsheet"
        if "Spreadsheet" in props:
            val = props["Spreadsheet"]
            if val.get("type") == "url":
                return val.get("url")
            if val.get("type") in ("rich_text", "title"):
                arr = val.get(val.get("type"), [])
                if arr:
                    return arr[0].get("plain_text") or arr[0].get("text", {}).get("content")
        # Otherwise, heuristically search for the first URL property
        for name, val in props.items():
            if val.get("type") == "url" and val.get("url"):
                return val.get("url")
        return None

    def _extract_budget_from_page(self, page: Dict[str, Any]) -> Optional[float]:
        props = page.get("properties", {})
        # Prefer property named "Budget"
        if "Budget" in props and props["Budget"].get("type") == "number":
            return props["Budget"].get("number")
        # Heuristic: first number property
        for name, val in props.items():
            if val.get("type") == "number":
                return val.get("number")
        return None

    def _update_page_metrics(self, page_id: str, total_views: int, perf_score: float) -> None:
        self.logger.debug(
            "Update metrics for page %s: total_views=%s perf=%s",
            page_id,
            total_views,
            perf_score,
        )
        if self.config.dry_run:
            return
        self.notion.pages.update(
            page_id=page_id,
            properties={
                "Total views": {"number": float(total_views)},
                "Performance score (views per $100)": {"number": float(perf_score)},
            },
        )

    # --------------------- Sheets: Links sync ------------------------
    def _get_tracker_links_sheet_title(self) -> str:
        if self.config.links_sheet_name:
            return self.config.links_sheet_name
        # Fetch first sheet title if not specified
        meta = (
            self.sheets.spreadsheets()
            .get(spreadsheetId=self.config.tracker_spreadsheet_id)
            .execute()
        )
        sheets = meta.get("sheets", [])
        if not sheets:
            raise RuntimeError("Tracker spreadsheet has no sheets")
        return sheets[0]["properties"]["title"]

    def _read_existing_tracker_links(self) -> Tuple[Set[str], List[str]]:
        sheet_title = self._get_tracker_links_sheet_title()
        rng = f"'{sheet_title}'!{self.config.links_column}:{self.config.links_column}"
        resp = (
            self.sheets.spreadsheets()
            .values()
            .get(spreadsheetId=self.config.tracker_spreadsheet_id, range=rng)
            .execute()
        )
        values = resp.get("values", [])
        flat: List[str] = [row[0] for row in values if row]
        existing_ids: Set[str] = set()
        for url in flat:
            sid = extract_sheet_id(url)
            if sid:
                existing_ids.add(sid)
        return existing_ids, flat

    def _sync_links_to_tracker(self, urls: List[str]) -> List[str]:
        if not urls:
            return []
        existing_ids, existing_raw = self._read_existing_tracker_links()
        to_add: List[str] = []
        for url in urls:
            sid = extract_sheet_id(url)
            if not sid:
                continue
            if sid not in existing_ids:
                to_add.append(url)
        if not to_add:
            self.logger.info("No new links to add; tracker is up to date")
            return []

        self.logger.info("Adding %d new links to tracker", len(to_add))
        if self.config.dry_run:
            return to_add

        sheet_title = self._get_tracker_links_sheet_title()
        rng = f"'{sheet_title}'!{self.config.links_column}:{self.config.links_column}"
        body = {"values": [[u] for u in to_add]}
        (
            self.sheets.spreadsheets()
            .values()
            .append(
                spreadsheetId=self.config.tracker_spreadsheet_id,
                range=rng,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )
        return to_add

    # --------------------- Sheets: Output views ----------------------
    def _read_output_views_map(self) -> Tuple[Dict[str, int], int]:
        """Return mapping of sheet_id -> views, and global total views.

        If the output sheet has a column naming a spreadsheet or link, we
        aggregate per-sheet. Otherwise we compute the global sum only.
        """
        title = self.config.output_sheet_name
        rng = f"'{title}'!A:ZZ"
        try:
            resp = (
                self.sheets.spreadsheets()
                .values()
                .get(spreadsheetId=self.config.tracker_spreadsheet_id, range=rng)
                .execute()
            )
        except Exception as e:
            self.logger.warning("Failed to read output sheet '%s': %s", title, e)
            return {}, 0

        rows: List[List[str]] = resp.get("values", [])
        if not rows:
            return {}, 0

        headers = [h.strip() for h in rows[0]] if rows else []
        header_to_index = {h.lower(): i for i, h in enumerate(headers)}

        link_col_idx = self._find_header_index(headers, [
            "spreadsheet", "sheet", "url", "link", "spreadsheet id", "sheet id"
        ])
        views_col_idx = self._find_header_index(headers, [
            "views", "total views", "view count"
        ])

        def parse_int(cell: str) -> int:
            if cell is None:
                return 0
            txt = str(cell)
            m = re.findall(r"[-+]?\d+", txt.replace(",", ""))
            if not m:
                return 0
            try:
                return int(m[0])
            except Exception:
                return 0

        per_sheet: Dict[str, int] = {}
        global_sum = 0
        for r in rows[1:]:
            views = parse_int(r[views_col_idx]) if (views_col_idx is not None and views_col_idx < len(r)) else 0
            global_sum += views
            if link_col_idx is not None and link_col_idx < len(r):
                sid = extract_sheet_id(r[link_col_idx])
                if sid:
                    per_sheet[sid] = per_sheet.get(sid, 0) + views

        return per_sheet, global_sum

    @staticmethod
    def _find_header_index(headers: List[str], candidates: Iterable[str]) -> Optional[int]:
        lower = [h.lower() for h in headers]
        for i, h in enumerate(lower):
            for cand in candidates:
                if cand in h:
                    return i
        return None


# --------------------------- Helpers ---------------------------------

def extract_sheet_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    return m.group(1) if m else None


def parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


# ------------------------------ CLI ---------------------------------

def build_config_from_env_and_args() -> SyncConfig:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Sync Notion database with Google Sheets tracker: links + metrics"
    )
    parser.add_argument("--notion-database-id", default=os.getenv("NOTION_DATABASE_ID"))
    parser.add_argument("--notion-token", default=os.getenv("NOTION_TOKEN"))
    parser.add_argument(
        "--service-account-file",
        default=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE"),
    )
    parser.add_argument(
        "--tracker-spreadsheet-id",
        default=os.getenv("TIKTOK_TRACKER_SPREADSHEET_ID"),
    )
    parser.add_argument(
        "--output-sheet-name",
        default=os.getenv("OUTPUT_SHEET_NAME", "output"),
    )
    parser.add_argument(
        "--links-sheet-name",
        default=os.getenv("TIKTOK_LINKS_SHEET_NAME"),
        help="Optional: Sheet tab name where links are stored (defaults to first tab)",
    )
    parser.add_argument(
        "--links-column",
        default=os.getenv("TIKTOK_LINKS_COLUMN", "A"),
        help="Column letter where links are stored (default A)",
    )
    parser.add_argument(
        "--dry-run",
        default=os.getenv("DRY_RUN", "True"),
        help="True/False: if True, no writes to Notion or Sheets",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level: DEBUG, INFO, WARNING, ERROR",
    )

    args = parser.parse_args()

    cfg = SyncConfig(
        notion_token=args.notion_token,
        notion_database_id=args.notion_database_id,
        service_account_file=args.service_account_file,
        tracker_spreadsheet_id=args.tracker_spreadsheet_id,
        output_sheet_name=args.output_sheet_name,
        links_sheet_name=args.links_sheet_name,
        links_column=args.links_column,
        dry_run=parse_bool(args.dry_run, default=True),
        log_level=args.log_level,
    )
    return cfg


def main() -> None:
    cfg = build_config_from_env_and_args()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    sync = NotionSheetsSync(cfg)
    result = sync.run()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
