from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import gspread
from gspread.utils import rowcol_to_a1


@dataclass
class MetaRecord:
    key: str
    notion_page_id: Optional[str]
    notion_last_edited_time: Optional[str]
    sheets_row_hash: Optional[str]
    last_synced_at: Optional[str]


class SheetsWrapper:
    def __init__(
        self,
        spreadsheet_id: str,
        worksheet_title: str,
        credentials_json: Optional[str],
        meta_sheet_title: str = "_SyncMeta",
    ) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_title = worksheet_title
        self.meta_sheet_title = meta_sheet_title
        self.gc = self._authorize(credentials_json)
        self.spreadsheet = self.gc.open_by_key(spreadsheet_id)
        self.worksheet = self._get_or_create_worksheet(worksheet_title)
        self.meta_sheet = self._get_or_create_worksheet(meta_sheet_title)

    def _authorize(self, credentials_json: Optional[str]):
        if credentials_json and os.path.exists(credentials_json):
            return gspread.service_account(filename=credentials_json)
        # Try env var JSON
        env_json_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if env_json_path and os.path.exists(env_json_path):
            return gspread.service_account(filename=env_json_path)
        env_json_blob = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if env_json_blob:
            info = json.loads(env_json_blob)
            return gspread.service_account_from_dict(info)
        raise RuntimeError(
            "Google credentials not found. Provide 'credentials_json' in config or set GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_CREDENTIALS_JSON."
        )

    def _get_or_create_worksheet(self, title: str):
        try:
            return self.spreadsheet.worksheet(title)
        except gspread.WorksheetNotFound:
            return self.spreadsheet.add_worksheet(title=title, rows=100, cols=26)

    def ensure_headers(self, headers: List[str]) -> None:
        current = self.worksheet.row_values(1)
        if not current:
            self.worksheet.update(f"A1:{rowcol_to_a1(1, len(headers))}", [headers])
            return
        # Ensure all headers exist, append missing at end
        header_set = {h for h in current if h}
        missing = [h for h in headers if h not in header_set]
        if missing:
            new_headers = current + missing
            self.worksheet.update(f"A1:{rowcol_to_a1(1, len(new_headers))}", [new_headers])

    def get_all_rows(self) -> List[Dict[str, Any]]:
        return self.worksheet.get_all_records()

    def _get_header_index(self, header: str) -> Optional[int]:
        headers = self.worksheet.row_values(1)
        for idx, h in enumerate(headers, start=1):
            if h == header:
                return idx
        return None

    def find_row_number_by_key(self, key_column: str, key_value: str) -> Optional[int]:
        col_idx = self._get_header_index(key_column)
        if col_idx is None:
            return None
        col_values = self.worksheet.col_values(col_idx)
        # Skip header row at index 0
        for i in range(1, len(col_values)):
            if str(col_values[i]).strip() == str(key_value).strip():
                return i + 1  # 1-based row number including header
        return None

    def _headers(self) -> List[str]:
        return self.worksheet.row_values(1)

    def upsert_row(self, key_column: str, key_value: str, row_values_by_header: Dict[str, Any]) -> int:
        headers = self._headers()
        # Ensure all headers present
        for h in row_values_by_header.keys():
            if h not in headers:
                headers.append(h)
        self.worksheet.update(f"A1:{rowcol_to_a1(1, len(headers))}", [headers])

        row_num = self.find_row_number_by_key(key_column, key_value)
        values_in_order = [row_values_by_header.get(h, "") for h in headers]
        if row_num is None:
            # Append new row
            self.worksheet.append_row(values_in_order, value_input_option="USER_ENTERED")
            # Determine row number as last non-empty row
            row_num = len(self.worksheet.col_values(1))
            return row_num
        else:
            # Update existing row
            range_a1 = f"A{row_num}:{rowcol_to_a1(row_num, len(headers))}"
            self.worksheet.update(range_a1, [values_in_order], value_input_option="USER_ENTERED")
            return row_num

    # Meta sheet operations
    def ensure_meta_headers(self) -> None:
        headers = [
            "key",
            "notion_page_id",
            "notion_last_edited_time",
            "sheets_row_hash",
            "last_synced_at",
        ]
        current = self.meta_sheet.row_values(1)
        if not current:
            self.meta_sheet.update(f"A1:{rowcol_to_a1(1, len(headers))}", [headers])
            return
        header_set = {h for h in current if h}
        missing = [h for h in headers if h not in header_set]
        if missing:
            new_headers = current + missing
            self.meta_sheet.update(f"A1:{rowcol_to_a1(1, len(new_headers))}", [new_headers])

    def load_meta(self) -> Dict[str, MetaRecord]:
        self.ensure_meta_headers()
        records = self.meta_sheet.get_all_records()
        meta: Dict[str, MetaRecord] = {}
        for r in records:
            k = str(r.get("key")) if r.get("key") is not None else None
            if not k:
                continue
            meta[k] = MetaRecord(
                key=k,
                notion_page_id=r.get("notion_page_id") or None,
                notion_last_edited_time=r.get("notion_last_edited_time") or None,
                sheets_row_hash=r.get("sheets_row_hash") or None,
                last_synced_at=r.get("last_synced_at") or None,
            )
        return meta

    def upsert_meta(self, rec: MetaRecord) -> None:
        self.ensure_meta_headers()
        headers = self.meta_sheet.row_values(1)
        # Find row by key
        key_col_idx = None
        for idx, h in enumerate(headers, start=1):
            if h == "key":
                key_col_idx = idx
                break
        if key_col_idx is None:
            return
        col_values = self.meta_sheet.col_values(key_col_idx)
        row_num: Optional[int] = None
        for i in range(1, len(col_values)):
            if str(col_values[i]).strip() == str(rec.key).strip():
                row_num = i + 1
                break
        row_values_by_header = {
            "key": rec.key,
            "notion_page_id": rec.notion_page_id or "",
            "notion_last_edited_time": rec.notion_last_edited_time or "",
            "sheets_row_hash": rec.sheets_row_hash or "",
            "last_synced_at": rec.last_synced_at or "",
        }
        # Ensure headers exist
        for h in row_values_by_header.keys():
            if h not in headers:
                headers.append(h)
        self.meta_sheet.update(f"A1:{rowcol_to_a1(1, len(headers))}", [headers])
        values_in_order = [row_values_by_header.get(h, "") for h in headers]
        if row_num is None:
            self.meta_sheet.append_row(values_in_order, value_input_option="USER_ENTERED")
        else:
            range_a1 = f"A{row_num}:{rowcol_to_a1(row_num, len(headers))}"
            self.meta_sheet.update(range_a1, [values_in_order], value_input_option="USER_ENTERED")
