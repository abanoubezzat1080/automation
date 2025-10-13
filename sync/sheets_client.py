from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import gspread
from gspread.utils import rowcol_to_a1


class SheetsClient:
    def __init__(
        self,
        service_account_info: Dict[str, Any],
        sheet_id: str,
        worksheet_name: str,
    ) -> None:
        self.gc = gspread.service_account_from_dict(service_account_info)
        self.sheet = self.gc.open_by_key(sheet_id)
        try:
            self.ws = self.sheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            self.ws = self.sheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)

    def get_header(self) -> List[str]:
        values = self.ws.get_values("1:1")
        if not values or not values[0]:
            return []
        return [str(h).strip() for h in values[0]]

    def ensure_columns(self, required_columns: List[str]) -> List[str]:
        header = self.get_header()
        if not header:
            header = []
        existing = set([h for h in header if h])
        changed = False
        for col in required_columns:
            if col not in existing:
                header.append(col)
                existing.add(col)
                changed = True
        if changed:
            # Update header row
            end_a1 = rowcol_to_a1(1, len(header))
            self.ws.update(f"A1:{end_a1}", [header])
        return header

    def fetch_all_with_row_numbers(self) -> Tuple[List[str], List[Dict[str, Any]]]:
        # Return header and list of dicts including _row_number
        all_values = self.ws.get_all_values()
        if not all_values:
            return [], []
        header = [str(h).strip() for h in all_values[0]]
        rows: List[Dict[str, Any]] = []
        for idx, row in enumerate(all_values[1:], start=2):
            row_dict: Dict[str, Any] = {h: (row[i] if i < len(row) else "") for i, h in enumerate(header)}
            row_dict["_row_number"] = idx
            rows.append(row_dict)
        return header, rows

    def update_row(self, row_number: int, header: List[str], row_data: Dict[str, Any]) -> None:
        # Build values in header order
        values: List[Any] = [row_data.get(h, "") for h in header]
        start = rowcol_to_a1(row_number, 1)
        end = rowcol_to_a1(row_number, len(header))
        self.ws.update(f"{start}:{end}", [values])

    def append_rows(self, header: List[str], rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        values = [[row.get(h, "") for h in header] for row in rows]
        self.ws.append_rows(values, value_input_option="USER_ENTERED")
