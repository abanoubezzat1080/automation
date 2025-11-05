from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from dateutil import parser as date_parser
from .sheets_client import SheetsClient
from .notion_client import NotionClientWrapper
from .mapping import parse_iso_datetime, to_iso_datetime


class SyncService:
    def __init__(
        self,
        sheets: SheetsClient,
        notion: NotionClientWrapper,
        sync_id_column: str,
        updated_at_column: str,
        notion_sync_id_property: str,
        notion_sync_updated_at_property: str,
    ) -> None:
        self.sheets = sheets
        self.notion = notion
        self.sync_id_column = sync_id_column
        self.updated_at_column = updated_at_column
        self.notion_sync_id_property = notion_sync_id_property
        self.notion_sync_updated_at_property = notion_sync_updated_at_property

    def infer_mapping(self, header: List[str]) -> Dict[str, str]:
        # Map same-name columns to same-name Notion properties where types are compatible-ish
        db_props = self.notion.database_properties()
        mapping: Dict[str, str] = {}
        for col in header:
            if col in (self.sync_id_column, self.updated_at_column):
                continue
            if col in db_props:
                mapping[col] = col
        # Ensure sync fields are present if exist in db
        if self.notion_sync_id_property in db_props:
            mapping[self.sync_id_column] = self.notion_sync_id_property
        if self.notion_sync_updated_at_property in db_props:
            mapping[self.updated_at_column] = self.notion_sync_updated_at_property
        return mapping

    def _row_updated_at(self, row: Dict[str, Any]) -> Optional[datetime]:
        return parse_iso_datetime(row.get(self.updated_at_column))

    def _page_updated_at(self, page_row: Dict[str, Any]) -> Optional[datetime]:
        return parse_iso_datetime(page_row.get(self.notion_sync_updated_at_property))

    def _row_sync_id(self, row: Dict[str, Any]) -> Optional[str]:
        sid = row.get(self.sync_id_column)
        return sid or None

    def _page_sync_id(self, page_row: Dict[str, Any]) -> Optional[str]:
        sid = page_row.get(self.notion_sync_id_property)
        return sid or None

    def _generate_sync_id(self) -> str:
        from uuid import uuid4
        return str(uuid4())

    def two_way_sync(self) -> Dict[str, int]:
        # Ensure header has the required sync columns
        header = self.sheets.ensure_columns([self.sync_id_column, self.updated_at_column])

        mapping = self.infer_mapping(header)

        # Read Sheets and Notion
        header, sheet_rows = self.sheets.fetch_all_with_row_numbers()
        notion_pages = self.notion.list_all_pages()
        notion_rows = [self.notion.extract_plain_row(p) for p in notion_pages]

        # Index by sync_id; for rows/pages without sync_id, index by content hash placeholder
        by_sync_sheet: Dict[str, Dict[str, Any]] = {}
        unsynced_sheet: List[Dict[str, Any]] = []
        for r in sheet_rows:
            sid = self._row_sync_id(r)
            if sid:
                by_sync_sheet[sid] = r
            else:
                unsynced_sheet.append(r)

        by_sync_notion: Dict[str, Dict[str, Any]] = {}
        unsynced_notion: List[Dict[str, Any]] = []
        for r in notion_rows:
            sid = self._page_sync_id(r)
            if sid:
                by_sync_notion[sid] = r
            else:
                unsynced_notion.append(r)

        updates_to_sheet: List[Tuple[int, Dict[str, Any]]] = []  # (row_number, row_data)
        appends_to_sheet: List[Dict[str, Any]] = []
        upserts_to_notion: List[Tuple[Optional[str], Dict[str, Any]]] = []  # (page_id, properties)

        # Reconcile by sync_id
        for sid, srow in by_sync_sheet.items():
            nrow = by_sync_notion.get(sid)
            s_updated = self._row_updated_at(srow)
            n_updated = self._page_updated_at(nrow) if nrow else None
            # If counterpart missing, treat as new on the other side
            if not nrow:
                props = self.notion.prepare_properties_from_sheet_row(srow, mapping)
                props[self.notion_sync_id_property] = {"rich_text": [{"type": "text", "text": {"content": sid}}]}
                props[self.notion_sync_updated_at_property] = {"date": {"start": to_iso_datetime(s_updated or datetime.now(timezone.utc))}}
                upserts_to_notion.append((None, props))
                continue
            # Both present: choose the newer
            if (s_updated and n_updated and s_updated > n_updated) or (s_updated and not n_updated):
                # Update Notion from Sheets
                props = self.notion.prepare_properties_from_sheet_row(srow, mapping)
                props[self.notion_sync_updated_at_property] = {"date": {"start": to_iso_datetime(s_updated)}}
                upserts_to_notion.append((nrow.get("_page_id"), props))
            elif (n_updated and s_updated and n_updated > s_updated) or (n_updated and not s_updated):
                # Update Sheets from Notion
                row_number = srow.get("_row_number")
                merged = {**srow}
                for scol, nprop in mapping.items():
                    if scol in (self.sync_id_column, self.updated_at_column):
                        continue
                    merged[scol] = nrow.get(nprop, "")
                merged[self.updated_at_column] = to_iso_datetime(n_updated)
                updates_to_sheet.append((row_number, merged))
            # else equal or both None -> no-op

        # Handle items only in Notion (with sync_id)
        for sid, nrow in by_sync_notion.items():
            if sid in by_sync_sheet:
                continue
            # Create new sheet row
            row: Dict[str, Any] = {}
            for scol, nprop in mapping.items():
                if scol in (self.sync_id_column, self.updated_at_column):
                    continue
                row[scol] = nrow.get(nprop, "")
            row[self.sync_id_column] = sid
            n_updated = self._page_updated_at(nrow) or datetime.now(timezone.utc)
            row[self.updated_at_column] = to_iso_datetime(n_updated)
            appends_to_sheet.append(row)

        # Assign sync_id for unsynced rows
        for srow in unsynced_sheet:
            sid = self._generate_sync_id()
            srow[self.sync_id_column] = sid
            now_iso = to_iso_datetime(datetime.now(timezone.utc))
            srow[self.updated_at_column] = now_iso
            # Push to Notion
            props = self.notion.prepare_properties_from_sheet_row(srow, mapping)
            props[self.notion_sync_id_property] = {"rich_text": [{"type": "text", "text": {"content": sid}}]}
            props[self.notion_sync_updated_at_property] = {"date": {"start": now_iso}}
            upserts_to_notion.append((None, props))
            # Update sheet row in place
            updates_to_sheet.append((srow.get("_row_number"), srow))

        # Assign sync_id for unsynced notion rows
        for nrow in unsynced_notion:
            sid = self._generate_sync_id()
            # Update Notion page with new SyncID and SyncUpdatedAt
            props = {self.notion_sync_id_property: {"rich_text": [{"type": "text", "text": {"content": sid}}]},
                     self.notion_sync_updated_at_property: {"date": {"start": to_iso_datetime(datetime.now(timezone.utc))}}}
            upserts_to_notion.append((nrow.get("_page_id"), props))
            # Append to sheet
            row: Dict[str, Any] = {}
            for scol, nprop in mapping.items():
                if scol in (self.sync_id_column, self.updated_at_column):
                    continue
                row[scol] = nrow.get(nprop, "")
            row[self.sync_id_column] = sid
            row[self.updated_at_column] = to_iso_datetime(datetime.now(timezone.utc))
            appends_to_sheet.append(row)

        # Apply changes
        header = self.sheets.ensure_columns(header)
        for row_number, row_data in updates_to_sheet:
            self.sheets.update_row(row_number, header, row_data)
        if appends_to_sheet:
            self.sheets.append_rows(header, appends_to_sheet)
        for page_id, props in upserts_to_notion:
            self.notion.upsert_page(page_id, props)

        return {
            "updated_sheet_rows": len(updates_to_sheet),
            "appended_sheet_rows": len(appends_to_sheet),
            "upserted_notion_pages": len(upserts_to_notion),
        }
