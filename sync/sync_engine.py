from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from tenacity import retry, stop_after_attempt, wait_exponential

from .config import AppConfig, ColumnMapping
from .notion_wrapper import NotionWrapper
from .sheets_wrapper import MetaRecord, SheetsWrapper


@dataclass
class DiffResult:
    to_create_in_sheets: List[str]
    to_create_in_notion: List[str]
    to_update_sheets: List[str]
    to_update_notion: List[str]
    conflicts: List[str]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_value_for_hash(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float, bool)):
        return str(value)
    return str(value).strip()


def compute_row_hash(columns: List[ColumnMapping], row_values_by_sheet_col: Dict[str, Any]) -> str:
    parts: List[str] = []
    for col in columns:
        parts.append(f"{col.sheet}={_normalize_value_for_hash(row_values_by_sheet_col.get(col.sheet))}")
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


class SyncEngine:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.notion = NotionWrapper(cfg.notion.token)
        self.sheets = SheetsWrapper(
            spreadsheet_id=cfg.sheets.spreadsheet_id,
            worksheet_title=cfg.sheets.worksheet_title,
            credentials_json=cfg.sheets.credentials_json,
            meta_sheet_title=cfg.sheets.meta_sheet_title,
        )

    def _mapping_triplets(self) -> List[Tuple[str, str, str]]:
        return [(c.sheet, c.notion, c.type) for c in self.cfg.sync.columns]

    def _notion_columns(self) -> List[Tuple[str, str]]:
        return [(c.notion, c.type) for c in self.cfg.sync.columns]

    def _headers(self) -> List[str]:
        return [c.sheet for c in self.cfg.sync.columns]

    def load_state(self):
        # Ensure sheet headers
        self.sheets.ensure_headers(self._headers())
        # Load
        sheet_rows = self.sheets.get_all_rows()
        meta = self.sheets.load_meta()
        pages = self.notion.query_all_pages(self.cfg.notion.database_id)
        return sheet_rows, meta, pages

    def _row_to_sheet_headers(self, row_values_by_sheet_col: Dict[str, Any]) -> Dict[str, Any]:
        # Restrict to configured headers, keep order handled by wrapper
        return {c.sheet: row_values_by_sheet_col.get(c.sheet, "") for c in self.cfg.sync.columns}

    def _row_from_notion_page(self, page: Dict[str, Any]) -> Dict[str, Any]:
        # Build row values keyed by sheet columns using mapping
        row: Dict[str, Any] = {}
        props = page.get("properties", {})
        for col in self.cfg.sync.columns:
            prop = props.get(col.notion)
            row[col.sheet] = self.notion.extract_property_plain(prop, col.type) if prop else None
        return row

    def _build_notion_properties_from_sheet_row(self, sheet_row: Dict[str, Any]) -> Dict[str, Any]:
        properties: Dict[str, Any] = {}
        for col in self.cfg.sync.columns:
            value = sheet_row.get(col.sheet)
            properties[col.notion] = self.notion.build_property_value(col.type, None if value is None else str(value))
        return properties

    def _key_value_from_row(self, row_by_sheet_col: Dict[str, Any]) -> str:
        val = row_by_sheet_col.get(self.cfg.sync.key)
        if val is None:
            return ""
        return str(val)

    def _build_index_by_key(self, rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        idx: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            k = self._key_value_from_row(r)
            if k:
                idx[k] = r
        return idx

    def _build_notion_index(self, pages: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        idx: Dict[str, Dict[str, Any]] = {}
        for p in pages:
            row = self._row_from_notion_page(p)
            k = self._key_value_from_row(row)
            if k:
                row["__notion_page_id__"] = p.get("id")
                row["__notion_last_edited__"] = p.get("last_edited_time")
                idx[k] = row
        return idx

    def _diff(self, sheets_by_key: Dict[str, Dict[str, Any]], notion_by_key: Dict[str, Dict[str, Any]], meta: Dict[str, MetaRecord]) -> DiffResult:
        keys = set(sheets_by_key.keys()) | set(notion_by_key.keys())
        to_create_in_sheets: List[str] = []
        to_create_in_notion: List[str] = []
        to_update_sheets: List[str] = []
        to_update_notion: List[str] = []
        conflicts: List[str] = []

        for k in sorted(keys):
            sheet_row = sheets_by_key.get(k)
            notion_row = notion_by_key.get(k)
            m = meta.get(k)

            if sheet_row is None and notion_row is not None:
                to_create_in_sheets.append(k)
                continue
            if sheet_row is not None and notion_row is None:
                to_create_in_notion.append(k)
                continue
            if sheet_row is None or notion_row is None:
                continue

            # Both exist: compare
            # Compute current hash
            current_hash = compute_row_hash(self.cfg.sync.columns, sheet_row)
            notion_last = notion_row.get("__notion_last_edited__")
            # Determine deltas since meta
            changed_in_sheets = True
            changed_in_notion = True
            if m:
                changed_in_sheets = (m.sheets_row_hash or "") != current_hash
                if notion_last and m.notion_last_edited_time:
                    changed_in_notion = notion_last > m.notion_last_edited_time
                elif m.notion_last_edited_time:
                    changed_in_notion = False
            else:
                # No meta: treat as conflict if values differ
                pass

            # Compare values field-by-field
            values_equal = True
            for col in self.cfg.sync.columns:
                if str(sheet_row.get(col.sheet, "")) != str(notion_row.get(col.sheet, "")):
                    values_equal = False
                    break
            if values_equal:
                # No update needed; still update meta later
                continue

            if changed_in_sheets and not changed_in_notion:
                to_update_notion.append(k)
            elif changed_in_notion and not changed_in_sheets:
                to_update_sheets.append(k)
            elif changed_in_notion and changed_in_sheets:
                conflicts.append(k)
            else:
                # Values differ but neither appears changed (e.g., mapping normalization);
                # prefer Notion -> Sheets to reach consistency
                to_update_sheets.append(k)

        return DiffResult(
            to_create_in_sheets=to_create_in_sheets,
            to_create_in_notion=to_create_in_notion,
            to_update_sheets=to_update_sheets,
            to_update_notion=to_update_notion,
            conflicts=conflicts,
        )

    @retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(5))
    def _create_or_update_notion(self, page_id: Optional[str], properties: Dict[str, Any]) -> Dict[str, Any]:
        if page_id:
            return self.notion.update_page(page_id, properties)
        else:
            return self.notion.create_page(self.cfg.notion.database_id, properties)

    def run(self, direction: str = "both", dry_run: bool = False) -> Dict[str, Any]:
        # direction: both | to-sheets | to-notion
        if direction not in {"both", "to-sheets", "to-notion"}:
            raise ValueError("direction must be one of: both, to-sheets, to-notion")

        sheet_rows, meta_map, pages = self.load_state()
        sheets_by_key = self._build_index_by_key(sheet_rows)
        notion_by_key = self._build_notion_index(pages)

        diff = self._diff(sheets_by_key, notion_by_key, meta_map)

        summary: Dict[str, Any] = {
            "created_in_sheets": 0,
            "created_in_notion": 0,
            "updated_sheets": 0,
            "updated_notion": 0,
            "conflicts": [],
            "dry_run": dry_run,
            "direction": direction,
        }

        # Create in destination sides
        if direction in {"both", "to-sheets"}:
            for k in diff.to_create_in_sheets:
                notion_row = notion_by_key[k]
                if dry_run:
                    summary["created_in_sheets"] += 1
                    continue
                row_values = {c.sheet: notion_row.get(c.sheet, "") for c in self.cfg.sync.columns}
                self.sheets.upsert_row(self.cfg.sync.key, k, row_values)
                # Update meta
                page_id = notion_row.get("__notion_page_id__")
                page_last = notion_row.get("__notion_last_edited__")
                new_hash = compute_row_hash(self.cfg.sync.columns, row_values)
                self.sheets.upsert_meta(
                    MetaRecord(
                        key=k,
                        notion_page_id=page_id,
                        notion_last_edited_time=page_last,
                        sheets_row_hash=new_hash,
                        last_synced_at=_now_iso(),
                    )
                )
                summary["created_in_sheets"] += 1

        if direction in {"both", "to-notion"}:
            for k in diff.to_create_in_notion:
                sheet_row = sheets_by_key[k]
                if dry_run:
                    summary["created_in_notion"] += 1
                    continue
                properties = self._build_notion_properties_from_sheet_row(sheet_row)
                page = self._create_or_update_notion(None, properties)
                page_id = page.get("id")
                page_last = page.get("last_edited_time")
                new_hash = compute_row_hash(self.cfg.sync.columns, sheet_row)
                self.sheets.upsert_meta(
                    MetaRecord(
                        key=k,
                        notion_page_id=page_id,
                        notion_last_edited_time=page_last,
                        sheets_row_hash=new_hash,
                        last_synced_at=_now_iso(),
                    )
                )
                summary["created_in_notion"] += 1

        # Updates
        if direction in {"both", "to-sheets"}:
            for k in diff.to_update_sheets:
                notion_row = notion_by_key[k]
                if dry_run:
                    summary["updated_sheets"] += 1
                    continue
                row_values = {c.sheet: notion_row.get(c.sheet, "") for c in self.cfg.sync.columns}
                self.sheets.upsert_row(self.cfg.sync.key, k, row_values)
                page_id = notion_row.get("__notion_page_id__")
                page_last = notion_row.get("__notion_last_edited__")
                new_hash = compute_row_hash(self.cfg.sync.columns, row_values)
                self.sheets.upsert_meta(
                    MetaRecord(
                        key=k,
                        notion_page_id=page_id,
                        notion_last_edited_time=page_last,
                        sheets_row_hash=new_hash,
                        last_synced_at=_now_iso(),
                    )
                )
                summary["updated_sheets"] += 1

        if direction in {"both", "to-notion"}:
            for k in diff.to_update_notion:
                sheet_row = sheets_by_key[k]
                page_id = (meta_map.get(k).notion_page_id if k in meta_map else None) or notion_by_key.get(k, {}).get("__notion_page_id__")
                if dry_run:
                    summary["updated_notion"] += 1
                    continue
                properties = self._build_notion_properties_from_sheet_row(sheet_row)
                page = self._create_or_update_notion(page_id, properties)
                page_id = page.get("id")
                page_last = page.get("last_edited_time")
                new_hash = compute_row_hash(self.cfg.sync.columns, sheet_row)
                self.sheets.upsert_meta(
                    MetaRecord(
                        key=k,
                        notion_page_id=page_id,
                        notion_last_edited_time=page_last,
                        sheets_row_hash=new_hash,
                        last_synced_at=_now_iso(),
                    )
                )
                summary["updated_notion"] += 1

        # Conflicts
        for k in diff.conflicts:
            if self.cfg.sync.conflict_strategy == "fail":
                summary["conflicts"].append(k)
                continue
            if self.cfg.sync.conflict_strategy == "notion_wins":
                if direction in {"both", "to-sheets"}:
                    if not dry_run:
                        notion_row = notion_by_key[k]
                        row_values = {c.sheet: notion_row.get(c.sheet, "") for c in self.cfg.sync.columns}
                        self.sheets.upsert_row(self.cfg.sync.key, k, row_values)
                        page_id = notion_row.get("__notion_page_id__")
                        page_last = notion_row.get("__notion_last_edited__")
                        new_hash = compute_row_hash(self.cfg.sync.columns, row_values)
                        self.sheets.upsert_meta(
                            MetaRecord(
                                key=k,
                                notion_page_id=page_id,
                                notion_last_edited_time=page_last,
                                sheets_row_hash=new_hash,
                                last_synced_at=_now_iso(),
                            )
                        )
                    summary["updated_sheets"] += 1
                else:
                    summary["conflicts"].append(k)
            elif self.cfg.sync.conflict_strategy == "sheets_wins":
                if direction in {"both", "to-notion"}:
                    sheet_row = sheets_by_key[k]
                    if not dry_run:
                        page_id = (meta_map.get(k).notion_page_id if k in meta_map else None) or notion_by_key.get(k, {}).get("__notion_page_id__")
                        properties = self._build_notion_properties_from_sheet_row(sheet_row)
                        page = self._create_or_update_notion(page_id, properties)
                        page_id = page.get("id")
                        page_last = page.get("last_edited_time")
                        new_hash = compute_row_hash(self.cfg.sync.columns, sheet_row)
                        self.sheets.upsert_meta(
                            MetaRecord(
                                key=k,
                                notion_page_id=page_id,
                                notion_last_edited_time=page_last,
                                sheets_row_hash=new_hash,
                                last_synced_at=_now_iso(),
                            )
                        )
                    summary["updated_notion"] += 1
                else:
                    summary["conflicts"].append(k)

        # Mirror deletes if enabled
        if self.cfg.sync.mirror_deletes:
            # Notion-only deletes in Sheets
            if direction in {"both", "to-sheets"}:
                for k in list(sheets_by_key.keys()):
                    if k not in notion_by_key:
                        # Deletion: we cannot delete rows deterministically without locating row number; we can clear values
                        # Here we skip destructive deletes for safety. Future: implement delete by row number.
                        pass
            # Sheets-only deletes in Notion
            if direction in {"both", "to-notion"}:
                # Not implemented for safety
                pass

        return summary
