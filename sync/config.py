from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class ColumnMapping:
    sheet: str
    notion: str
    type: str  # title, rich_text, number, select, multi_select, checkbox, date, url, email, phone, status


@dataclass
class NotionConfig:
    token: str
    database_id: str


@dataclass
class SheetsConfig:
    spreadsheet_id: str
    worksheet_title: str
    credentials_json: Optional[str]
    meta_sheet_title: str


@dataclass
class SyncPolicy:
    key: str
    conflict_strategy: str  # notion_wins | sheets_wins | fail
    mirror_deletes: bool
    columns: List[ColumnMapping]


@dataclass
class AppConfig:
    notion: NotionConfig
    sheets: SheetsConfig
    sync: SyncPolicy


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return default
    return value


def load_config(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    notion_raw = raw.get("notion", {})
    sheets_raw = raw.get("sheets", {})
    sync_raw = raw.get("sync", {})

    token: Optional[str] = notion_raw.get("token")
    token_env: Optional[str] = notion_raw.get("token_env")
    if not token:
        if token_env:
            token = _get_env(token_env)
        if not token:
            token = _get_env("NOTION_TOKEN")
    if not token:
        raise ValueError("Notion token not provided. Set in config.notion.token or via env NOTION_TOKEN.")

    database_id = notion_raw.get("database_id")
    if not database_id:
        raise ValueError("Notion database_id is required in config.notion.database_id")

    spreadsheet_id = sheets_raw.get("spreadsheet_id")
    if not spreadsheet_id:
        raise ValueError("Google Sheets spreadsheet_id is required in config.sheets.spreadsheet_id")

    worksheet_title = sheets_raw.get("worksheet_title") or "Sheet1"
    credentials_json = sheets_raw.get("credentials_json") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    meta_sheet_title = sheets_raw.get("meta_sheet_title") or "_SyncMeta"

    key = sync_raw.get("key")
    if not key:
        raise ValueError("sync.key is required and must match a Sheets column and Notion property")

    conflict_strategy = (sync_raw.get("conflict_strategy") or "notion_wins").strip()
    if conflict_strategy not in {"notion_wins", "sheets_wins", "fail"}:
        raise ValueError("sync.conflict_strategy must be one of: notion_wins, sheets_wins, fail")

    mirror_deletes = bool(sync_raw.get("mirror_deletes", False))

    columns_raw = sync_raw.get("columns") or []
    if not columns_raw:
        raise ValueError("sync.columns is required and must map Sheets columns to Notion properties")

    columns: List[ColumnMapping] = []
    for item in columns_raw:
        sheet_col = item.get("sheet")
        notion_prop = item.get("notion")
        typ = (item.get("type") or "rich_text").strip()
        if not sheet_col or not notion_prop:
            raise ValueError("Each sync.columns item must include 'sheet' and 'notion'")
        columns.append(ColumnMapping(sheet=sheet_col, notion=notion_prop, type=typ))

    # Validate that key is included among mapped columns
    mapped_sheet_cols = {c.sheet for c in columns}
    mapped_notion_props = {c.notion for c in columns}
    if key not in mapped_sheet_cols:
        raise ValueError("sync.key must appear among sync.columns.sheet entries")
    if key not in mapped_notion_props:
        # Allow different notion property name for key, but warn only; engine will look up by mapping
        pass

    cfg = AppConfig(
        notion=NotionConfig(token=token, database_id=database_id),
        sheets=SheetsConfig(
            spreadsheet_id=spreadsheet_id,
            worksheet_title=worksheet_title,
            credentials_json=credentials_json,
            meta_sheet_title=meta_sheet_title,
        ),
        sync=SyncPolicy(
            key=key,
            conflict_strategy=conflict_strategy,
            mirror_deletes=mirror_deletes,
            columns=columns,
        ),
    )
    return cfg
