import os
from dataclasses import dataclass
from typing import Dict, Optional
import json


@dataclass
class SyncConfig:
    notion_token: str
    notion_database_id: str
    google_sheet_id: str
    google_worksheet_name: str = "Sheet1"
    sync_id_column: str = "SYNC_ID"
    updated_at_column: str = "UpdatedAt"
    notion_sync_updated_at_property: str = "SyncUpdatedAt"
    notion_sync_id_property: str = "SyncID"
    google_service_account_info: Optional[Dict] = None


def _load_service_account_info() -> Optional[Dict]:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        return None
    # Allow either a file path or JSON string
    if os.path.exists(raw):
        with open(raw, "r", encoding="utf-8") as f:
            return json.load(f)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError(
            "GOOGLE_SERVICE_ACCOUNT_JSON must be a JSON string or path to a JSON file"
        )


def get_config(overrides: Optional[Dict[str, str]] = None) -> SyncConfig:
    overrides = overrides or {}
    notion_token = overrides.get("notion_token") or os.environ.get("NOTION_TOKEN")
    notion_database_id = overrides.get("notion_database_id") or os.environ.get("NOTION_DATABASE_ID")
    google_sheet_id = overrides.get("google_sheet_id") or os.environ.get("GOOGLE_SHEET_ID")
    google_worksheet_name = overrides.get("google_worksheet_name") or os.environ.get("GOOGLE_SHEET_NAME", "Sheet1")

    if not notion_token:
        raise ValueError("Missing NOTION_TOKEN. Set env var or pass --notion-token.")
    if not notion_database_id:
        raise ValueError("Missing NOTION_DATABASE_ID. Set env var or pass --notion-database-id.")
    if not google_sheet_id:
        raise ValueError("Missing GOOGLE_SHEET_ID. Set env var or pass --sheet-id.")

    sync_id_column = overrides.get("sync_id_column") or os.environ.get("SYNC_ID_COLUMN", "SYNC_ID")
    updated_at_column = overrides.get("updated_at_column") or os.environ.get("UPDATED_AT_COLUMN", "UpdatedAt")

    notion_sync_updated_at_property = (
        overrides.get("notion_sync_updated_at_property")
        or os.environ.get("NOTION_SYNC_UPDATED_AT_PROPERTY", "SyncUpdatedAt")
    )
    notion_sync_id_property = (
        overrides.get("notion_sync_id_property")
        or os.environ.get("NOTION_SYNC_ID_PROPERTY", "SyncID")
    )

    service_info = _load_service_account_info()

    return SyncConfig(
        notion_token=notion_token,
        notion_database_id=notion_database_id,
        google_sheet_id=google_sheet_id,
        google_worksheet_name=google_worksheet_name,
        sync_id_column=sync_id_column,
        updated_at_column=updated_at_column,
        notion_sync_updated_at_property=notion_sync_updated_at_property,
        notion_sync_id_property=notion_sync_id_property,
        google_service_account_info=service_info,
    )
