from __future__ import annotations
import argparse
from .config import get_config
from .sheets_client import SheetsClient
from .notion_client import NotionClientWrapper
from .sync_service import SyncService


def main() -> None:
    parser = argparse.ArgumentParser(description="Two-way sync between Google Sheets and Notion database")
    parser.add_argument("--notion-token", dest="notion_token")
    parser.add_argument("--notion-database-id", dest="notion_database_id")
    parser.add_argument("--sheet-id", dest="google_sheet_id")
    parser.add_argument("--sheet-name", dest="google_worksheet_name")
    parser.add_argument("--sync-id-column", dest="sync_id_column")
    parser.add_argument("--updated-at-column", dest="updated_at_column")
    parser.add_argument("--notion-sync-id-property", dest="notion_sync_id_property")
    parser.add_argument("--notion-sync-updated-at-property", dest="notion_sync_updated_at_property")

    args = parser.parse_args()

    cfg = get_config({k: v for k, v in vars(args).items() if v is not None})
    if not cfg.google_service_account_info:
        raise SystemExit("Provide Google service account via env GOOGLE_SERVICE_ACCOUNT_JSON (path or JSON)")

    sheets = SheetsClient(cfg.google_service_account_info, cfg.google_sheet_id, cfg.google_worksheet_name)
    notion = NotionClientWrapper(cfg.notion_token, cfg.notion_database_id)

    service = SyncService(
        sheets=sheets,
        notion=notion,
        sync_id_column=cfg.sync_id_column,
        updated_at_column=cfg.updated_at_column,
        notion_sync_id_property=cfg.notion_sync_id_property,
        notion_sync_updated_at_property=cfg.notion_sync_updated_at_property,
    )

    result = service.two_way_sync()
    print(result)


if __name__ == "__main__":
    main()
