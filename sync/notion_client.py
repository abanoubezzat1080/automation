from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from notion_client import Client
from .mapping import notion_property_to_plain, sheet_value_to_notion_property


class NotionClientWrapper:
    def __init__(self, token: str, database_id: str) -> None:
        self.client = Client(auth=token)
        self.database_id = database_id
        self._db = self.client.databases.retrieve(database_id)

    def database_properties(self) -> Dict[str, Dict[str, Any]]:
        return self._db.get("properties", {})

    def list_all_pages(self, page_size: int = 100) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        has_more = True
        start_cursor: Optional[str] = None
        while has_more:
            resp = self.client.databases.query(
                database_id=self.database_id,
                start_cursor=start_cursor,
                page_size=page_size,
            )
            results.extend(resp.get("results", []))
            has_more = resp.get("has_more", False)
            start_cursor = resp.get("next_cursor")
        return results

    def extract_plain_row(self, page: Dict[str, Any]) -> Dict[str, Any]:
        props = page.get("properties", {})
        db_props = self.database_properties()
        row: Dict[str, Any] = {"_page_id": page.get("id")}
        for name, meta in db_props.items():
            ptype = meta.get("type")
            pval = props.get(name)
            row[name] = notion_property_to_plain(ptype, pval)
        return row

    def upsert_page(self, page_id: Optional[str], properties: Dict[str, Any]) -> str:
        if page_id:
            self.client.pages.update(page_id=page_id, properties=properties)
            return page_id
        else:
            r = self.client.pages.create(parent={"database_id": self.database_id}, properties=properties)
            return r["id"]

    def prepare_properties_from_sheet_row(self, sheet_row: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
        db_props = self.database_properties()
        props: Dict[str, Any] = {}
        for sheet_col, notion_prop in mapping.items():
            meta = db_props.get(notion_prop)
            if not meta:
                continue
            ptype = meta.get("type")
            value = sheet_row.get(sheet_col)
            props[notion_prop] = sheet_value_to_notion_property(value, ptype)
        return props
