from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as dateparser
from notion_client import Client


class NotionWrapper:
    def __init__(self, token: str) -> None:
        self.client = Client(auth=token)

    def query_all_pages(self, database_id: str) -> List[Dict[str, Any]]:
        pages: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            resp = self.client.databases.query(database_id=database_id, start_cursor=cursor)
            pages.extend(resp.get("results", []))
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return pages

    def get_last_edited_time(self, page: Dict[str, Any]) -> str:
        return page.get("last_edited_time")

    def get_page_id(self, page: Dict[str, Any]) -> str:
        return page.get("id")

    def extract_property_plain(self, prop: Dict[str, Any], typ: str) -> Optional[str]:
        if prop is None:
            return None
        try:
            if typ == "title":
                parts = prop.get("title", [])
                return "".join(p.get("plain_text", "") for p in parts) if parts else ""
            if typ == "rich_text":
                parts = prop.get("rich_text", [])
                return "".join(p.get("plain_text", "") for p in parts) if parts else ""
            if typ == "number":
                v = prop.get("number")
                return None if v is None else str(v)
            if typ == "select":
                v = prop.get("select")
                return None if v is None else v.get("name")
            if typ == "multi_select":
                vs = prop.get("multi_select", [])
                return ", ".join(o.get("name") for o in vs) if vs else ""
            if typ == "checkbox":
                v = prop.get("checkbox")
                return "TRUE" if v else "FALSE"
            if typ == "date":
                v = prop.get("date")
                if not v:
                    return None
                start = v.get("start")
                end = v.get("end")
                if end:
                    return f"{start}..{end}"
                return start
            if typ == "url":
                return prop.get("url")
            if typ == "email":
                return prop.get("email")
            if typ == "phone":
                return prop.get("phone_number")
            if typ == "status":
                v = prop.get("status")
                return None if v is None else v.get("name")
            # Fallback: try title then rich_text then plain_text fields
            for key in ("title", "rich_text"):
                parts = prop.get(key)
                if isinstance(parts, list):
                    return "".join(p.get("plain_text", "") for p in parts) if parts else ""
            return None
        except Exception:
            return None

    def build_property_value(self, typ: str, value: Optional[str]) -> Dict[str, Any]:
        if typ == "title":
            return {"title": [{"type": "text", "text": {"content": value or ""}}]}
        if typ == "rich_text":
            return {"rich_text": [{"type": "text", "text": {"content": value or ""}}]}
        if typ == "number":
            try:
                return {"number": None if value in (None, "") else float(value)}
            except Exception:
                return {"number": None}
        if typ == "select":
            return {"select": None if not value else {"name": value}}
        if typ == "multi_select":
            items = []
            if value:
                for part in [p.strip() for p in str(value).split(",") if p.strip()]:
                    items.append({"name": part})
            return {"multi_select": items}
        if typ == "checkbox":
            lowered = str(value).strip().lower() if value is not None else "false"
            truthy = lowered in {"true", "1", "yes", "y", "t"}
            return {"checkbox": truthy}
        if typ == "date":
            if not value:
                return {"date": None}
            if ".." in value:
                start, end = [v.strip() for v in value.split("..", 1)]
            else:
                start, end = value.strip(), None
            # Let Notion parse ISO8601 strings
            return {"date": {"start": start, "end": end}}
        if typ == "url":
            return {"url": value or None}
        if typ == "email":
            return {"email": value or None}
        if typ == "phone":
            return {"phone_number": value or None}
        if typ == "status":
            return {"status": None if not value else {"name": value}}
        # Default: rich_text
        return {"rich_text": [{"type": "text", "text": {"content": value or ""}}]}

    def page_to_row(
        self,
        page: Dict[str, Any],
        columns: List[Tuple[str, str]],  # list of (notion_property_name, type)
    ) -> Dict[str, Optional[str]]:
        props = page.get("properties", {})
        row: Dict[str, Optional[str]] = {}
        for notion_prop, typ in columns:
            prop = props.get(notion_prop)
            row[notion_prop] = self.extract_property_plain(prop, typ) if prop else None
        return row

    def build_properties_from_row(
        self,
        mapping: List[Tuple[str, str, str]],  # (sheet_col, notion_prop, type)
        row_values_by_sheet_col: Dict[str, Any],
    ) -> Dict[str, Any]:
        properties: Dict[str, Any] = {}
        for sheet_col, notion_prop, typ in mapping:
            value = row_values_by_sheet_col.get(sheet_col)
            properties[notion_prop] = self.build_property_value(typ, None if value is None else str(value))
        return properties

    def create_page(self, database_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.pages.create(parent={"database_id": database_id}, properties=properties)

    def update_page(self, page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.pages.update(page_id=page_id, properties=properties)
