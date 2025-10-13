from __future__ import annotations
from typing import Any, Dict, List, Optional
from dateutil import parser as date_parser
from datetime import datetime, timezone


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = date_parser.isoparse(value)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def to_iso_datetime(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ---------- Notion <-> Plain conversions ----------

def notion_property_to_plain(property_type: str, property_value: Dict[str, Any]) -> Any:
    if property_value is None:
        return None
    if property_type == "title":
        parts = property_value.get("title", [])
        return "".join([p.get("plain_text", "") for p in parts])
    if property_type == "rich_text":
        parts = property_value.get("rich_text", [])
        return "".join([p.get("plain_text", "") for p in parts])
    if property_type == "number":
        return property_value.get("number")
    if property_type == "checkbox":
        return property_value.get("checkbox")
    if property_type == "date":
        date_val = property_value.get("date")
        if not date_val:
            return None
        return date_val.get("start")
    if property_type == "select":
        sel = property_value.get("select")
        return sel.get("name") if sel else None
    if property_type == "multi_select":
        items = property_value.get("multi_select", [])
        return ", ".join([i.get("name", "") for i in items if i])
    if property_type in ("url", "email", "phone_number"):
        return property_value.get(property_type)
    # Fallback for unsupported types
    return None


def sheet_value_to_notion_property(value: Any, property_type: str) -> Dict[str, Any]:
    if property_type == "title":
        text = "" if value is None else str(value)
        return {"title": [{"type": "text", "text": {"content": text}}]}
    if property_type == "rich_text":
        text = "" if value is None else str(value)
        return {"rich_text": [{"type": "text", "text": {"content": text}}]}
    if property_type == "number":
        if value in ("", None):
            return {"number": None}
        try:
            return {"number": float(value)}
        except Exception:
            return {"number": None}
    if property_type == "checkbox":
        if isinstance(value, bool):
            return {"checkbox": value}
        if isinstance(value, str):
            lowered = value.strip().lower()
            return {"checkbox": lowered in ("true", "1", "yes", "y", "checked")}
        if isinstance(value, (int, float)):
            return {"checkbox": bool(value)}
        return {"checkbox": False}
    if property_type == "date":
        if not value:
            return {"date": None}
        try:
            dt = parse_iso_datetime(str(value))
            if not dt:
                return {"date": None}
            return {"date": {"start": to_iso_datetime(dt)}}
        except Exception:
            return {"date": None}
    if property_type == "select":
        if not value:
            return {"select": None}
        return {"select": {"name": str(value)}}
    if property_type == "multi_select":
        if not value:
            return {"multi_select": []}
        if isinstance(value, str):
            names = [v.strip() for v in value.split(",") if v.strip()]
        elif isinstance(value, list):
            names = [str(v).strip() for v in value if str(v).strip()]
        else:
            names = [str(value).strip()]
        return {"multi_select": [{"name": n} for n in names]}
    if property_type in ("url", "email", "phone_number"):
        return {property_type: (None if value in ("", None) else str(value))}

    # Unsupported types fallback
    return {}
