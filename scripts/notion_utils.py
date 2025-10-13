import os
import re
from typing import Dict, List, Any, Optional

from notion_client import Client


def init_notion_client(token: Optional[str] = None) -> Client:
    token = token or os.getenv("NOTION_TOKEN")
    if not token:
        raise ValueError("NOTION_TOKEN is not set")
    return Client(auth=token)


NOTION_URL_ID_PATTERN = re.compile(r"([0-9a-fA-F]{32})|([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})")


def extract_id_from_url(possible_url_or_id: str) -> str:
    """Return a clean Notion ID given a URL or raw id.

    Accepts: page url, database url, or raw id.
    Returns a dashed UUID (preferred by API) when possible.
    """
    if not possible_url_or_id:
        raise ValueError("Empty Notion URL or ID")

    # If it looks like a pure 32-hex id without dashes, dash it
    raw = possible_url_or_id.strip()
    match = NOTION_URL_ID_PATTERN.search(raw)
    if not match:
        raise ValueError(f"Could not parse Notion ID from: {possible_url_or_id}")

    # Pick the longest group (dashed form preferred)
    id_candidate = max((g for g in match.groups() if g), key=len)

    if len(id_candidate) == 32:  # add dashes 8-4-4-4-12
        return f"{id_candidate[0:8]}-{id_candidate[8:12]}-{id_candidate[12:16]}-{id_candidate[16:20]}-{id_candidate[20:32]}".lower()
    return id_candidate.lower()


def get_database_rows(notion: Client, database_id: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    has_more = True
    start_cursor: Optional[str] = None
    while has_more:
        resp = notion.databases.query(database_id=database_id, start_cursor=start_cursor) if start_cursor else notion.databases.query(database_id=database_id)
        for page in resp.get("results", []):
            props: Dict[str, Any] = {}
            for name, value in page.get("properties", {}).items():
                value_type = value.get("type")
                if value_type == "title":
                    title = value.get("title", [])
                    props[name] = title[0]["plain_text"] if title else ""
                elif value_type == "rich_text":
                    rt = value.get("rich_text", [])
                    props[name] = rt[0]["plain_text"] if rt else ""
                elif value_type == "number":
                    props[name] = value.get("number")
                elif value_type == "select":
                    sel = value.get("select")
                    props[name] = sel.get("name") if sel else ""
                elif value_type == "multi_select":
                    props[name] = [opt.get("name") for opt in value.get("multi_select", [])]
                elif value_type == "checkbox":
                    props[name] = value.get("checkbox")
                elif value_type == "date":
                    date = value.get("date")
                    props[name] = date.get("start") if date else ""
                elif value_type == "people":
                    props[name] = [p.get("name") or p.get("id") for p in value.get("people", [])]
                elif value_type == "url":
                    props[name] = value.get("url")
                elif value_type == "email":
                    props[name] = value.get("email")
                elif value_type == "phone_number":
                    props[name] = value.get("phone_number")
                elif value_type == "files":
                    props[name] = [f.get("name") for f in value.get("files", [])]
                else:
                    props[name] = f"[{value_type}]"
            rows.append(props)
        has_more = resp.get("has_more", False)
        start_cursor = resp.get("next_cursor")

    return rows


def get_block_children_recursive(notion: Client, block_id: str, db_collector: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    all_blocks: List[Dict[str, Any]] = []

    has_more = True
    start_cursor: Optional[str] = None
    while has_more:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=start_cursor) if start_cursor else notion.blocks.children.list(block_id=block_id)
        blocks = resp.get("results", [])

        for block in blocks:
            block_type = block.get("type")
            block_data: Dict[str, Any] = {
                "id": block.get("id"),
                "type": block_type,
                "has_children": block.get("has_children", False),
                "text": "",
            }

            block_content = block.get(block_type, {}) if block_type else {}

            if isinstance(block_content, dict) and block_content.get("rich_text"):
                block_data["text"] = "".join(t.get("plain_text", "") for t in block_content.get("rich_text", []))

            if block_type == "child_database":
                db_id = block.get("id")
                db_title = block_content.get("title") or "Untitled Database"
                try:
                    db_rows = get_database_rows(notion, db_id)
                    db_collector[db_title] = db_rows
                except Exception as exc:
                    db_collector[db_title] = [{"error": str(exc)}]

            if block.get("has_children"):
                block_data["children"] = get_block_children_recursive(notion, block.get("id"), db_collector)

            all_blocks.append(block_data)

        has_more = resp.get("has_more", False)
        start_cursor = resp.get("next_cursor")

    return all_blocks


def get_full_page_with_child_databases(notion: Client, page_id_or_url: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {"properties": {}, "content": [], "child_databases": {}}

    page_id = extract_id_from_url(page_id_or_url)
    page = notion.pages.retrieve(page_id=page_id)

    for name, value in page.get("properties", {}).items():
        value_type = value.get("type")
        if value_type == "title":
            title = value.get("title", [])
            result["properties"][name] = title[0]["plain_text"] if title else ""
        elif value_type == "rich_text":
            rt = value.get("rich_text", [])
            result["properties"][name] = rt[0]["plain_text"] if rt else ""
        elif value_type == "number":
            result["properties"][name] = value.get("number")
        elif value_type == "select":
            sel = value.get("select")
            result["properties"][name] = sel.get("name") if sel else ""
        elif value_type == "multi_select":
            result["properties"][name] = [opt.get("name") for opt in value.get("multi_select", [])]
        elif value_type == "date":
            date = value.get("date")
            result["properties"][name] = date.get("start") if date else ""
        elif value_type == "checkbox":
            result["properties"][name] = value.get("checkbox")
        elif value_type == "url":
            result["properties"][name] = value.get("url")
        elif value_type == "email":
            result["properties"][name] = value.get("email")
        elif value_type == "phone_number":
            result["properties"][name] = value.get("phone_number")
        else:
            result["properties"][name] = f"[{value_type}] not parsed"

    result["content"] = get_block_children_recursive(notion, page_id, result["child_databases"]) 
    return result
