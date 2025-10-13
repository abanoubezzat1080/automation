#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from notion_client import Client


@dataclass
class ExportOptions:
    page_id: str
    notion_token: str
    pretty: bool = True


class NotionExporter:
    def __init__(self, notion_token: str):
        if not notion_token:
            raise ValueError(
                "NOTION_TOKEN is required. Set env var or pass --token."
            )
        self.client = Client(auth=notion_token)

    # --------------------------- Public API ---------------------------
    def export_page(self, page_id: str) -> Dict[str, Any]:
        properties = self._get_page_properties(page_id)
        child_databases: Dict[str, Any] = {}
        content = self._get_block_children_recursive(page_id, child_databases)

        return {
            "page_id": page_id,
            "properties": properties,
            "content": content,
            "child_databases": child_databases,
        }

    # ------------------------- Page Properties ------------------------
    def _get_page_properties(self, page_id: str) -> Dict[str, Any]:
        page = self.client.pages.retrieve(page_id=page_id)
        parsed: Dict[str, Any] = {}
        for name, value in page.get("properties", {}).items():
            parsed[name] = self._parse_property_value(value)
        return parsed

    def _parse_property_value(self, value: Dict[str, Any]) -> Any:
        value_type = value.get("type")
        if value_type == "title":
            title = value.get("title", [])
            return title[0]["text"]["content"] if title else ""
        if value_type == "rich_text":
            rich = value.get("rich_text", [])
            return rich[0]["text"]["content"] if rich else ""
        if value_type == "number":
            return value.get("number")
        if value_type == "select":
            sel = value.get("select")
            return sel.get("name") if sel else ""
        if value_type == "multi_select":
            return [opt.get("name") for opt in value.get("multi_select", [])]
        if value_type == "date":
            date = value.get("date")
            return date.get("start") if date else ""
        if value_type == "checkbox":
            return value.get("checkbox", False)
        if value_type == "url":
            return value.get("url")
        if value_type == "email":
            return value.get("email")
        if value_type == "phone_number":
            return value.get("phone_number")
        if value_type == "status":
            status = value.get("status")
            return status.get("name") if status else ""
        if value_type == "people":
            return [p.get("name") or p.get("id") for p in value.get("people", [])]
        if value_type == "files":
            files = []
            for f in value.get("files", []):
                ftype = f.get("type")
                if ftype == "file":
                    files.append(f.get("file", {}).get("url"))
                elif ftype == "external":
                    files.append(f.get("external", {}).get("url"))
            return files
        if value_type == "relation":
            return [r.get("id") for r in value.get("relation", [])]
        if value_type == "formula":
            formula = value.get("formula", {})
            # Return the first non-null primitive representation
            for key in ("string", "number", "boolean", "date"):
                if key in formula and formula[key] is not None:
                    if key == "date" and formula[key]:
                        return formula[key].get("start")
                    return formula[key]
            return None
        if value_type == "rollup":
            roll = value.get("rollup", {})
            # Simplify common rollup kinds
            if roll.get("type") == "number":
                return roll.get("number")
            if roll.get("type") == "date":
                d = roll.get("date")
                return d.get("start") if d else None
            if roll.get("type") == "array":
                return [self._parse_property_value(item) for item in roll.get("array", [])]
            return None
        # Fallback to a tagged placeholder for unsupported/rare types
        return f"[{value_type}] not parsed"

    # ------------------------ Blocks and Children ---------------------
    def _list_block_children_all(self, block_id: str) -> List[Dict[str, Any]]:
        """Fetch all children for a block, handling pagination."""
        results: List[Dict[str, Any]] = []
        start_cursor: Optional[str] = None
        while True:
            response = self.client.blocks.children.list(
                block_id=block_id, start_cursor=start_cursor
            )
            batch = response.get("results", [])
            results.extend(batch)
            if not response.get("has_more"):
                break
            start_cursor = response.get("next_cursor")
        return results

    def _get_block_children_recursive(
        self, block_id: str, db_collector: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        blocks = self._list_block_children_all(block_id)
        rendered: List[Dict[str, Any]] = []

        for block in blocks:
            block_type = block.get("type")
            block_content = block.get(block_type, {}) if block_type else {}

            block_repr: Dict[str, Any] = {
                "id": block.get("id"),
                "type": block_type,
                "has_children": bool(block.get("has_children")),
                "text": self._extract_visible_text(block_content),
            }

            # If this is a child database, fetch its rows and store in collector
            if block_type == "child_database":
                database_id = block.get("id")
                title = block_content.get("title") or "Untitled Database"
                rows = self._get_database_rows_all(database_id)
                key = f"{title} ({database_id})"
                db_collector[key] = {
                    "database_id": database_id,
                    "title": title,
                    "rows": rows,
                }

            if block.get("has_children"):
                block_repr["children"] = self._get_block_children_recursive(
                    block.get("id"), db_collector
                )

            rendered.append(block_repr)

        return rendered

    def _extract_visible_text(self, block_content: Dict[str, Any]) -> str:
        rich = block_content.get("rich_text")
        if isinstance(rich, list) and rich:
            return "".join([t.get("plain_text", "") for t in rich])
        # Some blocks like headings have "text" shape in earlier API versions
        text = block_content.get("text")
        if isinstance(text, list) and text:
            return "".join([t.get("plain_text", "") for t in text])
        # Captions (for images, files)
        caption = block_content.get("caption")
        if isinstance(caption, list) and caption:
            return "".join([t.get("plain_text", "") for t in caption])
        return ""

    # ------------------------- Database Rows --------------------------
    def _get_database_rows_all(self, database_id: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        start_cursor: Optional[str] = None
        while True:
            resp = self.client.databases.query(
                database_id=database_id, start_cursor=start_cursor
            )
            for page in resp.get("results", []):
                props: Dict[str, Any] = {}
                for name, value in page.get("properties", {}).items():
                    props[name] = self._parse_property_value(value)
                rows.append(props)

            if not resp.get("has_more"):
                break
            start_cursor = resp.get("next_cursor")
        return rows


# ------------------------------ CLI ---------------------------------

def parse_args() -> ExportOptions:
    load_dotenv()  # Load .env if present

    parser = argparse.ArgumentParser(
        description=(
            "Export a Notion page: properties, blocks, and child databases (with pagination)."
        )
    )
    parser.add_argument("page_id", help="Notion page ID (UUID, with or without dashes)")
    parser.add_argument(
        "--token",
        dest="notion_token",
        default=os.getenv("NOTION_TOKEN"),
        help="Notion integration token (defaults to $NOTION_TOKEN)",
    )
    parser.add_argument(
        "--no-pretty",
        action="store_true",
        help="Disable pretty-printed JSON output",
    )

    args = parser.parse_args()
    return ExportOptions(
        page_id=args.page_id,
        notion_token=args.notion_token,
        pretty=not args.no_pretty,
    )


def main() -> None:
    options = parse_args()
    exporter = NotionExporter(options.notion_token)
    result = exporter.export_page(options.page_id)

    if options.pretty:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(json.dumps(result, separators=(",", ":"), ensure_ascii=False))


if __name__ == "__main__":
    main()
