import argparse
import json
import os
from dotenv import load_dotenv

from notion_utils import init_notion_client, get_full_page_with_child_databases


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Extract a Notion page including child databases")
    parser.add_argument("page", help="Notion page URL or ID")
    parser.add_argument("--out", dest="out", default="-", help="Output file path or '-' for stdout")
    args = parser.parse_args()

    notion = init_notion_client()
    data = get_full_page_with_child_databases(notion, args.page)

    as_text = json.dumps(data, indent=2, ensure_ascii=False)
    if args.out == "-":
        print(as_text)
    else:
        out_dir = os.path.dirname(os.path.abspath(args.out))
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(as_text)
        print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
