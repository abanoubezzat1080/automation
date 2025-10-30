from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from .config import load_config
from .sync_engine import SyncEngine


def main(argv: Any = None) -> int:
    parser = argparse.ArgumentParser(description="Two-way sync between Google Sheets and a Notion database")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--direction", default="both", choices=["both", "to-sheets", "to-notion"], help="Sync direction")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without modifying anything")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    engine = SyncEngine(cfg)
    summary = engine.run(direction=args.direction, dry_run=args.dry_run)

    print(json.dumps(summary, indent=2))
    if summary.get("conflicts"):
        print("Conflicts detected:", summary["conflicts"]) 
        if cfg.sync.conflict_strategy == "fail":
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
