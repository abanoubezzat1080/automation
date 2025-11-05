from __future__ import annotations
from typing import Any, Dict, Optional
import json
import os
from datetime import datetime, timezone

DEFAULT_STATE_PATH = ".sync_state.json"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class StateStore:
    def __init__(self, path: str = DEFAULT_STATE_PATH) -> None:
        self.path = path
        self._state: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self._state = json.load(f)
        else:
            self._state = {}

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._state, f, indent=2, sort_keys=True)

    def get_last_sync_time(self, key: str) -> Optional[str]:
        return self._state.get("last_sync", {}).get(key)

    def set_last_sync_time(self, key: str, iso_ts: Optional[str] = None) -> None:
        if "last_sync" not in self._state:
            self._state["last_sync"] = {}
        self._state["last_sync"][key] = iso_ts or _utcnow_iso()
        self.save()
