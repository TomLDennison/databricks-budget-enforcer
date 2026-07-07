"""Persistent state between runs: the current weekly plan, calibration,
active throttles, and an append-only audit log.

A single JSON file - a local path when run standalone, or a /Volumes/... path
when run inside a Databricks workspace (Unity Catalog volumes mount as posix
paths on cluster nodes).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from databricks_budget_enforcer.enforce.actions import Action

MAX_AUDIT_ENTRIES = 5000


class JsonStateStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self._data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            return json.loads(self.path.read_text())
        return {"plan": None, "calibration": None, "active_actions": [], "audit": []}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, default=str))
        tmp.replace(self.path)

    # -- weekly plan ----------------------------------------------------

    def get_plan(self) -> dict | None:
        return self._data.get("plan")

    def set_plan(self, plan: dict) -> None:
        self._data["plan"] = plan
        self._save()

    # -- calibration ------------------------------------------------------

    def get_calibration(self) -> dict | None:
        return self._data.get("calibration")

    def set_calibration(self, calibration: dict) -> None:
        self._data["calibration"] = calibration
        self._save()

    # -- active throttles -------------------------------------------------

    def get_active_actions(self) -> list[Action]:
        return [Action.from_dict(d) for d in self._data.get("active_actions", [])]

    def set_active_actions(self, actions: list[Action]) -> None:
        self._data["active_actions"] = [a.to_dict() for a in actions]
        self._save()

    # -- audit log -------------------------------------------------------

    def append_audit(self, event: str, detail: dict) -> None:
        self._data.setdefault("audit", []).append(
            {
                "at": datetime.now(timezone.utc).isoformat(),
                "event": event,
                **detail,
            }
        )
        self._data["audit"] = self._data["audit"][-MAX_AUDIT_ENTRIES:]
        self._save()

    def audit_entries(self, limit: int = 50) -> list[dict]:
        return self._data.get("audit", [])[-limit:]
