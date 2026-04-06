from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .models import FreshnessDecision

OUTPUTS_DIR = Path(__file__).resolve().parents[2] / "outputs"
FRESHNESS_EVENTS_DIR = OUTPUTS_DIR / "freshness_events"


@dataclass
class LabScribe:
    base_dir: Path = FRESHNESS_EVENTS_DIR

    async def persist(self, decision: FreshnessDecision) -> Dict[str, Any]:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        ticker_key = self._safe_key(decision.event.ticker or decision.baseline_run.ticker or "unknown")
        event_key = self._safe_key(decision.event.event_id or ts)
        run_key = self._safe_key(decision.baseline_run.run_id or "unknown_run")

        ticker_dir = self.base_dir / ticker_key
        run_dir = self.base_dir / "by_run" / run_key
        ticker_dir.mkdir(parents=True, exist_ok=True)
        run_dir.mkdir(parents=True, exist_ok=True)

        payload = decision.to_dict()
        payload["saved_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        filename = f"{ts}__{event_key}.json"
        primary_path = ticker_dir / filename
        by_run_path = run_dir / filename
        latest_ticker_path = ticker_dir / "latest.json"
        latest_by_run_path = run_dir / "latest.json"

        serialized = json.dumps(payload, indent=2, ensure_ascii=True)
        primary_path.write_text(serialized, encoding="utf-8")
        by_run_path.write_text(serialized, encoding="utf-8")
        latest_ticker_path.write_text(serialized, encoding="utf-8")
        latest_by_run_path.write_text(serialized, encoding="utf-8")

        return {
            "event_artifact": str(primary_path),
            "by_run_artifact": str(by_run_path),
            "latest_ticker_artifact": str(latest_ticker_path),
            "latest_by_run_artifact": str(latest_by_run_path),
            "run_id": decision.baseline_run.run_id,
            "ticker": decision.event.ticker,
            "action": decision.action_decision.action,
        }

    @classmethod
    def load_latest_for_run(cls, run_id: str, *, base_dir: Path = FRESHNESS_EVENTS_DIR) -> Dict[str, Any]:
        run_key = cls._safe_key(run_id or "unknown_run")
        path = Path(base_dir) / "by_run" / run_key / "latest.json"
        if not path.exists() or not path.is_file():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    @staticmethod
    def _safe_key(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._:-]+", "_", str(value or "").strip())[:180] or "unknown"
