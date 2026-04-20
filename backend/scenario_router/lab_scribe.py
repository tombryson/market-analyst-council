from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .models import AnnouncementEvent, AnnouncementPacket, ScenarioRouterDecision

OUTPUTS_DIR = Path(
    os.getenv("ANALYSIS_OUTPUTS_DIR", str(Path(__file__).resolve().parents[2] / "outputs"))
)
SCENARIO_ROUTER_EVENTS_DIR = OUTPUTS_DIR / "scenario_router_events"
LEGACY_FRESHNESS_EVENTS_DIR = OUTPUTS_DIR / "freshness_events"


@dataclass
class LabScribe:
    base_dir: Path = SCENARIO_ROUTER_EVENTS_DIR

    async def persist(self, decision: ScenarioRouterDecision) -> Dict[str, Any]:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        ticker_key = self._safe_key(decision.event.ticker or decision.baseline_run.ticker or "unknown")
        event_key = self._safe_key(decision.event.event_id or ts)
        run_key = self._safe_key(decision.baseline_run.run_id or "unknown_run")

        ticker_dir = self.base_dir / ticker_key
        run_dir = self.base_dir / "by_run" / run_key
        ticker_dir.mkdir(parents=True, exist_ok=True)
        run_dir.mkdir(parents=True, exist_ok=True)

        payload = decision.to_dict()
        payload["status"] = "ok"
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

    async def persist_status(
        self,
        *,
        event: AnnouncementEvent,
        status: str,
        reason: str,
        announcement_packet: AnnouncementPacket | None = None,
        baseline_run_id: str = "",
        current_path: str = "",
        path_transition: str = "",
        action: str = "",
        impact_level: str = "",
        processing_duration_ms: int = 0,
        processing_trace: list[dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        ticker_key = self._safe_key(event.ticker or "unknown")
        event_key = self._safe_key(event.event_id or ts)

        ticker_dir = self.base_dir / ticker_key
        ticker_dir.mkdir(parents=True, exist_ok=True)

        packet = announcement_packet or AnnouncementPacket(
            event_id=event.event_id,
            ticker=event.ticker,
            exchange=event.exchange,
            title=event.subject,
            company_name=event.company_hint,
            body_text=event.body_text,
        )
        payload = {
            "status": str(status or "").strip() or "processing_error",
            "saved_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "event": event.to_dict(),
            "announcement_packet": packet.to_dict(),
            "baseline_run": {
                "run_id": str(baseline_run_id or "").strip(),
                "ticker": event.ticker,
                "exchange": event.exchange,
                "company_name": event.company_hint,
            },
            "comparison_report": {
                "ticker": event.ticker,
                "baseline_run_id": str(baseline_run_id or "").strip(),
                "current_path": str(current_path or "").strip(),
                "baseline_path": "",
                "path_transition": str(path_transition or "").strip(),
                "impact_level": str(impact_level or "").strip(),
                "notes": [str(reason or "").strip()] if str(reason or "").strip() else [],
            },
            "action_decision": {
                "action": str(action or "").strip(),
                "confidence": 0.0,
                "reason": str(reason or "").strip(),
                "should_trigger_workflow": False,
                "run_reuse_ok": False,
                "requires_human_ack": False,
                "invalidated_sections": [],
                "follow_up_steps": [],
                "tags": [str(status or "").strip()] if str(status or "").strip() else [],
            },
            "processing_started_at_utc": "",
            "processing_completed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "processing_duration_ms": max(0, int(processing_duration_ms or 0)),
            "processing_trace": list(processing_trace or []),
            "persisted_artifacts": {},
            "error": {
                "status": str(status or "").strip(),
                "reason": str(reason or "").strip(),
            },
        }

        filename = f"{ts}__{event_key}.json"
        primary_path = ticker_dir / filename
        latest_ticker_path = ticker_dir / "latest.json"
        serialized = json.dumps(payload, indent=2, ensure_ascii=True)
        primary_path.write_text(serialized, encoding="utf-8")
        latest_ticker_path.write_text(serialized, encoding="utf-8")

        return {
            "event_artifact": str(primary_path),
            "latest_ticker_artifact": str(latest_ticker_path),
            "ticker": event.ticker,
            "status": payload["status"],
        }

    @classmethod
    def load_latest_for_run(cls, run_id: str, *, base_dir: Path = SCENARIO_ROUTER_EVENTS_DIR) -> Dict[str, Any]:
        run_key = cls._safe_key(run_id or "unknown_run")
        path = Path(base_dir) / "by_run" / run_key / "latest.json"
        if not path.exists() or not path.is_file():
            legacy_path = LEGACY_FRESHNESS_EVENTS_DIR / "by_run" / run_key / "latest.json"
            if legacy_path.exists() and legacy_path.is_file():
                path = legacy_path
            else:
                return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    @staticmethod
    def _safe_key(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._:-]+", "_", str(value or "").strip())[:180] or "unknown"
