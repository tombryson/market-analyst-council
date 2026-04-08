from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .. import main as main_api
from .models import BaselineRunPacket


@dataclass
class LatestRunSelector:
    """Select and load the latest saved lab run for a ticker."""

    limit: int = 25

    async def select_latest(self, ticker: str, exchange: str = "") -> BaselineRunPacket:
        normalized_ticker = str(ticker or "").strip().upper()
        if not normalized_ticker:
            raise RuntimeError("LatestRunSelector requires a ticker.")

        listing = await main_api.list_gantt_runs(limit=max(1, int(self.limit)), ticker=normalized_ticker)
        runs = list((listing or {}).get("runs") or [])
        if not runs:
            raise RuntimeError(f"No saved lab runs found for {normalized_ticker}.")

        selected = self._pick_best_run(runs, normalized_ticker, exchange)
        run_id = str((selected or {}).get("id") or "").strip()
        if not run_id:
            raise RuntimeError(f"Saved run entry for {normalized_ticker} is missing an id.")

        packet = await main_api.get_gantt_run_report_packet(run_id)
        return self._coerce_report_packet(packet)

    def _pick_best_run(self, runs: list, ticker: str, exchange: str) -> Dict[str, Any]:
        exchange_norm = str(exchange or "").strip().upper()
        if not exchange_norm:
            return dict(runs[0])

        for row in runs:
            row_ticker = str((row or {}).get("ticker") or "").strip().upper()
            if row_ticker == ticker and row_ticker.startswith(f"{exchange_norm}:"):
                return dict(row)
        return dict(runs[0])

    def _coerce_report_packet(self, packet: Dict[str, Any]) -> BaselineRunPacket:
        summary_fields = packet.get("summary_fields") if isinstance(packet.get("summary_fields"), dict) else {}
        lab_payload = packet.get("lab_payload") if isinstance(packet.get("lab_payload"), dict) else {}
        freshness = lab_payload.get("freshness") if isinstance(lab_payload.get("freshness"), dict) else {}
        return BaselineRunPacket(
            run_id=str(packet.get("run_id") or "").strip(),
            ticker=str(summary_fields.get("ticker") or "").strip(),
            exchange=self._infer_exchange(summary_fields.get("ticker")),
            company_name=str(summary_fields.get("company_name") or "").strip(),
            template_id=str(summary_fields.get("template_id") or "").strip(),
            freshness_status=str(summary_fields.get("freshness_status") or freshness.get("status") or "").strip(),
            freshness_age_days=self._coerce_int(summary_fields.get("freshness_age_days")),
            summary_fields=dict(summary_fields),
            lab_payload=dict(lab_payload),
            timeline_rows=list(packet.get("timeline_rows") or []),
            memos=dict(packet.get("memos") or {}),
        )

    @staticmethod
    def _infer_exchange(ticker: Any) -> str:
        text = str(ticker or "").strip().upper()
        if ":" in text:
            return text.split(":", 1)[0].strip()
        return ""

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        try:
            if value is None or value == "":
                return None
            return int(value)
        except Exception:
            return None
