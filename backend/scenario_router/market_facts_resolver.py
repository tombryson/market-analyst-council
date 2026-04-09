from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from ..market_facts import gather_market_facts_prepass, minimal_market_facts_payload
from .models import AnnouncementFacts, BaselineRunPacket


@dataclass
class ScenarioMarketFactsResolver:
    """Resolve lightweight market context for scenario-router condition checks."""

    async def resolve(
        self,
        facts: AnnouncementFacts,
        baseline_run: BaselineRunPacket,
    ) -> Dict[str, Any]:
        ticker = str(facts.ticker or baseline_run.ticker or "").strip().upper()
        if not ticker:
            return {}

        market_facts = await gather_market_facts_prepass(
            ticker=ticker,
            company_name=facts.company_name or baseline_run.company_name,
            exchange=baseline_run.exchange,
            template_id=baseline_run.template_id,
            company_type=self._resolve_company_type(baseline_run),
        )
        return minimal_market_facts_payload(market_facts)

    @staticmethod
    def _resolve_company_type(baseline_run: BaselineRunPacket) -> str:
        summary = baseline_run.summary_fields if isinstance(baseline_run.summary_fields, dict) else {}
        structured = baseline_run.lab_payload.get("structured_data") if isinstance(baseline_run.lab_payload, dict) else {}
        if isinstance(summary, dict):
            value = str(summary.get("company_type") or "").strip()
            if value:
                return value
        if isinstance(structured, dict):
            value = str(structured.get("company_type") or "").strip()
            if value:
                return value
        return ""
