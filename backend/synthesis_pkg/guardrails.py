"""Source-fact and energy guardrails for Stage-3 synthesis output."""

import re
from typing import Any, Dict, List, Optional
import logging

from ..market_facts import minimal_market_facts_payload
from ..source_fact_context import build_source_fact_context
from .text_utils import _strip_markdown_formatting

logger = logging.getLogger(__name__)

def _split_source_fact_candidates(text: str) -> List[str]:
    """Return compact source lines/sentences suitable for deterministic guardrails."""
    raw = str(text or "")
    if not raw:
        return []

    candidates: List[str] = []
    for raw_line in raw.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip().strip('",')
        if not line:
            continue
        if len(line) <= 650:
            candidates.append(line)
            continue
        for sentence in re.split(r"(?<=[.!?])\s+", line):
            sentence = re.sub(r"\s+", " ", sentence).strip().strip('",')
            if sentence:
                candidates.append(sentence[:650].rstrip())
    return candidates


def _dedupe_source_fact_lines(lines: List[str], *, limit: int = 12) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for line in lines:
        cleaned = _strip_markdown_formatting(line)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" -")
        if not cleaned:
            continue
        key = re.sub(r"[^a-z0-9]+", " ", cleaned.lower()).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(cleaned[:520].rstrip())
        if len(out) >= limit:
            break
    return out


def _rank_energy_guardrail_line(line: str, *, kind: str) -> Tuple[int, int]:
    lower = str(line or "").lower()
    score = 0
    if kind == "production":
        if re.search(r"\bfy20\d{2}\b", lower):
            score += 50
        if re.search(r"\b(boepd|boe/day|boe/d)\b", lower):
            score += 40
        if re.search(r"\b(average|daily|baseline|track record|scaled)\b", lower):
            score += 25
        if re.search(r"\b(1,790|1790|1,814|1814|2,000|2000|3,000|3000)\b", lower):
            score += 25
        if "operational declines" in lower:
            score -= 20
        if "one_line" in lower or "key_facts_paragraph" in lower or "keyfactsparagraph" in lower:
            score -= 10
    elif kind == "hedge":
        if re.search(r"\b(open|position|positions|contracted|settled)\b", lower):
            score += 30
        if re.search(r"\b(bbl|mmbtu|jan|jun|dec|avg|us\$)\b", lower):
            score += 20
        if "milestone" in lower or "source_snippet" in lower:
            score -= 20
    return (-score, len(line))


def _build_stage3_source_fact_guardrails(
    enhanced_context: str,
    *,
    template_id: str,
    evidence_pack: Optional[Dict[str, Any]] = None,
    max_chars: int = 4500,
) -> str:
    """Extract compact primary-source facts that Stage 3 must not contradict."""
    rendered_fact_packet = build_source_fact_context(
        evidence_pack,
        max_source_rows=12,
        max_excerpt_rows=4,
        max_sections=8,
        max_facts_per_section=5,
        max_chars=max_chars,
    )
    if rendered_fact_packet:
        return rendered_fact_packet

    if str(template_id or "").strip() != "energy_oil_gas":
        return ""

    candidates = _split_source_fact_candidates(enhanced_context)
    if not candidates:
        return ""

    hedge_lines: List[str] = []
    production_lines: List[str] = []
    well_rate_lines: List[str] = []
    financing_lines: List[str] = []

    for line in candidates:
        lower = line.lower()
        has_boe_rate = bool(
            re.search(r"\bboe\s*/?\s*d(?:ay)?\b|\bboepd\b|\bboe/day\b", lower)
        )
        if "hedg" in lower:
            hedge_lines.append(line)
        if (
            ("production" in lower and ("boe" in lower or "boepd" in lower))
            or has_boe_rate
            or re.search(r"\bnet production\b", lower)
        ):
            production_lines.append(line)
        if re.search(r"\bip(?:24|30|90)?\b", lower) and ("boe" in lower or "bbl" in lower):
            well_rate_lines.append(line)
        if (
            ("debt" in lower or "facility" in lower or "cash" in lower)
            and re.search(r"\b(a\$|us\$|\$)\s*[0-9]", lower)
        ):
            financing_lines.append(line)

    sections: List[str] = []
    hedge_lines = sorted(hedge_lines, key=lambda line: _rank_energy_guardrail_line(line, kind="hedge"))
    production_lines = sorted(
        production_lines,
        key=lambda line: _rank_energy_guardrail_line(line, kind="production"),
    )

    hedge_facts = _dedupe_source_fact_lines(hedge_lines, limit=6)
    production_facts = _dedupe_source_fact_lines(production_lines, limit=6)
    well_rate_facts = _dedupe_source_fact_lines(well_rate_lines, limit=3)
    financing_facts = _dedupe_source_fact_lines(financing_lines, limit=4)

    if hedge_facts:
        sections.append(
            "Hedging facts present in source packet:\n"
            + "\n".join(f"- {item}" for item in hedge_facts)
            + "\nInstruction: do not call the company unhedged or hedging unknown. If coverage is incomplete, say partial hedge coverage with residual commodity exposure."
        )
    if production_facts:
        sections.append(
            "Production baseline facts present in source packet:\n"
            + "\n".join(f"- {item}" for item in production_facts)
            + "\nInstruction: do not use a production trigger below the latest disclosed baseline as a future bull/base milestone."
        )
    if well_rate_facts:
        sections.append(
            "Well-rate facts present in source packet:\n"
            + "\n".join(f"- {item}" for item in well_rate_facts)
        )
    if financing_facts:
        sections.append(
            "Balance-sheet facts present in source packet:\n"
            + "\n".join(f"- {item}" for item in financing_facts)
        )

    rendered = "\n\n".join(sections).strip()
    if max_chars > 0 and len(rendered) > max_chars:
        rendered = rendered[: max_chars - 3].rstrip() + "..."
    return rendered


def _inject_stage3_audit_context(
    structured_data: Dict[str, Any],
    market_facts: Optional[Dict[str, Any]],
    template_contract: Optional[Dict[str, Any]],
) -> None:
    if not isinstance(structured_data, dict):
        return

    market_payload = minimal_market_facts_payload(market_facts)
    if market_payload:
        structured_data["market_facts"] = market_payload

    contract = template_contract if isinstance(template_contract, dict) else {}
    if contract:
        structured_data["template_contract"] = {
            "id": str(contract.get("id", "") or ""),
            "family": str(contract.get("family", "") or ""),
            "industry_label": str(contract.get("industry_label", "") or ""),
        }


def _apply_source_fact_guardrails(
    structured_data: Dict[str, Any],
    source_fact_guardrails: str,
) -> None:
    """Record deterministic source guardrails and clean obvious hedge-gap contradictions."""
    if not isinstance(structured_data, dict):
        return
    guardrails = str(source_fact_guardrails or "").strip()
    if not guardrails:
        return

    metadata = structured_data.get("council_metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        structured_data["council_metadata"] = metadata
    metadata["source_fact_guardrails"] = {
        "kind": "primary_source_fact_packet",
        "has_explicit_hedging_facts": "hedging facts present in source packet" in guardrails.lower(),
        "has_explicit_production_baseline": "production baseline facts present in source packet" in guardrails.lower(),
        "excerpt": guardrails[:2500],
    }

    if "hedging facts present in source packet" not in guardrails.lower():
        return

    queue = structured_data.get("verification_queue")
    if not isinstance(queue, list):
        return

    patched = False
    for item in queue:
        if not isinstance(item, dict):
            continue
        combined = " ".join(
            str(item.get(key) or "")
            for key in ("field", "reason", "required_source")
        ).lower()
        if "hedg" not in combined:
            continue
        if re.search(r"\b(unknown|not disclosed|absent|no data|unhedged|if hedged)\b", combined):
            item["field"] = "Residual hedge coverage / commodity exposure"
            item["reason"] = (
                "Primary filings disclose hedge positions; verify current coverage ratio "
                "and residual unhedged exposure rather than whether a hedge book exists."
            )
            item["required_source"] = "Latest quarterly hedge table / annual financial risk notes"
            item["priority"] = str(item.get("priority") or "high").lower()
            if item["priority"] not in {"high", "medium", "low"}:
                item["priority"] = "high"
            patched = True

    if patched:
        metadata.setdefault("source_fact_guardrail_warnings", []).append(
            "Hedging verification queue was rewritten because source guardrails contained explicit hedge positions."
        )


