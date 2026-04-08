from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Set, Tuple

from .models import (
    AnnouncementFacts,
    BaselineRunPacket,
    ComparisonFinding,
    ComparisonReport,
    EvidenceRef,
)

POSITIVE_TOKENS = {"approved", "secured", "completed", "achieved", "on track", "ahead", "accelerated", "funded", "signed"}
NEGATIVE_TOKENS = {
    "delay",
    "delayed",
    "risk",
    "at risk",
    "suspend",
    "halt",
    "withdrawn",
    "default",
    "cost overrun",
    "dilution",
    "shortfall",
    "funding gap",
    "cancelled",
}
DOMAIN_KEYWORDS = {
    "financing": {"funding", "facility", "debt", "loan", "placement", "capital", "liquidity"},
    "permitting": {"permit", "approval", "license", "licence", "heritage", "environmental", "regulator"},
    "timeline": {"timeline", "quarter", "milestone", "delay", "delayed", "ahead of schedule", "on track"},
    "resource": {"resource", "reserve", "jorc", "ore reserve", "mineral resource"},
    "production": {"production", "throughput", "first gold", "ramp-up", "ramp up", "processing"},
    "guidance": {"guidance", "forecast", "outlook", "aisc", "cost guidance"},
    "capital_structure": {"shares", "dilution", "placement", "capital structure", "escrow"},
    "m_and_a": {"acquisition", "scheme", "takeover", "merger", "joint venture", "farm-in", "farm in"},
    "management": {"director", "ceo", "cfo", "chair", "management", "executive"},
    "operations": {"operations", "plant", "mill", "mine", "contractor", "site"},
}


@dataclass
class ThesisComparator:
    """Compare announcement evidence to the lab thesis map and current path."""

    def compare(self, facts: AnnouncementFacts, baseline_run: BaselineRunPacket) -> ComparisonReport:
        structured = self._structured(baseline_run)
        thesis_map = structured.get("thesis_map") if isinstance(structured.get("thesis_map"), dict) else {}
        current_state = (structured.get("extended_analysis") or {}).get("current_thesis_state") if isinstance(structured.get("extended_analysis"), dict) else {}
        baseline_path = self._normalize_path((current_state or {}).get("leaning"))

        haystack = self._build_haystack(facts)
        scenario_hits: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for scenario in ("bull", "base", "bear"):
            block = thesis_map.get(scenario) if isinstance(thesis_map, dict) else {}
            scenario_hits[scenario] = {
                "required": self._match_conditions(block.get("required_conditions") or [], haystack),
                "failure": self._match_conditions(block.get("failure_conditions") or [], haystack),
            }

        affected_domains = self._infer_domains(haystack, facts.material_topics)
        positive = self._contains_any(haystack, POSITIVE_TOKENS)
        negative = self._contains_any(haystack, NEGATIVE_TOKENS)

        bull_strength = len(scenario_hits["bull"]["required"])
        base_strength = len(scenario_hits["base"]["required"])
        bear_strength = len(scenario_hits["bear"]["required"]) + len(scenario_hits["bull"]["failure"]) + len(scenario_hits["base"]["failure"])

        current_path = self._choose_current_path(
            baseline_path,
            bull_strength,
            base_strength,
            bear_strength,
            positive,
            negative,
        )
        path_transition = f"{baseline_path}->{current_path}" if baseline_path and current_path and baseline_path != current_path else ""

        key_findings, conflicts = self._build_findings(scenario_hits, facts)
        material_change_types = list(sorted(affected_domains))
        impact_level = self._impact_level(affected_domains, current_path, baseline_path, conflicts)
        thesis_effect = self._thesis_effect(baseline_path, current_path, conflicts, positive, negative)
        timeline_effect = self._timeline_effect(affected_domains, positive, negative)
        capital_effect = self._capital_effect(affected_domains, positive, negative)
        run_validity = self._run_validity(impact_level, current_path, baseline_path, conflicts)

        return ComparisonReport(
            ticker=facts.ticker,
            baseline_run_id=baseline_run.run_id,
            announcement_title=facts.title,
            baseline_path=baseline_path,
            current_path=current_path,
            path_transition=path_transition,
            path_confidence=self._path_confidence(bull_strength, base_strength, bear_strength),
            run_validity=run_validity,
            impact_level=impact_level,
            thesis_effect=thesis_effect,
            timeline_effect=timeline_effect,
            capital_effect=capital_effect,
            affected_domains=material_change_types,
            material_change_types=material_change_types,
            key_findings=key_findings,
            conflicts_with_run=conflicts,
            notes=[f"bull_hits={bull_strength}", f"base_hits={base_strength}", f"bear_hits={bear_strength}"],
        )

    @staticmethod
    def _structured(baseline_run: BaselineRunPacket) -> Dict[str, Any]:
        lab_payload = baseline_run.lab_payload if isinstance(baseline_run.lab_payload, dict) else {}
        structured = lab_payload.get("structured_data") if isinstance(lab_payload.get("structured_data"), dict) else {}
        return structured if isinstance(structured, dict) else {}

    @staticmethod
    def _normalize_path(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text.startswith("bull"):
            return "bull"
        if text.startswith("base"):
            return "base"
        if text.startswith("bear"):
            return "bear"
        if "mixed" in text:
            return "mixed"
        return "unknown"

    @staticmethod
    def _build_haystack(facts: AnnouncementFacts) -> str:
        parts = [facts.title, facts.summary, facts.raw_text_excerpt] + list(facts.extracted_facts or [])
        return "\n".join(str(part or "") for part in parts).lower()

    def _match_conditions(self, conditions: Iterable[Any], haystack: str) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []
        for item in conditions or []:
            if not isinstance(item, dict):
                continue
            phrases = self._condition_phrases(item)
            if not phrases:
                continue
            if any(self._phrase_matches(phrase, haystack) for phrase in phrases):
                matches.append(item)
        return matches

    @staticmethod
    def _condition_phrases(item: Dict[str, Any]) -> List[str]:
        phrases: List[str] = []
        for key in ("condition", "condition_id"):
            value = str(item.get(key) or "").strip()
            if value:
                phrases.append(value)
        for value in item.get("evidence_hooks") or []:
            text = str(value or "").strip()
            if ThesisComparator._is_meaningful_support_phrase(text):
                phrases.append(text)
        return phrases[:6]

    @staticmethod
    def _is_meaningful_support_phrase(text: str) -> bool:
        phrase = str(text or "").strip().lower()
        if not phrase:
            return False
        if " " in phrase:
            return True
        return len(phrase) >= 12

    @staticmethod
    def _phrase_matches(phrase: str, haystack: str) -> bool:
        low = str(phrase or "").strip().lower()
        if not low:
            return False
        if low in haystack:
            return True
        terms = [term for term in re.split(r"[^a-z0-9]+", low) if len(term) >= 4]
        if not terms:
            return False
        required = terms[:3]
        return all(term in haystack for term in required)

    @staticmethod
    def _infer_domains(haystack: str, material_topics: List[str]) -> Set[str]:
        domains: Set[str] = {str(item or "").strip().lower() for item in (material_topics or []) if str(item or "").strip()}
        for domain, keywords in DOMAIN_KEYWORDS.items():
            if any(keyword in haystack for keyword in keywords):
                domains.add(domain)
        return domains

    @staticmethod
    def _contains_any(haystack: str, tokens: Set[str]) -> bool:
        return any(token in haystack for token in tokens)

    @staticmethod
    def _choose_current_path(
        baseline_path: str,
        bull_strength: int,
        base_strength: int,
        bear_strength: int,
        positive: bool,
        negative: bool,
    ) -> str:
        if bear_strength > 0 and (negative or bear_strength >= max(bull_strength, base_strength)):
            return "bear"
        if bear_strength == 0 and bull_strength > base_strength:
            return "bull"
        if bear_strength == 0 and bull_strength > 0 and base_strength == 0 and positive:
            return "bull"
        if base_strength > 0:
            return "base"
        if baseline_path in {"bull", "base", "bear"}:
            return baseline_path
        return "mixed" if positive and negative else "unknown"

    @staticmethod
    def _impact_level(affected_domains: Set[str], current_path: str, baseline_path: str, conflicts: List[ComparisonFinding]) -> str:
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "high"
        if current_path == "bull" and baseline_path == "bear":
            return "high"
        if conflicts:
            return "high"
        if affected_domains & {"financing", "permitting", "resource", "production", "guidance", "m_and_a"}:
            return "medium"
        if affected_domains:
            return "low"
        return "none"

    @staticmethod
    def _thesis_effect(baseline_path: str, current_path: str, conflicts: List[ComparisonFinding], positive: bool, negative: bool) -> str:
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "undermines"
        if conflicts:
            return "undermines"
        if current_path == "bull" and baseline_path in {"base", "bear"}:
            return "accelerates"
        if current_path == baseline_path and positive:
            return "confirms"
        if positive or negative:
            return "partially_confirms"
        return "no_change"

    @staticmethod
    def _timeline_effect(domains: Set[str], positive: bool, negative: bool) -> str:
        if "timeline" not in domains:
            return "unknown"
        if negative:
            return "delayed"
        if positive:
            return "accelerated"
        return "on_track"

    @staticmethod
    def _capital_effect(domains: Set[str], positive: bool, negative: bool) -> str:
        if "financing" not in domains and "capital_structure" not in domains:
            return "unknown"
        if negative:
            return "worsens"
        if positive:
            return "improves"
        return "material_change"

    @staticmethod
    def _run_validity(impact_level: str, current_path: str, baseline_path: str, conflicts: List[ComparisonFinding]) -> str:
        if impact_level == "critical":
            return "invalidated"
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "partial_invalidation"
        if current_path == "bull" and baseline_path == "bear":
            return "partial_invalidation"
        if conflicts:
            return "partial_invalidation"
        if impact_level in {"high", "medium"}:
            return "watch"
        return "intact"

    @staticmethod
    def _path_confidence(bull_strength: int, base_strength: int, bear_strength: int) -> float:
        total = bull_strength + base_strength + bear_strength
        if total <= 0:
            return 0.0
        strongest = max(bull_strength, base_strength, bear_strength)
        return round(float(strongest / total), 3)

    def _build_findings(self, scenario_hits: Dict[str, Dict[str, List[Dict[str, Any]]]], facts: AnnouncementFacts) -> Tuple[List[ComparisonFinding], List[ComparisonFinding]]:
        evidence = facts.evidence[0] if facts.evidence else EvidenceRef(source_title=facts.title)
        findings: List[ComparisonFinding] = []
        conflicts: List[ComparisonFinding] = []

        for scenario_name in ("bull", "base"):
            for item in scenario_hits.get(scenario_name, {}).get("required", [])[:2]:
                findings.append(
                    ComparisonFinding(
                        type=f"{scenario_name}_required_match",
                        summary=(
                            f"Matched {scenario_name} required condition: "
                            f"{self._condition_label(item)}"
                        ),
                        severity="low",
                        evidence=evidence,
                    )
                )

        for scenario_name in ("bull", "base"):
            for item in scenario_hits.get(scenario_name, {}).get("failure", [])[:2]:
                conflicts.append(
                    ComparisonFinding(
                        type=f"{scenario_name}_failure_match",
                        summary=(
                            f"Matched {scenario_name} failure condition: "
                            f"{self._condition_label(item)}"
                        ),
                        severity="high",
                        evidence=evidence,
                    )
                )

        for item in scenario_hits.get("bear", {}).get("required", [])[:2]:
            conflicts.append(
                ComparisonFinding(
                    type="bear_required_match",
                    summary=f"Matched bear required condition: {self._condition_label(item)}",
                    severity="high",
                    evidence=evidence,
                )
            )

        return findings, conflicts

    @staticmethod
    def _condition_label(item: Dict[str, Any]) -> str:
        return str(item.get("condition") or item.get("condition_id") or "").strip()
