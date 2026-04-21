from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .models import (
    AnnouncementFacts,
    BaselineRunPacket,
    ComparisonFinding,
    ComparisonReport,
    ConditionEvaluation,
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
    "breached",
}
DOMAIN_KEYWORDS = {
    "financing": {"funding", "facility", "debt", "loan", "placement", "capital", "liquidity", "covenant"},
    "permitting": {"permit", "approval", "license", "licence", "heritage", "environmental", "regulator"},
    "timeline": {"timeline", "milestone", "delay", "delayed", "ahead of schedule", "on track", "mid-2027", "2028"},
    "resource": {"resource", "reserve", "jorc", "ore reserve", "mineral resource"},
    "production": {"production", "throughput", "first gold", "ramp-up", "ramp up", "processing", "run-rate", "run rate", "kozpa"},
    "guidance": {"guidance", "forecast", "outlook", "aisc", "cost guidance", "cash margin"},
    "capital_structure": {"shares", "dilution", "placement", "capital structure", "escrow", "equity raise"},
    "m_and_a": {"acquisition", "scheme", "takeover", "merger", "joint venture", "farm-in", "farm in"},
    "management": {"director", "ceo", "cfo", "chair", "management", "executive"},
    "operations": {"operations", "plant", "mill", "mine", "contractor", "site", "power", "grid", "load shedding"},
}
MARKET_RULE_RE = re.compile(
    r"\b(?P<asset>gold|silver|copper|lithium|uranium|brent|wti|henry hub|henry_hub|natural gas)\b"
    r"[^<>]{0,60}?(?P<op>>=|<=|>|<)\s*(?P<currency>US\$|USD|A\$|AU\$|AUD)\s*(?P<value>[0-9][0-9,]*(?:\.[0-9]+)?)",
    flags=re.IGNORECASE,
)
MARKET_NATURAL_RULE_RE = re.compile(
    r"\b(?P<asset>gold|silver|copper|lithium|uranium|brent|wti|henry hub|henry_hub|natural gas)\b"
    r"[^0-9]{0,60}?\b(?P<word>above|over|greater than|exceeds|exceeding|below|under|less than)\b"
    r"[^0-9]{0,30}?(?P<currency>US\$|USD|A\$|AU\$|AUD)\s*(?P<value>[0-9][0-9,]*(?:\.[0-9]+)?)",
    flags=re.IGNORECASE,
)
MARKET_FIELD_MAP = {
    ("gold", "usd"): "gold_price_usd_oz",
    ("gold", "aud"): "gold_price_aud_oz",
    ("silver", "usd"): "silver_price_usd_oz",
    ("silver", "aud"): "silver_price_aud_oz",
    ("copper", "usd"): "copper_price_usd_lb",
    ("copper", "aud"): "copper_price_aud_lb",
    ("lithium", "usd"): "lithium_price_usd_kg",
    ("lithium", "aud"): "lithium_price_aud_kg",
    ("uranium", "usd"): "uranium_price_usd_lb",
    ("uranium", "aud"): "uranium_price_aud_lb",
    ("brent", "usd"): "brent_price_usd_bbl",
    ("brent", "aud"): "brent_price_aud_bbl",
    ("wti", "usd"): "wti_price_usd_bbl",
    ("wti", "aud"): "wti_price_aud_bbl",
    ("henry hub", "usd"): "henry_hub_price_usd_mmbtu",
    ("henry hub", "aud"): "henry_hub_price_aud_mmbtu",
    ("henry_hub", "usd"): "henry_hub_price_usd_mmbtu",
    ("henry_hub", "aud"): "henry_hub_price_aud_mmbtu",
    ("natural gas", "usd"): "henry_hub_price_usd_mmbtu",
    ("natural gas", "aud"): "henry_hub_price_aud_mmbtu",
}


@dataclass
class ThesisComparator:
    """Compare announcement evidence to explicit thesis-map and watchlist conditions."""

    def compare(self, facts: AnnouncementFacts, baseline_run: BaselineRunPacket) -> ComparisonReport:
        structured = self._structured(baseline_run)
        thesis_map = structured.get("thesis_map") if isinstance(structured.get("thesis_map"), dict) else {}
        current_state = (structured.get("extended_analysis") or {}).get("current_thesis_state") if isinstance(structured.get("extended_analysis"), dict) else {}
        watchlist = structured.get("monitoring_watchlist") if isinstance(structured.get("monitoring_watchlist"), dict) else {}
        baseline_path = self._normalize_path((current_state or {}).get("leaning"))

        haystack = self._build_haystack(facts)
        evidence = facts.evidence[0] if facts.evidence else EvidenceRef(source_title=facts.title)
        market_facts = self._normalized_market_facts(facts.market_facts)

        evaluations: List[ConditionEvaluation] = []
        for scenario in ("bull", "base", "bear"):
            block = thesis_map.get(scenario) if isinstance(thesis_map, dict) else {}
            evaluations.extend(self._evaluate_items(block.get("required_conditions") or [], scenario, "required", haystack, market_facts, evidence))
            evaluations.extend(self._evaluate_items(block.get("failure_conditions") or [], scenario, "failure", haystack, market_facts, evidence))

        evaluations.extend(self._evaluate_watchlist(watchlist.get("red_flags") or [], "red_flag", haystack, market_facts, evidence))
        evaluations.extend(self._evaluate_watchlist(watchlist.get("confirmatory_signals") or [], "confirmatory", haystack, market_facts, evidence))

        matched_evals = [item for item in evaluations if item.status == "matched"]
        announcement_matched_evals = [
            item for item in matched_evals if str(item.matched_via or "").strip() != "market_facts"
        ]
        matched_condition_ids = [
            item.condition_id
            for item in announcement_matched_evals
            if item.group in {"required", "failure"} and item.condition_id
        ]
        triggered_watchlist_ids = [
            item.condition_id
            for item in announcement_matched_evals
            if item.group in {"red_flag", "confirmatory"} and item.condition_id
        ]

        bull_required = self._matched_count(announcement_matched_evals, scenario="bull", group="required")
        base_required = self._matched_count(announcement_matched_evals, scenario="base", group="required")
        bear_required = self._matched_count(announcement_matched_evals, scenario="bear", group="required")
        bull_failure = self._matched_count(announcement_matched_evals, scenario="bull", group="failure")
        base_failure = self._matched_count(announcement_matched_evals, scenario="base", group="failure")
        red_flag_hits = self._matched_count(announcement_matched_evals, group="red_flag")
        confirmatory_hits = self._matched_count(announcement_matched_evals, group="confirmatory")

        positive = self._contains_any(haystack, POSITIVE_TOKENS)
        negative = self._contains_any(haystack, NEGATIVE_TOKENS)
        affected_domains = self._infer_domains(
            facts=facts,
            matched_evaluations=announcement_matched_evals,
        )

        current_path = self._choose_current_path(
            baseline_path=baseline_path,
            bull_required=bull_required,
            base_required=base_required,
            bear_required=bear_required,
            bull_failure=bull_failure,
            base_failure=base_failure,
            red_flag_hits=red_flag_hits,
            confirmatory_hits=confirmatory_hits,
            positive=positive,
            negative=negative,
        )
        path_transition = f"{baseline_path}->{current_path}" if baseline_path and current_path and baseline_path != current_path else ""

        key_findings, conflicts = self._build_findings(announcement_matched_evals)
        material_change_types = list(sorted(affected_domains))
        impact_level = self._impact_level(affected_domains, current_path, baseline_path, conflicts, red_flag_hits, confirmatory_hits)
        thesis_effect = self._thesis_effect(baseline_path, current_path, conflicts, confirmatory_hits, red_flag_hits, positive, negative)
        timeline_effect = self._timeline_effect(affected_domains, positive, negative, evaluations)
        capital_effect = self._capital_effect(affected_domains, positive, negative, evaluations)
        run_validity = self._run_validity(impact_level, current_path, baseline_path, conflicts, red_flag_hits)

        used_market_fields = {
            item.market_field: market_facts.get(item.market_field)
            for item in evaluations
            if item.market_field and market_facts.get(item.market_field) is not None
        }
        notes = [
            f"announcement_bull_required_matches={bull_required}",
            f"announcement_base_required_matches={base_required}",
            f"announcement_bear_required_matches={bear_required}",
            f"announcement_red_flag_hits={red_flag_hits}",
            f"announcement_confirmatory_hits={confirmatory_hits}",
            f"market_condition_matches={self._matched_count([item for item in matched_evals if str(item.matched_via or '').strip() == 'market_facts'])}",
        ]

        return ComparisonReport(
            ticker=facts.ticker,
            baseline_run_id=baseline_run.run_id,
            announcement_title=facts.title,
            baseline_path=baseline_path,
            current_path=current_path,
            path_transition=path_transition,
            path_confidence=self._path_confidence(bull_required, base_required, bear_required, red_flag_hits, confirmatory_hits),
            run_validity=run_validity,
            impact_level=impact_level,
            thesis_effect=thesis_effect,
            timeline_effect=timeline_effect,
            capital_effect=capital_effect,
            affected_domains=material_change_types,
            material_change_types=material_change_types,
            condition_evaluations=evaluations,
            matched_condition_ids=matched_condition_ids,
            triggered_watchlist_ids=triggered_watchlist_ids,
            market_facts_used=used_market_fields,
            key_findings=key_findings,
            conflicts_with_run=conflicts,
            notes=notes,
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

    @staticmethod
    def _normalized_market_facts(payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        normalized = payload.get("normalized_facts") if isinstance(payload.get("normalized_facts"), dict) else {}
        return {str(key): value for key, value in normalized.items()} if isinstance(normalized, dict) else {}

    def _evaluate_items(
        self,
        items: Iterable[Any],
        scenario: str,
        group: str,
        haystack: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> List[ConditionEvaluation]:
        evaluations: List[ConditionEvaluation] = []
        for item in items or []:
            if isinstance(item, dict):
                evaluations.append(self._evaluate_item(item, scenario, group, haystack, market_facts, evidence))
        return evaluations

    def _evaluate_watchlist(
        self,
        items: Iterable[Any],
        group: str,
        haystack: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> List[ConditionEvaluation]:
        evaluations: List[ConditionEvaluation] = []
        for idx, item in enumerate(items or []):
            if isinstance(item, str):
                item = {
                    "watch_id": f"{group}_{idx}",
                    "condition": str(item or "").strip(),
                }
            if not isinstance(item, dict):
                continue
            condition_id = str(item.get("watch_id") or item.get("condition_id") or f"{group}_{idx}").strip()
            payload = {
                "condition_id": condition_id,
                "condition": str(item.get("condition") or item.get("title") or "").strip(),
                "severity": str(item.get("severity") or "").strip().lower(),
                "linked_milestones": item.get("linked_milestones") or [],
            }
            evaluations.append(self._evaluate_item(payload, "", group, haystack, market_facts, evidence))
        return evaluations

    def _evaluate_item(
        self,
        item: Dict[str, Any],
        scenario: str,
        group: str,
        haystack: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> ConditionEvaluation:
        condition_id = str(item.get("condition_id") or item.get("watch_id") or "").strip()
        label = self._condition_label(item)
        linked_milestones = [str(value or "").strip() for value in (item.get("linked_milestones") or []) if str(value or "").strip()]
        severity = str(item.get("severity") or ("high" if group in {"failure", "red_flag"} else "medium")).strip().lower()

        market_eval = self._try_market_evaluation(
            condition_id=condition_id,
            scenario=scenario,
            group=group,
            label=label,
            linked_milestones=linked_milestones,
            severity=severity,
            market_facts=market_facts,
            evidence=evidence,
        )
        if market_eval is not None:
            return market_eval

        if self._is_market_condition(label):
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="unclear",
                reason="Market-price condition was not text-matched; no parseable market rule was available.",
                confidence=0.35,
                matched_via="market_facts",
                severity=severity,
                linked_milestones=linked_milestones,
                evidence=evidence,
            )

        phrases = self._condition_phrases(item)
        matched_phrase = next((phrase for phrase in phrases if self._phrase_matches(phrase, haystack)), "")
        if matched_phrase:
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="matched",
                reason=f"Matched announcement text via phrase: {matched_phrase}",
                confidence=0.78 if group in {"required", "confirmatory"} else 0.84,
                matched_via="text",
                severity=severity,
                linked_milestones=linked_milestones,
                evidence=evidence,
            )

        return ConditionEvaluation(
            condition_id=condition_id,
            scenario=scenario,
            group=group,
            label=label,
            status="not_matched",
            reason="No explicit support found in the announcement text or market context.",
            confidence=0.5,
            matched_via="",
            severity=severity,
            linked_milestones=linked_milestones,
            evidence=evidence,
        )

    def _try_market_evaluation(
        self,
        *,
        condition_id: str,
        scenario: str,
        group: str,
        label: str,
        linked_milestones: List[str],
        severity: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> Optional[ConditionEvaluation]:
        match = MARKET_RULE_RE.search(label or "") or MARKET_NATURAL_RULE_RE.search(label or "")
        if not match:
            return None

        asset = str(match.group("asset") or "").strip().lower()
        op = str(match.groupdict().get("op") or "").strip()
        if not op:
            op = self._natural_market_comparator(str(match.groupdict().get("word") or ""))
        currency = str(match.group("currency") or "").strip().lower()
        currency_key = "aud" if currency in {"a$", "au$", "aud"} else "usd"
        raw_value = str(match.group("value") or "").replace(",", "")
        try:
            threshold_value = float(raw_value)
        except ValueError:
            threshold_value = None
        market_field = MARKET_FIELD_MAP.get((asset, currency_key), "")
        observed_value = market_facts.get(market_field) if market_field else None

        if threshold_value is None or not market_field:
            return None

        if observed_value is None:
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="unclear",
                reason=f"Condition depends on {market_field}, but no fresh market fact was available.",
                confidence=0.35,
                matched_via="market_facts",
                market_field=market_field,
                observed_value=None,
                comparator=op,
                threshold_value=threshold_value,
                severity=severity,
                linked_milestones=linked_milestones,
                evidence=evidence,
            )

        comparison_ok = self._compare_numeric(float(observed_value), op, threshold_value)
        return ConditionEvaluation(
            condition_id=condition_id,
            scenario=scenario,
            group=group,
            label=label,
            status="matched" if comparison_ok else "contradicted",
            reason=(
                f"Resolved via {market_field}: observed {float(observed_value):.2f} {op} {threshold_value:.2f}."
                if comparison_ok
                else f"Resolved via {market_field}: observed {float(observed_value):.2f}, which does not satisfy {op} {threshold_value:.2f}."
            ),
            confidence=0.92,
            matched_via="market_facts",
            market_field=market_field,
            observed_value=float(observed_value),
            comparator=op,
            threshold_value=threshold_value,
            severity=severity,
            linked_milestones=linked_milestones,
            evidence=evidence,
        )

    @staticmethod
    def _compare_numeric(observed_value: float, comparator: str, threshold_value: float) -> bool:
        if comparator == ">":
            return observed_value > threshold_value
        if comparator == "<":
            return observed_value < threshold_value
        if comparator == ">=":
            return observed_value >= threshold_value
        if comparator == "<=":
            return observed_value <= threshold_value
        return False

    @staticmethod
    def _natural_market_comparator(word: str) -> str:
        normalized = str(word or "").strip().lower()
        if normalized in {"above", "over", "greater than", "exceeds", "exceeding"}:
            return ">"
        if normalized in {"below", "under", "less than"}:
            return "<"
        return ""

    @staticmethod
    def _is_market_condition(label: str) -> bool:
        text = str(label or "").strip().lower()
        if not text:
            return False
        if re.search(r"\b(gold|silver|copper|lithium|uranium|brent|wti|henry hub|natural gas)\b", text) and re.search(
            r"(us\$|usd|a\$|au\$|aud|\$)\s*[0-9]|[<>]|above|below|under|over|greater than|less than",
            text,
        ):
            return True
        return False

    @staticmethod
    def _condition_phrases(item: Dict[str, Any]) -> List[str]:
        phrases: List[str] = []
        value = str(item.get("condition") or "").strip()
        if value and ThesisComparator._is_meaningful_support_phrase(value):
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
        terms = [term for term in re.split(r"[^a-z0-9]+", low) if len(term) >= 5]
        if len(terms) < 3:
            return False
        # Loose token matching is only a fallback for explicit evidence hooks.
        # Requiring all meaningful terms avoids matching conditions on generic
        # words like "gold", "quarter", "resource", or dates.
        return all(term in haystack for term in terms)

    @staticmethod
    def _condition_label(item: Dict[str, Any]) -> str:
        return str(item.get("condition") or item.get("title") or item.get("condition_id") or item.get("watch_id") or "").strip()

    @staticmethod
    def _contains_any(haystack: str, tokens: Set[str]) -> bool:
        return any(token in haystack for token in tokens)

    @staticmethod
    def _infer_domains(facts: AnnouncementFacts, matched_evaluations: List[ConditionEvaluation]) -> Set[str]:
        labels = "\n".join(item.label for item in matched_evaluations if str(item.label or "").strip()).lower()
        fact_text = "\n".join(
            [facts.title or "", facts.summary or ""] + [str(item or "") for item in (facts.extracted_facts or [])]
        ).lower()
        haystack = f"{fact_text}\n{labels}"
        domains: Set[str] = set()
        for domain, keywords in DOMAIN_KEYWORDS.items():
            if any(ThesisComparator._keyword_in_text(keyword, haystack) for keyword in keywords):
                domains.add(domain)
        if not domains:
            topics = [str(item or "").strip().lower() for item in (facts.material_topics or []) if str(item or "").strip()]
            if len(topics) <= 3:
                domains.update(topics)
        return domains

    @staticmethod
    def _keyword_in_text(keyword: str, haystack: str) -> bool:
        term = str(keyword or "").strip().lower()
        if not term:
            return False
        if " " in term or "-" in term:
            return term in haystack
        return re.search(rf"\b{re.escape(term)}\b", haystack) is not None

    @staticmethod
    def _matched_count(evaluations: List[ConditionEvaluation], *, scenario: str = "", group: str = "") -> int:
        return sum(
            1
            for item in evaluations
            if item.status == "matched"
            and (not scenario or item.scenario == scenario)
            and (not group or item.group == group)
        )

    @staticmethod
    def _choose_current_path(
        *,
        baseline_path: str,
        bull_required: int,
        base_required: int,
        bear_required: int,
        bull_failure: int,
        base_failure: int,
        red_flag_hits: int,
        confirmatory_hits: int,
        positive: bool,
        negative: bool,
    ) -> str:
        if bear_required > 0 or bull_failure > 0 or base_failure > 0:
            return "bear"
        if red_flag_hits > 0 and negative and baseline_path in {"bull", "base"}:
            return "bear"
        if bull_required > 0 and (
            bull_required > base_required
            or (bull_required > 0 and base_required == 0 and (confirmatory_hits > 0 or positive))
        ):
            return "bull"
        if base_required > 0 or confirmatory_hits > 0:
            return "base"
        if positive and baseline_path in {"base", "bear"}:
            return baseline_path
        if negative and baseline_path in {"bull", "base"}:
            return baseline_path
        if baseline_path in {"bull", "base", "bear"}:
            return baseline_path
        return "mixed" if positive and negative else "unknown"

    def _build_findings(self, evaluations: List[ConditionEvaluation]) -> Tuple[List[ComparisonFinding], List[ComparisonFinding]]:
        findings: List[ComparisonFinding] = []
        conflicts: List[ComparisonFinding] = []
        for item in evaluations:
            if item.status != "matched":
                continue
            finding = ComparisonFinding(
                type=f"{item.group}_match" if item.group else "condition_match",
                summary=f"Matched {item.group or 'condition'}: {item.label}",
                severity="high" if item.group in {"failure", "red_flag"} else "low",
                evidence=item.evidence,
            )
            if item.group in {"failure", "red_flag"} or item.scenario == "bear":
                conflicts.append(finding)
            else:
                findings.append(finding)
        return findings[:8], conflicts[:8]

    @staticmethod
    def _impact_level(
        affected_domains: Set[str],
        current_path: str,
        baseline_path: str,
        conflicts: List[ComparisonFinding],
        red_flag_hits: int,
        confirmatory_hits: int,
    ) -> str:
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "high"
        if conflicts or red_flag_hits > 0:
            return "high"
        if current_path == "bull" and baseline_path in {"base", "bear"}:
            return "medium"
        if confirmatory_hits > 0:
            return "medium"
        if affected_domains & {"timeline", "operations", "management"}:
            return "medium"
        if affected_domains:
            return "low"
        return "none"

    @staticmethod
    def _thesis_effect(
        baseline_path: str,
        current_path: str,
        conflicts: List[ComparisonFinding],
        confirmatory_hits: int,
        red_flag_hits: int,
        positive: bool,
        negative: bool,
    ) -> str:
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "undermines"
        if conflicts or red_flag_hits > 0:
            return "undermines"
        if current_path == "bull" and baseline_path in {"base", "bear"}:
            return "accelerates"
        if confirmatory_hits > 0 or positive:
            return "confirms"
        if negative:
            return "partially_confirms"
        return "no_change"

    @staticmethod
    def _timeline_effect(domains: Set[str], positive: bool, negative: bool, evaluations: List[ConditionEvaluation]) -> str:
        if "timeline" not in domains:
            return "unknown"
        if any(item.group in {"failure", "red_flag"} and item.status == "matched" for item in evaluations):
            return "delayed"
        if positive:
            return "accelerated"
        if negative:
            return "delayed"
        return "on_track"

    @staticmethod
    def _capital_effect(domains: Set[str], positive: bool, negative: bool, evaluations: List[ConditionEvaluation]) -> str:
        if "financing" not in domains and "capital_structure" not in domains:
            return "unknown"
        if any(item.group in {"failure", "red_flag"} and item.status == "matched" for item in evaluations):
            return "worsens"
        if positive:
            return "improves"
        if negative:
            return "worsens"
        return "material_change"

    @staticmethod
    def _run_validity(
        impact_level: str,
        current_path: str,
        baseline_path: str,
        conflicts: List[ComparisonFinding],
        red_flag_hits: int,
    ) -> str:
        if impact_level == "critical":
            return "invalidated"
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "partial_invalidation"
        if conflicts or red_flag_hits > 0:
            return "partial_invalidation"
        if impact_level in {"high", "medium"}:
            return "watch"
        return "intact"

    @staticmethod
    def _path_confidence(
        bull_required: int,
        base_required: int,
        bear_required: int,
        red_flag_hits: int,
        confirmatory_hits: int,
    ) -> float:
        total = bull_required + base_required + bear_required + red_flag_hits + confirmatory_hits
        if total <= 0:
            return 0.0
        strongest = max(bull_required, base_required, bear_required + red_flag_hits)
        return round(float(strongest / total), 3)
