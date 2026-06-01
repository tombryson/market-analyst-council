from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
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
    "financing": {"funding", "facility", "debt", "loan", "placement", "capital raise", "equity raise", "liquidity", "covenant"},
    "permitting": {"permit", "approval", "license", "licence", "heritage", "environmental", "regulator"},
    "regulatory": {"regulator", "regulatory", "approval", "compliance", "investigation", "licence", "license"},
    "timeline": {"timeline", "milestone", "delay", "delayed", "ahead of schedule", "on track", "mid-2027", "2028"},
    "resource": {"resource", "reserve", "jorc", "ore reserve", "mineral resource"},
    "production": {"production", "throughput", "first gold", "ramp-up", "ramp up", "processing", "run-rate", "run rate", "kozpa"},
    "guidance": {"guidance", "forecast", "outlook", "aisc", "cost guidance", "cash margin", "revenue guidance", "earnings guidance"},
    "capital_structure": {"shares", "dilution", "placement", "capital structure", "escrow", "equity raise"},
    "capital_management": {"buyback", "buy-back", "buy back", "share repurchase", "return of capital", "dividend", "capital management"},
    "m_and_a": {"acquisition", "scheme", "takeover", "merger", "joint venture", "farm-in", "farm in"},
    "management": {"director", "ceo", "cfo", "chair", "executive", "resignation", "appointment"},
    "governance": {"board", "director", "chair", "resignation", "appointment", "governance", "audit"},
    "operations": {"operations", "plant", "mill", "mine", "contractor", "site", "power", "grid", "load shedding", "facility", "service", "platform"},
    "commercial": {"contract", "agreement", "customer", "client", "partner", "distribution", "order", "purchase order"},
    "customer": {"customer", "client", "subscriber", "user", "account", "churn", "retention"},
    "product": {"product", "launch", "release", "trial", "platform", "software", "service", "device"},
    "technology": {"technology", "software", "platform", "patent", "clinical", "data", "cyber", "ai"},
    "legal": {"litigation", "claim", "proceeding", "settlement", "court", "dispute", "breach"},
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
        verification_queue = structured.get("verification_queue") if isinstance(structured.get("verification_queue"), list) else []
        baseline_path = self._normalize_path((current_state or {}).get("leaning"))

        context_haystack = self._build_haystack(facts)
        evidence_haystack = self._build_evidence_haystack(facts) or context_haystack
        evidence = facts.evidence[0] if facts.evidence else EvidenceRef(source_title=facts.title)
        market_facts = self._normalized_market_facts(facts.market_facts)

        evaluations: List[ConditionEvaluation] = []
        for scenario in ("bull", "base", "bear"):
            block = thesis_map.get(scenario) if isinstance(thesis_map, dict) else {}
            evaluations.extend(self._evaluate_items(block.get("required_conditions") or [], scenario, "required", evidence_haystack, market_facts, evidence))
            evaluations.extend(self._evaluate_items(block.get("failure_conditions") or [], scenario, "failure", evidence_haystack, market_facts, evidence))

        evaluations.extend(self._evaluate_watchlist(watchlist.get("red_flags") or [], "red_flag", evidence_haystack, market_facts, evidence))
        evaluations.extend(self._evaluate_watchlist(watchlist.get("confirmatory_signals") or [], "confirmatory", evidence_haystack, market_facts, evidence))
        evaluations.extend(self._evaluate_verification_queue(verification_queue, evidence_haystack, market_facts, evidence))

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
        triggered_verification_ids = [
            item.condition_id
            for item in announcement_matched_evals
            if item.group == "verification" and item.condition_id
        ]
        thesis_match_confidence = self._thesis_match_confidence(evaluations, announcement_matched_evals, facts)

        bull_required = self._matched_count(announcement_matched_evals, scenario="bull", group="required")
        base_required = self._matched_count(announcement_matched_evals, scenario="base", group="required")
        bear_required = self._matched_count(announcement_matched_evals, scenario="bear", group="required")
        bull_failure = self._matched_count(announcement_matched_evals, scenario="bull", group="failure")
        base_failure = self._matched_count(announcement_matched_evals, scenario="base", group="failure")
        red_flag_hits = self._matched_count(announcement_matched_evals, group="red_flag")
        confirmatory_hits = self._matched_count(announcement_matched_evals, group="confirmatory")
        verification_hits = self._matched_count(announcement_matched_evals, group="verification")

        positive = self._contains_any(evidence_haystack, POSITIVE_TOKENS)
        negative = self._contains_any(evidence_haystack, NEGATIVE_TOKENS)
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

        semantic_materiality = str(facts.materiality or "").strip().lower()
        announcement_class = str(facts.announcement_class or "").strip().lower()
        trajectory_effect = str(facts.trajectory_effect or "").strip().lower()
        key_findings, conflicts = self._build_findings(announcement_matched_evals)
        material_change_types = list(sorted(affected_domains))
        impact_level = self._impact_level(
            affected_domains,
            current_path,
            baseline_path,
            conflicts,
            red_flag_hits,
            confirmatory_hits,
            verification_hits,
            semantic_materiality=semantic_materiality,
        )
        unmapped_material = self._is_unmapped_material_filing(
            semantic_materiality=semantic_materiality,
            announcement_class=announcement_class,
            direct_match_count=len(announcement_matched_evals),
        )
        if unmapped_material:
            key_findings.insert(
                0,
                ComparisonFinding(
                    type="unmapped_material_filing",
                    summary=(
                        "Material announcement did not match a saved thesis-map or watchlist condition; "
                        "review thesis-map coverage before treating it as immaterial."
                    ),
                    severity=impact_level if impact_level in {"low", "medium", "high", "critical"} else "medium",
                    evidence=evidence,
                ),
            )
        thesis_effect = self._thesis_effect(
            baseline_path,
            current_path,
            conflicts,
            confirmatory_hits,
            red_flag_hits,
            positive,
            negative,
            trajectory_effect=trajectory_effect,
        )
        timeline_effect = self._timeline_effect(affected_domains, positive, negative, evaluations)
        capital_effect = self._capital_effect(affected_domains, positive, negative, evaluations)
        run_validity = self._run_validity(impact_level, current_path, baseline_path, conflicts, red_flag_hits)
        trajectory_state = self._trajectory_state(
            announcement_class=announcement_class,
            semantic_materiality=semantic_materiality,
            trajectory_effect=trajectory_effect,
            thesis_effect=thesis_effect,
            timeline_effect=timeline_effect,
            direct_match_count=len(announcement_matched_evals),
            market_match_count=self._matched_count([item for item in matched_evals if str(item.matched_via or '').strip() == "market_facts"]),
            conflicts=conflicts,
            path_transition=path_transition,
            verification_match_count=verification_hits,
        )

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
            f"announcement_verification_hits={verification_hits}",
            f"market_condition_matches={self._matched_count([item for item in matched_evals if str(item.matched_via or '').strip() == 'market_facts'])}",
            f"announcement_class={announcement_class or 'unknown'}",
            f"materiality={semantic_materiality or 'unknown'}",
            f"trajectory_state={trajectory_state or 'unknown'}",
        ]
        trajectory_projection = self._trajectory_projection(
            structured=structured,
            baseline_run=baseline_run,
            facts=facts,
            baseline_path=baseline_path,
            current_path=current_path,
            impact_level=impact_level,
            trajectory_state=trajectory_state,
            direct_match_count=len(announcement_matched_evals),
            verification_match_count=verification_hits,
        )

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
            announcement_class=announcement_class,
            materiality=semantic_materiality,
            trajectory_state=trajectory_state,
            trajectory_effect=trajectory_effect,
            price_time_effect=str(facts.price_time_effect or "").strip(),
            semantic_summary=str(facts.semantic_summary or "").strip(),
            filing_summary=str(facts.filing_summary or "").strip(),
            parser_confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
            source_confidence=float(facts.source_confidence or 0.0),
            extraction_confidence=float(facts.extraction_confidence or 0.0),
            classification_confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
            thesis_match_confidence=thesis_match_confidence,
            classification_reason=str(facts.classification_reason or "").strip(),
            confidence_breakdown=self._confidence_breakdown(facts, thesis_match_confidence, evaluations, announcement_matched_evals),
            affected_domains=material_change_types,
            material_change_types=material_change_types,
            condition_evaluations=evaluations,
            matched_condition_ids=matched_condition_ids,
            triggered_watchlist_ids=triggered_watchlist_ids,
            triggered_verification_ids=triggered_verification_ids,
            market_facts_used=used_market_fields,
            trajectory_projection=trajectory_projection,
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
    def _build_evidence_haystack(facts: AnnouncementFacts) -> str:
        """Primary filing text only; excludes derived summaries and router labels."""
        quote_excerpts = [item.quote_excerpt for item in (facts.evidence or []) if str(item.quote_excerpt or "").strip()]
        parts = [facts.title, facts.raw_text_excerpt] + quote_excerpts
        return "\n".join(str(part or "") for part in parts if str(part or "").strip()).lower()

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

    def _evaluate_verification_queue(
        self,
        items: Iterable[Any],
        haystack: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> List[ConditionEvaluation]:
        evaluations: List[ConditionEvaluation] = []
        for idx, item in enumerate(items or []):
            if isinstance(item, str):
                item = {
                    "verification_id": f"verification_{idx}",
                    "field": str(item or "").strip(),
                    "reason": str(item or "").strip(),
                }
            if not isinstance(item, dict):
                continue
            field = str(item.get("field") or item.get("field_path") or item.get("title") or "").strip()
            reason = str(item.get("reason") or item.get("condition") or "").strip()
            required_source = str(item.get("required_source") or item.get("source_to_monitor") or "").strip()
            label = " | ".join(part for part in [field, reason, required_source] if part)
            if not label:
                continue
            condition_id = str(
                item.get("verification_id")
                or item.get("condition_id")
                or item.get("field_path")
                or f"verification_{idx}"
            ).strip()
            hooks = list(item.get("evidence_hooks") or item.get("source_terms") or [])
            hooks.extend(part for part in [field, reason, required_source] if part)
            payload = {
                "condition_id": condition_id,
                "condition": label,
                "evidence_hooks": hooks,
                "severity": str(item.get("priority") or item.get("severity") or "medium").strip().lower(),
                "linked_milestones": item.get("linked_milestones") or [],
            }
            evaluations.append(self._evaluate_item(payload, "", "verification", haystack, market_facts, evidence))
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
        matched = next(
            (
                (phrase, source)
                for phrase, source in phrases
                if self._phrase_matches(
                    phrase,
                    haystack,
                    allow_token_fallback=(source == "evidence_hook"),
                )
            ),
            ("", ""),
        )
        matched_phrase, matched_source = matched
        if matched_phrase:
            source_label = "evidence hook" if matched_source == "evidence_hook" else "condition phrase"
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="matched",
                reason=f"Matched primary filing text via {source_label}: {matched_phrase}",
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
    def _condition_phrases(item: Dict[str, Any]) -> List[Tuple[str, str]]:
        phrases: List[Tuple[str, str]] = []
        value = str(item.get("condition") or "").strip()
        if value and ThesisComparator._is_meaningful_support_phrase(value):
            phrases.append((value, "condition"))
        for value in item.get("evidence_hooks") or []:
            text = str(value or "").strip()
            if ThesisComparator._is_meaningful_support_phrase(text):
                phrases.append((text, "evidence_hook"))
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
    def _phrase_matches(phrase: str, haystack: str, *, allow_token_fallback: bool = False) -> bool:
        low = str(phrase or "").strip().lower()
        if not low:
            return False
        if low in haystack:
            return True
        if not allow_token_fallback:
            return False
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
        semantic_drivers = [
            str(item or "").strip().lower()
            for item in list(facts.affected_drivers or []) + list(facts.material_topics or [])
            if str(item or "").strip()
        ]
        if str(facts.announcement_class or "").strip().lower() == "administrative" and str(facts.materiality or "").strip().lower() in {"", "none"}:
            return {"administrative"}
        domains.update(driver for driver in semantic_drivers if driver not in {"administrative", "needs_classification"})
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
        verification_hits: int = 0,
        *,
        semantic_materiality: str = "",
    ) -> str:
        materiality = str(semantic_materiality or "").strip().lower()
        if materiality in {"", "none"} and affected_domains <= {"administrative"}:
            return "none"
        if materiality == "critical":
            return "critical"
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "high"
        if conflicts or red_flag_hits > 0:
            return "high"
        if materiality == "high":
            return "high"
        if current_path == "bull" and baseline_path in {"base", "bear"}:
            return "medium"
        if confirmatory_hits > 0:
            return "medium"
        if verification_hits > 0 and materiality in {"medium", "high", "critical"}:
            return "medium"
        if materiality == "medium":
            return "medium"
        if affected_domains & {"timeline", "operations", "management", "asset_project", "commercial_customer", "drilling_exploration", "clinical_regulatory"}:
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
        *,
        trajectory_effect: str = "",
    ) -> str:
        effect = str(trajectory_effect or "").strip().lower()
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "undermines"
        if conflicts or red_flag_hits > 0:
            return "undermines"
        if effect in {"weakens", "delays"}:
            return "undermines" if effect == "weakens" else "delays"
        if current_path == "bull" and baseline_path in {"base", "bear"}:
            return "accelerates"
        if effect in {"strengthens", "risk_reduced"}:
            return "confirms"
        if confirmatory_hits > 0 or positive:
            return "confirms"
        if negative:
            return "partially_confirms"
        return "no_change"

    @staticmethod
    def _is_unmapped_material_filing(
        *,
        semantic_materiality: str,
        announcement_class: str,
        direct_match_count: int,
    ) -> bool:
        if int(direct_match_count or 0) > 0:
            return False
        if str(announcement_class or "").strip().lower() in {"administrative", "market_backdrop"}:
            return False
        return str(semantic_materiality or "").strip().lower() in {"medium", "high", "critical"}

    @staticmethod
    def _trajectory_state(
        *,
        announcement_class: str,
        semantic_materiality: str,
        trajectory_effect: str,
        thesis_effect: str,
        timeline_effect: str,
        direct_match_count: int,
        market_match_count: int,
        conflicts: List[ComparisonFinding],
        path_transition: str,
        verification_match_count: int = 0,
    ) -> str:
        announcement = str(announcement_class or "").strip().lower()
        materiality = str(semantic_materiality or "").strip().lower()
        effect = str(trajectory_effect or "").strip().lower()
        thesis = str(thesis_effect or "").strip().lower()
        timeline = str(timeline_effect or "").strip().lower()
        if announcement == "administrative" and materiality in {"", "none", "low"} and direct_match_count <= 0:
            return "administrative_filing"
        if direct_match_count <= 0 and announcement == "needs_classification":
            return "needs_classification"
        if ThesisComparator._is_unmapped_material_filing(
            semantic_materiality=materiality,
            announcement_class=announcement,
            direct_match_count=direct_match_count,
        ):
            return "material_unmapped"
        if direct_match_count <= 0 and materiality in {"", "none", "low"} and announcement not in {"needs_classification"}:
            return "no_thesis_change"
        if direct_match_count <= 0 and market_match_count > 0:
            return "market_backdrop_only"
        if conflicts or thesis in {"undermines", "invalidates"}:
            return "thesis_weakened"
        if timeline == "delayed" or effect == "delays":
            return "timeline_delayed"
        if timeline == "accelerated" or effect == "accelerates":
            return "timeline_accelerated"
        if thesis in {"confirms", "accelerates"} or effect in {"strengthens", "risk_reduced"} or path_transition:
            return "thesis_strengthened"
        if verification_match_count > 0:
            return "no_thesis_change"
        return "no_thesis_change"

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
        if "financing" not in domains and "capital_structure" not in domains and "capital_management" not in domains:
            return "unknown"
        if any(item.group in {"failure", "red_flag"} and item.status == "matched" for item in evaluations):
            return "worsens"
        if positive:
            return "improves"
        if negative:
            return "worsens"
        if domains <= {"capital_management"}:
            return "no_change"
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

    @staticmethod
    def _thesis_match_confidence(
        evaluations: List[ConditionEvaluation],
        announcement_matched_evals: List[ConditionEvaluation],
        facts: AnnouncementFacts,
    ) -> float:
        relevant = [
            item
            for item in evaluations
            if item.group in {"required", "failure", "red_flag", "confirmatory", "verification"}
            and str(item.matched_via or "").strip() != "market_facts"
        ]
        if announcement_matched_evals:
            strongest = max(float(item.confidence or 0.0) for item in announcement_matched_evals)
            return round(min(0.96, strongest + min(0.12, 0.03 * (len(announcement_matched_evals) - 1))), 3)
        if not relevant:
            return 0.0
        if str(facts.announcement_class or "").strip().lower() == "needs_classification":
            return 0.22
        return 0.4

    @staticmethod
    def _confidence_breakdown(
        facts: AnnouncementFacts,
        thesis_match_confidence: float,
        evaluations: List[ConditionEvaluation],
        announcement_matched_evals: List[ConditionEvaluation],
    ) -> Dict[str, Any]:
        payload = dict(facts.confidence_breakdown or {}) if isinstance(facts.confidence_breakdown, dict) else {}
        payload["source_confidence"] = round(float(facts.source_confidence or payload.get("source_confidence") or 0.0), 3)
        payload["extraction_confidence"] = round(float(facts.extraction_confidence or payload.get("extraction_confidence") or 0.0), 3)
        payload["classification_confidence"] = round(
            float(facts.classification_confidence or facts.semantic_confidence or payload.get("classification_confidence") or 0.0),
            3,
        )
        payload["thesis_match_confidence"] = round(float(thesis_match_confidence or 0.0), 3)
        payload["thesis_match"] = {
            "direct_matches": len(announcement_matched_evals),
            "announcement_conditions_checked": len(
                [
                    item
                    for item in evaluations
                    if item.group in {"required", "failure"}
                    and str(item.matched_via or "").strip() != "market_facts"
                ]
            ),
            "watchlist_conditions_checked": len(
                [
                    item
                    for item in evaluations
                    if item.group in {"red_flag", "confirmatory"}
                    and str(item.matched_via or "").strip() != "market_facts"
                ]
            ),
            "verification_conditions_checked": len(
                [
                    item
                    for item in evaluations
                    if item.group == "verification"
                    and str(item.matched_via or "").strip() != "market_facts"
                ]
            ),
        }
        return payload

    def _trajectory_projection(
        self,
        *,
        structured: Dict[str, Any],
        baseline_run: BaselineRunPacket,
        facts: AnnouncementFacts,
        baseline_path: str,
        current_path: str,
        impact_level: str,
        trajectory_state: str,
        direct_match_count: int,
        verification_match_count: int,
    ) -> Dict[str, Any]:
        price_targets = structured.get("price_targets") if isinstance(structured.get("price_targets"), dict) else {}
        market_data = structured.get("market_data") if isinstance(structured.get("market_data"), dict) else {}
        market_data_provenance = (
            structured.get("market_data_provenance")
            if isinstance(structured.get("market_data_provenance"), dict)
            else {}
        )
        market_facts = structured.get("market_facts") if isinstance(structured.get("market_facts"), dict) else {}
        normalized_market = market_facts.get("normalized_facts") if isinstance(market_facts.get("normalized_facts"), dict) else {}
        scenario_targets = price_targets.get("scenario_targets") if isinstance(price_targets.get("scenario_targets"), dict) else {}
        scenario_probabilities = (
            price_targets.get("scenario_probabilities")
            if isinstance(price_targets.get("scenario_probabilities"), dict)
            else {}
        )
        targets_24m_raw = scenario_targets.get("24m") if isinstance(scenario_targets.get("24m"), dict) else {}
        probs_24m_raw = scenario_probabilities.get("24m") if isinstance(scenario_probabilities.get("24m"), dict) else {}
        base_24m = self._to_float(targets_24m_raw.get("base"))
        if base_24m is None:
            base_24m = self._to_float(price_targets.get("target_24m"))
        targets_24m = {
            "bear": self._to_float(targets_24m_raw.get("bear")),
            "base": base_24m,
            "bull": self._to_float(targets_24m_raw.get("bull")),
        }
        targets_12m_raw = scenario_targets.get("12m") if isinstance(scenario_targets.get("12m"), dict) else {}
        base_12m = self._to_float(targets_12m_raw.get("base"))
        if base_12m is None:
            base_12m = self._to_float(price_targets.get("target_12m"))
        targets_12m = {
            "bear": self._to_float(targets_12m_raw.get("bear")),
            "base": base_12m,
            "bull": self._to_float(targets_12m_raw.get("bull")),
        }
        probabilities_24m = {
            "bear": self._to_float(probs_24m_raw.get("bear")),
            "base": self._to_float(probs_24m_raw.get("base")),
            "bull": self._to_float(probs_24m_raw.get("bull")),
        }
        current_price = self._to_float(market_data.get("current_price"))
        if current_price is None:
            current_price = self._to_float(price_targets.get("current_price"))
        prob_weighted_24m = self._to_float(price_targets.get("prob_weighted_target_24m"))
        if prob_weighted_24m is None:
            prob_weighted_24m = self._weighted_target(targets_24m, probabilities_24m)

        baseline_started_at = self._baseline_started_at(baseline_run)
        as_of = self._as_of_datetime(facts) or datetime.now(timezone.utc)
        elapsed_days = None
        elapsed_pct = None
        if baseline_started_at is not None:
            elapsed_days = max(0, (as_of.date() - baseline_started_at.date()).days)
            elapsed_pct = round(min(100.0, (elapsed_days / 730.0) * 100.0), 1)

        market_path = self._market_implied_path(current_price, targets_24m)
        rerun_signal, rerun_reason = self._projection_rerun_signal(
            impact_level=impact_level,
            trajectory_state=trajectory_state,
            baseline_path=baseline_path,
            current_path=current_path,
            direct_match_count=direct_match_count,
            verification_match_count=verification_match_count,
        )

        available = bool(current_price is not None and any(value is not None for value in targets_24m.values()))
        return {
            "available": available,
            "currency": str(
                market_data_provenance.get("prepass_currency")
                or market_data.get("currency")
                or normalized_market.get("currency")
                or "AUD"
            ).strip().upper(),
            "as_of_utc": self._iso(as_of),
            "baseline_started_at_utc": self._iso(baseline_started_at),
            "elapsed_days": elapsed_days,
            "elapsed_pct_24m": elapsed_pct,
            "horizon_months": 24,
            "current_price": current_price,
            "target_12m": targets_12m,
            "target_24m": targets_24m,
            "probability_24m": probabilities_24m,
            "prob_weighted_target_24m": prob_weighted_24m,
            "market_implied_path_24m": market_path,
            "saved_path": baseline_path,
            "router_path": current_path,
            "path_transition": f"{baseline_path}->{current_path}" if baseline_path and current_path and baseline_path != current_path else "",
            "timeline_rows": self._projection_timeline_rows(structured, baseline_run),
            "rerun_signal": rerun_signal,
            "rerun_reason": rerun_reason,
        }

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text or text.lower() in {"n/a", "na", "none", "null"}:
            return None
        text = re.sub(r"[^0-9.\-]", "", text)
        try:
            return float(text)
        except Exception:
            return None

    @classmethod
    def _weighted_target(cls, targets: Dict[str, Optional[float]], probabilities: Dict[str, Optional[float]]) -> Optional[float]:
        rows = [
            (targets.get(name), probabilities.get(name))
            for name in ("bear", "base", "bull")
            if targets.get(name) is not None and probabilities.get(name) is not None
        ]
        total_prob = sum(float(prob or 0.0) for _, prob in rows)
        if not rows or total_prob <= 0:
            return None
        scale = 100.0 if total_prob > 1.5 else 1.0
        return round(sum(float(target) * (float(prob) / scale) for target, prob in rows), 4)

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        if re.fullmatch(r"\d{8}_\d{6}", text):
            text = f"{text[:4]}-{text[4:6]}-{text[6:8]}T{text[9:11]}:{text[11:13]}:{text[13:15]}Z"
        match = re.search(r"(\d{8}_\d{6})", text)
        if match:
            return ThesisComparator._parse_datetime(match.group(1))
        try:
            normalized = text.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    @classmethod
    def _baseline_started_at(cls, baseline_run: BaselineRunPacket) -> Optional[datetime]:
        summary = baseline_run.summary_fields if isinstance(baseline_run.summary_fields, dict) else {}
        lab = baseline_run.lab_payload if isinstance(baseline_run.lab_payload, dict) else {}
        for value in (
            lab.get("created_at"),
            lab.get("updated_at"),
            summary.get("analysis_date"),
            summary.get("created_at"),
            summary.get("updated_at"),
            baseline_run.run_id,
        ):
            parsed = cls._parse_datetime(value)
            if parsed is not None:
                return parsed
        return None

    @classmethod
    def _as_of_datetime(cls, facts: AnnouncementFacts) -> Optional[datetime]:
        for item in facts.evidence or []:
            parsed = cls._parse_datetime(getattr(item, "source_date_utc", ""))
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _iso(value: Optional[datetime]) -> str:
        if value is None:
            return ""
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    @classmethod
    def _market_implied_path(cls, current_price: Optional[float], targets: Dict[str, Optional[float]]) -> str:
        if current_price is None:
            return "unknown"
        rows = [
            (name, float(value))
            for name, value in targets.items()
            if name in {"bear", "base", "bull"} and value is not None
        ]
        if not rows:
            return "unknown"
        rows.sort(key=lambda item: item[1])
        if len(rows) >= 2:
            low_name, low_value = rows[0]
            high_name, high_value = rows[-1]
            if current_price < low_value:
                return f"below_{low_name}"
            if current_price > high_value:
                return f"above_{high_name}"
        closest = min(rows, key=lambda item: abs(item[1] - float(current_price)))
        return closest[0]

    @staticmethod
    def _projection_rerun_signal(
        *,
        impact_level: str,
        trajectory_state: str,
        baseline_path: str,
        current_path: str,
        direct_match_count: int,
        verification_match_count: int,
    ) -> Tuple[str, str]:
        impact = str(impact_level or "").strip().lower()
        state = str(trajectory_state or "").strip().lower()
        if impact in {"critical", "high"} or state in {"thesis_weakened", "timeline_delayed", "risk_increased"}:
            return "rebuild_analysis", "The filing changes or threatens the saved thesis path."
        if baseline_path and current_path and baseline_path != current_path:
            return "refresh_evidence", "The router path differs from the saved run path."
        if state == "material_unmapped":
            return "review_thesis_map", "The filing looks material but no saved thesis condition covers it."
        if verification_match_count > 0:
            return "annotate_evidence", "A verification queue item was touched and should be surfaced in the run evidence."
        if direct_match_count > 0:
            return "annotate_evidence", "The filing matched saved evidence checks without forcing a rebuild."
        return "none", "No thesis-path update was detected."

    @staticmethod
    def _projection_timeline_rows(structured: Dict[str, Any], baseline_run: BaselineRunPacket) -> List[Dict[str, Any]]:
        rows = structured.get("development_timeline")
        if not isinstance(rows, list):
            rows = baseline_run.timeline_rows if isinstance(baseline_run.timeline_rows, list) else []
        out: List[Dict[str, Any]] = []
        for item in rows[:8]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or item.get("milestone") or item.get("event") or item.get("label") or "").strip()
            if not title:
                continue
            out.append(
                {
                    "title": title,
                    "timing": str(item.get("timing") or item.get("date") or item.get("period") or "").strip(),
                    "status": str(item.get("status") or "").strip(),
                }
            )
        return out
