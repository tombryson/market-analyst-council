from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from .models import AnnouncementFacts, BaselineRunPacket


EVENT_CLASS_RULES: Dict[str, Tuple[str, ...]] = {
    "administrative": (
        "cleansing notice",
        "application for quotation",
        "appendix 2a",
        "appendix 3b",
        "change of director",
        "initial substantial holder",
        "ceasing to be a substantial holder",
        "notice of meeting",
        "results of meeting",
        "quotation of securities",
    ),
    "capital_financing": (
        "placement",
        "capital raise",
        "entitlement offer",
        "rights issue",
        "loan facility",
        "debt facility",
        "funding",
        "refinancing",
        "convertible note",
        "working capital",
        "liquidity",
    ),
    "capital_management": (
        "buyback",
        "buy-back",
        "buy back",
        "share buyback",
        "share buy-back",
        "on-market buyback",
        "on-market buy-back",
        "capital management",
        "return of capital",
        "share repurchase",
        "special dividend",
        "dividend",
    ),
    "earnings_guidance": (
        "trading update",
        "guidance",
        "outlook",
        "earnings",
        "revenue",
        "ebitda",
        "margin",
        "cash flow",
        "profit",
        "loss",
        "quarterly",
        "annual report",
        "half-year",
    ),
    "operations": (
        "operations",
        "production",
        "manufacturing",
        "facility",
        "plant",
        "capacity",
        "throughput",
        "supply chain",
        "inventory",
        "commissioning",
    ),
    "commercial_customer": (
        "contract",
        "customer",
        "client",
        "subscriber",
        "order",
        "purchase order",
        "offtake",
        "partnership",
        "distribution agreement",
    ),
    "product_technology": (
        "product",
        "launch",
        "release",
        "platform",
        "software",
        "patent",
        "technology",
        "trial",
        "prototype",
    ),
    "regulatory_legal": (
        "regulator",
        "regulatory",
        "approval",
        "licence",
        "license",
        "permit",
        "court",
        "litigation",
        "settlement",
        "claim",
        "investigation",
        "compliance",
    ),
    "strategy_mna": (
        "acquisition",
        "divestment",
        "merger",
        "takeover",
        "scheme",
        "joint venture",
        "farm-in",
        "farm in",
        "strategic review",
    ),
    "asset_project": (
        "project",
        "development",
        "study",
        "feasibility",
        "milestone",
        "timeline",
        "delay",
        "commissioning",
        "construction",
    ),
    "governance_management": (
        "director",
        "ceo",
        "cfo",
        "chair",
        "board",
        "resignation",
        "appointment",
    ),
}

DRIVER_LABELS = {
    "administrative": "administrative filings",
    "asset_project": "project delivery",
    "capital_financing": "funding",
    "capital_management": "capital management",
    "clinical_regulatory": "clinical and regulatory progress",
    "commercial_customer": "commercial agreements",
    "development_timeline": "delivery timeline",
    "drilling_exploration": "exploration results",
    "earnings_guidance": "guidance",
    "governance_management": "governance",
    "legal": "legal matters",
    "market_backdrop": "market backdrop",
    "operations": "operations",
    "permitting": "permitting",
    "product_technology": "product or technology progress",
    "production_operations": "production",
    "regulatory_legal": "regulatory or legal matters",
    "resource": "resource or reserve data",
    "strategy_mna": "strategy or M&A",
}


DOMAIN_PROFILE_RULES: Dict[str, Dict[str, Tuple[str, ...]]] = {
    "resources": {
        "drilling_exploration": (
            "drill",
            "drilling",
            "assay",
            "assays",
            "intercept",
            "intercepts",
            "mineralisation",
            "mineralization",
            "grade",
            "strike",
            "width",
            "depth",
            "extension",
            "metre",
            "meter",
        ),
        "resource": (
            "resource",
            "reserve",
            "jorc",
            "mineral resource",
            "ore reserve",
            "mre",
            "maiden resource",
        ),
        "asset_project": (
            "mine",
            "mining",
            "scoping study",
            "pre-feasibility",
            "pfs",
            "dfs",
            "development approval",
        ),
        "permitting": (
            "permit",
            "licence",
            "license",
            "ministry",
            "approval",
            "environmental",
            "heritage",
        ),
    },
    "software": {
        "commercial_customer": (
            "arr",
            "annual recurring revenue",
            "nrr",
            "net revenue retention",
            "churn",
            "customer contract",
            "enterprise customer",
            "subscriber",
            "subscription",
        ),
        "product_technology": (
            "platform",
            "software",
            "product launch",
            "module",
            "deployment",
            "integration",
            "ai",
        ),
    },
    "biotech": {
        "clinical_regulatory": (
            "clinical",
            "trial",
            "phase 1",
            "phase 2",
            "phase 3",
            "endpoint",
            "fda",
            "tga",
            "ema",
            "approval",
            "patient",
            "dose",
        ),
        "capital_financing": ("cash runway", "raise", "placement", "funding", "cash balance"),
    },
    "financials": {
        "credit_risk": (
            "arrears",
            "impairment",
            "provision",
            "loan growth",
            "net interest margin",
            "nim",
            "cet1",
            "capital ratio",
        ),
        "earnings_guidance": ("aum", "funds under management", "premium", "claims", "loss ratio"),
    },
    "real_estate": {
        "real_estate_portfolio": (
            "occupancy",
            "leasing",
            "rental",
            "cap rate",
            "valuation",
            "development",
            "settlement",
            "asset sale",
        ),
        "capital_financing": ("debt facility", "gearing", "refinancing", "covenant"),
    },
    "oil_gas": {
        "production": (
            "production",
            "produced",
            "boe",
            "bbl",
            "barrel",
            "oil",
            "gas",
            "well",
            "well pad",
            "ip30",
            "flow test",
            "completion",
            "workover",
            "spud",
        ),
        "resource": (
            "reserve",
            "reserves",
            "contingent resource",
            "prospective resource",
            "2p",
            "2c",
        ),
        "asset_project": (
            "field development",
            "development well",
            "drilling program",
            "pipeline",
            "gathering system",
        ),
        "capital_management": (
            "buyback",
            "buy-back",
            "share buyback",
            "capital management",
            "return of capital",
        ),
    },
}


POSITIVE_TRAJECTORY_TERMS = (
    "approved",
    "secured",
    "completed",
    "achieved",
    "on track",
    "ahead of schedule",
    "accelerated",
    "expanded",
    "extended",
    "increased",
    "signed",
    "awarded",
)
POSITIVE_TRAJECTORY_ACTION_TERMS = (
    "approved",
    "approval",
    "secured",
    "completed",
    "achieved",
    "accelerated",
    "expanded",
    "increased",
    "signed",
    "awarded",
    "granted",
    "renewed",
    "launched",
)
NEGATIVE_TRAJECTORY_TERMS = (
    "delay",
    "delayed",
    "at risk",
    "suspended",
    "withdrawn",
    "revoked",
    "default",
    "breach",
    "shortfall",
    "downgrade",
    "reduced",
    "cost overrun",
)


@dataclass
class AnnouncementInterpreter:
    """Context-aware announcement interpretation.

    The document reader deliberately stays close to text extraction. This layer
    runs only after the saved lab/council context is available, so filing meaning
    is interpreted against the actual company profile instead of a global
    resource-sector keyword bucket.
    """

    def interpret(self, facts: AnnouncementFacts, baseline_run: BaselineRunPacket) -> AnnouncementFacts:
        text = self._facts_text(facts)
        title_text = str(facts.title or "").strip().lower()
        profile = self._domain_profile(baseline_run)
        event_scores = self._score_rules(text, EVENT_CLASS_RULES)
        profile_scores = self._score_rules(text, DOMAIN_PROFILE_RULES.get(profile, {}))
        profile_drivers = [key for key, score in sorted(profile_scores.items(), key=lambda item: (-item[1], item[0])) if score > 0]
        forced_class = self._title_forced_class(title_text)
        event_class = forced_class or self._best_key(event_scores) or ("asset_project" if profile_drivers else "needs_classification")
        universal_drivers = (
            [forced_class]
            if forced_class
            else [key for key, score in sorted(event_scores.items(), key=lambda item: (-item[1], item[0])) if score > 0]
        )
        context_drivers = self._matched_context_drivers(text, baseline_run)
        affected_drivers = self._dedupe(profile_drivers + universal_drivers + context_drivers)
        materiality = self._materiality(event_class, affected_drivers, text, title_text=title_text)
        trajectory_effect = self._trajectory_effect(event_class, materiality, text)
        semantic_summary = self._summary(facts, event_class, affected_drivers, trajectory_effect)
        basis = self._basis(event_scores, profile_scores, context_drivers)
        warnings = self._warnings(facts, event_class, materiality, basis)
        classification_confidence, confidence_components = self._classification_confidence(
            event_scores,
            profile_scores,
            context_drivers,
            facts,
        )
        filing_summary = self._filing_summary(facts, event_class, affected_drivers)
        classification_reason = self._classification_reason(
            event_class=event_class,
            forced_class=forced_class,
            basis=basis,
            profile=profile,
        )
        confidence_breakdown = self._confidence_breakdown(
            facts=facts,
            event_scores=event_scores,
            profile_scores=profile_scores,
            context_drivers=context_drivers,
            classification_confidence=classification_confidence,
            components=confidence_components,
            reason=classification_reason,
        )

        facts.announcement_class = event_class
        facts.materiality = materiality
        facts.affected_drivers = affected_drivers[:8]
        facts.material_topics = self._legacy_topics(event_class, facts.affected_drivers)
        facts.trajectory_effect = trajectory_effect
        facts.price_time_effect = self._price_time_effect(trajectory_effect, materiality, facts.affected_drivers)
        facts.filing_summary = filing_summary
        facts.semantic_summary = semantic_summary
        facts.semantic_confidence = classification_confidence
        facts.classification_confidence = classification_confidence
        facts.source_confidence = facts.source_confidence or confidence_breakdown.get("source_confidence", 0.0)
        facts.extraction_confidence = facts.extraction_confidence or confidence_breakdown.get("extraction_confidence", 0.0)
        facts.domain_profile = profile
        facts.classification_basis = basis[:8]
        facts.parser_warnings = warnings
        facts.classification_reason = classification_reason
        facts.confidence_breakdown = confidence_breakdown
        return facts

    @staticmethod
    def _facts_text(facts: AnnouncementFacts) -> str:
        parts = [
            facts.title,
            facts.summary,
            facts.raw_text_excerpt,
            *(facts.extracted_facts or []),
            *(item.quote_excerpt for item in (facts.evidence or []) if str(item.quote_excerpt or "").strip()),
        ]
        return "\n".join(str(part or "") for part in parts if str(part or "").strip()).lower()

    @staticmethod
    def _score_rules(text: str, rules: Dict[str, Sequence[str]]) -> Dict[str, int]:
        scores: Dict[str, int] = {}
        for key, terms in rules.items():
            scores[key] = sum(1 for term in terms if _term_in_text(term, text))
        return scores

    @staticmethod
    def _best_key(scores: Dict[str, int]) -> str:
        ranked = sorted(((score, key) for key, score in scores.items() if score > 0), reverse=True)
        return ranked[0][1] if ranked else ""

    @staticmethod
    def _domain_profile(baseline_run: BaselineRunPacket) -> str:
        haystack = " ".join(
            [
                baseline_run.template_id,
                str((baseline_run.summary_fields or {}).get("sector") or ""),
                str((baseline_run.summary_fields or {}).get("industry") or ""),
                str((baseline_run.summary_fields or {}).get("asset_class") or ""),
                str((baseline_run.summary_fields or {}).get("template_family") or ""),
            ]
        ).lower()
        structured = _structured_payload(baseline_run)
        haystack = f"{haystack} {str(structured.get('template_family') or '')} {str(structured.get('sector') or '')}".lower()
        if any(token in haystack for token in ("oil_gas", "oil gas", "oil and gas", "petroleum", "hydrocarbon", "brent", "wti", "lng", "natural gas")):
            return "oil_gas"
        if any(token in haystack for token in ("mining", "miner", "resources", "metals", "materials", "gold", "uranium", "lithium", "critical_minerals", "iron_ore", "coal")):
            return "resources"
        if any(token in haystack for token in ("software", "saas", "technology", "telecommunications")):
            return "software"
        if any(token in haystack for token in ("biotech", "pharma", "healthcare", "clinical")):
            return "biotech"
        if any(token in haystack for token in ("bank", "financial", "insurance", "asset_managers", "diversified_financials")):
            return "financials"
        if any(token in haystack for token in ("reit", "real_estate", "property")):
            return "real_estate"
        return "general"

    @staticmethod
    def _matched_context_drivers(text: str, baseline_run: BaselineRunPacket) -> List[str]:
        labels = _baseline_condition_labels(baseline_run)
        matched: List[str] = []
        for label in labels:
            normalized = str(label or "").strip().lower()
            if not normalized:
                continue
            words = [word for word in re.findall(r"[a-z0-9][a-z0-9-]{2,}", normalized) if word not in STOP_WORDS]
            if len(words) >= 2 and sum(1 for word in words if _term_in_text(word, text)) >= min(3, len(words)):
                matched.append("saved_thesis_driver")
                break
        return matched

    @staticmethod
    def _title_forced_class(title_text: str) -> str:
        normalized_title = _normalize_match_text(title_text)
        capital_management_titles = (
            "buyback",
            "buy back",
            "share buyback",
            "share buy back",
            "on market buyback",
            "on market buy back",
            "notification of buy back",
            "update notification of buy back",
            "capital management",
            "return of capital",
        )
        if any(term in normalized_title or term.replace(" ", "") in normalized_title.replace(" ", "") for term in capital_management_titles):
            return "capital_management"
        administrative_titles = (
            "cleansing notice",
            "application for quotation",
            "appendix 2a",
            "appendix 3b",
            "change of director",
            "initial substantial holder",
            "ceasing to be a substantial holder",
            "notice of meeting",
            "results of meeting",
        )
        if any(term in title_text for term in administrative_titles):
            return "administrative"
        return ""

    @staticmethod
    def _materiality(event_class: str, drivers: Sequence[str], text: str, *, title_text: str = "") -> str:
        if event_class == "administrative":
            administrative_titles = (
                "cleansing notice",
                "application for quotation",
                "appendix 2a",
                "appendix 3b",
                "change of director",
                "initial substantial holder",
                "ceasing to be a substantial holder",
                "notice of meeting",
                "results of meeting",
            )
            if any(term in title_text for term in administrative_titles):
                return "none"
            if any(_term_in_text(term, text) for term in ("placement", "capital raise", "funding", "acquisition", "takeover")):
                return "medium"
            return "none"
        if event_class == "capital_management":
            if any(_term_in_text(term, text) for term in ("return of capital", "special dividend", "material capital management", "off-market buyback", "off-market buy-back")):
                return "medium"
            return "low"
        if event_class == "needs_classification":
            return "low" if not drivers else "medium"
        if any(driver in {"regulatory_legal", "permitting", "clinical_regulatory"} for driver in drivers):
            if any(_term_in_text(term, text) for term in NEGATIVE_TRAJECTORY_TERMS):
                return "high"
            return "medium"
        if any(driver in {"capital_financing", "strategy_mna", "earnings_guidance"} for driver in drivers):
            return "medium"
        if any(driver in {"drilling_exploration", "resource", "asset_project", "commercial_customer", "operations"} for driver in drivers):
            return "medium"
        return "low"

    @staticmethod
    def _trajectory_effect(event_class: str, materiality: str, text: str) -> str:
        if event_class == "administrative" and materiality == "none":
            return "administrative"
        if event_class == "capital_management" and materiality in {"none", "low"}:
            return "no_clear_change"
        positive = _has_directional_term(text, POSITIVE_TRAJECTORY_TERMS, positive=True)
        negative = any(_term_in_text(term, text) for term in NEGATIVE_TRAJECTORY_TERMS)
        if negative and any(_term_in_text(term, text) for term in ("delay", "delayed", "schedule", "timeline")):
            return "delays"
        if negative:
            return "weakens"
        if positive and _has_directional_term(text, ("approved", "secured", "completed", "achieved", "risk"), positive=True):
            return "risk_reduced"
        if positive:
            return "strengthens"
        if materiality in {"medium", "high", "critical"}:
            return "material_update"
        return "no_clear_change"

    @staticmethod
    def _price_time_effect(effect: str, materiality: str, drivers: Sequence[str]) -> str:
        driver_text = AnnouncementInterpreter._driver_phrase(drivers[:2]) or "company trajectory"
        if effect == "delays":
            return f"Likely pushes the price/time path out by delaying {driver_text}."
        if effect == "weakens":
            return f"Likely weakens the expected price/time path through {driver_text}."
        if effect in {"strengthens", "risk_reduced"}:
            return f"Likely improves the price/time path through {driver_text}."
        if effect == "material_update" and materiality in {"medium", "high", "critical"}:
            return f"Material update to {driver_text}; direction depends on mapped thesis evidence."
        if effect == "administrative":
            return "Administrative filing; no direct price/time trajectory change identified."
        if "capital_management" in set(drivers) and effect == "no_clear_change":
            return "Capital-management filing; no direct operating thesis or price/time change identified."
        return "No direct price/time trajectory change identified."

    @staticmethod
    def _filing_summary(facts: AnnouncementFacts, event_class: str, drivers: Sequence[str]) -> str:
        ticker = str(facts.ticker or "The company").strip() or "The company"
        title = _clean_sentence(facts.title or "")
        fact = _clean_sentence((facts.extracted_facts or [""])[0] if facts.extracted_facts else "")
        source_text = title or fact or "the announcement"
        if event_class == "capital_management":
            return f"{ticker} filed a buy-back or capital-management update: {source_text}."
        if event_class == "administrative":
            return f"{ticker} filed a procedural market announcement: {source_text}."
        if event_class == "needs_classification":
            return f"{ticker} filed an announcement that the router could not classify from the extracted text: {source_text}."
        driver_text = AnnouncementInterpreter._driver_phrase(drivers[:2])
        if driver_text:
            return f"{ticker} filed an update about {driver_text}: {source_text}."
        return f"{ticker} filed an update: {source_text}."

    @staticmethod
    def _summary(facts: AnnouncementFacts, event_class: str, drivers: Sequence[str], effect: str) -> str:
        title = _clean_sentence(facts.title or "The announcement")
        driver_text = AnnouncementInterpreter._driver_phrase(drivers[:2]) or AnnouncementInterpreter._driver_label(event_class)
        if event_class == "capital_management":
            return f"The filing is a capital-management update about {title}; no operating thesis change is implied on its own."
        if event_class == "administrative":
            return f"The filing is procedural: {title}."
        if event_class == "needs_classification":
            return f"The filing was read, but the extracted text was not specific enough to classify its thesis effect: {title}."
        if effect == "delays":
            return f"The filing points to a possible delay in {driver_text}: {title}."
        if effect == "weakens":
            return f"The filing appears to weaken {driver_text}: {title}."
        if effect in {"strengthens", "risk_reduced"}:
            return f"The filing appears to support {driver_text}: {title}."
        if effect == "material_update":
            return f"The filing is a material update about {driver_text}: {title}. Direction depends on the saved thesis conditions and evidence."
        return f"The filing updates {driver_text}: {title}. No direct thesis direction was identified."

    @staticmethod
    def _driver_label(value: str) -> str:
        key = str(value or "").strip().lower()
        return DRIVER_LABELS.get(key) or key.replace("_", " ") or "company trajectory"

    @staticmethod
    def _driver_phrase(drivers: Sequence[str]) -> str:
        labels = []
        for driver in drivers:
            label = AnnouncementInterpreter._driver_label(str(driver or ""))
            if label and label not in labels:
                labels.append(label)
        if not labels:
            return ""
        if len(labels) == 1:
            return labels[0]
        return f"{', '.join(labels[:-1])} and {labels[-1]}"

    @staticmethod
    def _classification_reason(*, event_class: str, forced_class: str, basis: Sequence[str], profile: str) -> str:
        if forced_class:
            return f"Title matched a high-confidence {forced_class.replace('_', ' ')} filing pattern."
        if event_class == "needs_classification":
            return "No announcement taxonomy rule, domain driver, or saved thesis context matched the extracted filing text."
        if any(str(item).startswith("universal:") for item in basis):
            return f"Announcement taxonomy matched {event_class.replace('_', ' ')}."
        if any(str(item).startswith("profile:") for item in basis):
            return f"{profile.replace('_', ' ').title()} domain rules matched the extracted filing text."
        if any(str(item).startswith("saved_thesis_context") for item in basis):
            return "The filing text overlapped with saved thesis-map language."
        return f"Classified as {event_class.replace('_', ' ')} from weak contextual signals."

    @staticmethod
    def _basis(event_scores: Dict[str, int], profile_scores: Dict[str, int], context_drivers: Sequence[str]) -> List[str]:
        rows: List[str] = []
        for key, score in sorted(event_scores.items(), key=lambda item: (-item[1], item[0])):
            if score > 0:
                rows.append(f"universal:{key}:{score}")
        for key, score in sorted(profile_scores.items(), key=lambda item: (-item[1], item[0])):
            if score > 0:
                rows.append(f"profile:{key}:{score}")
        if context_drivers:
            rows.append("saved_thesis_context:matched")
        return rows

    @staticmethod
    def _warnings(facts: AnnouncementFacts, event_class: str, materiality: str, basis: Sequence[str]) -> List[str]:
        warnings: List[str] = []
        quality = facts.parse_quality if isinstance(facts.parse_quality, dict) else {}
        if int(quality.get("decoded_chars") or 0) < 400:
            warnings.append("short_or_sparse_text")
        if event_class == "needs_classification":
            warnings.append("announcement_class_unclear")
        if materiality in {"medium", "high", "critical"} and not basis:
            warnings.append("materiality_without_strong_basis")
        return warnings

    @staticmethod
    def _confidence(
        event_scores: Dict[str, int],
        profile_scores: Dict[str, int],
        context_drivers: Sequence[str],
        facts: AnnouncementFacts,
    ) -> float:
        confidence, _components = AnnouncementInterpreter._classification_confidence(
            event_scores,
            profile_scores,
            context_drivers,
            facts,
        )
        return confidence

    @staticmethod
    def _classification_confidence(
        event_scores: Dict[str, int],
        profile_scores: Dict[str, int],
        context_drivers: Sequence[str],
        facts: AnnouncementFacts,
    ) -> Tuple[float, Dict[str, Any]]:
        best = max(list(event_scores.values() or [0]) + list(profile_scores.values() or [0]))
        rule_bonus = min(0.35, best * 0.08)
        context_bonus = 0.1 if context_drivers else 0.0
        sparse_text_penalty = 0.0
        confidence = 0.45 + rule_bonus
        if context_drivers:
            confidence += context_bonus
        quality = facts.parse_quality if isinstance(facts.parse_quality, dict) else {}
        if int(quality.get("decoded_chars") or 0) < 400:
            sparse_text_penalty = 0.1
            confidence -= sparse_text_penalty
        final = round(max(0.1, min(0.95, confidence)), 3)
        return final, {
            "base": 0.45,
            "best_rule_score": best,
            "rule_bonus": round(rule_bonus, 3),
            "saved_thesis_context_bonus": round(context_bonus, 3),
            "sparse_text_penalty": round(sparse_text_penalty, 3),
            "final": final,
        }

    @staticmethod
    def _confidence_breakdown(
        *,
        facts: AnnouncementFacts,
        event_scores: Dict[str, int],
        profile_scores: Dict[str, int],
        context_drivers: Sequence[str],
        classification_confidence: float,
        components: Dict[str, Any],
        reason: str,
    ) -> Dict[str, Any]:
        quality = facts.parse_quality if isinstance(facts.parse_quality, dict) else {}
        source_confidence = float(facts.source_confidence or _fallback_source_confidence(facts))
        extraction_confidence = float(facts.extraction_confidence or _fallback_extraction_confidence(quality))
        event_hits = {
            key: score
            for key, score in sorted(event_scores.items(), key=lambda item: (-item[1], item[0]))
            if score > 0
        }
        profile_hits = {
            key: score
            for key, score in sorted(profile_scores.items(), key=lambda item: (-item[1], item[0]))
            if score > 0
        }
        return {
            **(facts.confidence_breakdown if isinstance(facts.confidence_breakdown, dict) else {}),
            "source_confidence": round(source_confidence, 3),
            "extraction_confidence": round(extraction_confidence, 3),
            "classification_confidence": round(float(classification_confidence or 0.0), 3),
            "thesis_match_confidence": round(float(facts.thesis_match_confidence or 0.0), 3),
            "classification_reason": reason,
            "classification_components": components,
            "matched_event_rules": event_hits,
            "matched_domain_rules": profile_hits,
            "saved_thesis_context_matched": bool(context_drivers),
            "parse_quality": quality,
        }

    @staticmethod
    def _legacy_topics(event_class: str, drivers: Sequence[str]) -> List[str]:
        mapping = {
            "capital_financing": ["financing", "capital_structure"],
            "capital_management": ["capital_management"],
            "earnings_guidance": ["guidance"],
            "operations": ["operations"],
            "commercial_customer": ["commercial", "customer"],
            "product_technology": ["product", "technology"],
            "regulatory_legal": ["regulatory", "legal"],
            "strategy_mna": ["m_and_a"],
            "asset_project": ["operations", "timeline"],
            "governance_management": ["governance", "management"],
            "administrative": ["administrative"],
        }
        topics = list(mapping.get(event_class, []))
        topics.extend(str(driver or "").strip() for driver in drivers if str(driver or "").strip())
        return AnnouncementInterpreter._dedupe(topics)[:8]

    @staticmethod
    def _dedupe(values: Iterable[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for value in values:
            text = str(value or "").strip().lower()
            if not text or text in seen:
                continue
            seen.add(text)
            out.append(text)
        return out


STOP_WORDS = {
    "and",
    "the",
    "for",
    "with",
    "from",
    "that",
    "this",
    "will",
    "should",
    "into",
    "over",
    "under",
    "above",
    "below",
    "company",
    "project",
}


def _term_in_text(term: str, text: str) -> bool:
    value = _normalize_match_text(term)
    if not value:
        return False
    normalized_text = _normalize_match_text(text)
    if " " in value:
        return value in normalized_text or value.replace(" ", "") in normalized_text.replace(" ", "")
    return re.search(rf"\b{re.escape(value)}\b", normalized_text) is not None


def _has_directional_term(text: str, terms: Sequence[str], *, positive: bool = False) -> bool:
    for sentence in _sentences_for_matching(text):
        if positive and _negates_positive_action(sentence):
            continue
        if any(_term_in_text(term, sentence) for term in terms):
            return True
    return False


def _sentences_for_matching(text: str) -> List[str]:
    return [
        sentence.strip().lower()
        for sentence in re.split(r"(?<=[.!?])\s+|\n+", str(text or ""))
        if sentence.strip()
    ]


def _negates_positive_action(sentence: str) -> bool:
    return any(_negates_term(sentence, term) for term in POSITIVE_TRAJECTORY_ACTION_TERMS)


def _negates_term(sentence: str, term: str) -> bool:
    escaped = re.escape(str(term or "").strip().lower())
    if not escaped:
        return False
    return bool(
        re.search(
            rf"\b(?:has|have|had|is|was|were|does|did|do|management)?\s*not\s+(?:yet\s+)?(?:been\s+)?(?:[a-z0-9]+\s+){{0,4}}{escaped}\b",
            sentence,
        )
        or re.search(
            rf"\bno\s+(?:binding\s+)?(?:[a-z0-9]+\s+){{0,4}}{escaped}\b",
            sentence,
        )
    )


def _normalize_match_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\u2010-\u2015\-]+", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _clean_sentence(value: str, *, limit: int = 170) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" .")
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}..."


def _fallback_source_confidence(facts: AnnouncementFacts) -> float:
    if any(str(item.source_url or "").strip() for item in (facts.evidence or [])):
        return 0.88
    return 0.45 if str(facts.raw_text_excerpt or "").strip() else 0.1


def _fallback_extraction_confidence(parse_quality: Dict[str, Any]) -> float:
    decoded = int(parse_quality.get("decoded_chars") or 0)
    facts = int(parse_quality.get("fact_count") or 0)
    if decoded >= 1200:
        return 0.84
    if decoded >= 400:
        return 0.7
    if decoded > 0:
        return 0.45 + min(0.08, facts * 0.01)
    return 0.1


def _structured_payload(baseline_run: BaselineRunPacket) -> Dict[str, Any]:
    payload = baseline_run.lab_payload if isinstance(baseline_run.lab_payload, dict) else {}
    structured = payload.get("structured_data") if isinstance(payload.get("structured_data"), dict) else {}
    return structured if isinstance(structured, dict) else {}


def _baseline_condition_labels(baseline_run: BaselineRunPacket) -> List[str]:
    structured = _structured_payload(baseline_run)
    thesis_map = structured.get("thesis_map") if isinstance(structured.get("thesis_map"), dict) else {}
    watchlist = structured.get("monitoring_watchlist") if isinstance(structured.get("monitoring_watchlist"), dict) else {}
    verification = structured.get("verification_queue") if isinstance(structured.get("verification_queue"), list) else []
    labels: List[str] = []
    for scenario in ("bull", "base", "bear"):
        block = thesis_map.get(scenario) if isinstance(thesis_map.get(scenario), dict) else {}
        for key in ("required_conditions", "failure_conditions"):
            for item in block.get(key) or []:
                if isinstance(item, dict):
                    labels.append(str(item.get("condition") or item.get("title") or item.get("condition_id") or ""))
    for key in ("red_flags", "confirmatory_signals"):
        for item in watchlist.get(key) or []:
            if isinstance(item, dict):
                labels.append(str(item.get("condition") or item.get("title") or item.get("watch_id") or ""))
            else:
                labels.append(str(item or ""))
    for item in verification:
        if isinstance(item, dict):
            labels.append(
                " ".join(
                    str(item.get(key) or "").strip()
                    for key in ("field", "field_path", "reason", "required_source")
                    if str(item.get(key) or "").strip()
                )
            )
        else:
            labels.append(str(item or ""))
    return labels
