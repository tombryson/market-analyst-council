from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from ..pdf_processor import extract_text_from_pdf
from .models import AnnouncementFacts, AnnouncementPacket, EvidenceRef

TOPIC_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "financing": ("funding", "facility", "debt", "loan", "placement", "capital raise", "liquidity"),
    "permitting": ("permit", "approval", "licence", "license", "regulator", "environmental", "heritage"),
    "regulatory": ("regulator", "regulatory", "approval", "licence", "license", "compliance", "investigation"),
    "timeline": ("timeline", "delay", "accelerat", "ahead of schedule", "on track", "milestone"),
    "resource": ("resource", "reserve", "jorc", "ore reserve", "mineral resource"),
    "production": ("production", "throughput", "first gold", "ramp-up", "ramp up", "processing"),
    "guidance": ("guidance", "forecast", "outlook", "aisc", "cost guidance", "revenue guidance", "earnings guidance"),
    "operations": ("operations", "plant", "mine", "mill", "contractor", "site", "facility", "service", "platform", "launch"),
    "commercial": ("contract", "agreement", "customer", "client", "partner", "distribution", "order", "purchase order"),
    "customer": ("customer", "client", "subscriber", "user", "account", "churn", "retention"),
    "product": ("product", "launch", "release", "trial", "platform", "software", "service", "device"),
    "technology": ("technology", "software", "platform", "patent", "clinical", "data", "cyber", "ai"),
    "legal": ("litigation", "claim", "proceeding", "settlement", "court", "dispute", "breach"),
    "management": ("director", "ceo", "cfo", "chair", "management", "executive"),
    "governance": ("board", "director", "chair", "resignation", "appointment", "governance", "audit"),
    "m_and_a": ("acquisition", "merger", "scheme", "takeover", "farm-in", "farm in", "joint venture"),
}

BOILERPLATE_PATTERNS = (
    r"\bregistered office\b",
    r"\bcorporate directory\b",
    r"\bwww\.",
    r"\bhttps?://",
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
    r"\btelephone\b|\bphone\b",
    r"\bacn\b|\babn\b",
    r"\bpage\s+\d+\b",
    r"\basx\s*:\s*[a-z]{2,5}\b",
)

FACT_SIGNAL_RE = re.compile(
    r"\b("
    r"resource|reserve|jorc|production|produced|guidance|cash|funding|facility|permit|approval|"
    r"drill|intercept|grade|ounce|koz|moz|boe|bbl|revenue|cost|aisc|capex|quarter|completed|increased|"
    r"commenced|achieved|acquisition|placement|raise|debt|milestone|commissioning|timeline|schedule|on track|"
    r"contract|agreement|customer|client|subscriber|user|launch|platform|software|trial|regulatory|"
    r"litigation|settlement|court|director|board|ceo|cfo|patent|clinical"
    r")\b|[0-9]",
    flags=re.IGNORECASE,
)


@dataclass
class DocumentReader:
    """Read announcement documents into a normalized fact packet."""

    async def read(self, packet: AnnouncementPacket) -> AnnouncementFacts:
        full_text, evidence_excerpts = await self._read_text(packet)
        extracted_facts = self._extract_facts(full_text)
        summary = self._build_summary(full_text, extracted_facts)
        evidence = [
            EvidenceRef(
                source_url=packet.source_url,
                quote_excerpt=excerpt,
                source_title=packet.title,
                source_date_utc=packet.published_at_utc,
            )
            for excerpt in evidence_excerpts[:3]
        ]
        parse_quality = {
            "decoded_chars": len(full_text or ""),
            "fact_count": len(extracted_facts),
            "evidence_excerpt_count": len(evidence),
            "reader": "document_reader_raw",
        }
        source_confidence = self._source_confidence(packet)
        extraction_confidence = self._extraction_confidence(parse_quality)
        return AnnouncementFacts(
            event_id=packet.event_id,
            ticker=packet.ticker,
            company_name=packet.company_name,
            title=packet.title,
            summary=summary,
            extracted_facts=extracted_facts,
            material_topics=[],
            evidence=evidence,
            raw_text_excerpt=full_text[:1800],
            parse_quality=parse_quality,
            source_confidence=source_confidence,
            extraction_confidence=extraction_confidence,
            confidence_breakdown={
                "source_confidence": source_confidence,
                "extraction_confidence": extraction_confidence,
                "source": {
                    "source_type": packet.source_type,
                    "source_url_resolved": bool(str(packet.source_url or "").strip()),
                    "document_path_resolved": bool(str(packet.document_path or "").strip()),
                },
                "extraction": parse_quality,
            },
        )

    async def _read_text(self, packet: AnnouncementPacket) -> Tuple[str, List[str]]:
        local_path = Path(str(packet.document_path or "").strip())
        prefer_remote_exchange_filing = (
            str(packet.source_type or "").strip().lower() == "exchange_filing"
            and str(packet.source_url or "").strip()
        )
        if prefer_remote_exchange_filing:
            remote_text, remote_evidence = await self._read_remote(packet)
            if str(remote_text or "").strip():
                return remote_text, remote_evidence
        if local_path.exists() and local_path.is_file():
            return await self._read_local(local_path)
        if str(packet.source_url or "").strip():
            return await self._read_remote(packet)
        if str(packet.body_text or "").strip():
            text = self._normalize_text(packet.body_text)
            return text, self._pick_evidence_excerpts(text)
        return "", []

    async def _read_local(self, path: Path) -> Tuple[str, List[str]]:
        suffix = path.suffix.lower()
        if suffix == ".pdf":
            extracted = await extract_text_from_pdf(str(path))
            text = str(extracted.get("text", "") or "")
            return self._normalize_text(text), self._pick_evidence_excerpts(text)
        if suffix in {".json"}:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                text = json.dumps(payload, indent=2, ensure_ascii=True)
            except Exception:
                text = path.read_text(encoding="utf-8", errors="ignore")
            return self._normalize_text(text), self._pick_evidence_excerpts(text)
        text = path.read_text(encoding="utf-8", errors="ignore")
        return self._normalize_text(text), self._pick_evidence_excerpts(text)

    async def _read_remote(self, packet: AnnouncementPacket) -> Tuple[str, List[str]]:
        from ..document_pipeline.parse_service import parse_documents

        refs = [
            {
                "doc_id": packet.event_id or packet.ticker,
                "title": packet.title,
                "source_url": packet.source_url,
                "content_url": packet.source_url,
                "pdf_url": packet.source_url if str(packet.source_url or "").lower().endswith(".pdf") or "/asxpdf/" in str(packet.source_url or "").lower() else "",
                "exchange": packet.exchange,
                "issuer_hint": packet.company_name,
                "ticker_hint": packet.ticker,
            }
        ]
        parsed = await parse_documents(document_refs=refs, parser_id="smart_default")
        if not parsed:
            return "", []
        doc = parsed[0]
        text = str(doc.get("full_text") or doc.get("raw_text") or "")
        return self._normalize_text(text), self._pick_evidence_excerpts(text)

    @staticmethod
    def _normalize_text(text: str) -> str:
        value = str(text or "")
        value = value.replace("\r", "\n")
        value = re.sub(r"\n{3,}", "\n\n", value)
        return value.strip()

    @staticmethod
    def _pick_evidence_excerpts(text: str) -> List[str]:
        lines = [re.sub(r"\s+", " ", line).strip() for line in str(text or "").splitlines()]
        lines = [line for line in lines if len(line) >= 30 and not DocumentReader._is_boilerplate_line(line)]
        return lines[:6]

    @staticmethod
    def _extract_facts(text: str) -> List[str]:
        lines = [re.sub(r"\s+", " ", line).strip(" -*•\t") for line in str(text or "").splitlines()]
        facts: List[str] = []
        seen = set()
        for line in lines:
            if len(line) < 25:
                continue
            low = line.lower()
            if low in seen:
                continue
            if DocumentReader._is_boilerplate_line(line):
                continue
            if re.fullmatch(r"[A-Z0-9 .,:;()/-]+", line) and len(line.split()) <= 4:
                continue
            if not FACT_SIGNAL_RE.search(line):
                continue
            seen.add(low)
            facts.append(line)
            if len(facts) >= 8:
                break
        return facts

    @staticmethod
    def _infer_material_topics(text: str, facts: List[str]) -> List[str]:
        """Legacy topic hinting retained for compatibility tests.

        The live router no longer uses this as the primary classifier. Parsed
        filing text is interpreted after the baseline run is loaded by
        AnnouncementInterpreter, where company context and sector adapters are
        available.
        """
        haystack = "\n".join(facts) or str(text or "")[:2500]
        low = haystack.lower()
        topics: List[str] = []
        for topic, keywords in TOPIC_KEYWORDS.items():
            hit_count = sum(1 for keyword in keywords if DocumentReader._keyword_in_text(keyword, low))
            if hit_count >= 2 or any(DocumentReader._is_strong_topic_hit(topic, keyword, low) for keyword in keywords):
                topics.append(topic)
        return topics[:4]

    @staticmethod
    def _build_summary(text: str, facts: List[str]) -> str:
        if facts:
            return " ".join(facts[:3])[:500]
        snippet = re.sub(r"\s+", " ", str(text or "")).strip()
        return snippet[:500]

    @staticmethod
    def _source_confidence(packet: AnnouncementPacket) -> float:
        source_type = str(packet.source_type or "").strip().lower()
        has_url = bool(str(packet.source_url or "").strip())
        has_path = bool(str(packet.document_path or "").strip())
        if source_type == "exchange_filing" and has_url:
            return 1.0
        if has_url:
            return 0.88
        if has_path:
            return 0.78
        if str(packet.body_text or "").strip():
            return 0.55
        return 0.1

    @staticmethod
    def _extraction_confidence(parse_quality: Dict[str, int]) -> float:
        decoded = int(parse_quality.get("decoded_chars") or 0)
        facts = int(parse_quality.get("fact_count") or 0)
        excerpts = int(parse_quality.get("evidence_excerpt_count") or 0)
        if decoded >= 4000:
            base = 0.92
        elif decoded >= 1200:
            base = 0.84
        elif decoded >= 400:
            base = 0.7
        elif decoded > 0:
            base = 0.45
        else:
            base = 0.1
        bonus = min(0.06, facts * 0.01) + min(0.04, excerpts * 0.01)
        return round(min(0.98, base + bonus), 3)

    @staticmethod
    def _is_boilerplate_line(line: str) -> bool:
        low = str(line or "").strip().lower()
        if not low:
            return True
        if any(re.search(pattern, low, flags=re.IGNORECASE) for pattern in BOILERPLATE_PATTERNS):
            return True
        if len(low.split()) <= 3 and re.fullmatch(r"[a-z0-9 .,:;()/-]+", low):
            return True
        return False

    @staticmethod
    def _is_strong_topic_hit(topic: str, keyword: str, haystack: str) -> bool:
        strong_keywords = {
            "financing": {"funding", "debt", "loan", "placement", "capital raise"},
            "permitting": {"permit", "approval", "licence", "license"},
            "regulatory": {"regulatory", "regulator", "compliance", "investigation"},
            "resource": {"mineral resource", "ore reserve", "jorc", "reserve"},
            "production": {"production", "throughput", "first gold", "ramp-up", "ramp up"},
            "guidance": {"guidance", "aisc", "cost guidance", "revenue guidance", "earnings guidance"},
            "commercial": {"contract", "agreement", "customer", "client", "purchase order"},
            "customer": {"customer", "client", "subscriber", "user", "churn"},
            "product": {"product", "launch", "release", "trial", "platform"},
            "technology": {"technology", "software", "platform", "patent", "clinical"},
            "legal": {"litigation", "claim", "settlement", "court", "dispute"},
            "governance": {"board", "director", "resignation", "appointment"},
            "m_and_a": {"acquisition", "merger", "takeover", "joint venture"},
        }
        return keyword in strong_keywords.get(topic, set()) and DocumentReader._keyword_in_text(keyword, haystack)

    @staticmethod
    def _keyword_in_text(keyword: str, haystack: str) -> bool:
        term = str(keyword or "").strip().lower()
        if not term:
            return False
        if " " in term or "-" in term:
            return term in haystack
        return re.search(rf"\b{re.escape(term)}\b", haystack) is not None
