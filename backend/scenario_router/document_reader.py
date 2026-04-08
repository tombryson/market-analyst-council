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
    "timeline": ("timeline", "quarter", "delay", "accelerat", "ahead of schedule", "on track", "milestone"),
    "resource": ("resource", "reserve", "jorc", "ore reserve", "mineral resource"),
    "production": ("production", "throughput", "first gold", "ramp-up", "ramp up", "processing"),
    "guidance": ("guidance", "forecast", "outlook", "aisc", "cost guidance"),
    "operations": ("operations", "plant", "mine", "mill", "contractor", "site"),
    "management": ("director", "ceo", "cfo", "chair", "management", "executive"),
    "m_and_a": ("acquisition", "merger", "scheme", "takeover", "farm-in", "farm in", "joint venture"),
}


@dataclass
class DocumentReader:
    """Read announcement documents into a normalized fact packet."""

    async def read(self, packet: AnnouncementPacket) -> AnnouncementFacts:
        full_text, evidence_excerpts = await self._read_text(packet)
        extracted_facts = self._extract_facts(full_text)
        material_topics = self._infer_material_topics(full_text, extracted_facts)
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
        return AnnouncementFacts(
            event_id=packet.event_id,
            ticker=packet.ticker,
            company_name=packet.company_name,
            title=packet.title,
            summary=summary,
            extracted_facts=extracted_facts,
            material_topics=material_topics,
            evidence=evidence,
            raw_text_excerpt=full_text[:1800],
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
        lines = [line for line in lines if len(line) >= 30]
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
            if re.fullmatch(r"[A-Z0-9 .,:;()/-]+", line) and len(line.split()) <= 4:
                continue
            seen.add(low)
            facts.append(line)
            if len(facts) >= 8:
                break
        return facts

    @staticmethod
    def _infer_material_topics(text: str, facts: List[str]) -> List[str]:
        haystack = f"{text}\n" + "\n".join(facts)
        low = haystack.lower()
        topics: List[str] = []
        for topic, keywords in TOPIC_KEYWORDS.items():
            if any(keyword in low for keyword in keywords):
                topics.append(topic)
        return topics

    @staticmethod
    def _build_summary(text: str, facts: List[str]) -> str:
        if facts:
            return " ".join(facts[:3])[:500]
        snippet = re.sub(r"\s+", " ", str(text or "")).strip()
        return snippet[:500]
