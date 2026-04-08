from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from .models import AnnouncementAttachment, AnnouncementEvent

TICKER_PATTERNS = (
    re.compile(r"\b([A-Z]{2,8}:[A-Z0-9]{2,12})\b"),
    re.compile(r"\bASX[: ]([A-Z0-9]{2,8})\b", re.IGNORECASE),
    re.compile(r"\b([A-Z0-9]{2,8})\s*\((ASX)\)", re.IGNORECASE),
)


@dataclass
class InboxSentinel:
    """Coerce inbound ticker-coded email payloads into announcement events."""

    default_exchange: str = "ASX"

    def ingest_email_payload(self, payload: Dict[str, Any]) -> AnnouncementEvent:
        data = dict(payload or {})
        subject = str(data.get("subject") or "").strip()
        body_text = str(data.get("body_text") or "").strip()
        sender = str(data.get("sender") or "").strip()
        urls = self._coerce_urls(data.get("urls"))
        ticker = self._coerce_ticker(
            explicit=data.get("ticker"),
            subject=subject,
            body_text=body_text,
            urls=urls,
        )
        exchange = self._coerce_exchange(data.get("exchange"), ticker)
        event_id = str(
            data.get("event_id")
            or data.get("gmail_message_id")
            or data.get("message_id")
            or f"evt-{uuid.uuid4().hex}"
        ).strip()

        return AnnouncementEvent(
            event_id=event_id,
            ticker=ticker,
            exchange=exchange,
            subject=subject,
            sender=sender,
            body_text=body_text,
            company_hint=self._coerce_company_hint(
                explicit=data.get("company_hint") or data.get("company_name"),
                body_text=body_text,
            ),
            source_channel=str(data.get("source_channel") or "email").strip() or "email",
            received_at_utc=str(data.get("received_at_utc") or "").strip(),
            urls=urls,
            attachments=self._coerce_attachments(data.get("attachments")),
        )

    def _coerce_ticker(
        self,
        *,
        explicit: Any,
        subject: str,
        body_text: str,
        urls: List[str],
    ) -> str:
        text = str(explicit or "").strip().upper()
        if text:
            return text if ":" in text else f"{self.default_exchange}:{text}"

        for candidate in (subject, body_text, " ".join(urls)):
            ticker = self._extract_ticker(candidate)
            if ticker:
                return ticker
        return ""

    def _coerce_exchange(self, explicit: Any, ticker: str) -> str:
        text = str(explicit or "").strip().upper()
        if text:
            return text
        if ":" in str(ticker or ""):
            return str(ticker).split(":", 1)[0].strip().upper()
        return self.default_exchange if ticker else ""

    @staticmethod
    def _extract_ticker(text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        for pattern in TICKER_PATTERNS:
            match = pattern.search(raw)
            if not match:
                continue
            groups = [str(group or "").strip().upper() for group in match.groups()]
            if len(groups) >= 2 and groups[1] in {"ASX"} and ":" not in groups[0]:
                return f"{groups[1]}:{groups[0]}"
            value = groups[0]
            if ":" in value:
                return value
            return f"ASX:{value}"
        return ""

    @staticmethod
    def _coerce_company_hint(*, explicit: Any, body_text: str) -> str:
        text = str(explicit or "").strip()
        if text:
            return text

        lines = [str(line or "").strip() for line in str(body_text or "").splitlines()]
        lines = [line for line in lines if line]
        for line in lines:
            if re.match(r"^[A-Z0-9]{2,8}:\s+", line):
                continue
            lowered = line.lower()
            if "released an announcement" in lowered:
                prefix = re.split(r"released an announcement", line, flags=re.IGNORECASE)[0]
                candidate = prefix.rstrip(". ").strip()
                if candidate:
                    return candidate
                break
            if re.search(r"\b(limited|resources|mining|metals|energy|holdings|corp|corporation|pharmaceuticals|biotech)\b", line, flags=re.IGNORECASE):
                return line.rstrip(".")
        return ""

    @staticmethod
    def _coerce_urls(value: Any) -> List[str]:
        if isinstance(value, str):
            return [value.strip()] if value.strip() else []
        if not isinstance(value, Iterable):
            return []
        urls: List[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                urls.append(text)
        return urls

    @staticmethod
    def _coerce_attachments(value: Any) -> List[AnnouncementAttachment]:
        if not isinstance(value, list):
            return []
        attachments: List[AnnouncementAttachment] = []
        for item in value:
            if isinstance(item, AnnouncementAttachment):
                attachments.append(item)
                continue
            if not isinstance(item, dict):
                continue
            attachments.append(
                AnnouncementAttachment(
                    filename=str(item.get("filename") or "").strip(),
                    content_type=str(item.get("content_type") or "").strip(),
                    local_path=str(item.get("local_path") or "").strip(),
                )
            )
        return attachments
