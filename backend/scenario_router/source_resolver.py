from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from .models import AnnouncementEvent, AnnouncementPacket
from .official_source_finder import OfficialSourceFinder


@dataclass
class SourceResolver:
    """Normalize incoming announcement events into canonical packets."""

    official_source_finder: OfficialSourceFinder | None = None

    async def resolve(self, event: AnnouncementEvent) -> AnnouncementPacket:
        ticker = str(event.ticker or "").strip().upper()
        exchange = str(event.exchange or self._infer_exchange_from_ticker(ticker)).strip().upper()
        hinted_title = self._resolve_title(event.subject, event.body_text, ticker)
        official_source = await self._find_official_source(
            event,
            ticker=ticker,
            exchange=exchange,
            title_hint=hinted_title,
        )
        title = str(official_source.get("title") or hinted_title).strip()
        source_url = str(official_source.get("url") or self._select_source_url(event, exchange=exchange)).strip()
        attachment_path = self._select_attachment_path(event)
        source_type = self._infer_source_type(event, source_url, official_source_found=bool(official_source))
        company_name = str(event.company_hint or "").strip()
        document_sha256 = self._sha256_for_path(attachment_path)
        published_at_utc = str(
            official_source.get("published_at")
            or event.received_at_utc
            or ""
        ).strip()

        return AnnouncementPacket(
            event_id=str(event.event_id or "").strip(),
            ticker=ticker,
            exchange=exchange,
            title=title,
            published_at_utc=published_at_utc,
            source_url=source_url,
            source_type=source_type,
            document_path=attachment_path,
            document_sha256=document_sha256,
            company_name=company_name,
            body_text=str(event.body_text or "").strip(),
        )

    async def _find_official_source(
        self,
        event: AnnouncementEvent,
        *,
        ticker: str,
        exchange: str,
        title_hint: str,
    ) -> dict:
        if exchange != "ASX" or not ticker:
            return {}
        finder = self.official_source_finder or OfficialSourceFinder()
        try:
            return await finder.find_best_source(event, title_hint=title_hint)
        except Exception:
            return {}

    @staticmethod
    def _infer_exchange_from_ticker(ticker: str) -> str:
        text = str(ticker or "").strip().upper()
        if ":" in text:
            return text.split(":", 1)[0].strip()
        return ""

    @staticmethod
    def _resolve_title(subject: str, body_text: str, ticker: str) -> str:
        body_title = SourceResolver._title_from_body(body_text, ticker)
        if body_title:
            return body_title
        return SourceResolver._title_from_subject(subject, ticker)

    @staticmethod
    def _title_from_subject(subject: str, ticker: str) -> str:
        raw = str(subject or "").strip()
        if not raw:
            return "Company Announcement"
        symbol = ticker.split(":", 1)[1].strip() if ":" in ticker else ticker
        patterns = [
            rf"^{re.escape(ticker)}\s*[-:|]\s*",
            rf"^{re.escape(symbol)}\s*[-:|]\s*",
            rf"^{re.escape(symbol)}\s*\(ASX\)\s*announcement\s+on\s+HotCopper\s*$",
            r"^[A-Z]{2,8}:[A-Z0-9]{2,8}\s*[-:|]\s*",
            r"^[A-Z0-9]{2,8}\s*[-:|]\s*",
        ]
        title = raw
        for pattern in patterns:
            title = re.sub(pattern, "", title, flags=re.IGNORECASE).strip()
        return title or raw

    @staticmethod
    def _title_from_body(body_text: str, ticker: str) -> str:
        symbol = ticker.split(":", 1)[1].strip() if ":" in ticker else ticker
        lines = [str(line or "").strip() for line in str(body_text or "").splitlines()]
        for line in lines:
            if not line:
                continue
            match = re.match(
                rf"^{re.escape(symbol)}\s*:\s*(.+)$",
                line,
                flags=re.IGNORECASE,
            )
            if match:
                return str(match.group(1) or "").strip()
        return ""

    @staticmethod
    def _select_source_url(event: AnnouncementEvent, *, exchange: str = "") -> str:
        urls = [str(item or "").strip() for item in (event.urls or []) if str(item or "").strip()]
        if not urls:
            return ""

        def rank(url: str) -> tuple:
            host = urlparse(url).netloc.lower()
            if host == "announcements.asx.com.au":
                return (0, url)
            if host.endswith("asx.com.au"):
                return (1, url)
            if str(exchange or "").strip().upper() == "ASX":
                return (4, url)
            if "investor" in host or "ir." in host:
                return (2, url)
            return (3, url)

        return sorted(urls, key=rank)[0]

    @staticmethod
    def _select_attachment_path(event: AnnouncementEvent) -> str:
        for attachment in event.attachments or []:
            path = str(attachment.local_path or "").strip()
            if path:
                return path
        return ""

    @staticmethod
    def _infer_source_type(event: AnnouncementEvent, source_url: str, *, official_source_found: bool = False) -> str:
        sender = str(event.sender or "").strip().lower()
        host = urlparse(str(source_url or "").strip()).netloc.lower()
        if official_source_found or host == "announcements.asx.com.au" or host.endswith("asx.com.au") or "asx" in sender:
            return "exchange_filing"
        if "globenewswire" in host or "newsfile" in host or "prnewswire" in host:
            return "wire"
        if "investor" in host or "ir." in host:
            return "investor_relations"
        if source_url:
            return "web_source"
        if event.attachments:
            return "email_attachment"
        return "unknown"

    @staticmethod
    def _sha256_for_path(path_text: str) -> str:
        path = Path(str(path_text or "").strip())
        if not path.exists() or not path.is_file():
            return ""
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
