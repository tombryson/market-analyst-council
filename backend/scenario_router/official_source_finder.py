from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from difflib import SequenceMatcher
import re
from typing import Any, Dict, List
from urllib.parse import urlparse

from ..search import scrape_marketindex_announcements, search_asx_announcements
from .models import AnnouncementEvent


@dataclass
class OfficialSourceFinder:
    """Upgrade ticker-trigger events into primary-source announcement URLs."""

    marketindex_limit: int = 12
    asx_limit: int = 12
    lookback_years: int = 2

    async def find_best_source(self, event: AnnouncementEvent, *, title_hint: str = "") -> Dict[str, Any]:
        ticker = str(event.ticker or "").strip().upper()
        exchange = str(event.exchange or "").strip().upper()
        if exchange != "ASX" or not ticker:
            return {}

        candidates: List[Dict[str, Any]] = []
        candidates.extend(self._event_supplied_asx_candidates(event, title_hint=title_hint))

        marketindex_rows = await scrape_marketindex_announcements(
            ticker=ticker,
            max_results=self.marketindex_limit,
        )
        candidates.extend(
            self._normalize_candidates(
                marketindex_rows,
                source_kind="marketindex",
            )
        )

        official_rows = await search_asx_announcements(
            ticker=ticker,
            max_results=self.asx_limit,
            lookback_years=self.lookback_years,
        )
        candidates.extend(
            self._normalize_candidates(
                official_rows,
                source_kind="official_asx_search",
            )
        )

        ranked = self._rank_candidates(
            self._dedupe_candidates(candidates),
            title_hint=title_hint,
            received_at_utc=str(event.received_at_utc or "").strip(),
        )
        return ranked[0] if ranked else {}

    @staticmethod
    def _event_supplied_asx_candidates(event: AnnouncementEvent, *, title_hint: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for raw in event.urls or []:
            url = str(raw or "").strip()
            if not OfficialSourceFinder._is_asx_primary_url(url):
                continue
            rows.append(
                {
                    "title": str(title_hint or "").strip() or "ASX Announcement",
                    "url": url,
                    "published_at": "",
                    "category": "event_supplied",
                    "priority": 1,
                    "source_kind": "event_supplied",
                }
            )
        return rows

    @staticmethod
    def _normalize_candidates(rows: List[Dict[str, Any]], *, source_kind: str) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows or []:
            url = str(row.get("url") or "").strip()
            if not OfficialSourceFinder._is_asx_primary_url(url):
                continue
            normalized.append(
                {
                    "title": str(row.get("title") or "").strip() or "ASX Announcement",
                    "url": url,
                    "published_at": str(row.get("published_at") or "").strip(),
                    "category": str(row.get("category") or "").strip(),
                    "priority": int(row.get("priority", 99) or 99),
                    "source_kind": source_kind,
                }
            )
        return normalized

    @staticmethod
    def _dedupe_candidates(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        deduped: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            url = str(row.get("url") or "").strip().lower()
            if not url or url in seen:
                continue
            seen.add(url)
            deduped.append(row)
        return deduped

    def _rank_candidates(
        self,
        rows: List[Dict[str, Any]],
        *,
        title_hint: str,
        received_at_utc: str,
    ) -> List[Dict[str, Any]]:
        hint = self._normalize_title(title_hint)
        received_date = self._parse_date(received_at_utc)

        def score(row: Dict[str, Any]) -> tuple[float, str]:
            title = str(row.get("title") or "").strip()
            normalized_title = self._normalize_title(title)
            similarity = SequenceMatcher(None, hint, normalized_title).ratio() if hint and normalized_title else 0.0
            title_score = similarity * 100.0

            if hint and normalized_title and hint in normalized_title:
                title_score += 25.0
            if hint and normalized_title:
                hint_tokens = {token for token in hint.split() if len(token) >= 4}
                row_tokens = set(normalized_title.split())
                overlap = len(hint_tokens & row_tokens)
                title_score += min(24.0, overlap * 6.0)

            source_kind = str(row.get("source_kind") or "")
            source_score = {
                "official_asx_search": 18.0,
                "marketindex": 12.0,
                "event_supplied": 10.0,
            }.get(source_kind, 0.0)

            priority = int(row.get("priority", 99) or 99)
            priority_score = max(0.0, 12.0 - (priority * 2.0))

            published_date = self._parse_date(str(row.get("published_at") or "").strip())
            recency_score = 0.0
            if received_date and published_date:
                day_gap = abs((received_date - published_date).days)
                if day_gap == 0:
                    recency_score = 20.0
                elif day_gap <= 1:
                    recency_score = 12.0
                elif day_gap <= 3:
                    recency_score = 6.0

            total = title_score + source_score + priority_score + recency_score
            return total, title

        ranked = sorted(rows, key=score, reverse=True)
        return ranked

    @staticmethod
    def _normalize_title(value: str) -> str:
        text = str(value or "").strip().lower()
        text = re.sub(r"\basx[: ]?[a-z0-9]{2,8}\b", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\bannouncement on hotcopper\b", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"[^a-z0-9]+", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _parse_date(value: str) -> date | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        for parser in (
            lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")).astimezone(timezone.utc),
            lambda x: datetime.strptime(x, "%Y-%m-%d").replace(tzinfo=timezone.utc),
        ):
            try:
                return parser(raw).date()
            except Exception:
                continue
        return None

    @staticmethod
    def _is_asx_primary_url(url: str) -> bool:
        host = urlparse(str(url or "").strip()).netloc.lower()
        return host == "announcements.asx.com.au" or host.endswith(".asx.com.au")
