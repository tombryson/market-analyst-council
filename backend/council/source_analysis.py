"""Source-text analysis utilities shared across council sub-modules.

These functions are extracted from the Stage-1 pipeline because they are
also called by fact_digest, asx_ingestion, and stage1_multi_wave — pulling
them here breaks the potential circular-import loop.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

def _excerpt_material_signal_score(excerpt: str) -> int:
    """Rough materiality score for a decoded excerpt."""
    text = re.sub(r"\s+", " ", str(excerpt or "")).strip()
    if not text:
        return -5
    low = text.lower()
    score = 0
    if re.search(r"\d", low):
        score += 1
    signal_tokens = (
        "npv",
        "irr",
        "aisc",
        "capex",
        "resource",
        "reserve",
        "production",
        "first gold",
        "gold pour",
        "funding",
        "facility",
        "loan",
        "cash",
        "debt",
        "market cap",
        "shares",
        "enterprise value",
        "ev/oz",
        "milestone",
        "timeline",
    )
    score += min(8, sum(1 for token in signal_tokens if token in low))
    if _is_low_signal_legal_boilerplate(text):
        score -= 6
    if _is_heading_like_sentence(text):
        score -= 3
    if len(text) < 120:
        score -= 1
    return score


def _classify_fact_pack_section(sentence: str) -> str:
    """Assign sentence to best-fit rubric section via keyword scoring."""
    text = sentence.lower()
    best_section = "other_material_facts"
    best_score = 0

    for section, keywords in _FACT_PACK_KEYWORDS.items():
        score = sum(1 for token in keywords if token in text)
        if score > best_score:
            best_score = score
            best_section = section

    return best_section


def _extract_source_sentences(excerpt: str) -> List[str]:
    """Split decoded excerpt into cleaned sentence candidates."""
    if not excerpt:
        return []
    raw_parts = re.split(r"(?<=[\.\!\?])\s+|\n+", excerpt)
    out: List[str] = []
    for part in raw_parts:
        sentence = re.sub(r"\s+", " ", part).strip(" \t-")
        if len(sentence) < 40:
            continue
        if re.match(r"^[a-z]{3,}[,;:]\s", sentence):
            low_sentence = sentence.lower()
            if not any(
                token in low_sentence
                for token in (
                    "npv",
                    "irr",
                    "aisc",
                    "capex",
                    "resource",
                    "reserve",
                    "production",
                    "first gold",
                    "gold pour",
                    "funding",
                    "facility",
                    "cash",
                    "debt",
                    "market cap",
                    "shares",
                    "enterprise value",
                )
            ):
                continue
        if _is_low_signal_legal_boilerplate(sentence):
            continue
        if _is_heading_like_sentence(sentence):
            continue
        if len(sentence) > 420:
            sentence = sentence[:417].rstrip() + "..."
        out.append(sentence)
    return out


def _is_heading_like_sentence(sentence: str) -> bool:
    """Drop short heading/table-style lines that are not evidence-bearing facts."""
    text = re.sub(r"\s+", " ", str(sentence or "")).strip()
    if not text:
        return True
    low = text.lower()

    if low in {
        "contents",
        "table of contents",
        "for personal use only",
        "announcements",
        "presentations",
        "project highlights",
    }:
        return True

    # Keep compact heading-like strings only if they include strong signal.
    strong_tokens = (
        "npv",
        "irr",
        "aisc",
        "capex",
        "resource",
        "reserve",
        "production",
        "first gold",
        "gold pour",
        "funding",
        "facility",
        "cash",
        "debt",
        "market cap",
        "shares",
        "enterprise value",
    )
    if any(token in low for token in strong_tokens):
        return False

    words = [token for token in re.split(r"\s+", text) if token]
    if len(words) <= 12 and len(text) <= 95:
        if not re.search(r"[\.!?;:]", text):
            alpha = [c for c in text if c.isalpha()]
            if alpha:
                upper_ratio = sum(1 for c in alpha if c.isupper()) / float(len(alpha))
                if upper_ratio >= 0.72:
                    return True
            # Short title-style strings with no punctuation are often headings.
            if not re.search(r"\d", text):
                return True
    return False


def _is_low_signal_legal_boilerplate(sentence: str) -> bool:
    """Filter legal/admin boilerplate that adds minimal valuation signal."""
    low = str(sentence or "").lower()
    if not low:
        return True

    legal_patterns = (
        "708a cleansing notice",
        "cleansing notice",
        "application for quotation of securities",
        "notice for quotation of securities",
        "notice of quotation of securities",
        "proposed issue of securities",
        "proposed issue of quoted securities",
        "proposed issue of unquoted securities",
        "appendix 2a",
        "appendix 3b",
        "appendix 3c",
        "part 6d.2",
        "chapter 2m",
        "sections 674 and 674a",
        "corporations act 2001",
        "without disclosure to investors",
        "this notice is given under paragraph 5(e)",
        "for personal use only",
        "announcement summary entity name",
        "trading halt",
        "pause in trading",
        "voluntary suspension",
        "suspension from quotation",
        "request for trading halt",
        "request for voluntary suspension",
    )
    if any(token in low for token in legal_patterns):
        # Keep if the same sentence also carries unusually strong valuation signal.
        override_tokens = (
            "npv",
            "irr",
            "aisc",
            "capex",
            "resource",
            "reserve",
            "production",
            "first gold",
            "gold pour",
            "market cap",
            "shares outstanding",
            "enterprise value",
            "cash",
            "debt",
            "funding",
            "loan facility",
        )
        if any(token in low for token in override_tokens):
            return False
        return True
    return False


def _is_low_signal_notice_source_item(source: Dict[str, Any]) -> bool:
    """Detect legal/admin notice docs with low valuation signal."""
    title = str(source.get("title", "")).lower()
    content = str(
        source.get("decoded_excerpt")
        or source.get("content")
        or source.get("source_snippet")
        or ""
    ).lower()
    url = str(source.get("url", "")).lower()
    text = f"{title} {content} {url}"

    hard_block_patterns = (
        "trading halt",
        "pause in trading",
        "voluntary suspension",
        "suspension from quotation",
        "request for trading halt",
        "request for voluntary suspension",
        "application for quotation of securities",
        "notice for quotation of securities",
        "notice of quotation of securities",
        "proposed issue of securities",
        "proposed issue of quoted securities",
        "proposed issue of unquoted securities",
        "quotation of securities",
        "appendix 2a",
        "appendix 3b",
        "appendix 3c",
        "cleansing notice",
        "708a cleansing notice",
    )
    if any(token in text for token in hard_block_patterns):
        return True

    # Historical index/listing pages are usually retrieval scaffolding, not
    # evidence-bearing documents, unless they carry strong valuation terms.
    index_patterns = (
        "quarterly reports - 2017 to 2022",
        "presentations and interviews",
        "announcements and media releases",
        "investor centre",
    )
    if any(token in title for token in index_patterns):
        override_tokens = (
            "npv",
            "irr",
            "aisc",
            "capex",
            "resource",
            "reserve",
            "production",
            "first gold",
            "gold pour",
            "funding",
            "facility",
            "cash",
            "debt",
            "market cap",
            "shares",
            "enterprise value",
        )
        if not any(token in text for token in override_tokens):
            return True
    if (
        re.search(r"\b20\d{2}\s*(?:to|\-)\s*20\d{2}\b", title)
        and any(token in title for token in ("quarterly reports", "annual reports", "presentations"))
    ):
        if not any(token in text for token in ("npv", "irr", "aisc", "resource", "production", "first gold", "gold pour")):
            return True

    low_patterns = (
        "part 6d.2",
        "chapter 2m",
        "sections 674 and 674a",
        "corporations act 2001",
    )
    if not any(token in text for token in low_patterns):
        return False

    # Keep if there is clear valuation/timeline signal in the same source.
    override_tokens = (
        "npv",
        "irr",
        "aisc",
        "capex",
        "resource",
        "reserve",
        "production",
        "first gold",
        "gold pour",
        "funding",
        "loan facility",
        "cash",
        "debt",
        "market cap",
        "shares outstanding",
        "enterprise value",
    )
    return not any(token in text for token in override_tokens)


def _source_authority_rank(url: str) -> int:
    """Rough source-authority rank for timeline evidence ordering."""
    domain = ""
    try:
        domain = urlparse(url or "").netloc.lower()
    except Exception:
        domain = ""
    if domain.endswith("asx.com.au") or domain.endswith("sec.gov"):
        return 4
    if domain.endswith("wcsecure.weblink.com.au"):
        return 3
    if "investor" in domain or "announcements" in domain:
        return 2
    return 1


def _extract_timeline_windows(text: str) -> List[str]:
    """Extract quarter/month window tokens (e.g., Q1 2026, March 2026)."""
    raw = text or ""
    windows: List[str] = []

    quarter_matches = re.findall(r"\bq([1-4])\s*[\|/\-]?\s*(20\d{2})\b", raw, flags=re.IGNORECASE)
    for q, year in quarter_matches:
        windows.append(f"Q{int(q)} {int(year)}")

    month_matches = re.findall(
        r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\b",
        raw,
        flags=re.IGNORECASE,
    )
    for month, year in month_matches:
        windows.append(f"{month.title()} {int(year)}")

    deduped: List[str] = []
    seen = set()
    for token in windows:
        norm = token.lower()
        if norm in seen:
            continue
        seen.add(norm)
        deduped.append(token)
    return deduped


def _window_to_quarter_index(token: str) -> Optional[int]:
    """Map timeline token into sortable quarter index."""
    value = (token or "").strip()
    q_match = re.match(r"^Q([1-4])\s+(20\d{2})$", value, flags=re.IGNORECASE)
    if q_match:
        q = int(q_match.group(1))
        year = int(q_match.group(2))
        return (year * 4) + (q - 1)

    m_match = re.match(
        r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})$",
        value,
        flags=re.IGNORECASE,
    )
    if not m_match:
        return None
    month_name = m_match.group(1).lower()
    year = int(m_match.group(2))
    month_to_q = {
        "january": 1,
        "february": 1,
        "march": 1,
        "april": 2,
        "may": 2,
        "june": 2,
        "july": 3,
        "august": 3,
        "september": 3,
        "october": 4,
        "november": 4,
        "december": 4,
    }
    q = month_to_q.get(month_name)
    if q is None:
        return None
    return (year * 4) + (q - 1)


