"""Lightweight delta-monitor checks for existing Stage 3 run artifacts."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .research import ResearchService
from .template_loader import get_template_loader


OUTPUTS_DIR = Path(__file__).resolve().parents[1] / "outputs"
DELTA_DIR = OUTPUTS_DIR / "delta_monitor"


POSITIVE_TOKENS = [
    "on track",
    "completed",
    "achieved",
    "approved",
    "secured",
    "funded",
    "commenced",
    "started",
    "delivered",
    "signed",
    "progress",
]

NEGATIVE_TOKENS = [
    "delay",
    "delayed",
    "postpone",
    "postponed",
    "at risk",
    "missed",
    "withdrawn",
    "terminated",
    "cost overrun",
    "funding gap",
    "dilution",
    "default",
    "suspend",
    "halted",
]

ACHIEVED_TOKENS = [
    "completed",
    "achieved",
    "delivered",
    "closed",
    "received",
    "approved",
    "commenced",
]

TYPE_KEYWORDS: Dict[str, List[str]] = {
    "timeline": [
        "milestone",
        "timeline",
        "target date",
        "quarter",
        "q1",
        "q2",
        "q3",
        "q4",
        "first gold",
        "production",
        "phase",
        "commission",
        "ramp-up",
        "ramp up",
    ],
    "thesis": [
        "thesis",
        "scenario",
        "bull",
        "bear",
        "base case",
        "valuation",
        "upside",
        "downside",
    ],
    "funding": [
        "funding",
        "capital raise",
        "placement",
        "loan",
        "facility",
        "debt",
        "cash balance",
        "runway",
        "dilution",
    ],
    "valuation": [
        "npv",
        "valuation",
        "market cap",
        "enterprise value",
        "price target",
        "guidance",
        "aisc",
    ],
}

HIGH_IMPACT_TOKENS = [
    "first gold",
    "production",
    "fda",
    "clinical hold",
    "permit",
    "approval",
    "funding",
    "facility",
    "debt",
    "default",
    "going concern",
]

STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "into",
    "will",
    "have",
    "has",
    "are",
    "was",
    "were",
    "about",
    "under",
    "over",
    "than",
    "into",
    "within",
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc_iso(value: Optional[datetime]) -> str:
    if not isinstance(value, datetime):
        return ""
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_datetime_utc(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_run_key(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(run_id or "").strip())[:180]


def _extract_baseline_as_of(structured: Dict[str, Any], artifact_updated_at: str) -> Tuple[datetime, str, int]:
    now_utc = _now_utc()
    market_meta = structured.get("market_data_provenance")
    if not isinstance(market_meta, dict):
        market_meta = {}

    analysis_dt = _parse_iso_datetime_utc(structured.get("analysis_date"))
    market_dt = _parse_iso_datetime_utc(market_meta.get("prepass_as_of_utc"))
    artifact_dt = _parse_iso_datetime_utc(artifact_updated_at)

    baseline_dt = analysis_dt or market_dt or artifact_dt or now_utc
    baseline_source = "analysis_date"
    if analysis_dt is None and market_dt is not None:
        baseline_source = "market_data_provenance.prepass_as_of_utc"
    elif analysis_dt is None and market_dt is None:
        baseline_source = "artifact_updated_at"

    age_days = max(0, int((now_utc - baseline_dt).total_seconds() // 86400))
    return baseline_dt, baseline_source, age_days


def _host_matches_allowed(host: str, allowed_suffixes: List[str]) -> bool:
    h = (host or "").lower().strip(".")
    if h.startswith("www."):
        h = h[4:]
    for suffix in allowed_suffixes:
        s = str(suffix or "").lower().strip(".")
        if not s:
            continue
        if h == s or h.endswith(f".{s}"):
            return True
    return False


def _extract_conditions(thesis_map: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    if not isinstance(thesis_map, dict):
        return out

    for scenario in ("bull", "base", "bear"):
        block = thesis_map.get(scenario)
        if not isinstance(block, dict):
            continue
        for key in ("required_conditions", "failure_conditions", "conditions"):
            raw = block.get(key)
            if not isinstance(raw, list):
                continue
            for item in raw:
                if isinstance(item, str):
                    text = item.strip()
                elif isinstance(item, dict):
                    text = str(item.get("condition") or item.get("text") or "").strip()
                else:
                    text = ""
                if text:
                    out.append(text)
    return out


def _extract_milestones(structured: Dict[str, Any]) -> List[str]:
    milestones: List[str] = []
    timeline = structured.get("development_timeline")
    if isinstance(timeline, list):
        for row in timeline:
            if not isinstance(row, dict):
                continue
            text = " ".join(
                [
                    str(row.get("milestone") or "").strip(),
                    str(row.get("description") or "").strip(),
                    str(row.get("target_period") or "").strip(),
                ]
            ).strip()
            if text:
                milestones.append(text)

    if not milestones:
        catalysts = ((structured.get("extended_analysis") or {}).get("next_major_catalysts") or [])
        if isinstance(catalysts, list):
            for row in catalysts:
                if isinstance(row, dict):
                    text = str(row.get("catalyst") or row.get("description") or "").strip()
                    if text:
                        milestones.append(text)
                elif isinstance(row, str) and row.strip():
                    milestones.append(row.strip())
    return milestones


def _keywords_from_text(text: str, limit: int = 6) -> List[str]:
    words = re.findall(r"[A-Za-z0-9]{4,}", str(text or "").lower())
    seen = set()
    out: List[str] = []
    for word in words:
        if word in STOPWORDS or word in seen:
            continue
        seen.add(word)
        out.append(word)
        if len(out) >= limit:
            break
    return out


def _direction_for_text(text: str) -> str:
    hay = str(text or "").lower()
    pos = sum(1 for token in POSITIVE_TOKENS if token in hay)
    neg = sum(1 for token in NEGATIVE_TOKENS if token in hay)
    if neg > pos:
        return "negative"
    if pos > neg:
        return "positive"
    return "neutral"


def _classify_change_type(text: str) -> str:
    hay = str(text or "").lower()
    best_type = "other"
    best_hits = 0
    for kind, tokens in TYPE_KEYWORDS.items():
        hits = sum(1 for token in tokens if token in hay)
        if hits > best_hits:
            best_hits = hits
            best_type = kind
    return best_type


def _is_material_source(source: Dict[str, Any], material_tokens: List[str]) -> bool:
    hay = f"{source.get('title','')} {source.get('content','')}".lower()
    score = 0
    if source.get("source_type") in ("regulatory_filing", "exchange_notice", "company_filing"):
        score += 2
    if any(token in hay for token in material_tokens):
        score += 2
    direction = _direction_for_text(hay)
    if direction != "neutral":
        score += 1
    if len(source.get("content", "")) >= 300:
        score += 1
    return score >= 3


def _condition_status(condition: str, sources: List[Dict[str, Any]]) -> str:
    kws = _keywords_from_text(condition, limit=5)
    if not kws:
        return "unknown"
    matched = [s for s in sources if any(kw in s.get("_haystack", "") for kw in kws)]
    if not matched:
        return "unknown"
    dirs = [_direction_for_text(s.get("_haystack", "")) for s in matched]
    if "negative" in dirs:
        return "at_risk"
    if "positive" in dirs:
        return "confirmed"
    return "unknown"


def _timeline_status(milestone: str, sources: List[Dict[str, Any]]) -> str:
    kws = _keywords_from_text(milestone, limit=5)
    if not kws:
        return "unknown"
    matched = [s for s in sources if any(kw in s.get("_haystack", "") for kw in kws)]
    if not matched:
        return "unknown"
    hay_all = " ".join(s.get("_haystack", "") for s in matched)
    if any(token in hay_all for token in NEGATIVE_TOKENS):
        return "at_risk"
    if any(token in hay_all for token in ACHIEVED_TOKENS):
        return "achieved"
    if any(token in hay_all for token in POSITIVE_TOKENS):
        return "on_track"
    return "unknown"


def _make_top_changes(sources: List[Dict[str, Any]], max_items: int = 3) -> List[Dict[str, Any]]:
    ranked: List[Tuple[float, Dict[str, Any]]] = []
    for source in sources:
        hay = source.get("_haystack", "")
        direction = _direction_for_text(hay)
        change_type = _classify_change_type(hay)
        confidence = 55
        if direction != "neutral":
            confidence += 15
        if source.get("is_material"):
            confidence += 20
        confidence = max(0, min(95, confidence))
        score = float(source.get("score", 0.0))
        if source.get("is_material"):
            score += 1.5
        ranked.append(
            (
                score,
                {
                    "type": change_type,
                    "summary": source.get("title") or "Update",
                    "direction": direction,
                    "confidence_pct": confidence,
                    "source_url": source.get("url") or "",
                    "source_date": source.get("published_at") or "",
                },
            )
        )
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [row for _, row in ranked[:max_items]]


def _compute_status_and_action(age_days: int, high_impact_conflicts: int) -> Tuple[str, str]:
    if high_impact_conflicts > 0:
        return "stale", "full_rerun_recommended"
    if age_days <= 7:
        return "fresh", "no_action"
    if age_days <= 21:
        return "watch", "delta_again_tomorrow"
    return "stale", "full_rerun_recommended"


def _today_utc_date() -> str:
    return _now_utc().date().isoformat()


def _list_delta_artifacts(run_id: str) -> List[Path]:
    run_key = _safe_run_key(run_id)
    if not DELTA_DIR.exists():
        return []
    artifacts = sorted(
        DELTA_DIR.glob(f"{run_key}__*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return artifacts


def get_latest_delta(run_id: str) -> Optional[Dict[str, Any]]:
    for path in _list_delta_artifacts(run_id):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            payload.setdefault("_artifact_file", path.name)
            return payload
    return None


def get_cached_delta_for_today(run_id: str) -> Optional[Dict[str, Any]]:
    today = _today_utc_date()
    for path in _list_delta_artifacts(run_id):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        checked_at = _parse_iso_datetime_utc(payload.get("checked_at_utc"))
        if checked_at and checked_at.date().isoformat() == today:
            payload.setdefault("_artifact_file", path.name)
            payload["_cached"] = True
            return payload
    return None


def _save_delta_artifact(run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    DELTA_DIR.mkdir(parents=True, exist_ok=True)
    run_key = _safe_run_key(run_id)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = DELTA_DIR / f"{run_key}__{ts}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    payload["_artifact_file"] = path.name
    return payload


async def run_delta_check(
    *,
    run_id: str,
    structured: Dict[str, Any],
    artifact_updated_at: str,
    force: bool = False,
    max_sources: int = 12,
    lookback_days: int = 14,
) -> Dict[str, Any]:
    """Execute one lightweight delta check against recent filings/news."""
    if not force:
        cached = get_cached_delta_for_today(run_id)
        if cached:
            return cached

    ticker = str(structured.get("ticker") or "").strip()
    company = str(structured.get("company_name") or structured.get("company") or "").strip()
    exchange = str(structured.get("exchange") or structured.get("exchange_id") or "").strip().lower()
    loader = get_template_loader()
    retrieval_params = loader.get_exchange_retrieval_params(exchange)
    allowed_suffixes = retrieval_params.get("allowed_domain_suffixes") or []
    material_tokens = [str(x).lower() for x in (retrieval_params.get("material_filing_tokens") or [])]

    baseline_dt, baseline_source, age_days = _extract_baseline_as_of(structured, artifact_updated_at)
    baseline_str = baseline_dt.strftime("%Y-%m-%d")
    used_max_sources = max(1, int(max_sources))
    query = (
        f"Latest material company announcements and filings for {company or ticker} "
        f"({ticker}) since {baseline_str}. Focus on milestones, funding, valuation drivers, "
        "guidance changes, and regulatory/operational updates."
    )

    service = ResearchService(depth="basic", max_sources=used_max_sources)
    retrieval = await service.gather_research(user_query=query, ticker=ticker or None)
    results = retrieval.get("results", []) or []

    filtered: List[Dict[str, Any]] = []
    for row in results:
        if not isinstance(row, dict):
            continue
        url = str(row.get("url", "") or "").strip()
        host = urlparse(url).netloc.lower() if url else ""
        if allowed_suffixes and host and not _host_matches_allowed(host, allowed_suffixes):
            continue
        filtered.append(
            {
                "title": str(row.get("title", "") or "").strip(),
                "url": url,
                "content": str(row.get("content", "") or "").strip(),
                "published_at": str(row.get("published_at", "") or "").strip(),
                "source_type": str(row.get("source_type", "") or "").strip(),
                "score": float(row.get("score", 0.0) or 0.0),
            }
        )

    if not filtered:
        filtered = [
            {
                "title": str(row.get("title", "") or "").strip(),
                "url": str(row.get("url", "") or "").strip(),
                "content": str(row.get("content", "") or "").strip(),
                "published_at": str(row.get("published_at", "") or "").strip(),
                "source_type": str(row.get("source_type", "") or "").strip(),
                "score": float(row.get("score", 0.0) or 0.0),
            }
            for row in results
            if isinstance(row, dict)
        ]

    for row in filtered:
        row["_haystack"] = f"{row.get('title','')} {row.get('content','')}".lower()
        row["is_material"] = _is_material_source(row, material_tokens)

    conditions = _extract_conditions(structured.get("thesis_map") or {})
    condition_statuses = [_condition_status(cond, filtered) for cond in conditions]
    thesis_progress = {
        "confirmed_conditions": int(sum(1 for s in condition_statuses if s == "confirmed")),
        "at_risk_conditions": int(sum(1 for s in condition_statuses if s == "at_risk")),
        "unknown_conditions": int(sum(1 for s in condition_statuses if s == "unknown")),
    }

    milestones = _extract_milestones(structured)
    milestone_statuses = [_timeline_status(m, filtered) for m in milestones]
    timeline_progress = {
        "milestones_on_track": int(sum(1 for s in milestone_statuses if s == "on_track")),
        "milestones_at_risk": int(sum(1 for s in milestone_statuses if s == "at_risk")),
        "milestones_achieved": int(sum(1 for s in milestone_statuses if s == "achieved")),
    }

    conflicts: List[Dict[str, Any]] = []
    for row in filtered:
        hay = row.get("_haystack", "")
        if _direction_for_text(hay) != "negative":
            continue
        if any(token in hay for token in HIGH_IMPACT_TOKENS):
            conflicts.append(
                {
                    "summary": row.get("title") or "High-impact conflict",
                    "source_url": row.get("url") or "",
                    "source_date": row.get("published_at") or "",
                }
            )
    high_impact_conflicts = len(conflicts)

    freshness_status, rerun_recommendation = _compute_status_and_action(age_days, high_impact_conflicts)
    checked_at = _to_utc_iso(_now_utc())
    material_sources = [row for row in filtered if row.get("is_material")]
    top_changes = _make_top_changes(material_sources or filtered, max_items=3)

    payload = {
        "status": "ok",
        "run_id": run_id,
        "checked_at_utc": checked_at,
        "baseline_as_of_utc": _to_utc_iso(baseline_dt),
        "baseline_source": baseline_source,
        "lookback_days": max(1, int(lookback_days)),
        "new_sources_count": len(filtered),
        "material_sources_count": len(material_sources),
        "thesis_progress": thesis_progress,
        "timeline_progress": timeline_progress,
        "high_impact_conflicts": conflicts,
        "freshness_status": freshness_status,
        "rerun_recommendation": rerun_recommendation,
        "top_changes": top_changes,
        "provider": str(retrieval.get("provider") or ""),
        "provider_fallback": str(retrieval.get("provider_fallback") or ""),
        "query": query,
        "sources_used": [
            {
                "title": row.get("title") or "",
                "url": row.get("url") or "",
                "published_at": row.get("published_at") or "",
                "is_material": bool(row.get("is_material")),
            }
            for row in filtered
        ],
    }
    return _save_delta_artifact(run_id, payload)
