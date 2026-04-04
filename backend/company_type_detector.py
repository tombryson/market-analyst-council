"""Small API-driven company-type detector for template auto-selection."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from .config import (
    COMPANY_TYPE_DETECTION_MAX_OUTPUT_TOKENS,
    COMPANY_TYPE_DETECTION_MAX_RESULTS,
    COMPANY_TYPE_DETECTION_MIN_CONFIDENCE,
    COMPANY_TYPE_DETECTION_PERPLEXITY_MODEL,
    COMPANY_TYPE_DETECTION_PROVIDER,
    COMPANY_TYPE_DETECTION_TIMEOUT_SECONDS,
    ENABLE_COMPANY_TYPE_API_DETECTION,
    PERPLEXITY_API_KEY,
    PERPLEXITY_API_URL,
    TAVILY_API_KEY,
)
from .template_loader import PREALLOCATED_COMPANY_TYPES, get_template_loader

GENERIC_COMPANY_TYPE_KEYWORDS = {
    "mine",
    "mining",
    "minerals",
    "metals",
    "resource",
    "resources",
    "reserve",
    "reserves",
    "project",
    "projects",
    "production",
    "producer",
    "jorc",
    "aisc",
    "cash cost",
    "commodity",
}

COMMODITY_MINER_TYPES = {
    "gold_miner",
    "silver_miner",
    "uranium_miner",
    "copper_miner",
    "lithium_miner",
    "bauxite_miner",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_allowed_company_types() -> List[Dict[str, str]]:
    loader = get_template_loader()
    items = loader.list_company_types()
    out: List[Dict[str, str]] = []
    for item in items:
        company_type_id = str(item.get("id") or "").strip()
        if not company_type_id:
            continue
        out.append(
            {
                "id": company_type_id,
                "name": str(item.get("name") or company_type_id),
                "description": str(item.get("description") or ""),
            }
        )
    return out


def _score_text_against_company_types(text: str) -> Dict[str, Any]:
    corpus = str(text or "").lower()
    scores: Dict[str, float] = {}
    matched_keywords: Dict[str, List[str]] = {}
    score_details: Dict[str, Dict[str, float]] = {}

    def _keyword_present(keyword: str) -> bool:
        kw = str(keyword or "").strip().lower()
        if not kw:
            return False
        normalized = re.sub(r"\s+", " ", kw)
        if normalized in {"au", "ag", "cu", "li"}:
            return bool(re.search(rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])", corpus))
        pattern = re.escape(normalized).replace(r"\ ", r"\s+")
        return bool(re.search(rf"(?<![a-z0-9]){pattern}(?![a-z0-9])", corpus))

    for entry in PREALLOCATED_COMPANY_TYPES:
        company_type_id = str(entry.get("id") or "").strip()
        if not company_type_id:
            continue
        if company_type_id == "general_equity":
            continue

        score = 0.0
        specific_score = 0.0
        generic_score = 0.0
        matches: List[str] = []
        keywords = list(entry.get("detection_keywords", []) or [])
        keywords.extend(entry.get("aliases", []) or [])
        keywords.append(company_type_id.replace("_", " "))

        seen: set[str] = set()
        for raw in keywords:
            kw = str(raw or "").strip().lower()
            if not kw or kw in seen:
                continue
            seen.add(kw)
            if _keyword_present(kw):
                normalized_kw = re.sub(r"\s+", " ", kw)
                if normalized_kw in GENERIC_COMPANY_TYPE_KEYWORDS:
                    weight = 0.4
                    generic_score += weight
                else:
                    weight = 2.5 if (" " in normalized_kw or len(normalized_kw) > 5) else 1.5
                    specific_score += weight
                score += weight
                matches.append(kw)

        if score > 0:
            scores[company_type_id] = score
            matched_keywords[company_type_id] = matches
            score_details[company_type_id] = {
                "specific": round(specific_score, 3),
                "generic": round(generic_score, 3),
                "total": round(score, 3),
            }

    ranked: List[Tuple[str, float]] = sorted(
        scores.items(),
        key=lambda kv: (
            kv[1],
            score_details.get(kv[0], {}).get("specific", 0.0),
            -score_details.get(kv[0], {}).get("generic", 0.0),
        ),
        reverse=True,
    )
    if not ranked:
        return {
            "selected_company_type": None,
            "confidence": 0.0,
            "scores": {},
            "matched_keywords": {},
            "score_details": {},
            "ranked": [],
        }

    best_type, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    # Confidence heuristic: score strength + margin from runner-up.
    confidence = min(
        0.98,
        0.35 + (best_score / 14.0) + ((best_score - second_score) / 10.0),
    )
    confidence = max(0.0, confidence)
    return {
        "selected_company_type": best_type,
        "confidence": round(confidence, 3),
        "scores": scores,
        "matched_keywords": matched_keywords,
        "score_details": score_details,
        "ranked": [
            {
                "company_type": company_type,
                "score": round(score, 3),
                "specific_score": score_details.get(company_type, {}).get("specific", 0.0),
                "generic_score": score_details.get(company_type, {}).get("generic", 0.0),
            }
            for company_type, score in ranked
        ],
    }


def _extract_perplexity_text(payload: Dict[str, Any]) -> str:
    parts: List[str] = []
    output = payload.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
            content = item.get("content")
            if isinstance(content, list):
                for chunk in content:
                    if isinstance(chunk, dict):
                        t = chunk.get("text")
                        if isinstance(t, str) and t.strip():
                            parts.append(t.strip())
            elif isinstance(content, dict):
                t = content.get("text")
                if isinstance(t, str) and t.strip():
                    parts.append(t.strip())
            elif isinstance(content, str) and content.strip():
                parts.append(content.strip())
    if not parts:
        output_text = payload.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            parts.append(output_text.strip())
        elif isinstance(output_text, list):
            for item in output_text:
                if isinstance(item, str) and item.strip():
                    parts.append(item.strip())
    if not parts:
        choices = payload.get("choices")
        if isinstance(choices, list) and choices:
            message = (choices[0] or {}).get("message") or {}
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                parts.append(content.strip())
    return "\n".join(parts).strip()


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return None
    for candidate in [raw]:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    # Fallback: fenced json block or first {...} object.
    fence = re.search(r"```json\s*(\{.*?\})\s*```", raw, flags=re.IGNORECASE | re.DOTALL)
    if fence:
        try:
            parsed = json.loads(fence.group(1))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    brace = re.search(r"(\{.*\})", raw, flags=re.DOTALL)
    if brace:
        try:
            parsed = json.loads(brace.group(1))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _build_context_line(
    *,
    user_query: str,
    ticker: Optional[str],
    company_name: Optional[str],
    exchange: Optional[str],
) -> str:
    return (
        f"query={str(user_query or '').strip()}\n"
        f"ticker={str(ticker or '').strip()}\n"
        f"company_name={str(company_name or '').strip()}\n"
        f"exchange={str(exchange or '').strip()}"
    ).strip()


async def _detect_via_tavily(
    *,
    user_query: str,
    ticker: Optional[str],
    company_name: Optional[str],
    exchange: Optional[str],
) -> Dict[str, Any]:
    if not TAVILY_API_KEY:
        return {
            "status": "error",
            "provider": "tavily",
            "error": "missing_tavily_api_key",
        }

    query_bits = [
        str(company_name or "").strip(),
        str(ticker or "").strip(),
        str(exchange or "").strip().upper(),
        str(user_query or "").strip(),
        "company profile sector industry primary commodity business description",
    ]
    query = " ".join([part for part in query_bits if part]).strip()
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",
        "max_results": max(1, int(COMPANY_TYPE_DETECTION_MAX_RESULTS)),
        "include_answer": False,
        "include_raw_content": False,
    }

    async with httpx.AsyncClient(timeout=max(5.0, float(COMPANY_TYPE_DETECTION_TIMEOUT_SECONDS))) as client:
        response = await client.post(
            "https://api.tavily.com/search",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        if response.status_code != 200:
            return {
                "status": "error",
                "provider": "tavily",
                "error": f"tavily_http_{response.status_code}",
            }
        data = response.json()

    results = data.get("results") if isinstance(data, dict) else []
    rows: List[Dict[str, str]] = []
    combined_parts: List[str] = []
    if isinstance(results, list):
        for item in results:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            url = str(item.get("url") or "").strip()
            content = str(item.get("content") or "").strip()
            if title or content:
                combined_parts.append(f"{title}\n{content}")
            rows.append({"title": title, "url": url})

    scored = _score_text_against_company_types("\n".join(combined_parts))
    selected = scored.get("selected_company_type")
    confidence = float(scored.get("confidence") or 0.0)
    ranked = scored.get("ranked") or []
    ambiguous_top_match = False
    if len(ranked) >= 2:
        top = ranked[0] or {}
        runner_up = ranked[1] or {}
        top_type = str(top.get("company_type") or "").strip()
        runner_type = str(runner_up.get("company_type") or "").strip()
        top_score = float(top.get("score") or 0.0)
        runner_score = float(runner_up.get("score") or 0.0)
        if (
            top_type in COMMODITY_MINER_TYPES
            and runner_type in COMMODITY_MINER_TYPES
            and (top_score - runner_score) < 1.0
        ):
            ambiguous_top_match = True
    applied = bool(selected and confidence >= float(COMPANY_TYPE_DETECTION_MIN_CONFIDENCE))
    if ambiguous_top_match:
        applied = False
    status = "ok" if applied else "low_confidence"
    return {
        "status": status,
        "provider": "tavily",
        "selected_company_type": selected if applied else None,
        "candidate_company_type": selected,
        "confidence": confidence,
        "minimum_confidence": float(COMPANY_TYPE_DETECTION_MIN_CONFIDENCE),
        "applied": applied,
        "scores": scored.get("scores", {}),
        "matched_keywords": scored.get("matched_keywords", {}),
        "score_details": scored.get("score_details", {}),
        "ranked": ranked,
        "ambiguous_top_match": ambiguous_top_match,
        "sources": rows[: max(1, int(COMPANY_TYPE_DETECTION_MAX_RESULTS))],
    }


async def _detect_via_perplexity(
    *,
    user_query: str,
    ticker: Optional[str],
    company_name: Optional[str],
    exchange: Optional[str],
) -> Dict[str, Any]:
    if not PERPLEXITY_API_KEY:
        return {
            "status": "error",
            "provider": "perplexity",
            "error": "missing_perplexity_api_key",
        }

    allowed = _build_allowed_company_types()
    allowed_ids = [item["id"] for item in allowed]
    allowed_lines = "\n".join(
        [f"- {item['id']}: {item['description']}" for item in allowed]
    )
    context_line = _build_context_line(
        user_query=user_query,
        ticker=ticker,
        company_name=company_name,
        exchange=exchange,
    )
    prompt = (
        "Classify the listed company into exactly one company_type_id.\n"
        "Use web search briefly and return JSON only.\n\n"
        f"Allowed company_type_id values:\n{allowed_lines}\n\n"
        f"Context:\n{context_line}\n\n"
        "Return strict JSON:\n"
        "{\n"
        '  "company_type_id": "<one allowed id>",\n'
        '  "confidence": <0.0-1.0>,\n'
        '  "reason": "<short reason>",\n'
        '  "evidence": ["<source or fact>", "<source or fact>"]\n'
        "}"
    )
    payload: Dict[str, Any] = {
        "model": COMPANY_TYPE_DETECTION_PERPLEXITY_MODEL or "sonar-pro",
        "input": prompt,
        "max_steps": 2,
        "max_output_tokens": max(64, int(COMPANY_TYPE_DETECTION_MAX_OUTPUT_TOKENS)),
        "tools": [
            {
                "type": "web_search",
                "max_results_per_query": min(
                    max(1, int(COMPANY_TYPE_DETECTION_MAX_RESULTS)),
                    8,
                ),
                "max_tokens_per_page": 512,
            }
        ],
    }

    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=max(5.0, float(COMPANY_TYPE_DETECTION_TIMEOUT_SECONDS))) as client:
        response = await client.post(PERPLEXITY_API_URL, headers=headers, json=payload)
        if response.status_code != 200:
            return {
                "status": "error",
                "provider": "perplexity",
                "error": f"perplexity_http_{response.status_code}",
            }
        data = response.json()

    raw_text = _extract_perplexity_text(data)
    parsed = _extract_json_object(raw_text) or {}
    selected = str(parsed.get("company_type_id") or "").strip()
    if selected not in allowed_ids:
        return {
            "status": "error",
            "provider": "perplexity",
            "error": "invalid_company_type_id_from_model",
            "raw_text": raw_text[:500],
        }
    confidence = float(parsed.get("confidence") or 0.0)
    applied = bool(confidence >= float(COMPANY_TYPE_DETECTION_MIN_CONFIDENCE))
    evidence = parsed.get("evidence")
    return {
        "status": "ok" if applied else "low_confidence",
        "provider": "perplexity",
        "selected_company_type": selected if applied else None,
        "candidate_company_type": selected,
        "confidence": confidence,
        "minimum_confidence": float(COMPANY_TYPE_DETECTION_MIN_CONFIDENCE),
        "applied": applied,
        "reason": str(parsed.get("reason") or "").strip(),
        "evidence": evidence if isinstance(evidence, list) else [],
        "raw_text": raw_text[:800],
    }


async def detect_company_type_via_api(
    *,
    user_query: str,
    ticker: Optional[str] = None,
    company_name: Optional[str] = None,
    exchange: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Lightweight external lookup to classify company type for template routing.
    """
    provider = str(COMPANY_TYPE_DETECTION_PROVIDER or "tavily").strip().lower()
    base: Dict[str, Any] = {
        "requested_at": _now_iso(),
        "enabled": bool(ENABLE_COMPANY_TYPE_API_DETECTION),
        "provider": provider,
        "selected_company_type": None,
        "applied": False,
    }
    if not ENABLE_COMPANY_TYPE_API_DETECTION:
        base.update({"status": "disabled"})
        return base

    loader = get_template_loader()
    assigned_type = loader.detect_company_type(
        user_query=user_query,
        ticker=ticker,
        minimum_score=10**9,
    )
    if assigned_type:
        base.update(
            {
                "status": "assigned",
                "provider": "assignment",
                "selected_company_type": assigned_type,
                "candidate_company_type": assigned_type,
                "confidence": 1.0,
                "minimum_confidence": float(COMPANY_TYPE_DETECTION_MIN_CONFIDENCE),
                "applied": True,
            }
        )
        return base

    if provider == "perplexity":
        result = await _detect_via_perplexity(
            user_query=user_query,
            ticker=ticker,
            company_name=company_name,
            exchange=exchange,
        )
    else:
        result = await _detect_via_tavily(
            user_query=user_query,
            ticker=ticker,
            company_name=company_name,
            exchange=exchange,
        )

    base.update(result)
    return base
