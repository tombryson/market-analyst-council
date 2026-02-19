#!/usr/bin/env python3
"""
Worker summarizer for PDF dump announcements.

Reads markdown files produced by test_perplexity_pdf_dump.py, applies a rubric
with a lightweight model, and emits structured JSON summaries.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from dotenv import load_dotenv

from backend.config import OPENROUTER_API_KEY, OPENROUTER_API_URL
from backend.openrouter import query_model

try:
    import pymupdf  # type: ignore

    PYMUPDF_AVAILABLE = True
    PYMUPDF_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover
    PYMUPDF_AVAILABLE = False
    PYMUPDF_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

DEFAULT_WORKER_MODEL = "openai/gpt-4o-mini"
OUTPUT_MIN_IMPORTANCE_SCORE = 80
OUTPUT_INCLUDE_NUMERIC_FACTS = False


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize pdf_dump announcements with rubric")
    parser.add_argument(
        "--dump-dir",
        required=True,
        help="Directory containing pdf dump markdown files",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_WORKER_MODEL,
        help=f"Worker model (default: {DEFAULT_WORKER_MODEL})",
    )
    parser.add_argument(
        "--max-key-points",
        type=int,
        default=30,
        help="Maximum key points per kept document",
    )
    parser.add_argument(
        "--max-doc-chars",
        type=int,
        default=120000,
        help="Maximum decoded-text chars sent to model per doc",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=180.0,
        help="OpenRouter request timeout per document",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=2,
        help="Parallel worker calls",
    )
    parser.add_argument(
        "--max-docs",
        type=int,
        default=0,
        help="Optional cap for number of docs to process (0=all)",
    )
    parser.add_argument(
        "--output-markdown",
        default="",
        help="Optional output markdown path (default: <dump-dir>/announcement_summaries.md)",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional output json path (default: <dump-dir>/announcement_summaries.json)",
    )
    parser.add_argument(
        "--worker-enable-vision",
        action="store_true",
        help="Enable hybrid vision extraction before text summarization (default: enabled)",
    )
    parser.add_argument(
        "--worker-disable-vision",
        dest="worker_enable_vision",
        action="store_false",
        help="Disable hybrid vision extraction and run text-only worker",
    )
    parser.add_argument(
        "--vision-model",
        default=DEFAULT_WORKER_MODEL,
        help=f"Vision extraction model (default: {DEFAULT_WORKER_MODEL})",
    )
    parser.add_argument(
        "--vision-max-pages",
        type=int,
        default=50,
        help="Max visual pages per document (0 = all pages, default=50 soft cap)",
    )
    parser.add_argument(
        "--vision-page-batch-size",
        type=int,
        default=4,
        help="Number of PDF pages to send per vision call",
    )
    parser.add_argument(
        "--vision-max-page-facts",
        type=int,
        default=12,
        help="Max key facts extracted per page in vision stage",
    )
    parser.add_argument(
        "--vision-zoom",
        type=float,
        default=1.8,
        help="PDF render zoom for page images",
    )
    parser.add_argument(
        "--vision-timeout-seconds",
        type=float,
        default=180.0,
        help="Timeout for each vision extraction call",
    )
    parser.add_argument(
        "--vision-max-tokens",
        type=int,
        default=1200,
        help="Completion token cap for each vision extraction call",
    )
    parser.set_defaults(worker_enable_vision=True)
    return parser.parse_args()


def _extract_fenced_json(text: str) -> Optional[Dict[str, Any]]:
    payload = str(text or "").strip()
    if not payload:
        return None
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    fence = re.search(r"```json\s*(\{.*?\})\s*```", payload, flags=re.DOTALL | re.IGNORECASE)
    if fence:
        try:
            obj = json.loads(fence.group(1))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

    first = payload.find("{")
    last = payload.rfind("}")
    if first >= 0 and last > first:
        candidate = payload[first : last + 1]
        try:
            obj = json.loads(candidate)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return None
    return None


async def _query_openrouter_multimodal(
    *,
    model: str,
    content: List[Dict[str, Any]],
    timeout_seconds: float,
    max_tokens: int,
) -> Optional[str]:
    if not OPENROUTER_API_KEY:
        return None
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {
        "model": str(model),
        "messages": [
            {
                "role": "user",
                "content": content,
            }
        ],
        "max_tokens": int(max_tokens),
    }
    timeout = httpx.Timeout(float(timeout_seconds), connect=30.0, read=float(timeout_seconds), write=30.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(OPENROUTER_API_URL, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        return f"vision_http_error:{type(exc).__name__}:{exc}"

    choices = data.get("choices") or []
    if not choices:
        return ""
    message = (choices[0] or {}).get("message") or {}
    raw = message.get("content")
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        parts: List[str] = []
        for item in raw:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str) and text:
                    parts.append(text)
        return "\n".join(parts).strip()
    if raw is None:
        return ""
    return str(raw)


def _build_vision_batch_prompt(
    *,
    title: str,
    doc_id: str,
    page_numbers: List[int],
    max_page_facts: int,
) -> str:
    return (
        "You are extracting investment-relevant facts from PDF slide/page images.\n"
        "Return STRICT JSON only.\n\n"
        "Schema:\n"
        "{\n"
        '  "doc_id": "",\n'
        '  "page_batch": [1,2],\n'
        '  "pages": [\n'
        "    {\n"
        '      "page_number": 1,\n'
        '      "is_investment_relevant": true,\n'
        '      "key_facts": ["..."],\n'
        '      "numeric_facts": [{"metric":"","value":"","unit":"","context":"","confidence":0.0}],\n'
        '      "timeline_facts": [{"milestone":"","target_window":"","direction":"new|reconfirmed|delayed|accelerated|unclear","confidence":0.0}],\n'
        '      "capital_structure_facts": ["..."],\n'
        '      "risks_or_caveats": ["..."],\n'
        '      "confidence": 0.0\n'
        "    }\n"
        "  ],\n"
        '  "notes": ["..."]\n'
        "}\n\n"
        "Rules:\n"
        f"- Document title: {title}\n"
        f"- Document id: {doc_id}\n"
        f"- This batch includes pages: {page_numbers}\n"
        f"- Max key_facts per page: {max(1, int(max_page_facts))}\n"
        "- Focus on valuation, production, capex/opex, NPV/IRR/AISC, financing, dilution, milestones, catalysts, and risks.\n"
        "- Ignore decorative text.\n"
        "- If uncertain, lower confidence and note uncertainty.\n"
        "- Do not output markdown, prose, or code fences."
    )


def _render_page_to_data_url(
    *,
    doc: Any,
    page_index: int,
    zoom: float,
) -> str:
    page = doc.load_page(page_index)
    mat = pymupdf.Matrix(float(zoom), float(zoom))
    pix = page.get_pixmap(matrix=mat, alpha=False)
    raw = pix.tobytes("png")
    b64 = base64.b64encode(raw).decode("ascii")
    return f"data:image/png;base64,{b64}"


def _normalize_vision_page_obj(page_obj: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "page_number": int(page_obj.get("page_number", 0) or 0),
        "is_investment_relevant": bool(page_obj.get("is_investment_relevant", False)),
        "key_facts": [],
        "numeric_facts": [],
        "timeline_facts": [],
        "capital_structure_facts": [],
        "risks_or_caveats": [],
        "confidence": 0.0,
    }
    key_facts = page_obj.get("key_facts", [])
    if isinstance(key_facts, list):
        out["key_facts"] = [str(x).strip() for x in key_facts if str(x).strip()]
    numeric = page_obj.get("numeric_facts", [])
    if isinstance(numeric, list):
        clean_numeric: List[Dict[str, Any]] = []
        for row in numeric:
            if not isinstance(row, dict):
                continue
            clean_numeric.append(
                {
                    "metric": str(row.get("metric", "")).strip(),
                    "value": str(row.get("value", "")).strip(),
                    "unit": str(row.get("unit", "")).strip(),
                    "context": str(row.get("context", "")).strip(),
                    "confidence": float(row.get("confidence", 0.0) or 0.0),
                }
            )
        out["numeric_facts"] = clean_numeric
    timeline = page_obj.get("timeline_facts", [])
    if isinstance(timeline, list):
        clean_timeline: List[Dict[str, Any]] = []
        valid = {"new", "reconfirmed", "delayed", "accelerated", "unclear"}
        for row in timeline:
            if not isinstance(row, dict):
                continue
            direction = str(row.get("direction", "unclear")).strip().lower()
            if direction not in valid:
                direction = "unclear"
            clean_timeline.append(
                {
                    "milestone": str(row.get("milestone", "")).strip(),
                    "target_window": str(row.get("target_window", "")).strip(),
                    "direction": direction,
                    "confidence": float(row.get("confidence", 0.0) or 0.0),
                }
            )
        out["timeline_facts"] = clean_timeline
    cap_facts = page_obj.get("capital_structure_facts", [])
    if isinstance(cap_facts, list):
        out["capital_structure_facts"] = [str(x).strip() for x in cap_facts if str(x).strip()]
    risks = page_obj.get("risks_or_caveats", [])
    if isinstance(risks, list):
        out["risks_or_caveats"] = [str(x).strip() for x in risks if str(x).strip()]
    out["confidence"] = max(0.0, min(1.0, float(page_obj.get("confidence", 0.0) or 0.0)))
    return out


async def _extract_vision_bundle(
    *,
    doc: Dict[str, Any],
    model: str,
    max_pages: int,
    batch_size: int,
    max_page_facts: int,
    zoom: float,
    timeout_seconds: float,
    max_tokens: int,
) -> Dict[str, Any]:
    if not PYMUPDF_AVAILABLE:
        return {
            "enabled": False,
            "status": "skipped",
            "reason": f"pymupdf_unavailable:{PYMUPDF_IMPORT_ERROR}",
            "total_pages": 0,
            "pages_processed": 0,
            "page_cap": int(max_pages),
            "relevant_pages": 0,
            "aggregated": {},
        }
    pdf_url = str(doc.get("pdf_url", "")).strip()
    if not pdf_url:
        return {
            "enabled": False,
            "status": "skipped",
            "reason": "missing_pdf_url",
            "total_pages": 0,
            "pages_processed": 0,
            "page_cap": int(max_pages),
            "relevant_pages": 0,
            "aggregated": {},
        }

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        )
    }
    try:
        timeout = httpx.Timeout(60.0, connect=30.0, read=60.0, write=30.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
            resp = await client.get(pdf_url)
            resp.raise_for_status()
            pdf_bytes = bytes(resp.content)
    except Exception as exc:
        return {
            "enabled": True,
            "status": "failed",
            "reason": f"pdf_download_failed:{type(exc).__name__}:{exc}",
            "total_pages": 0,
            "pages_processed": 0,
            "page_cap": int(max_pages),
            "relevant_pages": 0,
            "aggregated": {},
        }

    try:
        pdf_doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    except Exception as exc:
        return {
            "enabled": True,
            "status": "failed",
            "reason": f"pdf_open_failed:{type(exc).__name__}:{exc}",
            "total_pages": 0,
            "pages_processed": 0,
            "page_cap": int(max_pages),
            "relevant_pages": 0,
            "aggregated": {},
        }

    try:
        total_pages = int(pdf_doc.page_count)
        if total_pages <= 0:
            return {
                "enabled": True,
                "status": "failed",
                "reason": "zero_pages",
                "total_pages": 0,
                "pages_processed": 0,
                "page_cap": int(max_pages),
                "relevant_pages": 0,
                "aggregated": {},
            }
        page_cap = int(max_pages)
        if page_cap <= 0:
            selected_pages = list(range(1, total_pages + 1))
        else:
            selected_pages = list(range(1, min(total_pages, page_cap) + 1))

        batch_n = max(1, int(batch_size))
        pages_out: List[Dict[str, Any]] = []
        notes: List[str] = []
        for start in range(0, len(selected_pages), batch_n):
            batch_pages = selected_pages[start : start + batch_n]
            content: List[Dict[str, Any]] = [
                {
                    "type": "text",
                    "text": _build_vision_batch_prompt(
                        title=str(doc.get("title", "")),
                        doc_id=str(doc.get("file_name", "")),
                        page_numbers=batch_pages,
                        max_page_facts=max_page_facts,
                    ),
                }
            ]
            for page_number in batch_pages:
                data_url = _render_page_to_data_url(
                    doc=pdf_doc,
                    page_index=int(page_number) - 1,
                    zoom=float(zoom),
                )
                content.append({"type": "image_url", "image_url": {"url": data_url}})

            resp_text = await _query_openrouter_multimodal(
                model=str(model),
                content=content,
                timeout_seconds=float(timeout_seconds),
                max_tokens=max(400, int(max_tokens)),
            )
            if str(resp_text or "").startswith("vision_http_error:"):
                notes.append(str(resp_text)[:220])
                continue
            resp_obj = _extract_fenced_json(str(resp_text or ""))
            if not isinstance(resp_obj, dict):
                notes.append(f"batch_{batch_pages[0]}_{batch_pages[-1]}:json_parse_failed")
                continue

            rows = resp_obj.get("pages", [])
            if not isinstance(rows, list):
                notes.append(f"batch_{batch_pages[0]}_{batch_pages[-1]}:missing_pages_array")
                continue
            for row in rows:
                if isinstance(row, dict):
                    page_norm = _normalize_vision_page_obj(row)
                    if page_norm.get("page_number", 0) <= 0:
                        continue
                    pages_out.append(page_norm)

        by_page: Dict[int, Dict[str, Any]] = {}
        for row in pages_out:
            page_number = int(row.get("page_number", 0) or 0)
            if page_number <= 0:
                continue
            if page_number not in by_page:
                by_page[page_number] = row
                continue
            # Merge duplicates by extending unique facts.
            cur = by_page[page_number]
            for key in ("key_facts", "capital_structure_facts", "risks_or_caveats"):
                combined = list(cur.get(key, []) or [])
                for item in row.get(key, []) or []:
                    if item not in combined:
                        combined.append(item)
                cur[key] = combined
            for key in ("numeric_facts", "timeline_facts"):
                combined = list(cur.get(key, []) or [])
                for item in row.get(key, []) or []:
                    if item not in combined:
                        combined.append(item)
                cur[key] = combined
            cur["is_investment_relevant"] = bool(cur.get("is_investment_relevant", False)) or bool(
                row.get("is_investment_relevant", False)
            )
            cur["confidence"] = max(
                float(cur.get("confidence", 0.0) or 0.0),
                float(row.get("confidence", 0.0) or 0.0),
            )
            by_page[page_number] = cur

        page_rows = [by_page[p] for p in sorted(by_page.keys())]
        key_facts: List[str] = []
        numeric_facts: List[Dict[str, Any]] = []
        timeline_facts: List[Dict[str, Any]] = []
        capital_facts: List[str] = []
        risk_facts: List[str] = []
        relevant_pages = 0
        for row in page_rows:
            if bool(row.get("is_investment_relevant", False)):
                relevant_pages += 1
            for item in row.get("key_facts", []) or []:
                if item not in key_facts:
                    key_facts.append(item)
            for item in row.get("numeric_facts", []) or []:
                if item not in numeric_facts:
                    numeric_facts.append(item)
            for item in row.get("timeline_facts", []) or []:
                if item not in timeline_facts:
                    timeline_facts.append(item)
            for item in row.get("capital_structure_facts", []) or []:
                if item not in capital_facts:
                    capital_facts.append(item)
            for item in row.get("risks_or_caveats", []) or []:
                if item not in risk_facts:
                    risk_facts.append(item)

        return {
            "enabled": True,
            "status": "ok",
            "reason": "",
            "total_pages": int(total_pages),
            "pages_processed": len(selected_pages),
            "page_cap": int(max_pages),
            "page_cap_applied": bool(int(max_pages) > 0 and int(total_pages) > int(max_pages)),
            "relevant_pages": int(relevant_pages),
            "notes": notes,
            "aggregated": {
                "key_facts": key_facts[:120],
                "numeric_facts": numeric_facts[:80],
                "timeline_facts": timeline_facts[:80],
                "capital_structure_facts": capital_facts[:60],
                "risks_or_caveats": risk_facts[:60],
            },
        }
    finally:
        try:
            pdf_doc.close()
        except Exception:
            pass


def _read_dump_markdown(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8", errors="replace")
    lines = raw.splitlines()

    title = ""
    source_url = ""
    pdf_url = ""
    domain = ""
    published_at = ""
    page_count = 0
    decoded_chars = 0

    for line in lines[:80]:
        stripped = line.strip()
        if stripped.startswith("# PDF Dump:"):
            title = stripped.replace("# PDF Dump:", "", 1).strip()
        elif stripped.startswith("- source_url:"):
            source_url = stripped.replace("- source_url:", "", 1).strip()
        elif stripped.startswith("- pdf_url:"):
            pdf_url = stripped.replace("- pdf_url:", "", 1).strip()
        elif stripped.startswith("- domain:"):
            domain = stripped.replace("- domain:", "", 1).strip()
        elif stripped.startswith("- published_at:"):
            published_at = stripped.replace("- published_at:", "", 1).strip()
        elif stripped.startswith("- page_count:"):
            try:
                page_count = int(stripped.replace("- page_count:", "", 1).strip())
            except Exception:
                page_count = 0
        elif stripped.startswith("- decoded_chars:"):
            try:
                decoded_chars = int(stripped.replace("- decoded_chars:", "", 1).strip())
            except Exception:
                decoded_chars = 0

    marker = "## Full Decoded Text"
    full_text = raw
    if marker in raw:
        after = raw.split(marker, 1)[1]
        full_text = after.strip()

    return {
        "file": str(path),
        "file_name": path.name,
        "title": title or path.stem,
        "source_url": source_url,
        "pdf_url": pdf_url,
        "domain": domain,
        "published_at": published_at,
        "page_count": page_count,
        "decoded_chars": decoded_chars,
        "full_text": full_text.strip(),
    }


def _build_worker_prompt(
    doc: Dict[str, Any],
    max_key_points: int,
    max_doc_chars: int,
    vision_bundle: Optional[Dict[str, Any]] = None,
) -> str:
    text = str(doc.get("full_text", ""))
    truncated = False
    if len(text) > max_doc_chars:
        text = text[:max_doc_chars]
        truncated = True

    rubric = f"""
You are a strict financial-announcement triage worker.
You are operating in HYBRID mode: use full decoded text plus visual fact pack when available.

Task:
1) Determine if the document is price-sensitive or important for investment analysis.
2) If NOT important, stop early and return minimal JSON classification only.
3) If important, return a compact structured summary with at most {max_key_points} key points.

Materiality/importance checks:
- Funding and capital structure: debt facilities, equity raises, dilution, covenant changes, liquidity changes
- Project economics: NPV, IRR, AISC, capex, opex, production profile, resource/reserve updates
- Development/timeline: first gold, commissioning, key milestones, delays/accelerations
- Regulatory/permitting/legal outcomes that alter project risk or timeline
- Management/strategy changes with likely execution impact
- Quantitative sensitivities (gold price, FX, cost assumptions)
- Guidance changes, major contracts, offtake, M&A, asset sales/acquisitions

Low-signal examples (usually NOT important by themselves):
- boilerplate legal notices, generic cleansing/quotation notices, procedural admin with no valuation impact.

Return STRICT JSON only with this schema:
{{
  "doc_id": "<file name>",
  "price_sensitive": {{
    "is_price_sensitive": true/false,
    "confidence": 0.0-1.0,
    "reason": "<short reason>"
  }},
  "importance": {{
    "is_important": true/false,
    "importance_score": 0-100,
    "tier": "critical|high|medium|low|ignore",
    "keep_for_injection": true/false,
    "reason": "<short reason>"
  }},
  "summary": {{
    "one_line": "<single line summary>",
    "key_points": ["... up to {max_key_points} ..."],
    "numeric_facts": [
      {{"metric":"", "value":"", "unit":"", "context":"", "source_snippet":""}}
    ],
    "timeline_milestones": [
      {{"milestone":"", "target_window":"", "direction":"new|reconfirmed|delayed|accelerated|unclear", "source_snippet":""}}
    ],
    "capital_structure": ["..."],
    "catalysts_next_12m": ["..."],
    "risks_headwinds": ["..."],
    "market_impact_assessment": "<2-4 lines>"
  }},
  "extraction_quality": {{
    "text_truncated_for_model": true/false,
    "signal_quality": "high|medium|low",
    "notes": ["..."]
  }}
}}

Rules:
- If importance.is_important=false, set keep_for_injection=false and keep summary fields empty/minimal.
- Never exceed {max_key_points} key points.
- Prefer concrete numeric/timeline facts and avoid fluff.
- Do not output markdown, code fences, or prose outside JSON.
""".strip()

    vision_status = dict(vision_bundle or {})
    aggregated = dict(vision_status.get("aggregated", {}) or {})
    vision_prompt_pack = {
        "status": str(vision_status.get("status", "disabled")),
        "reason": str(vision_status.get("reason", "")),
        "total_pages": int(vision_status.get("total_pages", 0) or 0),
        "pages_processed": int(vision_status.get("pages_processed", 0) or 0),
        "page_cap": int(vision_status.get("page_cap", 0) or 0),
        "page_cap_applied": bool(vision_status.get("page_cap_applied", False)),
        "relevant_pages": int(vision_status.get("relevant_pages", 0) or 0),
        "notes": list(vision_status.get("notes", []) or [])[:12],
        "aggregated": {
            "key_facts": list(aggregated.get("key_facts", []) or [])[:120],
            "numeric_facts": list(aggregated.get("numeric_facts", []) or [])[:80],
            "timeline_facts": list(aggregated.get("timeline_facts", []) or [])[:80],
            "capital_structure_facts": list(aggregated.get("capital_structure_facts", []) or [])[:60],
            "risks_or_caveats": list(aggregated.get("risks_or_caveats", []) or [])[:60],
        },
    }

    metadata = {
        "doc_id": doc.get("file_name", ""),
        "title": doc.get("title", ""),
        "source_url": doc.get("source_url", ""),
        "pdf_url": doc.get("pdf_url", ""),
        "domain": doc.get("domain", ""),
        "published_at": doc.get("published_at", ""),
        "decoded_chars_in_file": int(doc.get("decoded_chars", 0) or 0),
        "text_chars_sent_to_model": len(text),
        "text_truncated_for_model": truncated,
    }

    return (
        f"{rubric}\n\n"
        f"DOCUMENT METADATA:\n{json.dumps(metadata, ensure_ascii=True)}\n\n"
        f"VISION FACT PACK:\n{json.dumps(vision_prompt_pack, ensure_ascii=True)}\n\n"
        f"DOCUMENT TEXT:\n{text}"
    )


def _normalize_summary_object(
    obj: Dict[str, Any],
    doc: Dict[str, Any],
    max_key_points: int,
    text_truncated: bool,
    vision_bundle: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = dict(obj or {})
    out["doc_id"] = str(out.get("doc_id") or doc.get("file_name", ""))

    ps = out.get("price_sensitive", {})
    if not isinstance(ps, dict):
        ps = {}
    ps_out = {
        "is_price_sensitive": bool(ps.get("is_price_sensitive", False)),
        "confidence": float(ps.get("confidence", 0.0) or 0.0),
        "reason": str(ps.get("reason", "")).strip(),
    }
    ps_out["confidence"] = max(0.0, min(1.0, ps_out["confidence"]))
    out["price_sensitive"] = ps_out

    imp = out.get("importance", {})
    if not isinstance(imp, dict):
        imp = {}
    tier = str(imp.get("tier", "ignore")).strip().lower()
    if tier not in {"critical", "high", "medium", "low", "ignore"}:
        tier = "ignore"
    importance_score = int(imp.get("importance_score", 0) or 0)
    importance_score = max(0, min(100, importance_score))
    is_important = bool(imp.get("is_important", False))
    keep_requested = bool(imp.get("keep_for_injection", is_important))
    keep = bool(keep_requested and importance_score >= int(OUTPUT_MIN_IMPORTANCE_SCORE))
    reason_text = str(imp.get("reason", "")).strip()
    if keep_requested and not keep and importance_score < int(OUTPUT_MIN_IMPORTANCE_SCORE):
        reason_text = (reason_text + "; below_injection_threshold_80").strip("; ")
    out["importance"] = {
        "is_important": is_important,
        "importance_score": importance_score,
        "tier": tier,
        "keep_for_injection": keep,
        "reason": reason_text,
    }

    summary = out.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    key_points = summary.get("key_points", [])
    if not isinstance(key_points, list):
        key_points = []
    key_points = [str(item).strip() for item in key_points if str(item).strip()][:max_key_points]

    def _normalize_list(key: str) -> List[str]:
        val = summary.get(key, [])
        if not isinstance(val, list):
            return []
        return [str(item).strip() for item in val if str(item).strip()]

    numeric_facts = summary.get("numeric_facts", [])
    if not isinstance(numeric_facts, list):
        numeric_facts = []
    numeric_out: List[Dict[str, str]] = []
    for row in numeric_facts[:20]:
        if not isinstance(row, dict):
            continue
        numeric_out.append(
            {
                "metric": str(row.get("metric", "")).strip(),
                "value": str(row.get("value", "")).strip(),
                "unit": str(row.get("unit", "")).strip(),
                "context": str(row.get("context", "")).strip(),
                "source_snippet": str(row.get("source_snippet", "")).strip(),
            }
        )

    timeline_rows = summary.get("timeline_milestones", [])
    if not isinstance(timeline_rows, list):
        timeline_rows = []
    timeline_out: List[Dict[str, str]] = []
    valid_directions = {"new", "reconfirmed", "delayed", "accelerated", "unclear"}
    for row in timeline_rows[:20]:
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction", "unclear")).strip().lower()
        if direction not in valid_directions:
            direction = "unclear"
        timeline_out.append(
            {
                "milestone": str(row.get("milestone", "")).strip(),
                "target_window": str(row.get("target_window", "")).strip(),
                "direction": direction,
                "source_snippet": str(row.get("source_snippet", "")).strip(),
            }
        )

    if keep:
        out["summary"] = {
            "one_line": str(summary.get("one_line", "")).strip(),
            "key_points": key_points,
            "numeric_facts": numeric_out if bool(OUTPUT_INCLUDE_NUMERIC_FACTS) else [],
            "timeline_milestones": timeline_out,
            "capital_structure": _normalize_list("capital_structure"),
            "catalysts_next_12m": _normalize_list("catalysts_next_12m"),
            "risks_headwinds": _normalize_list("risks_headwinds"),
            "market_impact_assessment": str(summary.get("market_impact_assessment", "")).strip(),
        }
    else:
        out["summary"] = {
            "one_line": "",
            "key_points": [],
            "numeric_facts": [],
            "timeline_milestones": [],
            "capital_structure": [],
            "catalysts_next_12m": [],
            "risks_headwinds": [],
            "market_impact_assessment": "",
        }

    quality = out.get("extraction_quality", {})
    if not isinstance(quality, dict):
        quality = {}
    signal_quality = str(quality.get("signal_quality", "low")).strip().lower()
    if signal_quality not in {"high", "medium", "low"}:
        signal_quality = "low"
    notes = quality.get("notes", [])
    if not isinstance(notes, list):
        notes = []
    out["extraction_quality"] = {
        "text_truncated_for_model": bool(quality.get("text_truncated_for_model", text_truncated)),
        "signal_quality": signal_quality,
        "notes": [str(item).strip() for item in notes if str(item).strip()],
    }

    out["source_meta"] = {
        "file_name": str(doc.get("file_name", "")),
        "file": str(doc.get("file", "")),
        "title": str(doc.get("title", "")),
        "source_url": str(doc.get("source_url", "")),
        "pdf_url": str(doc.get("pdf_url", "")),
        "domain": str(doc.get("domain", "")),
        "published_at": str(doc.get("published_at", "")),
        "decoded_chars_in_file": int(doc.get("decoded_chars", 0) or 0),
    }
    if isinstance(vision_bundle, dict):
        out["source_meta"]["vision_meta"] = {
            "enabled": bool(vision_bundle.get("enabled", False)),
            "status": str(vision_bundle.get("status", "")),
            "reason": str(vision_bundle.get("reason", "")),
            "total_pages": int(vision_bundle.get("total_pages", 0) or 0),
            "pages_processed": int(vision_bundle.get("pages_processed", 0) or 0),
            "page_cap": int(vision_bundle.get("page_cap", 0) or 0),
            "page_cap_applied": bool(vision_bundle.get("page_cap_applied", False)),
            "relevant_pages": int(vision_bundle.get("relevant_pages", 0) or 0),
        }

    return out


async def _summarize_one(
    *,
    doc: Dict[str, Any],
    model: str,
    max_key_points: int,
    max_doc_chars: int,
    timeout_seconds: float,
    vision_enabled: bool,
    vision_model: str,
    vision_max_pages: int,
    vision_page_batch_size: int,
    vision_max_page_facts: int,
    vision_zoom: float,
    vision_timeout_seconds: float,
    vision_max_tokens: int,
) -> Dict[str, Any]:
    text = str(doc.get("full_text", "") or "")
    vision_bundle: Dict[str, Any] = {
        "enabled": False,
        "status": "disabled",
        "reason": "worker_disable_vision",
        "total_pages": 0,
        "pages_processed": 0,
        "page_cap": int(vision_max_pages),
        "page_cap_applied": False,
        "relevant_pages": 0,
        "notes": [],
        "aggregated": {},
    }
    if bool(vision_enabled):
        vision_bundle = await _extract_vision_bundle(
            doc=doc,
            model=str(vision_model),
            max_pages=int(vision_max_pages),
            batch_size=int(vision_page_batch_size),
            max_page_facts=int(vision_max_page_facts),
            zoom=float(vision_zoom),
            timeout_seconds=float(vision_timeout_seconds),
            max_tokens=int(vision_max_tokens),
        )

    prompt = _build_worker_prompt(
        doc,
        max_key_points=max_key_points,
        max_doc_chars=max_doc_chars,
        vision_bundle=vision_bundle,
    )
    text_truncated = len(text) > max_doc_chars

    errors: List[str] = []
    last_obj: Optional[Dict[str, Any]] = None
    attempts = 3
    for attempt in range(1, attempts + 1):
        response = await query_model(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            timeout=float(timeout_seconds),
            max_tokens=2200,
            reasoning_effort="low",
        )
        if not response or not response.get("content"):
            errors.append(f"attempt_{attempt}:empty_response")
            await asyncio.sleep(min(2.0 * attempt, 6.0))
            continue

        obj = _extract_fenced_json(str(response.get("content", "")))
        if isinstance(obj, dict):
            last_obj = obj
            break

        errors.append(f"attempt_{attempt}:json_parse_failed")
        repair_prompt = (
            "Return STRICT JSON only for the same schema. "
            "No markdown, no prose, no code fences.\n\n"
            f"Previous output:\n{response.get('content', '')}"
        )
        prompt = repair_prompt
        await asyncio.sleep(min(1.0 * attempt, 4.0))

    if last_obj is None:
        fallback = {
            "doc_id": doc.get("file_name", ""),
            "price_sensitive": {
                "is_price_sensitive": False,
                "confidence": 0.0,
                "reason": "worker_failed",
            },
            "importance": {
                "is_important": False,
                "importance_score": 0,
                "tier": "ignore",
                "keep_for_injection": False,
                "reason": "worker_failed",
            },
            "summary": {
                "one_line": "",
                "key_points": [],
                "numeric_facts": [],
                "timeline_milestones": [],
                "capital_structure": [],
                "catalysts_next_12m": [],
                "risks_headwinds": [],
                "market_impact_assessment": "",
            },
            "extraction_quality": {
                "text_truncated_for_model": text_truncated,
                "signal_quality": "low",
                "notes": errors or ["worker_failed"],
            },
        }
        return _normalize_summary_object(
            fallback,
            doc=doc,
            max_key_points=max_key_points,
            text_truncated=text_truncated,
            vision_bundle=vision_bundle,
        )

    normalized = _normalize_summary_object(
        last_obj,
        doc=doc,
        max_key_points=max_key_points,
        text_truncated=text_truncated,
        vision_bundle=vision_bundle,
    )
    if errors:
        notes = normalized["extraction_quality"].get("notes", [])
        normalized["extraction_quality"]["notes"] = [*notes, *errors]
    return normalized


def _render_markdown_report(
    *,
    model: str,
    dump_dir: Path,
    processed: List[Dict[str, Any]],
    kept: List[Dict[str, Any]],
    dropped: List[Dict[str, Any]],
    hybrid_vision: Dict[str, Any],
) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    lines: List[str] = [
        "# Announcement Summaries",
        "",
        f"- generated_at_utc: {generated_at}",
        f"- worker_model: {model}",
        f"- output_min_importance_score: {int(OUTPUT_MIN_IMPORTANCE_SCORE)}",
        f"- output_include_numeric_facts: {bool(OUTPUT_INCLUDE_NUMERIC_FACTS)}",
        f"- hybrid_vision_enabled: {bool(hybrid_vision.get('enabled', False))}",
        f"- hybrid_vision_model: {hybrid_vision.get('vision_model', '')}",
        f"- hybrid_vision_max_pages: {hybrid_vision.get('vision_max_pages', 0)} (0=all; default soft cap=50)",
        f"- dump_dir: {dump_dir}",
        f"- total_processed: {len(processed)}",
        f"- kept_for_injection: {len(kept)}",
        f"- dropped_as_unimportant: {len(dropped)}",
        "",
        "## Kept Documents (JSON Elements)",
        "",
    ]

    for item in kept:
        lines.append(f"### {item.get('source_meta', {}).get('file_name', item.get('doc_id', 'document'))}")
        lines.append("```json")
        lines.append(json.dumps(item, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")

    lines.extend(["## Dropped Documents (Classification Only)", ""])
    for item in dropped:
        compact = {
            "doc_id": item.get("doc_id", ""),
            "price_sensitive": item.get("price_sensitive", {}),
            "importance": item.get("importance", {}),
            "source_meta": item.get("source_meta", {}),
            "extraction_quality": item.get("extraction_quality", {}),
        }
        lines.append(f"### {compact.get('source_meta', {}).get('file_name', compact.get('doc_id', 'document'))}")
        lines.append("```json")
        lines.append(json.dumps(compact, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


async def _run(args: argparse.Namespace) -> int:
    load_dotenv()

    dump_dir = Path(args.dump_dir).resolve()
    if not dump_dir.exists() or not dump_dir.is_dir():
        raise FileNotFoundError(f"Dump directory not found: {dump_dir}")

    md_files = sorted(
        [
            path
            for path in dump_dir.glob("*.md")
            if path.is_file() and path.name != "index.md"
        ]
    )
    if not md_files:
        print(f"No markdown dump files found in: {dump_dir}")
        return 1

    if int(args.max_docs) > 0:
        md_files = md_files[: int(args.max_docs)]

    docs = [_read_dump_markdown(path) for path in md_files]

    sem = asyncio.Semaphore(max(1, int(args.concurrency)))
    results: List[Dict[str, Any]] = []

    async def _worker(doc: Dict[str, Any]) -> Dict[str, Any]:
        async with sem:
            return await _summarize_one(
                doc=doc,
                model=str(args.model),
                max_key_points=max(1, int(args.max_key_points)),
                max_doc_chars=max(5000, int(args.max_doc_chars)),
                timeout_seconds=float(args.timeout_seconds),
                vision_enabled=bool(args.worker_enable_vision),
                vision_model=str(args.vision_model),
                vision_max_pages=int(args.vision_max_pages),
                vision_page_batch_size=max(1, int(args.vision_page_batch_size)),
                vision_max_page_facts=max(1, int(args.vision_max_page_facts)),
                vision_zoom=float(args.vision_zoom),
                vision_timeout_seconds=max(30.0, float(args.vision_timeout_seconds)),
                vision_max_tokens=max(400, int(args.vision_max_tokens)),
            )

    tasks = [_worker(doc) for doc in docs]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        results.append(result)
        meta = result.get("source_meta", {}) or {}
        imp = result.get("importance", {}) or {}
        print(
            f"processed: {meta.get('file_name', result.get('doc_id', 'doc'))} "
            f"keep={imp.get('keep_for_injection', False)} "
            f"score={imp.get('importance_score', 0)} "
            f"tier={imp.get('tier', 'ignore')} "
            f"vision={((meta.get('vision_meta', {}) or {}).get('status', 'n/a'))}"
        )

    # Preserve input order in output.
    order_map = {doc.get("file_name", ""): idx for idx, doc in enumerate(docs)}
    results.sort(key=lambda item: order_map.get((item.get("source_meta", {}) or {}).get("file_name", ""), 10**9))

    kept = [item for item in results if bool((item.get("importance", {}) or {}).get("keep_for_injection", False))]
    dropped = [item for item in results if item not in kept]

    out_md = Path(args.output_markdown).resolve() if args.output_markdown else (dump_dir / "announcement_summaries.md")
    out_json = Path(args.output_json).resolve() if args.output_json else (dump_dir / "announcement_summaries.json")

    report = _render_markdown_report(
        model=str(args.model),
        dump_dir=dump_dir,
        processed=results,
        kept=kept,
        dropped=dropped,
        hybrid_vision={
            "enabled": bool(args.worker_enable_vision),
            "vision_model": str(args.vision_model),
            "vision_max_pages": int(args.vision_max_pages),
        },
    )
    out_md.write_text(report, encoding="utf-8")

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "worker_model": str(args.model),
        "output_policy": {
            "min_importance_score": int(OUTPUT_MIN_IMPORTANCE_SCORE),
            "include_numeric_facts": bool(OUTPUT_INCLUDE_NUMERIC_FACTS),
        },
        "hybrid_vision": {
            "enabled": bool(args.worker_enable_vision),
            "vision_model": str(args.vision_model),
            "vision_max_pages": int(args.vision_max_pages),
            "vision_page_batch_size": int(args.vision_page_batch_size),
            "vision_max_page_facts": int(args.vision_max_page_facts),
            "vision_zoom": float(args.vision_zoom),
            "vision_timeout_seconds": float(args.vision_timeout_seconds),
            "vision_max_tokens": int(args.vision_max_tokens),
        },
        "dump_dir": str(dump_dir),
        "total_processed": len(results),
        "kept_for_injection": len(kept),
        "dropped_as_unimportant": len(dropped),
        "results": results,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Output markdown: {out_md}")
    print(f"Output json: {out_json}")
    print(f"Processed={len(results)} kept={len(kept)} dropped={len(dropped)}")
    return 0


def main() -> None:
    args = _parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
