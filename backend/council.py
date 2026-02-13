"""3-stage LLM Council orchestration."""

import copy
import json
import re
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from time import perf_counter
from urllib.parse import urlparse
from .openrouter import query_models_parallel, query_model
from .config import (
    COUNCIL_MODELS,
    CHAIRMAN_MODEL,
    PERPLEXITY_COUNCIL_MODELS,
    RESEARCH_DEPTH,
    MAX_SOURCES,
    PERPLEXITY_STAGE1_EXECUTION_MODE,
    PERPLEXITY_STAGE1_STAGGER_SECONDS,
    PERPLEXITY_STAGE1_MAX_ATTEMPTS,
    PERPLEXITY_STAGE1_MAX_RETRIES,
    PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS,
    PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED,
    PERPLEXITY_STAGE1_SECOND_PASS_ENABLED,
    PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS,
    PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE,
    PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED,
    PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES,
    PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS,
    PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT,
    PROGRESS_LOGGING,
)


def _progress_log(message: str) -> None:
    """Timestamped progress logs for long-running research orchestration."""
    if not PROGRESS_LOGGING:
        return
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}][council] {message}", flush=True)


def _extract_status_code(error_text: str) -> Optional[int]:
    match = re.search(r"Perplexity API error:\s*(\d+)", error_text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _is_retryable_stage1_error(error_text: str) -> bool:
    text = (error_text or "").lower()
    if not text:
        return False
    if "timed out" in text or "timeout" in text:
        return True
    if "perplexity research failed" in text:
        return True
    status_code = _extract_status_code(error_text)
    if status_code in {408, 409, 425, 429, 500, 502, 503, 504}:
        return True
    return False


def _build_stage1_attempt_profile(
    model: str,
    attempt: int,
    base_max_sources: int,
    base_max_steps: int,
    base_max_output_tokens: int,
    base_reasoning_effort: str,
) -> Dict[str, Any]:
    """
    Build per-attempt request profile.

    For OpenAI-routed Stage 1 calls, retries progressively reduce workload while
    keeping the same analysis prompt/rubric.
    """
    profile: Dict[str, Any] = {
        "name": "default",
        "max_sources": max(1, int(base_max_sources)),
        "max_steps": max(1, int(base_max_steps)),
        "max_output_tokens": max(512, int(base_max_output_tokens)),
        "reasoning_effort": (base_reasoning_effort or "").strip().lower(),
    }

    model_key = (model or "").strip().lower()
    if not model_key.startswith("openai/"):
        return profile

    if PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED:
        max_sources_cap = max(1, int(PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES))
        max_steps_cap = max(1, int(PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS))
        profile["max_sources"] = min(int(profile["max_sources"]), max_sources_cap)
        profile["max_steps"] = min(int(profile["max_steps"]), max_steps_cap)

    if attempt == 1:
        if PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED:
            profile["name"] = "openai_base_guardrail"
            forced_effort = str(PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT or "").strip().lower()
            if forced_effort in {"low", "medium", "high"}:
                profile["reasoning_effort"] = forced_effort
            elif profile["reasoning_effort"] == "high":
                profile["reasoning_effort"] = "medium"
        return profile

    base_effort = (base_reasoning_effort or "").strip().lower()

    if attempt == 2:
        profile["name"] = "openai_retry_2"
        profile["max_sources"] = max(4, int(profile["max_sources"]) - 1)
        profile["max_steps"] = max(2, int(profile["max_steps"]) - 1)
        profile["max_output_tokens"] = max(3072, int(profile["max_output_tokens"] * 0.80))
        # Step down one level first: high -> medium -> low.
        if base_effort == "high":
            profile["reasoning_effort"] = "medium"
        elif base_effort == "medium":
            profile["reasoning_effort"] = "low"
        else:
            profile["reasoning_effort"] = "low"
        return profile

    profile["name"] = "openai_retry_3plus"
    profile["max_sources"] = max(3, int(profile["max_sources"]) - 2)
    profile["max_steps"] = max(1, int(profile["max_steps"]) - 2)
    profile["max_output_tokens"] = max(2048, int(profile["max_output_tokens"] * 0.65))
    profile["reasoning_effort"] = "low"
    return profile


def _extract_synthesis_block(summary_text: str) -> str:
    """Extract the synthesis section from normalized Stage 1 summary text."""
    text = summary_text or ""
    marker = "### Synthesis"
    latest_marker = "### Latest Updates"
    if marker not in text:
        return text.strip()
    tail = text.split(marker, 1)[1]
    if latest_marker in tail:
        tail = tail.split(latest_marker, 1)[0]
    return tail.strip()


def _stage1_requires_template_compliance(user_query: str, research_brief: str) -> bool:
    """Heuristic: enforce compliance when prompt clearly asks for scored/template analysis."""
    joined = f"{user_query or ''}\n{research_brief or ''}".lower()
    triggers = (
        "quality score",
        "value score",
        "out of 100",
        "npv",
        "price target",
        "certainty",
        "headwinds",
        "tailwinds",
        "rubric",
    )
    return any(token in joined for token in triggers)


def _evaluate_stage1_template_compliance(
    summary_text: str,
    user_query: str,
    research_brief: str,
) -> Dict[str, Any]:
    """
    Evaluate whether Stage 1 output is analysis-grade vs a shallow source log.
    """
    requires = _stage1_requires_template_compliance(user_query, research_brief)
    synthesis = _extract_synthesis_block(summary_text)
    synthesis_lower = synthesis.lower()

    primary_markers = [
        "quality score",
        "quality_score",
        "value score",
        "value_score",
        "npv",
        "price target",
        "price_targets",
    ]
    secondary_markers = [
        "certainty",
        "certainty_pct",
        "headwind",
        "tailwind",
        "headwinds_tailwinds",
        "timeline",
        "milestone",
        "development stage",
        "development_timeline",
        "catalyst",
        "next_major_catalysts",
        "sensitivity",
        "sensitivity_analysis",
    ]
    primary_hit_count = sum(1 for marker in primary_markers if marker in synthesis_lower)
    secondary_hit_count = sum(1 for marker in secondary_markers if marker in synthesis_lower)
    hit_count = primary_hit_count + secondary_hit_count
    minimum_chars = 220
    is_substantive = len(synthesis) >= minimum_chars
    compliant = (not requires) or (
        is_substantive
        and (
            (primary_hit_count >= 1 and hit_count >= 2)
            or hit_count >= 3
        )
    )

    reason = "ok"
    if requires and not compliant:
        reason = (
            "non_compliant_template_summary("
            f"chars={len(synthesis)}, "
            f"primary_hits={primary_hit_count}, "
            f"secondary_hits={secondary_hit_count})"
        )

    return {
        "required": requires,
        "compliant": compliant,
        "reason": reason,
        "synthesis_chars": len(synthesis),
        "marker_hits": hit_count,
        "primary_marker_hits": primary_hit_count,
        "secondary_marker_hits": secondary_hit_count,
    }


def _build_strict_research_brief(base_brief: str) -> str:
    """Append strict contract to force full rubric analysis on retry."""
    strict_contract = (
        "STRICT OUTPUT CONTRACT (must follow exactly):\n"
        "- Provide a full investment analysis, not a research log.\n"
        "- Include explicit Quality Score (0-100) and Value Score (0-100).\n"
        "- Include NPV/risked-NPV discussion with assumptions.\n"
        "- Include explicit 12-month and 24-month price targets.\n"
        "- Include certainty percentage for 24-month milestones.\n"
        "- Include key quantitative and qualitative headwinds/tailwinds.\n"
        "- Tie numeric claims to cited source URLs (or mark as ESTIMATE).\n"
        "- If data is missing, state assumptions clearly and proceed.\n"
    )
    combined = (base_brief or "").strip()
    if strict_contract not in combined:
        combined = f"{combined}\n\n{strict_contract}".strip()
    return combined


def _infer_exchange_from_ticker(ticker: str) -> str:
    """Infer exchange from common ticker formats (PREFIX:SYM or suffix)."""
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        return ""
    if ":" in normalized:
        return normalized.split(":", 1)[0]

    suffix_map = {
        ".AX": "ASX",
        ".N": "NYSE",
        ".O": "NASDAQ",
        ".Q": "NASDAQ",
        ".TO": "TSX",
        ".V": "TSXV",
        ".L": "LSE",
    }
    for suffix, exchange in suffix_map.items():
        if normalized.endswith(suffix):
            return exchange
    return ""


def _expected_domains_for_exchange(exchange: str) -> List[str]:
    """Exchange-primary domains for missing-data diagnostics."""
    key = (exchange or "").strip().upper()
    mapping = {
        "ASX": ["asx.com.au", "marketindex.com.au", "wcsecure.weblink.com.au"],
        "NYSE": ["sec.gov"],
        "NASDAQ": ["sec.gov"],
        "TSX": ["sedarplus.ca", "tsx.com"],
        "TSXV": ["sedarplus.ca", "tsx.com"],
        "LSE": ["londonstockexchange.com", "investegate.co.uk"],
        "AIM": ["londonstockexchange.com", "investegate.co.uk"],
    }
    return mapping.get(key, [])


def _has_expected_source_domain(results: List[Dict[str, Any]], expected_domains: List[str]) -> bool:
    expected = [domain.lower() for domain in expected_domains if domain]
    if not expected:
        return True
    for result in results:
        url = str(result.get("url", "")).strip()
        if not url:
            continue
        try:
            host = urlparse(url).netloc.lower()
        except Exception:
            continue
        if any(domain in host for domain in expected):
            return True
    return False


_FACT_PACK_SECTIONS = [
    "market_data",
    "project_economics_npv_inputs",
    "resource_and_reserve",
    "funding_and_balance_sheet",
    "development_timeline_and_milestones",
    "headwinds_and_risks",
    "tailwinds_and_catalysts",
    "management_and_governance",
    "valuation_and_peer_signals",
    "other_material_facts",
]

_FACT_PACK_KEYWORDS = {
    "market_data": [
        "market cap",
        "shares outstanding",
        "share price",
        "enterprise value",
        "ev ",
        "cash",
        "debt",
    ],
    "project_economics_npv_inputs": [
        "npv",
        "irr",
        "aisc",
        "capex",
        "opex",
        "mine life",
        "recovery",
        "royalty",
        "tax",
        "production",
        "oz",
    ],
    "resource_and_reserve": [
        "resource",
        "reserve",
        "jorc",
        "grade",
        "g/t",
        "moz",
        "ore",
    ],
    "funding_and_balance_sheet": [
        "facility",
        "loan",
        "placement",
        "capital raising",
        "financing",
        "liquidity",
        "runway",
        "cash",
    ],
    "development_timeline_and_milestones": [
        "first gold",
        "commission",
        "ramp-up",
        "production",
        "milestone",
        "q1",
        "q2",
        "q3",
        "q4",
        "202",
        "dfs",
        "pfs",
        "feasibility",
    ],
    "headwinds_and_risks": [
        "risk",
        "headwind",
        "delay",
        "dilution",
        "permit",
        "regulatory",
        "inflation",
        "power",
        "labor",
        "execution",
        "geopolitical",
    ],
    "tailwinds_and_catalysts": [
        "tailwind",
        "catalyst",
        "upside",
        "expansion",
        "drilling",
        "resource growth",
        "gold price",
        "strategic",
        "offtake",
    ],
    "management_and_governance": [
        "management",
        "board",
        "director",
        "ceo",
        "governance",
        "track record",
        "insider",
    ],
    "valuation_and_peer_signals": [
        "valuation",
        "ev/oz",
        "peer",
        "undervalued",
        "multiple",
        "discount",
        "premium",
    ],
}


def _prepare_stage1_source_rows(
    run: Dict[str, Any],
    max_sources: int,
    max_chars_per_source: int,
) -> List[Dict[str, Any]]:
    """Normalize top retrieved sources into reusable rows with stable source IDs."""
    safe_max_sources = max(1, int(max_sources))
    safe_max_chars = max(300, int(max_chars_per_source))
    rows: List[Dict[str, Any]] = []

    for source in (run.get("results") or []):
        if len(rows) >= safe_max_sources:
            break

        source_id = f"S{len(rows) + 1}"
        title = str(source.get("title", "Untitled")).strip() or "Untitled"
        url = str(source.get("url", "")).strip()
        published = str(source.get("published_at", "")).strip()
        decode_status = str(source.get("decode_status", "")).strip()
        decoded = bool(decode_status == "decoded" or source.get("decoded_excerpt"))

        excerpt = str(
            source.get("decoded_excerpt")
            or source.get("content")
            or source.get("source_snippet")
            or ""
        ).strip()
        if len(excerpt) > safe_max_chars:
            excerpt = excerpt[: safe_max_chars - 3].rstrip() + "..."

        rows.append(
            {
                "source_id": source_id,
                "title": title,
                "url": url,
                "published_at": published,
                "decode_status": decode_status,
                "decoded": decoded,
                "excerpt": excerpt,
            }
        )

    return rows


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
        if len(sentence) > 220:
            sentence = sentence[:217].rstrip() + "..."
        out.append(sentence)
    return out


def _build_stage1_rubric_fact_pack(
    source_rows: List[Dict[str, Any]],
    max_facts_per_section: int = 4,
) -> Dict[str, Any]:
    """Build rubric-aligned fact pack from decoded source rows."""
    sections: Dict[str, List[Dict[str, str]]] = {key: [] for key in _FACT_PACK_SECTIONS}
    seen_facts = set()
    safe_limit = max(2, int(max_facts_per_section))

    for row in source_rows:
        source_id = str(row.get("source_id", "S?"))
        excerpt = str(row.get("excerpt", ""))

        for sentence in _extract_source_sentences(excerpt):
            sentence_lower = sentence.lower()
            if sentence_lower in seen_facts:
                continue

            # Keep only materially useful sentences for financial rubric execution.
            signal_tokens = (
                "npv",
                "irr",
                "aisc",
                "capex",
                "resource",
                "reserve",
                "grade",
                "production",
                "gold",
                "funding",
                "facility",
                "debt",
                "cash",
                "market cap",
                "shares",
                "first gold",
                "milestone",
                "risk",
                "tailwind",
                "headwind",
                "valuation",
                "ev/oz",
            )
            if not re.search(r"\d", sentence_lower) and not any(
                token in sentence_lower for token in signal_tokens
            ):
                continue

            section = _classify_fact_pack_section(sentence)
            bucket = sections.get(section, [])
            if len(bucket) >= safe_limit:
                continue

            bucket.append(
                {
                    "source_id": source_id,
                    "fact": sentence,
                }
            )
            seen_facts.add(sentence_lower)

    # Starvation fallback: if keyword extraction is sparse, keep a few high-signal
    # generic facts so second-pass analysis still has minimum context.
    if sum(len(items) for items in sections.values()) < 4:
        fallback_bucket = sections["other_material_facts"]
        fallback_limit = max(safe_limit * 2, 6)
        for row in source_rows:
            source_id = str(row.get("source_id", "S?"))
            excerpt = str(row.get("excerpt", ""))
            for sentence in _extract_source_sentences(excerpt)[:2]:
                sentence_lower = sentence.lower()
                if sentence_lower in seen_facts:
                    continue
                if len(fallback_bucket) >= fallback_limit:
                    break
                fallback_bucket.append(
                    {
                        "source_id": source_id,
                        "fact": sentence,
                    }
                )
                seen_facts.add(sentence_lower)
            if len(fallback_bucket) >= fallback_limit:
                break

    compact_sections = {
        name: items
        for name, items in sections.items()
        if items
    }
    total_facts = sum(len(items) for items in compact_sections.values())
    sections_with_facts = list(compact_sections.keys())
    critical = [
        "market_data",
        "project_economics_npv_inputs",
        "resource_and_reserve",
        "development_timeline_and_milestones",
    ]
    critical_gaps = [
        f"Missing evidence for section: {name}"
        for name in critical
        if not sections.get(name)
    ]

    source_index = [
        {
            "source_id": row.get("source_id", ""),
            "title": row.get("title", ""),
            "url": row.get("url", ""),
            "published_at": row.get("published_at", ""),
            "decoded": bool(row.get("decoded")),
        }
        for row in source_rows
    ]

    return {
        "schema": "rubric_fact_pack_v1",
        "source_index": source_index,
        "sections": compact_sections,
        "critical_gaps": critical_gaps,
        "counts": {
            "source_count": len(source_rows),
            "decoded_source_count": sum(1 for row in source_rows if row.get("decoded")),
            "total_facts": total_facts,
            "sections_with_facts": len(sections_with_facts),
        },
    }


def _build_stage1_decoded_evidence_block(
    source_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build compact source excerpt appendix from prepared source rows."""
    rows: List[str] = []
    total_excerpt_chars = 0
    decoded_count = 0

    for source in source_rows:
        label = str(source.get("source_id", "S?"))
        title = str(source.get("title", "Untitled"))
        url = str(source.get("url", ""))
        published = str(source.get("published_at", ""))
        excerpt = str(source.get("excerpt", ""))
        decoded = bool(source.get("decoded"))

        total_excerpt_chars += len(excerpt)
        if decoded:
            decoded_count += 1

        rows.append(f"[{label}] {title}")
        if url:
            rows.append(f"URL: {url}")
        if published:
            rows.append(f"Published: {published}")
        if excerpt:
            rows.append(f"Excerpt: {excerpt}")
        rows.append("")

    block = "\n".join(rows).strip()
    return {
        "block": block,
        "source_count": len(source_rows),
        "decoded_count": decoded_count,
        "total_excerpt_chars": total_excerpt_chars,
    }


def _build_stage1_second_pass_prompt(
    *,
    user_query: str,
    research_brief: str,
    run: Dict[str, Any],
    fact_pack_json: str,
    evidence_appendix: str,
) -> str:
    """Build second-pass analysis prompt with rubric-aligned fact pack."""
    summary = str(run.get("research_summary", "")).strip()
    updates = run.get("latest_updates", []) or []
    updates_lines: List[str] = []
    for item in updates[:5]:
        date_value = str(item.get("date", "Unknown")).strip()
        update_text = str(item.get("update", "Update")).strip()
        source_url = str(item.get("source_url", "")).strip()
        updates_lines.append(f"- {date_value}: {update_text} ({source_url})")
    updates_block = "\n".join(updates_lines).strip()

    return (
        "You are producing a Stage 1 model analysis for an investment council.\n"
        "The user task below is the primary instruction and must be followed exactly.\n\n"
        "USER TASK:\n"
        f"{(user_query or '').strip()}\n\n"
        "ANALYSIS BRIEF:\n"
        f"{(research_brief or '').strip()}\n\n"
        "RESEARCH SUMMARY FROM RETRIEVAL PASS:\n"
        f"{summary or '(none)'}\n\n"
        "LATEST UPDATES (from retrieval):\n"
        f"{updates_block or '(none)'}\n\n"
        "RUBRIC-ALIGNED FACT PACK (primary evidence; use this first):\n"
        f"{fact_pack_json or '(none)'}\n\n"
        "SOURCE EXCERPT APPENDIX (for quote-level validation only):\n"
        f"{evidence_appendix or '(none)'}\n\n"
        "OUTPUT REQUIREMENTS:\n"
        "- Deliver a full investment analysis that answers the user task directly.\n"
        "- Use fact-pack evidence and cite sources with [S#] markers for key numeric claims.\n"
        "- Include explicit assumptions and mark inferred values as ESTIMATE.\n"
        "- If required rubric fields are unsupported by evidence, say UNKNOWN and explain the gap.\n"
        "- Do not return only a source list or research log.\n"
    ).strip()


async def _run_stage1_second_pass_analysis(
    *,
    model: str,
    user_query: str,
    research_brief: str,
    run: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Run a second-pass model analysis on decoded evidence.

    This pass uses the same model id through OpenRouter so the model can reason
    over the locally decoded source excerpts.
    """
    source_rows = _prepare_stage1_source_rows(
        run=run,
        max_sources=PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES,
        max_chars_per_source=PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE,
    )
    fact_pack = _build_stage1_rubric_fact_pack(source_rows)
    fact_pack_json = json.dumps(fact_pack, ensure_ascii=True, separators=(",", ":"))
    appendix_rows = _prepare_stage1_source_rows(
        run=run,
        max_sources=1,
        max_chars_per_source=min(450, PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE),
    )
    evidence = _build_stage1_decoded_evidence_block(appendix_rows)
    prompt = _build_stage1_second_pass_prompt(
        user_query=user_query,
        research_brief=research_brief,
        run=run,
        fact_pack_json=fact_pack_json,
        evidence_appendix=evidence.get("block", ""),
    )

    max_attempts = max(1, int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS))
    backoff = max(0.0, float(PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS))
    timeout = max(30.0, float(PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS))
    attempts_used = 0
    last_error = ""

    for attempt in range(1, max_attempts + 1):
        attempts_used = attempt
        if attempt > 1 and backoff > 0:
            sleep_seconds = backoff * (2 ** (attempt - 2))
            _progress_log(
                f"Stage1 second-pass backoff for {model}: sleeping {sleep_seconds:.1f}s "
                f"(attempt {attempt}/{max_attempts})"
            )
            import asyncio
            await asyncio.sleep(sleep_seconds)

        _progress_log(
            f"Stage1 second-pass start model={model} attempt={attempt}/{max_attempts} "
            f"sources={len(source_rows)} "
            f"decoded_sources={fact_pack.get('counts', {}).get('decoded_source_count', 0)} "
            f"prompt_chars={len(prompt)}"
        )
        response = await query_model(
            model,
            [{"role": "user", "content": prompt}],
            timeout=timeout,
        )
        content = ""
        if response and response.get("content"):
            content = str(response.get("content", "")).strip()

        if content:
            _progress_log(
                f"Stage1 second-pass success model={model} "
                f"attempt={attempt}/{max_attempts} response_chars={len(content)}"
            )
            return {
                "success": True,
                "response": content,
                "attempts": attempts_used,
                "error": "",
                "prompt": prompt,
                "prompt_chars": len(prompt),
                "response_chars": len(content),
                "evidence_source_count": int(fact_pack.get("counts", {}).get("source_count", 0)),
                "decoded_source_count": int(
                    fact_pack.get("counts", {}).get("decoded_source_count", 0)
                ),
                "evidence_total_excerpt_chars": int(
                    sum(len(str(row.get("excerpt", ""))) for row in source_rows)
                ),
                "fact_pack": fact_pack,
                "fact_pack_chars": len(fact_pack_json),
                "fact_pack_total_facts": int(fact_pack.get("counts", {}).get("total_facts", 0)),
                "fact_pack_sections_with_facts": int(
                    fact_pack.get("counts", {}).get("sections_with_facts", 0)
                ),
            }

        last_error = "empty_response"
        _progress_log(
            f"Stage1 second-pass empty response model={model} "
            f"attempt={attempt}/{max_attempts}"
        )

    return {
        "success": False,
        "response": "",
        "attempts": attempts_used,
        "error": last_error or "second_pass_failed",
        "prompt": prompt,
        "prompt_chars": len(prompt),
        "response_chars": 0,
        "evidence_source_count": int(fact_pack.get("counts", {}).get("source_count", 0)),
        "decoded_source_count": int(
            fact_pack.get("counts", {}).get("decoded_source_count", 0)
        ),
        "evidence_total_excerpt_chars": int(
            sum(len(str(row.get("excerpt", ""))) for row in source_rows)
        ),
        "fact_pack": fact_pack,
        "fact_pack_chars": len(fact_pack_json),
        "fact_pack_total_facts": int(fact_pack.get("counts", {}).get("total_facts", 0)),
        "fact_pack_sections_with_facts": int(
            fact_pack.get("counts", {}).get("sections_with_facts", 0)
        ),
    }


async def stage1_collect_responses(enhanced_context: str) -> List[Dict[str, Any]]:
    """
    Stage 1: Collect individual responses from all council models.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content

    Returns:
        List of dicts with 'model' and 'response' keys
    """
    messages = [{"role": "user", "content": enhanced_context}]

    # Query all models in parallel
    responses = await query_models_parallel(COUNCIL_MODELS, messages)

    # Format results
    stage1_results = []
    for model, response in responses.items():
        if response is not None:  # Only include successful responses
            stage1_results.append({
                "model": model,
                "response": response.get('content', '')
            })

    return stage1_results


async def stage1_collect_perplexity_research_responses(
    user_query: str,
    ticker: Optional[str] = None,
    attachment_context: str = "",
    depth: str = "deep",
    research_brief: str = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Stage 1 (emulated): run one Perplexity deep-research call per configured model.

    Args:
        user_query: User question
        ticker: Optional ticker symbol
        attachment_context: Optional attached-document context
        depth: basic|deep research depth
        research_brief: Optional template/company-type framing to steer retrieval

    Returns:
        Tuple of:
        - stage1_results: List[{"model": ..., "response": ...}]
        - metadata: {"per_model_research_runs": [...], "aggregated_search_results": {...}}
    """
    import asyncio
    from .research.providers.perplexity import PerplexityResearchProvider

    total_start = perf_counter()
    models = PERPLEXITY_COUNCIL_MODELS or COUNCIL_MODELS
    provider = PerplexityResearchProvider()
    _progress_log(
        "Stage1 perplexity emulation start: "
        f"models={models}, depth={depth}, max_sources={MAX_SOURCES}, "
        f"execution_mode={PERPLEXITY_STAGE1_EXECUTION_MODE}, "
        f"second_pass_enabled={PERPLEXITY_STAGE1_SECOND_PASS_ENABLED}"
    )

    # Keep attachment context bounded to avoid runaway token growth.
    bounded_attachment_context = attachment_context[:12000] if attachment_context else ""
    research_query = user_query
    if bounded_attachment_context:
        research_query = (
            f"{user_query}\n\n"
            "Additional attached-document context provided by the user:\n"
            f"{bounded_attachment_context}"
        )

    # Keep full brief by default for strict template fidelity.
    bounded_research_brief = (research_brief or "").strip()

    raw_runs: List[Dict[str, Any]] = []
    execution_mode = (PERPLEXITY_STAGE1_EXECUTION_MODE or "parallel").strip().lower()
    # Prefer explicit total attempts. Fall back to legacy retries+1 semantics.
    max_attempts_cfg = int(PERPLEXITY_STAGE1_MAX_ATTEMPTS)
    if max_attempts_cfg > 0:
        max_attempts = max_attempts_cfg
    else:
        max_attempts = max(1, int(PERPLEXITY_STAGE1_MAX_RETRIES) + 1)
    base_backoff = max(0.0, float(PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS))

    async def _gather_model_with_retries(model: str) -> Dict[str, Any]:
        run: Dict[str, Any] = {}
        last_successful_run: Optional[Dict[str, Any]] = None
        active_research_brief = bounded_research_brief
        template_retry_triggered = False
        template_retry_fallback_used = False
        final_retry_error = ""
        attempt_history: List[Dict[str, Any]] = []
        for attempt in range(1, max_attempts + 1):
            attempt_profile = _build_stage1_attempt_profile(
                model=model,
                attempt=attempt,
                base_max_sources=MAX_SOURCES,
                base_max_steps=int(provider.max_steps),
                base_max_output_tokens=int(provider.max_output_tokens),
                base_reasoning_effort=str(provider.reasoning_effort),
            )
            if attempt > 1:
                wait_seconds = base_backoff * (2 ** (attempt - 2))
                if wait_seconds > 0:
                    _progress_log(
                        f"Stage1 retry backoff for {model}: sleeping {wait_seconds:.1f}s "
                        f"(attempt {attempt}/{max_attempts})"
                    )
                    await asyncio.sleep(wait_seconds)
                _progress_log(f"Stage1 retry attempt {attempt}/{max_attempts} for {model}")
            _progress_log(
                f"Stage1 attempt profile {attempt}/{max_attempts} for {model}: "
                f"profile={attempt_profile['name']}, max_sources={attempt_profile['max_sources']}, "
                f"max_steps={attempt_profile['max_steps']}, "
                f"max_output_tokens={attempt_profile['max_output_tokens']}, "
                f"reasoning_effort={attempt_profile['reasoning_effort'] or 'none'}"
            )

            run = await provider.gather(
                research_query,
                ticker=ticker,
                depth=depth,
                max_sources=int(attempt_profile["max_sources"]),
                model_override=model,
                research_brief=active_research_brief,
                max_steps_override=int(attempt_profile["max_steps"]),
                max_output_tokens_override=int(attempt_profile["max_output_tokens"]),
                reasoning_effort_override=str(attempt_profile["reasoning_effort"]),
            ) or {}

            if not run.get("error"):
                last_successful_run = copy.deepcopy(run)
                provider_meta = run.setdefault("provider_metadata", {})
                if not isinstance(provider_meta, dict):
                    provider_meta = {}
                    run["provider_metadata"] = provider_meta

                compliance = _evaluate_stage1_template_compliance(
                    summary_text=str(run.get("research_summary", "")),
                    user_query=user_query,
                    research_brief=bounded_research_brief,
                )
                provider_meta["template_compliance_required"] = bool(compliance["required"])
                provider_meta["template_compliant"] = bool(compliance["compliant"])
                provider_meta["template_compliance_reason"] = str(compliance["reason"])
                provider_meta["template_synthesis_chars"] = int(compliance["synthesis_chars"])
                provider_meta["template_marker_hits"] = int(compliance["marker_hits"])
                if compliance.get("primary_marker_hits") is not None:
                    provider_meta["template_primary_marker_hits"] = int(
                        compliance["primary_marker_hits"]
                    )
                if compliance.get("secondary_marker_hits") is not None:
                    provider_meta["template_secondary_marker_hits"] = int(
                        compliance["secondary_marker_hits"]
                    )
                attempt_history.append(
                    {
                        "attempt": attempt,
                        "status": "success",
                        "profile": attempt_profile,
                        "template_compliant": bool(compliance["compliant"]),
                        "template_reason": str(compliance["reason"]),
                        "template_synthesis_chars": int(compliance["synthesis_chars"]),
                        "template_marker_hits": int(compliance["marker_hits"]),
                        "template_primary_marker_hits": int(
                            compliance.get("primary_marker_hits", 0)
                        ),
                        "template_secondary_marker_hits": int(
                            compliance.get("secondary_marker_hits", 0)
                        ),
                    }
                )

                if compliance["required"] and not compliance["compliant"] and attempt < max_attempts:
                    template_retry_allowed = (
                        PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED
                        and not PERPLEXITY_STAGE1_SECOND_PASS_ENABLED
                    )
                    if template_retry_allowed:
                        template_retry_triggered = True
                        active_research_brief = _build_strict_research_brief(bounded_research_brief)
                        _progress_log(
                            f"Stage1 template compliance retry for {model}: "
                            f"{compliance['reason']} (attempt {attempt}/{max_attempts})"
                        )
                        continue
                    if PERPLEXITY_STAGE1_SECOND_PASS_ENABLED:
                        _progress_log(
                            f"Stage1 template compliance warning for {model} before second pass: "
                            f"{compliance['reason']}"
                        )
                    else:
                        _progress_log(
                            f"Stage1 template compliance warning for {model} without retry: "
                            f"{compliance['reason']} (set PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED=true to retry)"
                        )
                elif compliance["required"] and not compliance["compliant"] and PERPLEXITY_STAGE1_SECOND_PASS_ENABLED:
                    _progress_log(
                        f"Stage1 template compliance warning for {model} before second pass: "
                        f"{compliance['reason']}"
                    )
                break

            error_text = str(run.get("error", ""))
            final_retry_error = error_text
            retryable = bool(_is_retryable_stage1_error(error_text))
            attempt_history.append(
                {
                    "attempt": attempt,
                    "profile": attempt_profile,
                    "status": "error",
                    "error": error_text,
                    "retryable": retryable,
                }
            )
            if attempt >= max_attempts or not retryable:
                break
            _progress_log(
                f"Stage1 transient failure for {model}: {error_text[:220]} "
                f"(will retry)"
            )

        # Preserve the last successful output if template retry attempts subsequently fail.
        # This avoids throwing away usable analysis due to transient API errors on strict retries.
        if (run is None or run.get("error")) and last_successful_run is not None and template_retry_triggered:
            run = last_successful_run
            template_retry_fallback_used = True
            _progress_log(
                f"Stage1 template retry fallback kept previous successful result for {model} "
                f"after final error: {final_retry_error[:220]}"
            )

        second_pass_result: Dict[str, Any] = {}
        if run and not run.get("error"):
            provider_meta = run.setdefault("provider_metadata", {})
            if not isinstance(provider_meta, dict):
                provider_meta = {}
                run["provider_metadata"] = provider_meta

            if PERPLEXITY_STAGE1_SECOND_PASS_ENABLED:
                second_pass_result = await _run_stage1_second_pass_analysis(
                    model=model,
                    user_query=user_query,
                    research_brief=bounded_research_brief,
                    run=run,
                )
                run["stage1_second_pass"] = second_pass_result
                provider_meta["stage1_second_pass_enabled"] = True
                provider_meta["stage1_second_pass_success"] = bool(second_pass_result.get("success"))
                provider_meta["stage1_second_pass_attempts"] = int(second_pass_result.get("attempts", 0))
                provider_meta["stage1_second_pass_error"] = str(second_pass_result.get("error", ""))
                provider_meta["stage1_second_pass_prompt_chars"] = int(
                    second_pass_result.get("prompt_chars", 0)
                )
                provider_meta["stage1_second_pass_response_chars"] = int(
                    second_pass_result.get("response_chars", 0)
                )
                provider_meta["stage1_second_pass_evidence_source_count"] = int(
                    second_pass_result.get("evidence_source_count", 0)
                )
                provider_meta["stage1_second_pass_decoded_source_count"] = int(
                    second_pass_result.get("decoded_source_count", 0)
                )
                provider_meta["stage1_second_pass_evidence_total_excerpt_chars"] = int(
                    second_pass_result.get("evidence_total_excerpt_chars", 0)
                )
                provider_meta["stage1_second_pass_fact_pack_chars"] = int(
                    second_pass_result.get("fact_pack_chars", 0)
                )
                provider_meta["stage1_second_pass_fact_pack_total_facts"] = int(
                    second_pass_result.get("fact_pack_total_facts", 0)
                )
                provider_meta["stage1_second_pass_fact_pack_sections_with_facts"] = int(
                    second_pass_result.get("fact_pack_sections_with_facts", 0)
                )

                if second_pass_result.get("prompt"):
                    run["stage1_second_pass_prompt"] = second_pass_result["prompt"]
                if second_pass_result.get("fact_pack"):
                    run["stage1_second_pass_fact_pack"] = second_pass_result["fact_pack"]
                if second_pass_result.get("success") and second_pass_result.get("response"):
                    run["stage1_analysis_response"] = str(second_pass_result["response"]).strip()
                    final_compliance = _evaluate_stage1_template_compliance(
                        summary_text=run["stage1_analysis_response"],
                        user_query=user_query,
                        research_brief=bounded_research_brief,
                    )
                    provider_meta["stage1_final_template_compliant"] = bool(
                        final_compliance["compliant"]
                    )
                    provider_meta["stage1_final_template_reason"] = str(
                        final_compliance["reason"]
                    )
                    provider_meta["stage1_final_template_marker_hits"] = int(
                        final_compliance.get("marker_hits", 0)
                    )
                    provider_meta["stage1_final_template_primary_marker_hits"] = int(
                        final_compliance.get("primary_marker_hits", 0)
                    )
                    provider_meta["stage1_final_template_secondary_marker_hits"] = int(
                        final_compliance.get("secondary_marker_hits", 0)
                    )
            else:
                provider_meta["stage1_second_pass_enabled"] = False

        provider_meta = run.setdefault("provider_metadata", {})
        if not isinstance(provider_meta, dict):
            provider_meta = {}
            run["provider_metadata"] = provider_meta
        provider_meta["stage1_attempts"] = attempt
        provider_meta["stage1_retried"] = attempt > 1
        provider_meta["stage1_template_retry_triggered"] = template_retry_triggered
        provider_meta["stage1_template_retry_fallback_used"] = template_retry_fallback_used
        provider_meta["stage1_attempt_history"] = attempt_history
        if final_retry_error:
            provider_meta["stage1_final_retry_error"] = final_retry_error
        return run

    if execution_mode == "staggered":
        stagger_seconds = max(0.0, float(PERPLEXITY_STAGE1_STAGGER_SECONDS))
        for index, model in enumerate(models):
            if index > 0 and stagger_seconds > 0:
                _progress_log(
                    f"Stage1 waiting {stagger_seconds:.1f}s before next model: {model}"
                )
                await asyncio.sleep(stagger_seconds)
            model_start = perf_counter()
            _progress_log(f"Stage1 model start: {model}")
            run = await _gather_model_with_retries(model)
            elapsed = perf_counter() - model_start
            if run and not run.get("error"):
                decode_meta = (run.get("provider_metadata", {}) or {}).get("source_decoding", {}) or {}
                provider_meta = (run.get("provider_metadata", {}) or {})
                stage1_attempts = int(provider_meta.get("stage1_attempts", 1))
                template_compliant = provider_meta.get("template_compliant")
                second_pass_success = provider_meta.get("stage1_second_pass_success")
                template_flag = (
                    f", template_compliant={template_compliant}"
                    if template_compliant is not None
                    else ""
                )
                second_pass_flag = (
                    f", second_pass_success={second_pass_success}"
                    if second_pass_success is not None
                    else ""
                )
                _progress_log(
                    f"Stage1 model done: {model} "
                    f"(elapsed={elapsed:.1f}s, result_count={run.get('result_count', 0)}, "
                    f"decoded={decode_meta.get('decoded', 0)}/{decode_meta.get('attempted', 0)}, "
                    f"attempts={stage1_attempts}{template_flag}{second_pass_flag})"
                )
            else:
                stage1_attempts = int((run.get("provider_metadata", {}) or {}).get("stage1_attempts", 1))
                _progress_log(
                    f"Stage1 model failed: {model} "
                    f"(elapsed={elapsed:.1f}s, attempts={stage1_attempts}, "
                    f"error={run.get('error') if run else 'unknown'})"
                )
            raw_runs.append(run)
    else:
        async def _run_one(model: str) -> Dict[str, Any]:
            model_start = perf_counter()
            _progress_log(f"Stage1 model start: {model}")
            run = await _gather_model_with_retries(model)
            elapsed = perf_counter() - model_start
            if run and not run.get("error"):
                decode_meta = (run.get("provider_metadata", {}) or {}).get("source_decoding", {}) or {}
                provider_meta = (run.get("provider_metadata", {}) or {})
                stage1_attempts = int(provider_meta.get("stage1_attempts", 1))
                template_compliant = provider_meta.get("template_compliant")
                second_pass_success = provider_meta.get("stage1_second_pass_success")
                template_flag = (
                    f", template_compliant={template_compliant}"
                    if template_compliant is not None
                    else ""
                )
                second_pass_flag = (
                    f", second_pass_success={second_pass_success}"
                    if second_pass_success is not None
                    else ""
                )
                _progress_log(
                    f"Stage1 model done: {model} "
                    f"(elapsed={elapsed:.1f}s, result_count={run.get('result_count', 0)}, "
                    f"decoded={decode_meta.get('decoded', 0)}/{decode_meta.get('attempted', 0)}, "
                    f"attempts={stage1_attempts}{template_flag}{second_pass_flag})"
                )
            else:
                stage1_attempts = int((run.get("provider_metadata", {}) or {}).get("stage1_attempts", 1))
                _progress_log(
                    f"Stage1 model failed: {model} "
                    f"(elapsed={elapsed:.1f}s, attempts={stage1_attempts}, "
                    f"error={run.get('error') if run else 'unknown'})"
                )
            return run

        tasks = [_run_one(model) for model in models]
        raw_runs = await asyncio.gather(*tasks)

    stage1_results: List[Dict[str, Any]] = []
    per_model_research_runs: List[Dict[str, Any]] = []

    for model, run in zip(models, raw_runs):
        model_run = {"model": model, "result": run}
        per_model_research_runs.append(model_run)

        if run is None or run.get("error"):
            continue

        stage1_results.append(
            {
                "model": model,
                "response": _format_perplexity_research_as_stage1_response(model, run),
            }
        )

    aggregated_search_results = _aggregate_perplexity_research_runs(
        user_query=user_query,
        ticker=ticker,
        model_runs=per_model_research_runs,
        depth=depth,
    )

    metadata = {
        "per_model_research_runs": per_model_research_runs,
        "aggregated_search_results": aggregated_search_results,
        "models_attempted": models,
        "models_succeeded": [item["model"] for item in stage1_results],
        "stage1_execution_mode": execution_mode,
        "stage1_stagger_seconds": float(PERPLEXITY_STAGE1_STAGGER_SECONDS),
        "stage1_second_pass_enabled": bool(PERPLEXITY_STAGE1_SECOND_PASS_ENABLED),
        "stage1_second_pass_max_sources": int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES),
        "stage1_second_pass_max_chars_per_source": int(
            PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE
        ),
        "stage1_openai_guardrails_enabled": bool(
            PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED
        ),
        "stage1_openai_base_max_sources": int(PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES),
        "stage1_openai_base_max_steps": int(PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS),
        "stage1_openai_base_reasoning_effort": str(
            PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT
        ),
    }

    total_elapsed = perf_counter() - total_start
    _progress_log(
        "Stage1 perplexity emulation complete: "
        f"succeeded={len(metadata['models_succeeded'])}/{len(models)}, "
        f"aggregated_sources={aggregated_search_results.get('result_count', 0)}, "
        f"elapsed={total_elapsed:.1f}s"
    )

    return stage1_results, metadata


def _format_perplexity_research_as_stage1_response(model: str, run: Dict[str, Any]) -> str:
    """Turn a Perplexity research run into Stage 1 response text."""
    second_pass_response = str(run.get("stage1_analysis_response", "")).strip()
    if second_pass_response:
        return second_pass_response

    lines = [
        f"Perplexity Deep Research Run for model: {model}",
        "",
    ]

    provider_meta = run.get("provider_metadata", {})
    if provider_meta:
        lines.append(
            f"Profile: model={provider_meta.get('model', model)} "
            f"preset={provider_meta.get('preset', 'n/a')} "
            f"tools={', '.join(provider_meta.get('tools', [])) or 'n/a'}"
        )
        decode_meta = provider_meta.get("source_decoding", {}) or {}
        attempted = int(decode_meta.get("attempted", 0))
        decoded = int(decode_meta.get("decoded", 0))
        if attempted > 0:
            lines.append(f"Decoding: {decoded}/{attempted} sources decoded locally")
        lines.append("")

    summary = (run.get("research_summary") or "").strip()
    if summary:
        lines.append("Findings:")
        lines.append(summary)
        lines.append("")
    else:
        updates = run.get("latest_updates", [])[:6]
        if updates:
            lines.append("Latest Updates (with links):")
            lines.append("| Date | Update | Why it matters | Source |")
            lines.append("|---|---|---|---|")
            for update in updates:
                date_value = str(update.get("date", "Unknown")).replace("|", "\\|")
                title = str(update.get("update", "Update")).replace("|", "\\|")
                why = str(update.get("why_it_matters", "")).replace("|", "\\|")
                source = update.get("source_url", "")
                source_cell = f"[link]({source})" if source else "N/A"
                lines.append(f"| {date_value} | {title} | {why} | {source_cell} |")
            lines.append("")

    sources = run.get("results", [])[:8]
    if sources:
        lines.append("Key Sources:")
        for idx, source in enumerate(sources, start=1):
            title = source.get("title", "Untitled")
            url = source.get("url", "")
            snippet = source.get("content", "")
            snippet = snippet[:220] + ("..." if len(snippet) > 220 else "")
            lines.append(f"{idx}. {title}")
            if url:
                lines.append(f"   URL: {url}")
            if snippet:
                lines.append(f"   Note: {snippet}")

    return "\n".join(lines).strip()


def _aggregate_perplexity_research_runs(
    user_query: str,
    ticker: Optional[str],
    model_runs: List[Dict[str, Any]],
    depth: str,
) -> Dict[str, Any]:
    """Aggregate per-model Perplexity runs into one search/evidence payload."""
    merged_by_url: Dict[str, Dict[str, Any]] = {}
    key_facts: List[str] = []
    failed_models: List[str] = []

    for model_run in model_runs:
        model = model_run["model"]
        result = model_run.get("result") or {}

        if result.get("error"):
            failed_models.append(model)
            continue

        latest_updates = result.get("latest_updates", []) or []
        for update in latest_updates[:2]:
            date_value = str(update.get("date", "Unknown")).strip()
            update_title = str(update.get("update", "")).strip()
            if update_title:
                key_facts.append(f"{model}: {date_value} - {update_title[:200]}")

        if not latest_updates:
            summary = (result.get("research_summary") or "").strip()
            if summary:
                first_line = summary.splitlines()[0].strip()
                if first_line:
                    key_facts.append(f"{model}: {first_line[:240]}")

        for src in result.get("results", []):
            url = src.get("url", "").strip()
            if not url:
                continue

            entry = merged_by_url.get(url)
            if entry is None:
                entry = {
                    "title": src.get("title", "Untitled"),
                    "url": url,
                    "content": src.get("content", ""),
                    "score": float(src.get("score", 0.0)),
                    "published_at": src.get("published_at", ""),
                    "models": [model],
                }
                merged_by_url[url] = entry
            else:
                if model not in entry["models"]:
                    entry["models"].append(model)
                entry["score"] = max(entry["score"], float(src.get("score", 0.0)))
                if len(src.get("content", "")) > len(entry.get("content", "")):
                    entry["content"] = src.get("content", "")
                if not entry.get("published_at") and src.get("published_at"):
                    entry["published_at"] = src.get("published_at")

    merged_results = list(merged_by_url.values())
    merged_results.sort(key=lambda item: (-len(item["models"]), -item["score"]))

    formatted_results = []
    for item in merged_results[:MAX_SOURCES]:
        model_tags = ", ".join(item["models"])
        content = item.get("content", "").strip()
        if content:
            content = f"[Models: {model_tags}] {content}"
        else:
            content = f"Referenced by models: {model_tags}"

        result_item = {
            "title": item.get("title", "Untitled"),
            "url": item.get("url", ""),
            "content": content,
            "score": item.get("score", 0.0),
        }
        if item.get("published_at"):
            result_item["published_at"] = item["published_at"]
        formatted_results.append(result_item)

    missing_data = []
    if not formatted_results:
        missing_data.append("No sources were retrieved from emulated Perplexity council runs.")
    if failed_models:
        missing_data.append(f"Models with failed research runs: {', '.join(failed_models)}")
    if ticker:
        exchange = _infer_exchange_from_ticker(ticker)
        expected_domains = _expected_domains_for_exchange(exchange)
        if expected_domains and not _has_expected_source_domain(formatted_results, expected_domains):
            missing_data.append(
                f"No expected primary-source domain found in aggregated {exchange.upper()} model research."
            )

    evidence_pack_sources = []
    for result in formatted_results:
        evidence_pack_sources.append(
            {
                "url": result.get("url", ""),
                "title": result.get("title", "Untitled"),
                "snippet": result.get("content", ""),
                "source_type": "web",
                "published_at": result.get("published_at", ""),
                "score": float(result.get("score", 0.0)),
                "provider": "perplexity",
            }
        )

    evidence_pack = {
        "question": user_query,
        "ticker": ticker or "",
        "provider": "perplexity_council_emulated",
        "depth": depth,
        "generated_at": datetime.utcnow().isoformat(),
        "sources": evidence_pack_sources,
        "key_facts": key_facts[:12],
        "missing_data": missing_data,
    }

    return {
        "query": user_query,
        "results": formatted_results,
        "result_count": len(formatted_results),
        "performed_at": datetime.utcnow().isoformat(),
        "search_type": "perplexity_emulated_council",
        "provider": "perplexity",
        "evidence_pack": evidence_pack,
    }


async def stage2_collect_rankings(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    ranking_models: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """
    Stage 2: Each model ranks the anonymized responses.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content
        stage1_results: Results from Stage 1
        ranking_models: Optional explicit judge model list (defaults to COUNCIL_MODELS)

    Returns:
        Tuple of (rankings list, label_to_model mapping)
    """
    # Create anonymized labels for responses (Response A, Response B, etc.)
    labels = [chr(65 + i) for i in range(len(stage1_results))]  # A, B, C, ...

    # Create mapping from label to model name
    label_to_model = {
        f"Response {label}": result['model']
        for label, result in zip(labels, stage1_results)
    }

    # Build the ranking prompt
    responses_text = "\n\n".join([
        f"Response {label}:\n{result['response']}"
        for label, result in zip(labels, stage1_results)
    ])

    ranking_prompt = f"""You are evaluating different responses to the following question:

{enhanced_context}

Here are the responses from different models (anonymized):

{responses_text}

Your task:
1. First, evaluate each response individually. For each response, explain what it does well and what it does poorly.
2. Then, at the very end of your response, provide a final ranking.

IMPORTANT: Your final ranking MUST be formatted EXACTLY as follows:
- Start with the line "FINAL RANKING:" (all caps, with colon)
- Then list the responses from best to worst as a numbered list
- Each line should be: number, period, space, then ONLY the response label (e.g., "1. Response A")
- Do not add any other text or explanations in the ranking section

Example of the correct format for your ENTIRE response:

Response A provides good detail on X but misses Y...
Response B is accurate but lacks depth on Z...
Response C offers the most comprehensive answer...

FINAL RANKING:
1. Response C
2. Response A
3. Response B

Now provide your evaluation and ranking:"""

    messages = [{"role": "user", "content": ranking_prompt}]

    # Get rankings from selected judge models in parallel
    judge_models = [m for m in (ranking_models or COUNCIL_MODELS) if m]
    responses = await query_models_parallel(judge_models, messages)

    # Format results
    stage2_results = []
    for model, response in responses.items():
        if response is not None:
            full_text = response.get('content', '')
            parsed = parse_ranking_from_text(full_text)
            stage2_results.append({
                "model": model,
                "ranking": full_text,
                "parsed_ranking": parsed
            })

    return stage2_results, label_to_model


async def stage3_synthesize_final(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str] = None,
    use_structured_analysis: bool = False,
    template_id: str = None,
    ticker: str = None,
    company_name: str = None,
    exchange: str = None,
    chairman_model: Optional[str] = None,
    market_facts: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Stage 3: Chairman synthesizes final response.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content
        stage1_results: Individual model responses from Stage 1
        stage2_results: Rankings from Stage 2
        label_to_model: Mapping from labels to models (for weighted synthesis)
        use_structured_analysis: If True, use investment analysis rubric
        template_id: Template ID to use for synthesis
        ticker: Stock ticker for structured analysis
        company_name: Optional explicit company name
        exchange: Optional exchange id/name
        chairman_model: Optional chairman model override for this run

    Returns:
        Dict with 'model' and 'response' keys (and 'structured_data' if applicable)
    """
    # Check if we should use structured investment analysis
    selected_chairman_model = chairman_model or CHAIRMAN_MODEL

    if use_structured_analysis and template_id and label_to_model:
        from .investment_synthesis import synthesize_structured_analysis
        return await synthesize_structured_analysis(
            enhanced_context,
            stage1_results,
            stage2_results,
            label_to_model,
            template_id,
            ticker,
            company_name=company_name,
            exchange=exchange,
            chairman_model=selected_chairman_model,
            market_facts=market_facts,
        )

    # Otherwise use standard synthesis
    # Build comprehensive context for chairman
    stage1_text = "\n\n".join([
        f"Model: {result['model']}\nResponse: {result['response']}"
        for result in stage1_results
    ])

    stage2_text = "\n\n".join([
        f"Model: {result['model']}\nRanking: {result['ranking']}"
        for result in stage2_results
    ])

    chairman_prompt = f"""You are the Chairman of an LLM Council. Multiple AI models have provided responses to a user's question, and then ranked each other's responses.

Original Question (with context):
{enhanced_context}

STAGE 1 - Individual Responses:
{stage1_text}

STAGE 2 - Peer Rankings:
{stage2_text}

Your task as Chairman is to synthesize all of this information into a single, comprehensive, accurate answer to the user's original question. Consider:
- The individual responses and their insights
- The peer rankings and what they reveal about response quality
- Any patterns of agreement or disagreement

Provide a clear, well-reasoned final answer that represents the council's collective wisdom:"""

    messages = [{"role": "user", "content": chairman_prompt}]

    # Query the chairman model
    response = await query_model(selected_chairman_model, messages)

    if response is None:
        # Fallback if chairman fails
        return {
            "model": selected_chairman_model,
            "response": "Error: Unable to generate final synthesis."
        }

    return {
        "model": selected_chairman_model,
        "response": response.get('content', '')
    }


def parse_ranking_from_text(ranking_text: str) -> List[str]:
    """
    Parse the FINAL RANKING section from the model's response.

    Args:
        ranking_text: The full text response from the model

    Returns:
        List of response labels in ranked order
    """
    import re

    # Look for "FINAL RANKING:" section
    if "FINAL RANKING:" in ranking_text:
        # Extract everything after "FINAL RANKING:"
        parts = ranking_text.split("FINAL RANKING:")
        if len(parts) >= 2:
            ranking_section = parts[1]
            # Try to extract numbered list format (e.g., "1. Response A")
            # This pattern looks for: number, period, optional space, "Response X"
            numbered_matches = re.findall(r'\d+\.\s*Response [A-Z]', ranking_section)
            if numbered_matches:
                # Extract just the "Response X" part
                return [re.search(r'Response [A-Z]', m).group() for m in numbered_matches]

            # Fallback: Extract all "Response X" patterns in order
            matches = re.findall(r'Response [A-Z]', ranking_section)
            return matches

    # Fallback: try to find any "Response X" patterns in order
    matches = re.findall(r'Response [A-Z]', ranking_text)
    return matches


def calculate_aggregate_rankings(
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Calculate aggregate rankings across all models.

    Args:
        stage2_results: Rankings from each model
        label_to_model: Mapping from anonymous labels to model names

    Returns:
        List of dicts with model name and average rank, sorted best to worst
    """
    from collections import defaultdict

    # Track positions for each model
    model_positions = defaultdict(list)

    for ranking in stage2_results:
        ranking_text = ranking['ranking']

        # Parse the ranking from the structured format
        parsed_ranking = parse_ranking_from_text(ranking_text)

        for position, label in enumerate(parsed_ranking, start=1):
            if label in label_to_model:
                model_name = label_to_model[label]
                model_positions[model_name].append(position)

    # Calculate average position for each model
    aggregate = []
    for model, positions in model_positions.items():
        if positions:
            avg_rank = sum(positions) / len(positions)
            aggregate.append({
                "model": model,
                "average_rank": round(avg_rank, 2),
                "rankings_count": len(positions)
            })

    # Sort by average rank (lower is better)
    aggregate.sort(key=lambda x: x['average_rank'])

    return aggregate


async def generate_conversation_title(user_query: str) -> str:
    """
    Generate a short title for a conversation based on the first user message.

    Args:
        user_query: The first user message

    Returns:
        A short title (3-5 words)
    """
    title_prompt = f"""Generate a very short title (3-5 words maximum) that summarizes the following question.
The title should be concise and descriptive. Do not use quotes or punctuation in the title.

Question: {user_query}

Title:"""

    messages = [{"role": "user", "content": title_prompt}]

    # Use gemini-2.5-flash for title generation (fast and cheap)
    response = await query_model("google/gemini-2.5-flash", messages, timeout=30.0)

    if response is None:
        # Fallback to a generic title
        return "New Conversation"

    title = response.get('content', 'New Conversation').strip()

    # Clean up the title - remove quotes, limit length
    title = title.strip('"\'')

    # Truncate if too long
    if len(title) > 50:
        title = title[:47] + "..."

    return title


async def run_full_council(
    enhanced_context: str,
    use_structured_analysis: bool = False,
    template_id: str = None,
    ticker: str = None,
    company_name: str = None,
    exchange: str = None,
) -> Tuple[List, List, Dict, Dict]:
    """
    Run the complete 3-stage council process.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content
        use_structured_analysis: If True, use analysis template with structured output
        template_id: Template ID to use for synthesis
        ticker: Stock ticker for structured analysis
        company_name: Optional explicit company name
        exchange: Optional exchange id/name

    Returns:
        Tuple of (stage1_results, stage2_results, stage3_result, metadata)
    """
    # Stage 1: Collect individual responses
    stage1_results = await stage1_collect_responses(enhanced_context)

    # If no models responded successfully, return error
    if not stage1_results:
        return [], [], {
            "model": "error",
            "response": "All models failed to respond. Please try again."
        }, {}

    # Stage 2: Collect rankings
    stage2_results, label_to_model = await stage2_collect_rankings(enhanced_context, stage1_results)

    # Calculate aggregate rankings
    aggregate_rankings = calculate_aggregate_rankings(stage2_results, label_to_model)

    # Stage 3: Synthesize final answer (with optional structured analysis)
    stage3_result = await stage3_synthesize_final(
        enhanced_context,
        stage1_results,
        stage2_results,
        label_to_model=label_to_model,
        use_structured_analysis=use_structured_analysis,
        template_id=template_id,
        ticker=ticker,
        company_name=company_name,
        exchange=exchange,
    )

    # Prepare metadata
    metadata = {
        "label_to_model": label_to_model,
        "aggregate_rankings": aggregate_rankings
    }

    return stage1_results, stage2_results, stage3_result, metadata
