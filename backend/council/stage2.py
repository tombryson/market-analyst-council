"""Stage-2 orchestration: rankings, revision deltas, and reconciliation.

Anonymises Stage-1 responses, collects peer rankings, parses them,
runs the optional revision-delta pass, and runs the reconciliation pass.
"""

import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..config import (
    CHAIRMAN_MODEL,
    COUNCIL_MODELS,
    OPENROUTER_API_KEY,
    STAGE2_RECONCILIATION_ENABLED,
    STAGE2_RECONCILIATION_MAX_OUTPUT_TOKENS,
    STAGE2_RECONCILIATION_MAX_RESPONSE_CHARS,
    STAGE2_RECONCILIATION_MAX_SOURCE_CHARS,
    STAGE2_RECONCILIATION_MODEL,
    STAGE2_RECONCILIATION_TIMEOUT_SECONDS,
    STAGE2_RECONCILIATION_TOP_N,
    STAGE2_REVISION_PASS_ENABLED,
    STAGE2_REVISION_PASS_MAX_OUTPUT_TOKENS,
    STAGE2_REVISION_PASS_TIMEOUT_SECONDS,
)
from ..openrouter import query_model, query_models_parallel
from .stage1_attempt import _progress_log

logger = logging.getLogger(__name__)

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
            ranking_entries = _ranking_entries_from_labels(parsed, label_to_model)
            stage2_results.append({
                "model": model,
                "ranking": full_text,
                "parsed_ranking": parsed,
                "parsed_ranking_models": [
                    str(item.get("model") or item.get("label") or "")
                    for item in ranking_entries
                ],
                "ranking_entries": ranking_entries,
                "top_choice_label": ranking_entries[0].get("label") if ranking_entries else None,
                "top_choice_model": ranking_entries[0].get("model") if ranking_entries else None,
            })

    return stage2_results, label_to_model


def _parse_json_object_from_text(text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Extract a JSON object from model text output using permissive fallbacks."""
    payload = (text or "").strip()
    if not payload:
        return None, "empty_response"

    try:
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed, None
    except json.JSONDecodeError:
        pass

    fenced = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", payload, re.IGNORECASE)
    if fenced:
        try:
            parsed = json.loads(fenced.group(1))
            if isinstance(parsed, dict):
                return parsed, None
        except json.JSONDecodeError:
            pass

    # Last fallback: scan for first decodable JSON object in the text.
    decoder = json.JSONDecoder()
    for idx, char in enumerate(payload):
        if char != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(payload[idx:])
            if isinstance(candidate, dict):
                return candidate, None
        except json.JSONDecodeError:
            continue

    return None, "no_json_object_found"


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    return None


def _coerce_float(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        token = value.strip().replace("%", "")
        try:
            return float(token)
        except ValueError:
            return None
    return None


def _normalize_stage2_revision_delta(raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Normalize revision payload returned by a model.

    This parser is intentionally permissive:
    - accepts missing/implicit changed flag
    - accepts notes-only payloads
    - only rejects truly empty/unusable payloads
    """
    changed = _coerce_bool(raw.get("changed"))
    reason_text = str(raw.get("reason", "") or "").strip()
    revision_notes = str(
        raw.get("revision_notes")
        or raw.get("notes")
        or raw.get("summary")
        or reason_text
        or ""
    ).strip()

    normalized: Dict[str, Any] = {
        "changed": False,
        "reason": reason_text,
        "revision_notes": revision_notes,
        "changes": raw.get("changes") if isinstance(raw.get("changes"), list) else [],
        "updated_scores": raw.get("updated_scores") if isinstance(raw.get("updated_scores"), dict) else {},
        "updated_price_targets": (
            raw.get("updated_price_targets")
            if isinstance(raw.get("updated_price_targets"), dict)
            else {}
        ),
        "updated_observations": (
            [str(item).strip() for item in (raw.get("updated_observations") or []) if str(item).strip()]
            if isinstance(raw.get("updated_observations"), list)
            else []
        ),
        "evidence_refs": raw.get("evidence_refs") if isinstance(raw.get("evidence_refs"), list) else [],
        "confidence": None,
    }

    if not normalized["updated_scores"] and isinstance(raw.get("scores"), dict):
        normalized["updated_scores"] = dict(raw.get("scores") or {})
    if not normalized["updated_price_targets"] and isinstance(raw.get("price_targets"), dict):
        normalized["updated_price_targets"] = dict(raw.get("price_targets") or {})

    confidence = _coerce_float(raw.get("confidence"))
    if confidence is not None:
        if confidence > 1.0 and confidence <= 100.0:
            confidence = confidence / 100.0
        if 0.0 <= confidence <= 1.0:
            normalized["confidence"] = round(confidence, 4)

    updated_scores = normalized["updated_scores"]
    if updated_scores:
        for key in ("quality", "value"):
            if key not in updated_scores:
                continue
            score = _coerce_float(updated_scores.get(key))
            if score is None or score < 0 or score > 100:
                updated_scores.pop(key, None)
            else:
                updated_scores[key] = round(float(score), 2)

    inferred_changed = bool(
        normalized["changes"]
        or normalized["updated_scores"]
        or normalized["updated_price_targets"]
        or normalized["updated_observations"]
        or normalized["revision_notes"]
    )
    if changed is None:
        normalized["changed"] = inferred_changed
    else:
        normalized["changed"] = changed

    if not inferred_changed and not reason_text:
        return None, "empty_revision_payload"
    return normalized, None


def _extract_changed_flag_from_text(text: str) -> Optional[bool]:
    payload = str(text or "")
    m = re.search(r"(?im)^\s*CHANGED\s*:\s*(YES|NO|TRUE|FALSE|1|0)\s*$", payload)
    if m:
        token = m.group(1).strip().lower()
        return token in {"yes", "true", "1"}
    m2 = re.search(r'(?i)"changed"\s*:\s*(true|false)', payload)
    if m2:
        return m2.group(1).lower() == "true"
    return None


def _extract_revision_notes_from_text(text: str) -> str:
    payload = str(text or "").strip()
    if not payload:
        return ""
    m = re.search(r"(?is)REVISION_NOTES\s*:\s*(.+)$", payload)
    if m:
        return m.group(1).strip()
    # Strip code fences when present.
    fence = re.search(r"(?is)```(?:json)?\s*(.+?)\s*```", payload)
    if fence:
        return fence.group(1).strip()
    return payload


def _ranking_labels_from_result(ranking: Dict[str, Any]) -> List[str]:
    parsed = ranking.get("parsed_ranking")
    if isinstance(parsed, list):
        labels = []
        for item in parsed:
            if isinstance(item, str):
                label = item.strip()
            elif isinstance(item, dict):
                label = str(item.get("label") or "").strip()
            else:
                label = ""
            if label:
                labels.append(label)
        if labels:
            return labels

    entries = ranking.get("ranking_entries")
    if isinstance(entries, list):
        labels = [
            str(item.get("label") or "").strip()
            for item in entries
            if isinstance(item, dict) and str(item.get("label") or "").strip()
        ]
        if labels:
            return labels

    ranking_text = str(ranking.get("ranking") or "")
    return parse_ranking_from_text(ranking_text)


def _ranking_entries_from_labels(
    labels: List[str],
    label_to_model: Dict[str, str],
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for position, label in enumerate(labels or [], start=1):
        clean_label = str(label or "").strip()
        if not clean_label:
            continue
        entries.append(
            {
                "rank": position,
                "label": clean_label,
                "model": label_to_model.get(clean_label),
            }
        )
    return entries


def compact_stage2_rankings_for_telemetry(
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Return compact per-judge Stage 2 ballots without full prose evaluations."""
    ballots: List[Dict[str, Any]] = []
    for result in stage2_results or []:
        if not isinstance(result, dict):
            continue
        labels = _ranking_labels_from_result(result)
        entries = _ranking_entries_from_labels(labels, label_to_model)
        ballots.append(
            {
                "judge_model": str(result.get("model") or "").strip(),
                "ranking": entries,
                "top_choice_model": entries[0].get("model") if entries else None,
                "ranked_count": len(entries),
            }
        )
    return ballots


def _build_stage2_revision_prompt(
    *,
    enhanced_context: str,
    own_label: str,
    responses_text: str,
    aggregate_rankings: List[Dict[str, Any]],
) -> str:
    ranking_lines: List[str] = []
    for i, item in enumerate(aggregate_rankings or [], start=1):
        ranking_lines.append(
            f"{i}. {item.get('model')} (avg_rank={item.get('average_rank')})"
        )
    ranking_block = "\n".join(ranking_lines) if ranking_lines else "(none)"

    return f"""You are re-evaluating your own Stage 1 analysis after seeing peer model outputs.

You authored: {own_label}

Original question/context:
{enhanced_context}

Peer outputs (anonymized):
{responses_text}

Stage 2 aggregate ranking by model:
{ranking_block}

Task:
1) Decide whether peer responses reveal material points you missed.
2) If yes, propose only incremental updates to your prior conclusions.
3) If no, keep unchanged.
4) Keep your output short.

Output format (strict plain text):
CHANGED: YES or NO
REVISION_NOTES:
- short bullet
- short bullet
OPTIONAL_UPDATES:
- quality: <value or unchanged>
- value: <value or unchanged>
- target_12m: <value/range or unchanged>
- target_24m: <value/range or unchanged>

Rules:
- Max 10 bullets total.
- If no material change: CHANGED: NO and one short note.
- Prefer concrete evidence references to peer responses.
- Do not restate your full report.
"""


async def stage2_collect_revision_deltas(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str],
    revision_models: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Stage 2.5 (WIP): ask each model to self-revise after peer review.

    If delta JSON is malformed/invalid, the caller should keep the previous Stage 1
    output unchanged.
    """
    labels = [chr(65 + i) for i in range(len(stage1_results))]
    model_to_label = {
        result.get("model", ""): f"Response {label}"
        for label, result in zip(labels, stage1_results)
    }
    def _clip(text: str, limit: int = 1800) -> str:
        payload = str(text or "").strip()
        if len(payload) <= limit:
            return payload
        return payload[:limit].rstrip() + "\n...[TRUNCATED FOR REVISION PASS]"

    def _build_responses_text(*, clip_limit: int, max_responses: Optional[int] = None) -> str:
        pairs = list(zip(labels, stage1_results))
        if isinstance(max_responses, int) and max_responses > 0:
            pairs = pairs[:max_responses]
        return "\n\n".join(
            f"Response {label}:\n{_clip(result.get('response', ''), limit=clip_limit)}"
            for label, result in pairs
        )

    responses_text = _build_responses_text(clip_limit=1800)
    compact_responses_text = _build_responses_text(clip_limit=900, max_responses=5)
    aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)
    targets = [m for m in (revision_models or [r.get("model") for r in stage1_results]) if m]
    timeout = float(STAGE2_REVISION_PASS_TIMEOUT_SECONDS)
    max_tokens = int(STAGE2_REVISION_PASS_MAX_OUTPUT_TOKENS)

    async def _run_one(model: str) -> Dict[str, Any]:
        own_label = model_to_label.get(model, "")
        prompt = _build_stage2_revision_prompt(
            enhanced_context=enhanced_context,
            own_label=own_label,
            responses_text=responses_text,
            aggregate_rankings=aggregate,
        )
        attempts = 0
        raw_text = ""
        used_compact_retry = False
        for prompt_text in (
            prompt,
            _build_stage2_revision_prompt(
                enhanced_context=enhanced_context,
                own_label=own_label,
                responses_text=compact_responses_text,
                aggregate_rankings=aggregate,
            ),
        ):
            attempts += 1
            response = await query_model(
                model,
                [{"role": "user", "content": prompt_text}],
                timeout=timeout,
                max_tokens=max_tokens,
            )
            raw_text = (response or {}).get("content", "") if response else ""
            if str(raw_text or "").strip():
                break
        used_compact_retry = attempts > 1
        parsed, parse_error = _parse_json_object_from_text(raw_text)
        normalized = None
        normalize_error = None
        if parsed is not None:
            normalized, normalize_error = _normalize_stage2_revision_delta(parsed)
        # Fallback: accept non-empty plain-text revision notes even when JSON fails.
        if normalized is None and str(raw_text or "").strip():
            fallback_changed = _extract_changed_flag_from_text(raw_text)
            fallback_notes = _extract_revision_notes_from_text(raw_text)
            normalized = {
                "changed": bool(fallback_changed) if fallback_changed is not None else bool(fallback_notes),
                "reason": "",
                "revision_notes": fallback_notes,
                "changes": [],
                "updated_scores": {},
                "updated_price_targets": {},
                "updated_observations": [],
                "evidence_refs": [],
                "confidence": None,
            }
            normalize_error = parse_error or "non_json_fallback_used"
            parse_error = None
        accepted = normalized is not None
        return {
            "model": model,
            "own_label": own_label,
            "prompt_chars": len(prompt),
            "response_chars": len(raw_text or ""),
            "accepted": accepted,
            "changed": bool((normalized or {}).get("changed")) if accepted else False,
            "delta_json": normalized,
            "parse_error": None if accepted else (parse_error or normalize_error),
            "decode_warning": normalize_error if accepted else None,
            "raw_response": raw_text,
            "attempts": attempts,
            "compact_retry_used": used_compact_retry,
        }

    _progress_log(
        "Stage2.5 revision pass start: "
        f"models={targets}, timeout={timeout:.1f}s, max_output_tokens={max_tokens}"
    )
    tasks = [_run_one(model) for model in targets]
    results = await asyncio.gather(*tasks)
    accepted = sum(1 for row in results if row.get("accepted"))
    changed = sum(1 for row in results if row.get("accepted") and row.get("changed"))
    unchanged_count = int(
        sum(
            1
            for row in results
            if row.get("accepted") and (not row.get("changed"))
        )
    )
    empty_response_count = int(
        sum(
            1
            for row in results
            if (not row.get("accepted")) and (row.get("parse_error") == "empty_response")
        )
    )
    parse_failed_count = int(
        sum(
            1
            for row in results
            if (not row.get("accepted")) and (row.get("parse_error") not in {None, "empty_response"})
        )
    )
    summary = {
        "enabled": True,
        "models_attempted": list(targets),
        "models_succeeded": [row.get("model") for row in results if row.get("raw_response")],
        "accepted_count": int(accepted),
        "changed_count": int(changed),
        "no_amendment_count": unchanged_count,
        "empty_response_count": empty_response_count,
        "parse_failed_count": parse_failed_count,
    }
    _progress_log(
        "Stage2.5 revision pass done: "
        f"accepted={accepted}/{len(targets)}, changed={changed}"
    )
    return results, summary


def apply_stage2_revision_deltas(
    stage1_results: List[Dict[str, Any]],
    revision_results: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Apply accepted Stage 2.5 deltas to Stage 1 responses.

    Safe behavior: if revision payload is not accepted, keep original Stage 1 response.
    """
    accepted_by_model: Dict[str, Dict[str, Any]] = {}
    for row in revision_results or []:
        if not row.get("accepted"):
            continue
        model = str(row.get("model") or "").strip()
        if not model:
            continue
        accepted_by_model[model] = row

    updated: List[Dict[str, Any]] = []
    changed_models: List[str] = []
    notes_applied_models: List[str] = []
    for item in stage1_results or []:
        row = dict(item or {})
        model = str(row.get("model") or "").strip()
        accepted = accepted_by_model.get(model)
        if not accepted:
            updated.append(row)
            continue
        delta = accepted.get("delta_json") or {}
        raw_response = str(accepted.get("raw_response") or "").strip()
        if not raw_response:
            updated.append(row)
            continue
        block = (
            "\n\n[STAGE2_REVISION_NOTES]\n"
            f"{raw_response}\n"
        )
        row["response"] = f"{str(row.get('response') or '').rstrip()}{block}"
        notes_applied_models.append(model)
        if bool((delta or {}).get("changed")):
            changed_models.append(model)
        updated.append(row)

    summary = {
        "models_total": len(stage1_results or []),
        "revisions_received": len(revision_results or []),
        "revisions_applied": len(notes_applied_models),
        "revision_notes_applied_models": notes_applied_models,
        "models_changed": changed_models,
        "models_unchanged_due_to_empty_response": [
            str(row.get("model") or "")
            for row in (revision_results or [])
            if (not row.get("accepted")) and (row.get("parse_error") == "empty_response")
        ],
        "models_unchanged_due_to_parse_or_validation": [
            str(row.get("model") or "")
            for row in (revision_results or [])
            if (not row.get("accepted")) and (row.get("parse_error") != "empty_response")
        ],
    }
    return updated, summary


def _clip_for_reconciliation(text: Any, limit: int, marker: str) -> str:
    payload = str(text or "").strip()
    if limit <= 0 or len(payload) <= limit:
        return payload
    head = max(1000, int(limit * 0.62))
    tail = max(1000, limit - head)
    if head + tail >= len(payload):
        return payload
    return (
        payload[:head].rstrip()
        + f"\n\n[{marker}: {len(payload) - head - tail} chars omitted]\n\n"
        + payload[-tail:].lstrip()
    )


def _normalize_reconciliation_issue(item: Any) -> Optional[Dict[str, Any]]:
    if isinstance(item, str):
        issue = item.strip()
        if not issue:
            return None
        return {
            "topic": "",
            "issue": issue,
            "source_resolved_position": "",
            "prefer_models": [],
            "downweight_models": [],
            "affected_claims": [],
            "stage3_instruction": issue,
            "confidence": None,
        }
    if not isinstance(item, dict):
        return None

    def _string_list(value: Any, max_items: int = 8) -> List[str]:
        if isinstance(value, str):
            return [value.strip()] if value.strip() else []
        if not isinstance(value, list):
            return []
        return [str(part).strip() for part in value[:max_items] if str(part).strip()]

    confidence = _coerce_float(item.get("confidence"))
    if confidence is not None:
        if confidence > 1.0 and confidence <= 100.0:
            confidence = confidence / 100.0
        if confidence < 0.0 or confidence > 1.0:
            confidence = None

    topic = str(item.get("topic") or "").strip()
    issue = str(item.get("issue") or item.get("finding") or "").strip()
    instruction = str(item.get("stage3_instruction") or item.get("instruction") or "").strip()
    if not issue and not instruction:
        return None
    return {
        "topic": topic,
        "issue": issue or instruction,
        "source_resolved_position": str(item.get("source_resolved_position") or "").strip(),
        "prefer_models": _string_list(item.get("prefer_models")),
        "downweight_models": _string_list(item.get("downweight_models")),
        "affected_claims": _string_list(item.get("affected_claims"), max_items=12),
        "stage3_instruction": instruction or issue,
        "confidence": round(confidence, 4) if confidence is not None else None,
    }


def _normalize_stage2_reconciliation_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    allowed_status = {
        "issues_found",
        "no_material_issues",
        "insufficient_source_context",
    }
    status = str(raw.get("status") or "").strip().lower()
    if status not in allowed_status:
        has_issues = any(raw.get(key) for key in ("blocking", "material", "unresolved", "topic_overrides"))
        status = "issues_found" if has_issues else "no_material_issues"

    def _issue_list(key: str, max_items: int = 10) -> List[Dict[str, Any]]:
        values = raw.get(key) if isinstance(raw.get(key), list) else []
        normalized: List[Dict[str, Any]] = []
        for value in values[:max_items]:
            row = _normalize_reconciliation_issue(value)
            if row:
                normalized.append(row)
        return normalized

    def _string_list(key: str, max_items: int = 12) -> List[str]:
        values = raw.get(key)
        if isinstance(values, str):
            return [values.strip()] if values.strip() else []
        if not isinstance(values, list):
            return []
        return [str(value).strip() for value in values[:max_items] if str(value).strip()]

    topic_overrides: List[Dict[str, Any]] = []
    values = raw.get("topic_overrides") if isinstance(raw.get("topic_overrides"), list) else []
    for value in values[:10]:
        if not isinstance(value, dict):
            continue
        issue = _normalize_reconciliation_issue(value)
        if not issue:
            continue
        topic_overrides.append(issue)

    normalized = {
        "status": status,
        "blocking": _issue_list("blocking", max_items=8),
        "material": _issue_list("material", max_items=12),
        "minor": _issue_list("minor", max_items=8),
        "unresolved": _issue_list("unresolved", max_items=10),
        "topic_overrides": topic_overrides,
        "stage3_constraints": _string_list("stage3_constraints", max_items=14),
        "summary": str(raw.get("summary") or "").strip(),
    }
    if (
        status == "no_material_issues"
        and (
            normalized["blocking"]
            or normalized["material"]
            or normalized["unresolved"]
            or normalized["topic_overrides"]
        )
    ):
        normalized["status"] = "issues_found"
    return normalized


def _build_stage2_reconciliation_prompt(
    *,
    source_context: str,
    responses_text: str,
    rankings_text: str,
) -> str:
    return f"""You are a lightweight discrepancy reviewer for an investment-analysis council.

You are NOT writing the investment memo. You are NOT re-running research. You are checking whether Stage 3 should trust, distrust, or qualify specific council claims.

INPUT A - PRIMARY/PREPASS CONTEXT
This may contain filings, attachment excerpts, deterministic market facts, injection bundles, and source summaries:
{source_context}

INPUT B - STAGE 1 COUNCIL RESPONSES
{responses_text}

INPUT C - STAGE 2 PEER RANKINGS
{rankings_text}

TASK
Run one compact pass across all inputs and identify:
1. Claims in Stage 1 that conflict with the primary/prepass context.
2. Stale assumptions, especially production, financing, hedging, reserves/resources, commodity exposure, or project-stage baselines that look superseded by dated source material.
3. Cases where a model says "unknown", "not disclosed", or "data gap" but the primary/prepass context appears to contain the answer.
4. Material disagreements between Stage 1 models that Stage 3 must explicitly adjudicate.
5. Topic-specific overrides where a lower-ranked response appears better aligned with source evidence than a higher-ranked response.

Rules:
- Do not introduce external facts not present in the inputs.
- Do not perform valuation or write a replacement memo.
- Prefer primary/prepass context over peer ranking when they conflict.
- Preserve uncertainty. If the source context is too thin, say so.
- Be strict: only list issues that could materially change the final synthesis or prevent a misleading memo.

Return JSON only with this schema:
{{
  "status": "issues_found | no_material_issues | insufficient_source_context",
  "blocking": [
    {{
      "topic": "short topic",
      "issue": "what is wrong or contradictory",
      "source_resolved_position": "what the primary/prepass context supports, if clear",
      "prefer_models": ["model names whose claim is more evidence-aligned"],
      "downweight_models": ["model names whose claim is contradicted or stale"],
      "affected_claims": ["short quoted/paraphrased claims"],
      "stage3_instruction": "specific instruction for the chairman",
      "confidence": 0.0
    }}
  ],
  "material": [],
  "minor": [],
  "unresolved": [
    {{
      "topic": "short topic",
      "issue": "what remains unresolved",
      "source_resolved_position": "",
      "prefer_models": [],
      "downweight_models": [],
      "affected_claims": [],
      "stage3_instruction": "how Stage 3 should qualify it",
      "confidence": 0.0
    }}
  ],
  "topic_overrides": [
    {{
      "topic": "short topic",
      "issue": "why ranking should be overridden for this topic",
      "source_resolved_position": "evidence-aligned position",
      "prefer_models": ["lower-ranked but better-supported models"],
      "downweight_models": ["higher-ranked but contradicted models"],
      "affected_claims": [],
      "stage3_instruction": "topic-specific synthesis rule",
      "confidence": 0.0
    }}
  ],
  "stage3_constraints": [
    "hard synthesis constraint the chairman must follow"
  ],
  "summary": "one-paragraph summary"
}}
"""


def _source_evidence_pack_from_stage1_results(
    stage1_results: Optional[List[Dict[str, Any]]],
    evidence_pack: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Ensure downstream stages can render Stage 1 second-pass source packets."""
    merged: Dict[str, Any] = dict(evidence_pack or {}) if isinstance(evidence_pack, dict) else {}
    has_stage1_runs = bool(merged.get("per_model_research_runs"))
    nested_metadata = merged.get("stage1_emulated_metadata") or merged.get("stage1_metadata")
    if isinstance(nested_metadata, dict) and nested_metadata.get("per_model_research_runs"):
        has_stage1_runs = True
    if not has_stage1_runs and stage1_results:
        merged["per_model_research_runs"] = [
            {
                "model": str(result.get("model") or ""),
                "result": result,
            }
            for result in stage1_results
            if isinstance(result, dict)
        ]
    return merged


async def stage2_collect_reconciliation(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str],
    reconciliation_model: Optional[str] = None,
    enabled: Optional[bool] = None,
    source_evidence_pack: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Single cheap discrepancy pass before Stage 3 synthesis."""
    should_run = bool(STAGE2_RECONCILIATION_ENABLED) if enabled is None else bool(enabled)
    if not should_run:
        return {"enabled": False, "accepted": False, "status": "disabled"}
    if not stage1_results:
        return {"enabled": True, "accepted": False, "status": "no_stage1_results"}

    aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)
    rank_by_model = {
        str(row.get("model") or ""): row
        for row in aggregate
        if row.get("model")
    }
    labels = [chr(65 + i) for i in range(len(stage1_results or []))]
    model_to_label = {
        str(result.get("model") or ""): f"Response {label}"
        for label, result in zip(labels, stage1_results or [])
    }
    result_by_model = {
        str(result.get("model") or ""): result
        for result in stage1_results or []
        if result.get("model")
    }

    ordered_models = [row["model"] for row in aggregate if row.get("model") in result_by_model]
    for result in stage1_results or []:
        model = str(result.get("model") or "").strip()
        if model and model not in ordered_models:
            ordered_models.append(model)
    top_n = int(STAGE2_RECONCILIATION_TOP_N)
    if top_n > 0:
        ordered_models = ordered_models[:top_n]

    max_response_chars = int(STAGE2_RECONCILIATION_MAX_RESPONSE_CHARS)
    response_blocks: List[str] = []
    for model in ordered_models:
        result = result_by_model.get(model) or {}
        rank = rank_by_model.get(model) or {}
        rank_text = (
            f"average_rank={rank.get('average_rank')} "
            f"rankings_count={rank.get('rankings_count')}"
            if rank
            else "not_ranked"
        )
        response_blocks.append(
            f"{model_to_label.get(model, '')} | model={model} | {rank_text}\n"
            f"{_clip_for_reconciliation(result.get('response', ''), max_response_chars, 'TRUNCATED RESPONSE')}"
        )

    ranking_lines = ["Aggregate peer ranking:"]
    if aggregate:
        for i, item in enumerate(aggregate, start=1):
            ranking_lines.append(
                f"{i}. {item.get('model')} "
                f"(avg_rank={item.get('average_rank')}, votes={item.get('rankings_count')})"
            )
    else:
        ranking_lines.append("(no parseable aggregate ranking)")

    source_pack = _source_evidence_pack_from_stage1_results(
        stage1_results,
        source_evidence_pack,
    )
    source_fact_context = build_source_fact_context(
        source_pack,
        max_chars=max(0, int(STAGE2_RECONCILIATION_MAX_SOURCE_CHARS) // 2),
    )
    source_parts = [
        part
        for part in (source_fact_context, enhanced_context)
        if str(part or "").strip()
    ]
    source_context = _clip_for_reconciliation(
        "\n\n".join(source_parts),
        int(STAGE2_RECONCILIATION_MAX_SOURCE_CHARS),
        "TRUNCATED SOURCE/PREPASS CONTEXT",
    )
    prompt = _build_stage2_reconciliation_prompt(
        source_context=source_context,
        responses_text="\n\n---\n\n".join(response_blocks),
        rankings_text="\n".join(ranking_lines),
    )

    selected_model = (reconciliation_model or STAGE2_RECONCILIATION_MODEL or CHAIRMAN_MODEL).strip()
    timeout = float(STAGE2_RECONCILIATION_TIMEOUT_SECONDS)
    max_tokens = int(STAGE2_RECONCILIATION_MAX_OUTPUT_TOKENS)
    _progress_log(
        "Stage2.5 reconciliation start: "
        f"model={selected_model}, responses={len(ordered_models)}, "
        f"source_fact_chars={len(source_fact_context)}, "
        f"prompt_chars={len(prompt)}, timeout={timeout:.1f}s"
    )
    response = await query_model(
        selected_model,
        [{"role": "user", "content": prompt}],
        timeout=timeout,
        max_tokens=max_tokens if max_tokens > 0 else None,
    )
    raw_text = (response or {}).get("content", "") if response else ""
    parsed, parse_error = _parse_json_object_from_text(raw_text)
    if not parsed:
        _progress_log(
            "Stage2.5 reconciliation failed: "
            f"model={selected_model}, parse_error={parse_error}"
        )
        return {
            "enabled": True,
            "accepted": False,
            "status": "parse_failed" if raw_text else "model_failed",
            "model": selected_model,
            "selected_models": ordered_models,
            "source_fact_context_chars": len(source_fact_context),
            "prompt_chars": len(prompt),
            "response_chars": len(raw_text or ""),
            "parse_error": parse_error or "empty_response",
            "raw_response": raw_text,
        }

    normalized = _normalize_stage2_reconciliation_payload(parsed)
    issue_count = sum(
        len(normalized.get(key) or [])
        for key in ("blocking", "material", "minor", "unresolved", "topic_overrides")
    )
    out = {
        "enabled": True,
        "accepted": True,
        "model": selected_model,
        "selected_models": ordered_models,
        "source_fact_context_chars": len(source_fact_context),
        "prompt_chars": len(prompt),
        "response_chars": len(raw_text or ""),
        "issue_count": int(issue_count),
        **normalized,
    }
    _progress_log(
        "Stage2.5 reconciliation done: "
        f"status={out.get('status')}, issues={issue_count}"
    )
    return out


