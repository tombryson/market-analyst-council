"""Segmented Perplexity pharma-biotech supplementary-facts workflow."""

from __future__ import annotations

import asyncio
import copy
from datetime import datetime
from time import perf_counter
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import httpx

from ...config import (
    CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS,
    CHAIRMAN_JSONIFIER_MODEL,
    CHAIRMAN_JSONIFIER_TIMEOUT_SECONDS,
    PERPLEXITY_API_URL,
    PERPLEXITY_MINING_CONTAMINATION_CHECKER_ENABLED,
    PERPLEXITY_MINING_CONTAMINATION_CHECKER_MAX_OUTPUT_TOKENS,
    PERPLEXITY_MINING_CONTAMINATION_CHECKER_MODEL,
    PERPLEXITY_MINING_CONTAMINATION_CHECKER_REASONING_EFFORT,
    PERPLEXITY_MINING_CONTAMINATION_CHECKER_TIMEOUT_SECONDS,
    PERPLEXITY_MINING_CONTAMINATION_MIN_CONFIDENCE_PCT,
    PERPLEXITY_MINING_ENRICHER_ENABLE_TARGETED_REPAIRS,
    PERPLEXITY_MINING_ENRICHER_MAX_PRIORITY_SOURCES,
    PERPLEXITY_MINING_ENRICHER_PRESET,
    PERPLEXITY_MINING_ENRICHER_REASONING_EFFORT,
    PERPLEXITY_MINING_ENRICHER_REPAIR_PRESET,
)
from ...openrouter import query_model
from ..pharma_biotech_supplementary import (
    SEGMENT_DEFINITIONS,
    apply_contamination_review,
    apply_deterministic_adjudication,
    build_contamination_review_prompt,
    build_discovery_prompt,
    build_repair_context,
    build_segment_extraction_prompt,
    build_targeted_repair_prompt,
    extract_json_payload,
    flatten_packet_rows,
    merge_segment_outputs,
    missing_or_not_found_items,
    resolve_pharma_biotech_enricher_context,
    segment_repairs_for_missing_items,
)

if TYPE_CHECKING:  # pragma: no cover
    from .perplexity import PerplexityResearchProvider


class PerplexityPharmaBiotechSupplementaryEnricher:
    """Reusable segmented pharma-biotech supplementary-facts orchestration."""

    def __init__(self, provider: "PerplexityResearchProvider"):
        self.provider = provider

    async def gather(
        self,
        *,
        user_query: str = "",
        company: str,
        ticker: str,
        exchange: str,
        template_id: str = "",
        company_type: str = "",
        preset: Optional[str] = None,
        repair_preset: Optional[str] = None,
        model_override: Optional[str] = None,
        max_priority_sources: Optional[int] = None,
        enable_targeted_repairs: Optional[bool] = None,
    ) -> Dict[str, Any]:
        context = resolve_pharma_biotech_enricher_context(
            user_query=user_query,
            ticker=ticker,
            company=company,
            exchange=exchange,
            template_id=template_id,
            company_type=company_type,
        )
        company_name = str(context.get("company") or "").strip()
        exchange_code = str(context.get("exchange") or "").strip().upper()
        ticker_symbol = str(context.get("ticker_symbol") or "").strip().upper()
        active_preset = str(preset or PERPLEXITY_MINING_ENRICHER_PRESET or "deep-research").strip() or "deep-research"
        active_repair_preset = str(repair_preset or PERPLEXITY_MINING_ENRICHER_REPAIR_PRESET or active_preset).strip() or active_preset
        target_sources = max(8, int(max_priority_sources or PERPLEXITY_MINING_ENRICHER_MAX_PRIORITY_SOURCES or 18))
        allow_repairs = (
            PERPLEXITY_MINING_ENRICHER_ENABLE_TARGETED_REPAIRS
            if enable_targeted_repairs is None
            else bool(enable_targeted_repairs)
        )

        run_start = perf_counter()
        self.provider._log(
            f"pharma_enricher start ticker={exchange_code}:{ticker_symbol} preset={active_preset} repair_preset={active_repair_preset} repairs={allow_repairs}"
        )

        discovery_prompt = build_discovery_prompt(
            company=company_name,
            ticker=ticker_symbol,
            exchange=exchange_code,
        )
        discovery_run = await self._run_structured_json_prompt(
            prompt=discovery_prompt,
            preset=active_preset,
            model_override=model_override,
            max_sources=target_sources,
            max_output_tokens_override=2200,
            stage_name="discovery",
        )
        discovery_json = discovery_run["parsed_json"]

        async def _run_segment(segment: Dict[str, Any]) -> Dict[str, Any]:
            prompt = build_segment_extraction_prompt(
                company=company_name,
                ticker=ticker_symbol,
                exchange=exchange_code,
                source_packet=discovery_json,
                checklist_items=list(segment["checklist_items"]),
                categories=list(segment["categories"]),
            )
            result = await self._run_structured_json_prompt(
                prompt=prompt,
                preset=active_preset,
                model_override=model_override,
                max_sources=target_sources,
                max_output_tokens_override=4200,
                stage_name=str(segment["name"]),
            )
            result["segment_name"] = str(segment["name"])
            result["segment_categories"] = list(segment["categories"])
            result["segment_checklist_items"] = list(segment["checklist_items"])
            return result

        segment_runs = await asyncio.gather(*[_run_segment(segment) for segment in SEGMENT_DEFINITIONS])
        segment_outputs = [run["parsed_json"] for run in segment_runs]

        merged_json = merge_segment_outputs(
            company=company_name,
            ticker=ticker_symbol,
            exchange=exchange_code,
            discovery_json=discovery_json,
            segment_outputs=segment_outputs,
        )
        adjudicated_json, adjudication_meta = apply_deterministic_adjudication(merged_json)
        missing_after_merge = missing_or_not_found_items(merged_json)
        missing_after_adjudication = missing_or_not_found_items(adjudicated_json)

        repair_runs: List[Dict[str, Any]] = []
        repair_errors: List[Dict[str, Any]] = []
        final_json = adjudicated_json
        final_adjudication_meta = adjudication_meta
        contamination_review: Dict[str, Any] = {}
        contamination_meta: Dict[str, Any] = {
            "enabled": bool(PERPLEXITY_MINING_CONTAMINATION_CHECKER_ENABLED),
            "applied": False,
            "drop_packet_applied": False,
            "dropped_row_count": 0,
        }

        if allow_repairs and missing_after_adjudication:
            repair_specs = segment_repairs_for_missing_items(missing_after_adjudication)
            repair_outputs: List[Dict[str, Any]] = []
            for repair_spec in repair_specs:
                repair_prompt = build_targeted_repair_prompt(
                    company=company_name,
                    ticker=ticker_symbol,
                    exchange=exchange_code,
                    source_packet=discovery_json,
                    current_slice=build_repair_context(
                        final_json,
                        categories=list(repair_spec["categories"]),
                        checklist_items=list(repair_spec["checklist_items"]),
                    ),
                    checklist_items=list(repair_spec["checklist_items"]),
                    categories=list(repair_spec["categories"]),
                )
                try:
                    repair_run = await self._run_structured_json_prompt(
                        prompt=repair_prompt,
                        preset=active_repair_preset,
                        model_override=model_override,
                        max_sources=target_sources,
                        max_output_tokens_override=2600,
                        stage_name=f"repair_{repair_spec['name']}",
                    )
                    repair_run["repair_name"] = str(repair_spec["name"])
                    repair_run["repair_checklist_items"] = list(repair_spec["checklist_items"])
                    repair_run["repair_categories"] = list(repair_spec["categories"])
                    repair_runs.append(repair_run)
                    repair_outputs.append(repair_run["parsed_json"])
                except Exception as exc:
                    repair_errors.append(
                        {
                            "repair_name": str(repair_spec["name"]),
                            "repair_checklist_items": list(repair_spec["checklist_items"]),
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    self.provider._log(
                        f"pharma_enricher repair_fail_open ticker={exchange_code}:{ticker_symbol} repair={repair_spec['name']} err={type(exc).__name__}: {exc}"
                    )

            if repair_outputs:
                merged_with_repairs = merge_segment_outputs(
                    company=company_name,
                    ticker=ticker_symbol,
                    exchange=exchange_code,
                    discovery_json=discovery_json,
                    segment_outputs=segment_outputs + repair_outputs,
                )
                final_json, final_adjudication_meta = apply_deterministic_adjudication(merged_with_repairs)

        if PERPLEXITY_MINING_CONTAMINATION_CHECKER_ENABLED:
            try:
                review_result = await self._run_contamination_check(
                    company=company_name,
                    exchange=exchange_code,
                    ticker_symbol=ticker_symbol,
                    packet_json=final_json,
                )
                contamination_review = review_result.get("review_json") or {}
                final_json, contamination_meta = apply_contamination_review(
                    final_json,
                    contamination_review,
                    min_confidence_pct=float(PERPLEXITY_MINING_CONTAMINATION_MIN_CONFIDENCE_PCT),
                )
                contamination_meta["enabled"] = True
                contamination_meta["applied"] = True
                contamination_meta["model"] = review_result.get("model")
                contamination_meta["usage"] = review_result.get("usage", {})
                contamination_meta["raw_text_chars"] = review_result.get("raw_text_chars", 0)
            except Exception as exc:
                contamination_meta = {
                    "enabled": True,
                    "applied": False,
                    "error": f"{type(exc).__name__}: {exc}",
                    "drop_packet_applied": False,
                    "dropped_row_count": 0,
                }
                self.provider._log(
                    f"pharma_enricher contamination_fail_open ticker={exchange_code}:{ticker_symbol} err={type(exc).__name__}: {exc}"
                )

        self.provider._log(
            f"pharma_enricher done ticker={exchange_code}:{ticker_symbol} missing_after_merge={len(missing_after_merge)} missing_after_final={len(missing_or_not_found_items(final_json))} elapsed={perf_counter() - run_start:.1f}s"
        )

        return {
            "provider": self.provider.name,
            "workflow": "perplexity_pharma_biotech_supplementary_v1",
            "company": company_name,
            "ticker": f"{exchange_code}:{ticker_symbol}",
            "exchange": exchange_code,
            "template_id": str(context.get("template_id") or "").strip(),
            "company_type": str(context.get("company_type") or "").strip(),
            "preset": active_preset,
            "repair_preset": active_repair_preset,
            "generated_at": datetime.utcnow().isoformat(),
            "discovery": {
                "model": discovery_run.get("model"),
                "usage": discovery_run.get("usage", {}),
                "parsed_json": discovery_json,
                "raw_text_chars": discovery_run.get("raw_text_chars", 0),
            },
            "segments": [
                {
                    "name": run.get("segment_name"),
                    "model": run.get("model"),
                    "usage": run.get("usage", {}),
                    "raw_text_chars": run.get("raw_text_chars", 0),
                    "missing_items": missing_or_not_found_items(
                        run.get("parsed_json", {}),
                        run.get("segment_checklist_items", []),
                    ),
                    "parsed_json": run.get("parsed_json", {}),
                }
                for run in segment_runs
            ],
            "repairs": [
                {
                    "name": run.get("repair_name"),
                    "model": run.get("model"),
                    "usage": run.get("usage", {}),
                    "raw_text_chars": run.get("raw_text_chars", 0),
                    "repair_checklist_items": run.get("repair_checklist_items", []),
                    "parsed_json": run.get("parsed_json", {}),
                }
                for run in repair_runs
            ],
            "repair_errors": repair_errors,
            "missing_items_after_merge": missing_after_merge,
            "missing_items_after_adjudication": missing_after_adjudication,
            "missing_items_after_final": missing_or_not_found_items(final_json),
            "adjudication": final_adjudication_meta,
            "contamination_review": contamination_review,
            "contamination_meta": contamination_meta,
            "final_json": final_json,
        }

    async def _run_contamination_check(
        self,
        *,
        company: str,
        exchange: str,
        ticker_symbol: str,
        packet_json: Dict[str, Any],
    ) -> Dict[str, Any]:
        packet_rows = flatten_packet_rows(packet_json)
        if not packet_rows:
            return {
                "model": "",
                "usage": {},
                "raw_text_chars": 0,
                "review_json": {
                    "packet_decision": "keep",
                    "packet_confidence_pct": 100,
                    "packet_reason": "No packet rows to review.",
                    "wrong_entity_detected": None,
                    "row_decisions": [],
                },
            }
        prompt = build_contamination_review_prompt(
            company=company,
            ticker=f"{exchange}:{ticker_symbol}",
            exchange=exchange,
            packet_rows=packet_rows,
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a conservative entity-contamination checker. "
                    "Return only a single JSON object. "
                    "Do not rewrite the packet. "
                    "Only flag clear wrong-company contamination."
                ),
            },
            {"role": "user", "content": prompt},
        ]
        result = await query_model(
            PERPLEXITY_MINING_CONTAMINATION_CHECKER_MODEL,
            messages,
            timeout=float(PERPLEXITY_MINING_CONTAMINATION_CHECKER_TIMEOUT_SECONDS),
            max_tokens=int(PERPLEXITY_MINING_CONTAMINATION_CHECKER_MAX_OUTPUT_TOKENS),
            reasoning_effort=PERPLEXITY_MINING_CONTAMINATION_CHECKER_REASONING_EFFORT,
        )
        if not isinstance(result, dict):
            raise RuntimeError("contamination_checker_no_response")
        parsed, err = extract_json_payload(str(result.get("content") or ""))
        if not isinstance(parsed, dict):
            raise RuntimeError(f"contamination_checker_parse_failed:{err or 'invalid_json'}")
        parsed.setdefault("packet_decision", "keep")
        parsed.setdefault("packet_confidence_pct", 0)
        parsed.setdefault("packet_reason", "")
        parsed.setdefault("wrong_entity_detected", None)
        if not isinstance(parsed.get("row_decisions"), list):
            parsed["row_decisions"] = []
        return {
            "model": PERPLEXITY_MINING_CONTAMINATION_CHECKER_MODEL,
            "usage": result.get("usage", {}) or {},
            "raw_text_chars": len(str(result.get("content") or "")),
            "review_json": parsed,
        }

    async def _run_structured_json_prompt(
        self,
        *,
        prompt: str,
        preset: str,
        stage_name: str,
        model_override: Optional[str],
        max_sources: int,
        max_output_tokens_override: int,
    ) -> Dict[str, Any]:
        payload = self.provider._build_payload(
            prompt,
            depth="deep",
            max_sources=max_sources,
            model_override=model_override,
            max_output_tokens_override=max_output_tokens_override,
            reasoning_effort_override=PERPLEXITY_MINING_ENRICHER_REASONING_EFFORT,
            preset_override=preset,
            preferred_domains=None,
        )
        payload.pop("stream", None)
        headers = {
            "Authorization": f"Bearer {self.provider.api_key}",
            "Content-Type": "application/json",
        }
        request_start = perf_counter()

        async def _post(req_payload: Dict[str, Any]) -> Dict[str, Any]:
            async with httpx.AsyncClient(timeout=self.provider.timeout_seconds) as client:
                response = await client.post(
                    PERPLEXITY_API_URL or self.provider.api_url,
                    headers=headers,
                    json=req_payload,
                )
                response.raise_for_status()
                return response.json()

        def _is_invalid_request(exc: httpx.HTTPStatusError) -> bool:
            if exc.response is None or exc.response.status_code != 400:
                return False
            body = exc.response.text.lower()
            return "invalid request" in body or "invalid_request" in body

        try:
            data = await _post(payload)
        except httpx.HTTPStatusError as exc:
            if _is_invalid_request(exc):
                retry_payload = copy.deepcopy(payload)
                retry_payload["reasoning"] = {"enabled": True, "effort": "low"}
                self.provider._log(
                    f"pharma_enricher retry stage={stage_name} reason=invalid_request fallback_reasoning=low"
                )
                data = await _post(retry_payload)
                payload = retry_payload
            else:
                raise
        except httpx.TimeoutException:
            if str(preset or "").strip().lower() == "advanced-deep-research":
                retry_payload = copy.deepcopy(payload)
                retry_payload["preset"] = "deep-research"
                self.provider._log(
                    f"pharma_enricher retry stage={stage_name} reason=timeout fallback_preset=deep-research"
                )
                data = await _post(retry_payload)
                payload = retry_payload
            else:
                raise

        raw_text = self.provider._extract_content(data)
        parsed_json, parse_error = extract_json_payload(raw_text)
        if not isinstance(parsed_json, dict):
            retry_prompt = (
                f"{prompt}\n\n"
                "IMPORTANT: Return ONLY one valid JSON object. "
                "No markdown fences. No prose before or after the JSON. "
                "Ensure all commas, quotes, and brackets are valid."
            )
            retry_payload = self.provider._build_payload(
                retry_prompt,
                depth="deep",
                max_sources=max_sources,
                model_override=model_override,
                max_output_tokens_override=max_output_tokens_override,
                reasoning_effort_override="low",
                preset_override=preset,
                preferred_domains=None,
            )
            retry_payload.pop("stream", None)
            self.provider._log(
                f"pharma_enricher retry stage={stage_name} reason=parse_error err={parse_error}"
            )
            data = await _post(retry_payload)
            payload = retry_payload
            raw_text = self.provider._extract_content(data)
            parsed_json, parse_error = extract_json_payload(raw_text)
        if not isinstance(parsed_json, dict):
            repaired = await self._repair_json_with_openrouter(
                stage_name=stage_name,
                prompt=prompt,
                raw_text=raw_text,
            )
            if isinstance(repaired, dict):
                parsed_json = repaired
                parse_error = None
        if not isinstance(parsed_json, dict):
            raise RuntimeError(
                f"stage={stage_name} could not parse JSON output: {parse_error or 'unknown_parse_error'}"
            )

        self.provider._log(
            f"pharma_enricher stage_done stage={stage_name} model={data.get('model') or payload.get('model')} chars={len(raw_text)} elapsed={perf_counter() - request_start:.1f}s"
        )
        return {
            "stage_name": stage_name,
            "model": data.get("model") or payload.get("model"),
            "usage": data.get("usage", {}),
            "preset": payload.get("preset"),
            "raw_text": raw_text,
            "raw_text_chars": len(raw_text),
            "response_payload": data,
            "parsed_json": parsed_json,
            "request_prompt_chars": len(prompt),
        }

    async def _repair_json_with_openrouter(
        self,
        *,
        stage_name: str,
        prompt: str,
        raw_text: str,
    ) -> Optional[Dict[str, Any]]:
        self.provider._log(
            f"pharma_enricher json_repair stage={stage_name} model={CHAIRMAN_JSONIFIER_MODEL}"
        )
        repair_prompt = (
            "Repair the malformed JSON-like response below into one valid JSON object.\n"
            "Do not add new facts. Do not remove supported facts. Fix formatting only.\n"
            "Return ONLY the repaired JSON object.\n\n"
            f"Original extraction instructions:\n{prompt}\n\n"
            f"Malformed JSON-like response:\n{raw_text}"
        )
        response = await query_model(
            CHAIRMAN_JSONIFIER_MODEL,
            [{"role": "user", "content": repair_prompt}],
            timeout=float(CHAIRMAN_JSONIFIER_TIMEOUT_SECONDS),
            max_tokens=(
                int(CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS)
                if int(CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS) > 0
                else None
            ),
            reasoning_effort="low",
        )
        if not isinstance(response, dict):
            return None
        repaired, _ = extract_json_payload(str(response.get("content") or ""))
        return repaired if isinstance(repaired, dict) else None
