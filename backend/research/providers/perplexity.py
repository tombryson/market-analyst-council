"""Perplexity-backed research provider."""

import asyncio
import copy
import json
import os
import re
import tempfile
from collections import Counter
from datetime import datetime
from html import unescape
from time import perf_counter
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx

from .base import ResearchProvider
from ...config import (
    PERPLEXITY_API_KEY,
    PERPLEXITY_API_URL,
    PERPLEXITY_MODEL,
    PERPLEXITY_PRESET,
    PERPLEXITY_STREAM_ENABLED,
    PERPLEXITY_SEARCH_MODE,
    PERPLEXITY_USE_LEGACY_TOOL_FILTER_FALLBACK,
    PERPLEXITY_TIMEOUT_SECONDS,
    PERPLEXITY_MAX_STEPS,
    PERPLEXITY_MAX_OUTPUT_TOKENS,
    PERPLEXITY_REASONING_EFFORT,
    PERPLEXITY_ENABLE_WEB_SEARCH_TOOL,
    PERPLEXITY_ENABLE_FETCH_URL_TOOL,
    PERPLEXITY_MAX_RESULTS_PER_QUERY,
    PERPLEXITY_MAX_TOKENS_PER_PAGE,
    PERPLEXITY_ALLOWED_DOMAINS,
    PERPLEXITY_BLOCKED_DOMAINS,
    PERPLEXITY_SEARCH_AFTER_DATE_FILTER,
    PERPLEXITY_SEARCH_BEFORE_DATE_FILTER,
    ENABLE_SOURCE_DECODING,
    SOURCE_DECODING_MAX_PER_MODEL,
    SOURCE_DECODING_MAX_CHARS,
    SOURCE_DECODING_TIMEOUT_SECONDS,
    PROGRESS_LOGGING,
)


class PerplexityResearchProvider(ResearchProvider):
    """
    Fetches web-grounded research using Perplexity Responses API.

    Uses preset + explicit tool configuration so we can run deep/baseline
    retrieval with consistent controls from env.
    """

    name = "perplexity"

    def _log(self, message: str) -> None:
        """Timestamped provider logs for long-running deep research."""
        if not PROGRESS_LOGGING:
            return
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}][perplexity] {message}", flush=True)

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_url: str = PERPLEXITY_API_URL,
        model: str = PERPLEXITY_MODEL,
        preset: str = PERPLEXITY_PRESET,
    ):
        self.api_key = api_key or PERPLEXITY_API_KEY
        self.api_url = api_url
        self.model = model
        self.preset = preset
        self.timeout_seconds = PERPLEXITY_TIMEOUT_SECONDS
        self.max_steps = PERPLEXITY_MAX_STEPS
        self.max_output_tokens = PERPLEXITY_MAX_OUTPUT_TOKENS
        self.reasoning_effort = PERPLEXITY_REASONING_EFFORT
        self.stream_enabled = bool(PERPLEXITY_STREAM_ENABLED)
        self.search_mode = str(PERPLEXITY_SEARCH_MODE or "standard").strip().lower()
        self.legacy_tool_filter_fallback = bool(PERPLEXITY_USE_LEGACY_TOOL_FILTER_FALLBACK)

    async def gather(
        self,
        user_query: str,
        ticker: Optional[str] = None,
        depth: str = "basic",
        max_sources: int = 10,
        model_override: Optional[str] = None,
        research_brief: str = "",
        max_steps_override: Optional[int] = None,
        max_output_tokens_override: Optional[int] = None,
        reasoning_effort_override: Optional[str] = None,
        preset_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run a grounded Perplexity research call and normalize sources."""
        run_start = perf_counter()
        selected_model = model_override.strip() if model_override else self.model
        effective_max_steps = (
            int(max_steps_override)
            if isinstance(max_steps_override, int) and max_steps_override > 0
            else int(self.max_steps)
        )
        effective_max_output_tokens = (
            int(max_output_tokens_override)
            if isinstance(max_output_tokens_override, int) and max_output_tokens_override > 0
            else int(self.max_output_tokens)
        )
        effective_reasoning_effort = (
            str(reasoning_effort_override).strip().lower()
            if isinstance(reasoning_effort_override, str) and reasoning_effort_override.strip()
            else str(self.reasoning_effort).strip().lower()
        )
        token_cap_log = (
            str(effective_max_output_tokens)
            if int(effective_max_output_tokens) > 0
            else "provider_default"
        )
        self._log(
            f"gather start model={selected_model} depth={depth} "
            f"max_sources={max_sources} max_steps={effective_max_steps} "
            f"max_output_tokens={token_cap_log} "
            f"reasoning_effort={effective_reasoning_effort or 'none'} "
            f"stream={self.stream_enabled} search_mode={self.search_mode} "
            f"timeout={self.timeout_seconds}s"
        )
        if not self.api_key:
            self._log(f"gather abort model={selected_model} reason=missing_api_key")
            return {
                "error": "Perplexity API key not configured",
                "results": [],
                "result_count": 0,
                "provider": self.name,
            }

        prompt = self._build_prompt(
            user_query=user_query,
            ticker=ticker,
            depth=depth,
            max_sources=max_sources,
            research_brief=research_brief,
        )
        decode_query_context = "\n".join(
            [
                str(ticker or "").strip(),
                str(user_query or "").strip(),
                str(research_brief or "").strip(),
            ]
        ).strip()
        payload = self._build_payload(
            prompt,
            depth=depth,
            max_sources=max_sources,
            model_override=model_override,
            max_steps_override=effective_max_steps,
            max_output_tokens_override=effective_max_output_tokens,
            reasoning_effort_override=effective_reasoning_effort,
            preset_override=preset_override,
        )

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        reasoning_retry_applied = "none"
        request_attempts = 0
        stream_mode_used = False
        stream_event_count = 0
        stream_delta_chars = 0
        stream_completed_event_seen = False
        stream_empty_retry_applied = False
        legacy_tool_filter_retry_applied = False
        timeout_retry_applied = "none"

        async def _post_once(client: httpx.AsyncClient, req_payload: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal request_attempts
            request_attempts += 1
            response = await client.post(
                self.api_url,
                headers=headers,
                json=req_payload,
            )
            response.raise_for_status()
            return response.json()

        async def _post_stream(
            client: httpx.AsyncClient,
            req_payload: Dict[str, Any],
        ) -> Dict[str, Any]:
            nonlocal request_attempts
            nonlocal stream_mode_used
            nonlocal stream_event_count
            nonlocal stream_delta_chars
            nonlocal stream_completed_event_seen
            request_attempts += 1
            stream_mode_used = True
            text_deltas: List[str] = []
            final_response: Dict[str, Any] = {}

            async with client.stream(
                "POST",
                self.api_url,
                headers=headers,
                json=req_payload,
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    chunk = line.strip()
                    if not chunk or chunk.startswith(":"):
                        continue
                    if chunk.startswith("event:"):
                        continue
                    if chunk.startswith("data:"):
                        chunk = chunk[5:].strip()
                    if not chunk or chunk == "[DONE]":
                        continue

                    try:
                        event = json.loads(chunk)
                    except Exception:
                        continue

                    stream_event_count += 1
                    event_type = str(event.get("type", "")).strip().lower()
                    if event_type in {"response.output_text.delta", "output_text.delta"}:
                        delta = event.get("delta")
                        if isinstance(delta, str) and delta:
                            text_deltas.append(delta)
                            stream_delta_chars += len(delta)

                    if event_type in {"response.completed", "completed"}:
                        stream_completed_event_seen = True
                        response_obj = event.get("response")
                        if isinstance(response_obj, dict):
                            final_response = response_obj
                    elif event_type in {"response", "output"} and isinstance(event, dict):
                        # Some streaming variants emit response-shaped payload chunks.
                        final_response = event

            merged_text = "".join(text_deltas).strip()
            if final_response:
                final_text = self._extract_content(final_response).strip()
                if merged_text:
                    # Some providers emit a partial terminal payload (tail fragment)
                    # while the stream deltas contain the full response body.
                    if (
                        not final_text
                        or len(final_text) < max(120, int(len(merged_text) * 0.85))
                    ):
                        final_response["output_text"] = merged_text
                return final_response

            if merged_text:
                return {
                    "output_text": merged_text,
                    "output": [{"type": "output_text", "text": merged_text}],
                }
            raise RuntimeError("Perplexity stream ended without response payload or text")

        async def _post_request(
            client: httpx.AsyncClient,
            req_payload: Dict[str, Any],
        ) -> Dict[str, Any]:
            nonlocal stream_empty_retry_applied
            if bool(req_payload.get("stream")):
                try:
                    return await _post_stream(client, req_payload)
                except RuntimeError as exc:
                    # Some provider/model combinations emit sparse stream events and no
                    # terminal response object. Retry once in non-stream mode.
                    if "stream ended without response payload or text" in str(exc).lower():
                        retry_payload = copy.deepcopy(req_payload)
                        retry_payload.pop("stream", None)
                        stream_empty_retry_applied = True
                        self._log(
                            "api retry reason=empty_stream_payload fallback=stream_off"
                        )
                        return await _post_once(client, retry_payload)
                    raise
            return await _post_once(client, req_payload)

        async def _safe_response_text(response: Optional[httpx.Response]) -> str:
            """Read response text safely for both normal and streaming HTTP errors."""
            if response is None:
                return ""
            try:
                return response.text
            except httpx.ResponseNotRead:
                try:
                    raw = await response.aread()
                    encoding = response.encoding or "utf-8"
                    return raw.decode(encoding, errors="replace")
                except Exception:
                    return ""
            except Exception:
                try:
                    raw = await response.aread()
                    encoding = response.encoding or "utf-8"
                    return raw.decode(encoding, errors="replace")
                except Exception:
                    return ""

        def _is_invalid_request(status_code: Optional[int], body_text: str) -> bool:
            if status_code != 400:
                return False
            text = (body_text or "").lower()
            return "invalid request" in text or "\"invalid_request\"" in text

        request_start = perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                try:
                    data = await _post_request(client, payload)
                    self._log(
                        f"api success model={selected_model} status=200 "
                        f"elapsed={perf_counter() - request_start:.1f}s attempts={request_attempts}"
                    )
                except httpx.HTTPStatusError as first_exc:
                    first_status = first_exc.response.status_code if first_exc.response else None
                    first_body = await _safe_response_text(first_exc.response)

                    # Per-model compatibility fallback:
                    # Some models reject medium/high reasoning effort with a generic 400 invalid request.
                    if _is_invalid_request(first_status, first_body):
                        retried = False
                        if (
                            self.legacy_tool_filter_fallback
                            and self._payload_has_tool_filter_blocks(payload)
                        ):
                            legacy_payload = self._payload_with_legacy_tool_filters(payload)
                            if legacy_payload != payload:
                                self._log(
                                    f"api retry model={selected_model} reason=invalid_request "
                                    "fallback_tool_filters=legacy_keys"
                                )
                                try:
                                    data = await _post_request(client, legacy_payload)
                                    payload = legacy_payload
                                    retried = True
                                    legacy_tool_filter_retry_applied = True
                                    self._log(
                                        f"api retry success model={selected_model} status=200 "
                                        f"elapsed={perf_counter() - request_start:.1f}s attempts={request_attempts}"
                                    )
                                except httpx.HTTPStatusError as retry_exc:
                                    first_exc = retry_exc
                                    first_status = (
                                        retry_exc.response.status_code
                                        if retry_exc.response
                                        else None
                                    )
                                    first_body = await _safe_response_text(retry_exc.response)

                        retry_payload = copy.deepcopy(payload)
                        retry_reasoning = retry_payload.get("reasoning")
                        retry_effort = (
                            retry_reasoning.get("effort")
                            if isinstance(retry_reasoning, dict)
                            else None
                        )

                        if retry_effort and retry_effort != "low":
                            retry_payload["reasoning"] = {"effort": "low"}
                            reasoning_retry_applied = "low"
                            self._log(
                                f"api retry model={selected_model} reason=invalid_request "
                                "fallback_reasoning=low"
                            )
                            try:
                                data = await _post_request(client, retry_payload)
                                payload = retry_payload
                                retried = True
                                self._log(
                                    f"api retry success model={selected_model} status=200 "
                                    f"elapsed={perf_counter() - request_start:.1f}s attempts={request_attempts}"
                                )
                            except httpx.HTTPStatusError as retry_exc:
                                first_exc = retry_exc
                                first_status = retry_exc.response.status_code if retry_exc.response else None
                                first_body = await _safe_response_text(retry_exc.response)

                        if (not retried) and ("reasoning" in retry_payload):
                            retry_payload.pop("reasoning", None)
                            reasoning_retry_applied = (
                                "dropped_after_low_failed"
                                if reasoning_retry_applied == "low"
                                else "dropped"
                            )
                            self._log(
                                f"api retry model={selected_model} reason=invalid_request "
                                "fallback_reasoning=off"
                            )
                            try:
                                data = await _post_request(client, retry_payload)
                                payload = retry_payload
                                retried = True
                                self._log(
                                    f"api retry success model={selected_model} status=200 "
                                    f"elapsed={perf_counter() - request_start:.1f}s attempts={request_attempts}"
                                )
                            except httpx.HTTPStatusError as retry_exc:
                                first_exc = retry_exc
                                first_status = retry_exc.response.status_code if retry_exc.response else None
                                first_body = await _safe_response_text(retry_exc.response)

                        if not retried:
                            raise first_exc
                    else:
                        raise first_exc
        except httpx.TimeoutException:
            timeout_elapsed = perf_counter() - request_start
            self._log(
                f"api timeout model={selected_model} elapsed={timeout_elapsed:.1f}s"
            )

            retry_payload: Optional[Dict[str, Any]] = None
            retry_tags: List[str] = []
            active_preset = str(payload.get("preset", "") or "").strip().lower()
            stream_enabled = bool(payload.get("stream"))

            if stream_enabled:
                retry_payload = copy.deepcopy(payload)
                retry_payload.pop("stream", None)
                retry_tags.append("stream_off")

            if active_preset == "advanced-deep-research":
                if retry_payload is None:
                    retry_payload = copy.deepcopy(payload)
                retry_payload["preset"] = "deep-research"
                retry_payload.pop("stream", None)
                retry_tags.append("preset_deep-research")

            if retry_payload and retry_payload != payload:
                timeout_retry_applied = "+".join(retry_tags) if retry_tags else "fallback"
                self._log(
                    f"api retry model={selected_model} reason=timeout "
                    f"fallback={timeout_retry_applied}"
                )
                try:
                    async with httpx.AsyncClient(timeout=self.timeout_seconds) as retry_client:
                        data = await _post_request(retry_client, retry_payload)
                        payload = retry_payload
                        self._log(
                            f"api retry success model={selected_model} status=200 "
                            f"elapsed={perf_counter() - request_start:.1f}s attempts={request_attempts}"
                        )
                except httpx.TimeoutException:
                    self._log(
                        f"api timeout model={selected_model} elapsed={perf_counter() - request_start:.1f}s "
                        f"after_fallback={timeout_retry_applied}"
                    )
                    return {
                        "error": "Perplexity request timed out",
                        "results": [],
                        "result_count": 0,
                        "provider": self.name,
                    }
                except httpx.HTTPStatusError as exc:
                    body = (await _safe_response_text(exc.response))[:500]
                    self._log(
                        f"api http_error model={selected_model} status="
                        f"{exc.response.status_code if exc.response else 'unknown'} "
                        f"elapsed={perf_counter() - request_start:.1f}s "
                        f"after_fallback={timeout_retry_applied}"
                    )
                    return {
                        "error": (
                            f"Perplexity API error: {exc.response.status_code if exc.response else 'unknown'} "
                            f"{body}"
                        ),
                        "results": [],
                        "result_count": 0,
                        "provider": self.name,
                    }
                except Exception as exc:
                    self._log(
                        f"api failure model={selected_model} type={type(exc).__name__} "
                        f"elapsed={perf_counter() - request_start:.1f}s "
                        f"after_fallback={timeout_retry_applied}"
                    )
                    return {
                        "error": f"Perplexity research failed: {str(exc)}",
                        "results": [],
                        "result_count": 0,
                        "provider": self.name,
                    }
            else:
                return {
                    "error": "Perplexity request timed out",
                    "results": [],
                    "result_count": 0,
                    "provider": self.name,
                }
        except httpx.HTTPStatusError as exc:
            body = (await _safe_response_text(exc.response))[:500]
            self._log(
                f"api http_error model={selected_model} status="
                f"{exc.response.status_code if exc.response else 'unknown'} "
                f"elapsed={perf_counter() - request_start:.1f}s"
            )
            return {
                "error": (
                    f"Perplexity API error: {exc.response.status_code if exc.response else 'unknown'} "
                    f"{body}"
                ),
                "results": [],
                "result_count": 0,
                "provider": self.name,
            }
        except Exception as exc:
            self._log(
                f"api failure model={selected_model} type={type(exc).__name__} "
                f"elapsed={perf_counter() - request_start:.1f}s"
            )
            return {
                "error": f"Perplexity research failed: {str(exc)}",
                "results": [],
                "result_count": 0,
                "provider": self.name,
            }

        raw_summary = self._extract_content(data)
        source_candidates = self._extract_source_candidates(data, raw_summary)
        self._log(
            f"candidate extraction model={selected_model} candidates={len(source_candidates)}"
        )
        entity_terms = self._build_entity_terms(
            ticker=ticker,
            user_query=user_query,
            research_brief=research_brief,
        )
        results = self._candidates_to_results(
            source_candidates,
            max_sources,
            entity_terms=entity_terms,
        )
        self._log(
            f"ranking complete model={selected_model} ranked_results={len(results)}"
        )
        decode_report: Dict[str, Any] = {}
        if ENABLE_SOURCE_DECODING and results:
            decode_start = perf_counter()
            self._log(
                f"decode start model={selected_model} "
                f"target={min(len(results), max(0, int(SOURCE_DECODING_MAX_PER_MODEL)))}"
            )
            decoded_map, decode_report = await self._decode_ranked_sources(
                results,
                query_context=decode_query_context,
            )
            if decoded_map:
                results = self._merge_decoded_content(results, decoded_map)
            self._log(
                f"decode complete model={selected_model} "
                f"decoded={decode_report.get('decoded', 0)}/{decode_report.get('attempted', 0)} "
                f"failed={decode_report.get('failed', 0)} elapsed={perf_counter() - decode_start:.1f}s"
            )
        else:
            decode_report = {
                "enabled": bool(ENABLE_SOURCE_DECODING),
                "attempted": 0,
                "decoded": 0,
                "failed": 0,
                "sources": [],
            }
        latest_updates = self._build_latest_updates(results, max_rows=min(max_sources, 8))
        summary = self._normalize_research_summary(
            raw_summary=raw_summary,
            user_query=user_query,
            ticker=ticker,
            latest_updates=latest_updates,
        )
        self._log(
            f"gather done model={selected_model} result_count={len(results)} "
            f"elapsed={perf_counter() - run_start:.1f}s"
        )

        return {
            "query": user_query,
            "research_prompt": prompt,
            "results": results,
            "result_count": len(results),
            "performed_at": datetime.utcnow().isoformat(),
            "search_type": "perplexity_research",
            "provider": self.name,
            "research_summary": summary,
            "latest_updates": latest_updates,
            "provider_metadata": {
                "api_url": self.api_url,
                "model": payload.get("model"),
                "preset": payload.get("preset"),
                "search_mode": self.search_mode,
                "search_type": payload.get("search_type", ""),
                "tools": [tool.get("type", "unknown") for tool in payload.get("tools", [])],
                "tool_filters_mode": (
                    "filters"
                    if self._payload_has_tool_filter_blocks(payload)
                    else (
                        "legacy_keys"
                        if self._payload_has_legacy_tool_filter_keys(payload)
                        else "none"
                    )
                ),
                "max_steps": payload.get("max_steps"),
                "max_output_tokens": payload.get("max_output_tokens"),
                "max_sources": max_sources,
                "research_prompt_chars": len(prompt),
                "research_brief_chars": len(research_brief or ""),
                "raw_summary_chars": len(raw_summary or ""),
                "raw_summary_preview": (raw_summary or "")[:280],
                "entity_terms": entity_terms,
                "source_decoding": decode_report,
                "source_decoding_excerpt_strategy": "query_aware_chunk_scoring_v1",
                "source_decoding_query_terms": int(
                    len(self._extract_query_terms(decode_query_context))
                ),
                "request_attempts": request_attempts,
                "stream_requested": bool(payload.get("stream", False)),
                "stream_used": bool(stream_mode_used),
                "stream_event_count": int(stream_event_count),
                "stream_delta_chars": int(stream_delta_chars),
                "stream_completed_event_seen": bool(stream_completed_event_seen),
                "stream_empty_retry_applied": bool(stream_empty_retry_applied),
                "legacy_tool_filter_retry_applied": bool(legacy_tool_filter_retry_applied),
                "timeout_retry_applied": timeout_retry_applied,
                "reasoning_retry_applied": reasoning_retry_applied,
                "reasoning_effort_applied": (
                    payload.get("reasoning", {}).get("effort")
                    if isinstance(payload.get("reasoning"), dict)
                    else ""
                ),
            },
        }

    def _build_payload(
        self,
        prompt: str,
        depth: str,
        max_sources: int,
        model_override: Optional[str] = None,
        max_steps_override: Optional[int] = None,
        max_output_tokens_override: Optional[int] = None,
        reasoning_effort_override: Optional[str] = None,
        preset_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build Responses API payload with preset + explicit tool controls."""
        selected_model = model_override.strip() if model_override else self.model
        max_steps = (
            int(max_steps_override)
            if isinstance(max_steps_override, int) and max_steps_override > 0
            else int(self.max_steps)
        )
        max_output_tokens = (
            int(max_output_tokens_override)
            if isinstance(max_output_tokens_override, int) and max_output_tokens_override > 0
            else int(self.max_output_tokens)
        )
        reasoning_effort = (
            str(reasoning_effort_override).strip().lower()
            if isinstance(reasoning_effort_override, str) and reasoning_effort_override.strip()
            else str(self.reasoning_effort).strip().lower()
        )
        payload: Dict[str, Any] = {
            "input": prompt,
            "model": selected_model,
            "max_steps": max_steps,
            "parallel_tool_calls": True,
            "tools": self._build_tools(
                depth=depth,
                max_sources=max_sources,
                use_filters=True,
            ),
        }
        if max_output_tokens > 0:
            payload["max_output_tokens"] = max_output_tokens
        if self.stream_enabled:
            payload["stream"] = True

        chosen_preset = self._select_preset(depth, preset_override=preset_override)
        if chosen_preset:
            payload["preset"] = chosen_preset

        search_type = self._resolve_search_type_for_model(selected_model)
        if search_type:
            payload["search_type"] = search_type

        if reasoning_effort in {"low", "medium", "high"}:
            payload["reasoning"] = {"effort": reasoning_effort}

        return payload

    def _select_preset(self, depth: str, preset_override: Optional[str] = None) -> str:
        """Choose preset based on depth and env preference."""
        active_preset = str(preset_override or self.preset or "").strip()
        if (
            depth == "deep"
            and self.search_mode == "pro"
            and (not active_preset or active_preset in {"deep-research", "search"})
        ):
            # Explicit opt-in for Pro Search behavior where supported.
            return "pro-search"
        if active_preset:
            if depth == "deep":
                return active_preset
            if active_preset == "deep-research":
                return "search"
            return active_preset
        return "deep-research" if depth == "deep" else "search"

    def _build_tools(
        self,
        depth: str,
        max_sources: int,
        *,
        use_filters: bool,
    ) -> List[Dict[str, Any]]:
        """Build explicit tool settings for research runs."""
        tools: List[Dict[str, Any]] = []

        if PERPLEXITY_ENABLE_WEB_SEARCH_TOOL:
            web_search: Dict[str, Any] = {
                "type": "web_search",
                "max_results_per_query": min(
                    max_sources,
                    max(1, PERPLEXITY_MAX_RESULTS_PER_QUERY),
                ),
                "max_tokens_per_page": max(256, PERPLEXITY_MAX_TOKENS_PER_PAGE),
            }
            if use_filters:
                filters = self._build_web_search_filters()
                if filters:
                    web_search["filters"] = filters
            else:
                if PERPLEXITY_ALLOWED_DOMAINS:
                    web_search["allowed_domains"] = PERPLEXITY_ALLOWED_DOMAINS
                if PERPLEXITY_BLOCKED_DOMAINS:
                    web_search["blocked_domains"] = PERPLEXITY_BLOCKED_DOMAINS
                if PERPLEXITY_SEARCH_AFTER_DATE_FILTER:
                    web_search["search_after_date_filter"] = PERPLEXITY_SEARCH_AFTER_DATE_FILTER
                if PERPLEXITY_SEARCH_BEFORE_DATE_FILTER:
                    web_search["search_before_date_filter"] = PERPLEXITY_SEARCH_BEFORE_DATE_FILTER
            tools.append(web_search)

        if PERPLEXITY_ENABLE_FETCH_URL_TOOL:
            tools.append({"type": "fetch_url"})

        return tools

    def _build_web_search_filters(self) -> Dict[str, Any]:
        """Build documented `tools[].filters` payload for web search tool."""
        filters: Dict[str, Any] = {}

        domain_filters: List[str] = []
        if PERPLEXITY_ALLOWED_DOMAINS:
            domain_filters.extend([item for item in PERPLEXITY_ALLOWED_DOMAINS if item])
        if PERPLEXITY_BLOCKED_DOMAINS:
            domain_filters.extend([f"-{item}" for item in PERPLEXITY_BLOCKED_DOMAINS if item])

        if domain_filters:
            filters["search_domain_filter"] = domain_filters

        after_value = self._normalize_filter_date(PERPLEXITY_SEARCH_AFTER_DATE_FILTER)
        if after_value:
            filters["search_after_date_filter"] = after_value

        before_value = self._normalize_filter_date(PERPLEXITY_SEARCH_BEFORE_DATE_FILTER)
        if before_value:
            filters["search_before_date_filter"] = before_value

        return filters

    def _normalize_filter_date(self, value: str) -> str:
        """
        Normalize date filters to provider-friendly format.

        Accepts YYYY-MM-DD or MM/DD/YYYY and emits M/D/YYYY.
        """
        raw = str(value or "").strip()
        if not raw:
            return ""

        iso_match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
        if iso_match:
            try:
                year = int(iso_match.group(1))
                month = int(iso_match.group(2))
                day = int(iso_match.group(3))
                return f"{month}/{day}/{year}"
            except Exception:
                return raw

        slash_match = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw)
        if slash_match:
            try:
                month = int(slash_match.group(1))
                day = int(slash_match.group(2))
                year = int(slash_match.group(3))
                return f"{month}/{day}/{year}"
            except Exception:
                return raw

        return raw

    def _payload_has_tool_filter_blocks(self, payload: Dict[str, Any]) -> bool:
        tools = payload.get("tools", [])
        if not isinstance(tools, list):
            return False
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            if tool.get("type") == "web_search" and isinstance(tool.get("filters"), dict):
                return True
        return False

    def _payload_has_legacy_tool_filter_keys(self, payload: Dict[str, Any]) -> bool:
        tools = payload.get("tools", [])
        if not isinstance(tools, list):
            return False
        for tool in tools:
            if not isinstance(tool, dict) or tool.get("type") != "web_search":
                continue
            if any(
                key in tool
                for key in (
                    "allowed_domains",
                    "blocked_domains",
                    "search_after_date_filter",
                    "search_before_date_filter",
                )
            ):
                return True
        return False

    def _payload_with_legacy_tool_filters(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert `tools[].filters` to legacy web_search keys for compatibility fallback.
        """
        retry_payload = copy.deepcopy(payload)
        tools = retry_payload.get("tools", [])
        if not isinstance(tools, list):
            return retry_payload

        for tool in tools:
            if not isinstance(tool, dict) or tool.get("type") != "web_search":
                continue
            filters = tool.pop("filters", None)
            if not isinstance(filters, dict):
                continue
            domain_filters = filters.get("search_domain_filter")
            if isinstance(domain_filters, list):
                allowed_domains = []
                blocked_domains = []
                for item in domain_filters:
                    value = str(item or "").strip()
                    if not value:
                        continue
                    if value.startswith("-"):
                        blocked_domains.append(value[1:])
                    else:
                        allowed_domains.append(value)
                if allowed_domains:
                    tool["allowed_domains"] = allowed_domains
                if blocked_domains:
                    tool["blocked_domains"] = blocked_domains

            after_date = str(filters.get("search_after_date_filter", "")).strip()
            if after_date:
                tool["search_after_date_filter"] = after_date

            before_date = str(filters.get("search_before_date_filter", "")).strip()
            if before_date:
                tool["search_before_date_filter"] = before_date

        return retry_payload

    def _resolve_search_type_for_model(self, model: str) -> str:
        """
        Map env search mode into API search_type when likely supported.

        Pro Search is currently Sonar-oriented and requires streaming.
        """
        mode = str(self.search_mode or "standard").strip().lower()
        if mode not in {"standard", "pro"}:
            return ""
        if mode != "pro":
            return ""
        if not self.stream_enabled:
            return ""
        model_key = str(model or "").strip().lower()
        if "sonar" not in model_key:
            return ""
        return "pro"

    def _build_prompt(
        self,
        user_query: str,
        ticker: Optional[str],
        depth: str,
        max_sources: int,
        research_brief: str = "",
    ) -> str:
        ticker_line = (
            f"Ticker focus: {ticker}\n"
            if ticker
            else "Ticker focus: infer from question if possible\n"
        )
        depth_line = (
            "Deep mode: prioritize comprehensive and recent primary sources."
            if depth == "deep"
            else "Basic mode: prioritize concise high-signal sources."
        )
        brief_block = ""
        if research_brief:
            brief_block = (
                "\nAnalysis framework and scoring requirements to honor:\n"
                f"{research_brief.strip()}\n"
            )

        return (
            "Research the user question with strong source coverage.\n"
            f"{ticker_line}"
            f"{depth_line}\n"
            f"Target source count: up to {max_sources}.\n"
            "Prioritize primary sources first (exchange filings, official announcements, "
            "company investor documents). Use secondary commentary only when needed.\n\n"
            "Avoid low-information legal/admin notices unless directly relevant to the user task "
            "(examples: 708A cleansing notices, Appendix 2A/3B/3C quotation notices).\n"
            "Do not optimize for latest notice recency alone; prioritize valuation-relevant filings even if slightly older.\n"
            "Prefer valuation-relevant filings/materials (DFS/PFS/FS, quarterly/annual reports, "
            "investor/corporate presentations, project/funding updates).\n\n"
            f"{brief_block}\n"
            f"User question: {user_query}\n\n"
            "Output:\n"
            "1) full investment analysis aligned to the analysis framework (not a source log)\n"
            "2) explicit scoring-relevant evidence and calculations where requested by framework\n"
            "3) table: Date | Update | Why it matters | Source URL\n"
            "4) explicit unknowns/gaps and assumptions"
        )

    def _extract_content(self, data: Dict[str, Any]) -> str:
        """Extract assistant text from Responses API and legacy shapes."""
        text_parts: List[str] = []
        output_text_fallback = ""

        output_text = data.get("output_text")
        if isinstance(output_text, str):
            output_text_fallback = output_text.strip()
        elif isinstance(output_text, list):
            output_text_fallback = "\n".join(
                [str(item).strip() for item in output_text if isinstance(item, str) and item.strip()]
            ).strip()

        # Responses API: output -> message/content and top-level output_text items.
        output = data.get("output", [])
        if isinstance(output, list):
            for item in output:
                if not isinstance(item, dict):
                    continue

                if item.get("type") in {"output_text", "text"} and item.get("text"):
                    text_parts.append(str(item["text"]))

                content = item.get("content")
                if isinstance(content, str) and content.strip():
                    text_parts.append(content)
                elif isinstance(content, dict):
                    text = content.get("text")
                    if isinstance(text, str) and text.strip():
                        text_parts.append(text)
                elif isinstance(content, list):
                    for chunk in content:
                        if isinstance(chunk, str) and chunk.strip():
                            text_parts.append(chunk)
                            continue
                        if not isinstance(chunk, dict):
                            continue
                        if chunk.get("type") in {"output_text", "text"} and chunk.get("text"):
                            text_parts.append(str(chunk["text"]))
                        elif chunk.get("text"):
                            text_parts.append(str(chunk["text"]))

        # Prefer output_text when it is materially richer than parsed output text.
        if text_parts and output_text_fallback:
            joined = "\n".join([part for part in text_parts if part]).strip()
            if len(output_text_fallback) > max(120, int(len(joined) * 1.15)):
                return output_text_fallback

        # Some responses include output_text convenience field.
        if not text_parts and output_text_fallback:
            text_parts.append(output_text_fallback)

        # Legacy chat-completions fallback shape.
        if not text_parts:
            choices = data.get("choices", [])
            if choices:
                message = choices[0].get("message", {})
                content = message.get("content", "")
                if isinstance(content, str):
                    text_parts.append(content)
                elif isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict) and item.get("text"):
                            text_parts.append(str(item["text"]))
                        elif isinstance(item, str):
                            text_parts.append(item)

        # Some wrappers place a response object around the payload.
        if not text_parts:
            wrapped = data.get("response")
            if isinstance(wrapped, dict):
                text_parts.append(self._extract_content(wrapped))

        return "\n".join([part for part in text_parts if part]).strip()

    def _extract_source_candidates(self, data: Dict[str, Any], summary: str) -> List[Dict[str, Any]]:
        """
        Extract source candidates from multiple response locations.

        Sources can appear in:
        - content annotations (citation spans)
        - search_results / citations / references blocks
        - output items with tool results
        - raw URLs present in text output
        """
        candidates: List[Dict[str, Any]] = []

        # Parse citation annotations with optional snippets.
        output = data.get("output", [])
        if isinstance(output, list):
            for item in output:
                if not isinstance(item, dict) or item.get("type") != "message":
                    continue
                content = item.get("content", [])
                if not isinstance(content, list):
                    continue
                for chunk in content:
                    if not isinstance(chunk, dict):
                        continue
                    chunk_text = str(chunk.get("text", ""))
                    annotations = chunk.get("annotations", [])
                    if not isinstance(annotations, list):
                        continue
                    for annotation in annotations:
                        parsed = self._source_from_annotation(annotation, chunk_text)
                        if parsed:
                            candidates.append(parsed)

        # Structured source containers from top-level and nested output items.
        for key in ("search_results", "citations", "references", "sources"):
            if key in data:
                candidates.extend(self._collect_source_like_entries(data[key]))

        if isinstance(output, list):
            for item in output:
                candidates.extend(self._collect_source_like_entries(item))

        # Fallback: raw URLs from summary text.
        for url in re.findall(r"https?://[^\s\)\]]+", summary):
            cleaned = self._clean_url(url)
            if cleaned:
                candidates.append({"url": cleaned, "score": 0.5})

        return candidates

    def _source_from_annotation(self, annotation: Any, chunk_text: str) -> Optional[Dict[str, Any]]:
        """Parse one annotation object into normalized source candidate."""
        if not isinstance(annotation, dict):
            return None

        url = self._extract_url_from_obj(annotation)
        if not url:
            return None

        snippet = ""
        start_idx = annotation.get("start_index")
        end_idx = annotation.get("end_index")
        if isinstance(start_idx, int) and isinstance(end_idx, int) and chunk_text:
            left = max(0, start_idx - 80)
            right = min(len(chunk_text), end_idx + 80)
            snippet = chunk_text[left:right].strip()
        if not snippet:
            snippet = str(annotation.get("snippet", "")).strip()

        return {
            "url": url,
            "title": annotation.get("title", "") or annotation.get("source", ""),
            "snippet": snippet,
            "published_at": annotation.get("published_at", "") or annotation.get("date", ""),
            "score": 1.0,
        }

    def _collect_source_like_entries(self, obj: Any) -> List[Dict[str, Any]]:
        """
        Recursively collect entries that look like citation/search sources.

        This is intentionally permissive because provider payload formats can vary.
        """
        found: List[Dict[str, Any]] = []

        if isinstance(obj, str):
            cleaned = self._clean_url(obj)
            if cleaned:
                found.append({"url": cleaned, "score": 0.4})
            return found

        if isinstance(obj, list):
            for item in obj:
                found.extend(self._collect_source_like_entries(item))
            return found

        if not isinstance(obj, dict):
            return found

        direct_url = self._extract_url_from_obj(obj)
        if direct_url:
            found.append(
                {
                    "url": direct_url,
                    "title": obj.get("title", "") or obj.get("name", ""),
                    "snippet": (
                        obj.get("snippet", "")
                        or obj.get("content", "")
                        or obj.get("summary", "")
                        or obj.get("description", "")
                    ),
                    "published_at": (
                        obj.get("published_at", "")
                        or obj.get("date", "")
                        or obj.get("published", "")
                    ),
                    "score": float(obj.get("score", 0.8)),
                }
            )

        for value in obj.values():
            if isinstance(value, (dict, list, str)):
                found.extend(self._collect_source_like_entries(value))

        return found

    def _extract_url_from_obj(self, obj: Dict[str, Any]) -> str:
        """Find URL-like fields in a source object."""
        for key in ("url", "link", "source", "uri", "href"):
            value = obj.get(key)
            if isinstance(value, str):
                cleaned = self._clean_url(value)
                if cleaned:
                    return cleaned
        return ""

    def _candidates_to_results(
        self,
        candidates: List[Dict[str, Any]],
        max_sources: int,
        *,
        entity_terms: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Convert source candidates to legacy-compatible result entries."""
        merged: Dict[str, Dict[str, Any]] = {}

        for candidate in candidates:
            url = self._clean_url(str(candidate.get("url", "")))
            if not url:
                continue

            title = self._normalize_title(
                str(candidate.get("title", "")).strip(),
                str(candidate.get("snippet", "")).strip(),
                url,
            )
            snippet = str(candidate.get("snippet", "")).strip()
            if not snippet:
                snippet = "Referenced by Perplexity research."

            published_at = self._extract_best_date(
                str(candidate.get("published_at", "")).strip(),
                title,
                snippet,
                url,
            )
            score = self._score_candidate(
                base_score=float(candidate.get("score", 1.0)),
                url=url,
                title=title,
                snippet=snippet,
                published_at=published_at,
                max_sources=max_sources,
                entity_terms=entity_terms,
            )

            existing = merged.get(url)
            if existing is None:
                merged[url] = {
                    "title": title,
                    "url": url,
                    "content": snippet,
                    "score": score,
                    "published_at": published_at,
                }
                continue

            # Keep the better candidate while preserving richer metadata.
            if score > float(existing.get("score", 0.0)):
                existing["score"] = score
            if len(snippet) > len(str(existing.get("content", ""))):
                existing["content"] = snippet
            if not existing.get("published_at") and published_at:
                existing["published_at"] = published_at
            existing_title = str(existing.get("title", "")).strip()
            if self._is_low_signal_title(existing_title) and not self._is_low_signal_title(title):
                existing["title"] = title

        ranked = list(merged.values())
        ranked.sort(
            key=lambda item: (
                float(item.get("score", 0.0)),
                str(item.get("published_at", "")),
            ),
            reverse=True,
        )

        return self._enforce_primary_source_quota(ranked, max_sources)

    def _enforce_primary_source_quota(
        self,
        ranked: List[Dict[str, Any]],
        max_sources: int,
    ) -> List[Dict[str, Any]]:
        """
        Ensure expanded windows keep enough primary sources.

        In larger source sets, require minimum primary-source coverage so
        stale secondary pages do not crowd out filings/investor materials.
        """
        limit = max(1, int(max_sources))

        # First pass: separate low-signal legal/admin notices from useful docs.
        preferred: List[Dict[str, Any]] = []
        low_signal: List[Dict[str, Any]] = []
        for item in ranked:
            if self._is_low_signal_notice_doc(
                title=str(item.get("title", "")),
                snippet=str(item.get("content", "")),
                url=str(item.get("url", "")),
            ):
                low_signal.append(item)
            else:
                preferred.append(item)

        # Prefer non-notice docs unless the candidate pool is exhausted.
        ranked = preferred + low_signal

        target_primary = 3 if limit >= 10 else 2
        target_high_signal = 3 if limit >= 8 else 2
        high_signal_docs: List[Dict[str, Any]] = []
        primaries: List[Dict[str, Any]] = []
        secondaries: List[Dict[str, Any]] = []
        for item in preferred:
            url = str(item.get("url", "")).strip()
            if self._is_high_signal_filing_doc(
                title=str(item.get("title", "")),
                snippet=str(item.get("content", "")),
                url=url,
            ):
                high_signal_docs.append(item)
            authority = self._source_authority_level(url)
            if authority >= 2:
                primaries.append(item)
            else:
                secondaries.append(item)

        selected: List[Dict[str, Any]] = []
        used_urls = set()
        low_signal_used = 0
        max_low_signal_allowed = 0
        if len(preferred) < limit:
            shortfall = limit - len(preferred)
            max_low_signal_allowed = 1 if limit <= 8 else 2
            max_low_signal_allowed = min(max_low_signal_allowed, max(0, shortfall))
            if len(preferred) <= 2:
                max_low_signal_allowed = min(2, max(0, shortfall))

        for item in high_signal_docs[:target_high_signal]:
            url = str(item.get("url", "")).strip()
            if not url or url in used_urls:
                continue
            selected.append(item)
            used_urls.add(url)

        for item in primaries[:target_primary]:
            url = str(item.get("url", "")).strip()
            if not url or url in used_urls:
                continue
            selected.append(item)
            used_urls.add(url)

        for item in ranked:
            if len(selected) >= limit:
                break
            url = str(item.get("url", "")).strip()
            if not url or url in used_urls:
                continue
            if self._is_low_signal_notice_doc(
                title=str(item.get("title", "")),
                snippet=str(item.get("content", "")),
                url=url,
            ):
                if low_signal_used >= max_low_signal_allowed:
                    continue
                low_signal_used += 1
            selected.append(item)
            used_urls.add(url)

        selected.sort(
            key=lambda item: (
                float(item.get("score", 0.0)),
                str(item.get("published_at", "")),
            ),
            reverse=True,
        )
        return selected[:limit]

    def _merge_decoded_content(
        self,
        results: List[Dict[str, Any]],
        decoded_by_url: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Merge decoded text back into ranked results for downstream synthesis."""
        merged_results: List[Dict[str, Any]] = []
        for item in results:
            merged = dict(item)
            url = self._clean_url(str(item.get("url", "")))
            decoded = decoded_by_url.get(url, {})
            decoded_excerpt = str(decoded.get("excerpt", "")).strip()
            decoded_title = str(decoded.get("decoded_title", "")).strip()

            if decoded_excerpt:
                merged["source_snippet"] = str(item.get("content", "")).strip()
                merged["content"] = decoded_excerpt
                merged["decoded_excerpt"] = decoded_excerpt
                merged["decoded_chars"] = int(decoded.get("decoded_chars", 0))
                merged["decoded_content_type"] = decoded.get("content_type", "")
                merged["decode_status"] = "decoded"
            elif decoded:
                merged["decode_status"] = str(decoded.get("status", "failed"))

            current_title = str(merged.get("title", "")).strip()
            if decoded_title and (
                self._is_low_signal_title(current_title)
                or current_title.startswith("ASX announcement PDF")
                or current_title.startswith("Company announcement PDF")
            ):
                merged["title"] = decoded_title

            merged_results.append(merged)

        return merged_results

    async def _decode_ranked_sources(
        self,
        results: List[Dict[str, Any]],
        query_context: str = "",
    ) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        """Decode top-ranked sources into text excerpts (PDF/HTML)."""
        decode_stage_start = perf_counter()
        max_to_decode = max(0, int(SOURCE_DECODING_MAX_PER_MODEL))
        if max_to_decode <= 0:
            return {}, {
                "enabled": True,
                "attempted": 0,
                "decoded": 0,
                "failed": 0,
                "sources": [],
            }

        selected: List[Dict[str, Any]] = []
        seen_urls = set()
        for item in results:
            url = self._clean_url(str(item.get("url", "")))
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            selected.append({"url": url, "title": str(item.get("title", "")).strip()})
            if len(selected) >= max_to_decode:
                break

        if not selected:
            return {}, {
                "enabled": True,
                "attempted": 0,
                "decoded": 0,
                "failed": 0,
                "sources": [],
            }

        semaphore = asyncio.Semaphore(2)

        async def _decode_with_limit(source: Dict[str, str]) -> Dict[str, Any]:
            async with semaphore:
                return await self._decode_one_source(
                    source["url"],
                    source.get("title", ""),
                    query_context=query_context,
                )

        decoded_outputs = await asyncio.gather(
            *[_decode_with_limit(source) for source in selected],
            return_exceptions=True,
        )

        decoded_by_url: Dict[str, Dict[str, Any]] = {}
        decoded_count = 0
        failed_count = 0
        per_source_report: List[Dict[str, Any]] = []

        for source, output in zip(selected, decoded_outputs):
            url = source["url"]
            title = source.get("title", "")
            if isinstance(output, Exception):
                failed_count += 1
                per_source_report.append(
                    {
                        "url": url,
                        "title": title,
                        "status": "failed",
                        "error": str(output),
                    }
                )
                continue

            status = str(output.get("status", "failed"))
            if status == "decoded":
                decoded_count += 1
                decoded_by_url[url] = output
            else:
                failed_count += 1

            per_source_report.append(
                {
                    "url": url,
                    "title": title,
                    "status": status,
                    "content_type": output.get("content_type", ""),
                    "decoded_chars": int(output.get("decoded_chars", 0)),
                    "error": output.get("error", ""),
                }
            )

        report = {
            "enabled": True,
            "attempted": len(selected),
            "decoded": decoded_count,
            "failed": failed_count,
            "sources": per_source_report,
        }
        self._log(
            f"decode batch done attempted={len(selected)} decoded={decoded_count} "
            f"failed={failed_count} elapsed={perf_counter() - decode_stage_start:.1f}s"
        )
        return decoded_by_url, report

    async def _decode_one_source(
        self,
        url: str,
        title: str,
        query_context: str = "",
    ) -> Dict[str, Any]:
        """Decode one source URL into text excerpt."""
        timeout = max(5.0, float(SOURCE_DECODING_TIMEOUT_SECONDS))
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            )
        }

        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
                headers=headers,
            ) as client:
                response = await client.get(url)
        except Exception as exc:
            return {
                "status": "failed",
                "error": f"Fetch failed: {str(exc)}",
                "content_type": "",
                "decoded_chars": 0,
            }

        if response.status_code >= 400:
            return {
                "status": "failed",
                "error": f"HTTP {response.status_code}",
                "content_type": response.headers.get("content-type", ""),
                "decoded_chars": 0,
            }

        content_type = response.headers.get("content-type", "").lower()
        is_pdf = self._looks_like_pdf_source(url, title, content_type)
        if is_pdf:
            full_text, decoded_title, error = await self._decode_pdf_bytes(response.content)
        else:
            full_text, decoded_title, error = self._decode_html_content(response.text)

        if error:
            return {
                "status": "failed",
                "error": error,
                "content_type": content_type,
                "decoded_chars": 0,
                "decoded_title": decoded_title,
            }

        excerpt = self._make_excerpt(full_text, query_context=query_context)
        if not excerpt:
            return {
                "status": "failed",
                "error": "Decoded content was empty",
                "content_type": content_type,
                "decoded_chars": 0,
                "decoded_title": decoded_title,
            }

        return {
            "status": "decoded",
            "content_type": content_type or ("application/pdf" if is_pdf else "text/html"),
            "decoded_chars": len(excerpt),
            "decoded_title": decoded_title,
            "excerpt": excerpt,
        }

    async def _decode_pdf_bytes(self, pdf_bytes: bytes) -> tuple[str, str, str]:
        """Decode PDF bytes with local text extraction."""
        if not pdf_bytes:
            return "", "", "Empty PDF payload"

        tmp_path = ""
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(pdf_bytes)
                tmp_path = tmp.name

            from ...pdf_processor import extract_text_from_pdf

            extraction = await extract_text_from_pdf(tmp_path)
            error = str(extraction.get("error", "")).strip()
            full_text = str(extraction.get("text", "")).strip()
            metadata = extraction.get("metadata", {}) or {}
            title = str(metadata.get("title", "")).strip()
            if error:
                return "", title, error
            if not full_text:
                return "", title, "No text extracted from PDF"
            return full_text, title, ""
        except Exception as exc:
            return "", "", f"PDF decode failed: {str(exc)}"
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    def _decode_html_content(self, html_text: str) -> tuple[str, str, str]:
        """Extract readable body text and title from HTML."""
        if not html_text:
            return "", "", "Empty HTML payload"

        title = ""
        title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", html_text)
        if title_match:
            title = re.sub(r"\s+", " ", unescape(title_match.group(1))).strip()

        body = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html_text)
        body = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", body)
        body = re.sub(r"(?is)<!--.*?-->", " ", body)
        # Preserve structure so de-noising can strip navigation/header lines.
        body = re.sub(
            r"(?is)</?(?:p|div|section|article|main|header|footer|nav|h[1-6]|li|ul|ol|table|tr|td|br)[^>]*>",
            "\n",
            body,
        )
        body = re.sub(r"(?is)<[^>]+>", " ", body)
        body = unescape(body)
        body = re.sub(r"[ \t]+", " ", body)
        body = re.sub(r"\n[ \t]+", "\n", body)
        body = re.sub(r"\n{3,}", "\n\n", body).strip()
        if not body:
            return "", title, "No readable text extracted from HTML"
        return body, title, ""

    def _make_excerpt(self, full_text: str, query_context: str = "") -> str:
        """
        Build prompt-safe excerpt using query-aware chunk scoring.

        This replaces naive prefix clipping so decoded evidence favors
        rubric-relevant numeric/timeline/finance content over nav boilerplate.
        """
        text = self._sanitize_decoded_text(full_text or "")
        if not text:
            text = re.sub(r"\s+", " ", full_text or "").strip()
        if not text:
            return ""

        max_chars = max(600, int(SOURCE_DECODING_MAX_CHARS))
        chunks = self._split_text_chunks(text, chunk_chars=820, overlap=140)
        if not chunks:
            return text[:max_chars]

        query_terms = self._extract_query_terms(query_context)
        scored: List[Dict[str, Any]] = []
        for idx, chunk in enumerate(chunks):
            score = self._score_excerpt_chunk(chunk, query_terms)
            bucket = self._classify_excerpt_chunk_bucket(chunk)
            scored.append({"idx": idx, "chunk": chunk, "score": score, "bucket": bucket})

        # Highest-signal chunks first, then restore original ordering for coherence.
        scored.sort(key=lambda item: (float(item["score"]), -int(item["idx"])), reverse=True)

        selected: List[Dict[str, Any]] = []
        selected_norm = set()
        total_chars = 0

        def _try_add(item: Dict[str, Any]) -> bool:
            nonlocal total_chars
            chunk = str(item["chunk"]).strip()
            if not chunk:
                return False
            norm = chunk.lower()
            if norm in selected_norm:
                return False
            projected = total_chars + len(chunk) + (2 if selected else 0)
            if projected > max_chars:
                return False
            selected.append(item)
            selected_norm.add(norm)
            total_chars = projected
            return True

        # Force topical diversity before score-only fill.
        for target_bucket in ("timeline", "economics", "funding", "market"):
            for item in scored:
                if str(item.get("bucket", "")) != target_bucket:
                    continue
                if float(item.get("score", 0.0)) < 1.5:
                    continue
                if _try_add(item):
                    break

        for item in scored:
            _try_add(item)
            if total_chars >= int(max_chars * 0.90):
                break

        if not selected:
            # Fallback: first chunk if scoring rejected everything.
            fallback = chunks[0][: max_chars - 3].rstrip() + "..."
            return fallback if len(chunks[0]) > max_chars else chunks[0]

        selected.sort(key=lambda item: int(item["idx"]))
        excerpt = "\n\n".join(str(item["chunk"]).strip() for item in selected if item.get("chunk"))
        excerpt = re.sub(r"\s+\n", "\n", excerpt).strip()
        if len(excerpt) <= max_chars:
            return excerpt
        return excerpt[: max_chars - 3].rstrip() + "..."

    def _sanitize_decoded_text(self, full_text: str) -> str:
        """Drop repeated headers/footers and legal/admin boilerplate before chunking."""
        raw = str(full_text or "").replace("\r", "\n")
        if not raw.strip():
            return ""

        lines = [re.sub(r"\s+", " ", line).strip() for line in raw.split("\n")]
        short_counter: Counter[str] = Counter()
        for line in lines:
            if not line:
                continue
            low = line.lower()
            if len(low) <= 160:
                short_counter[low] += 1

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

        kept_lines: List[str] = []
        for line in lines:
            if not line:
                continue
            low = line.lower()

            # Common OCR/PDF boundary artifact (e.g., "ommence, ...").
            if re.match(r"^[a-z]{3,}[,;:]\s", line):
                if not any(token in low for token in strong_tokens):
                    continue

            if len(low) <= 160 and short_counter.get(low, 0) >= 4:
                if not re.search(r"\d", low) and not any(token in low for token in strong_tokens):
                    continue

            if self._looks_like_low_signal_line(low):
                continue

            if self._looks_like_heading_line(line):
                if not re.search(r"\d", low) and not any(token in low for token in strong_tokens):
                    continue

            kept_lines.append(line)

        if not kept_lines:
            return ""

        paragraphs: List[str] = []
        current: List[str] = []
        for line in kept_lines:
            if len(line) < 30 and not re.search(r"\d", line):
                if current:
                    paragraphs.append(" ".join(current).strip())
                    current = []
                continue
            current.append(line)
            merged = " ".join(current).strip()
            if line.endswith((".", "!", "?", ";", ":")) or len(merged) >= 900:
                paragraphs.append(merged)
                current = []
        if current:
            paragraphs.append(" ".join(current).strip())

        cleaned = "\n\n".join(part for part in paragraphs if part).strip()
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned

    def _looks_like_heading_line(self, line: str) -> bool:
        """Detect short heading-like lines that rarely carry valuation signal."""
        text = re.sub(r"\s+", " ", str(line or "")).strip()
        if not text:
            return True
        low = text.lower()
        if low in {
            "contents",
            "table of contents",
            "for personal use only",
            "announcements",
            "presentations",
            "investor centre",
        }:
            return True
        words = [token for token in re.split(r"\s+", text) if token]
        if len(words) <= 12 and len(text) <= 95:
            if not re.search(r"[\.!?;:]", text):
                alpha = [c for c in text if c.isalpha()]
                if alpha:
                    upper_ratio = sum(1 for c in alpha if c.isupper()) / float(len(alpha))
                    if upper_ratio >= 0.72:
                        return True
        return False

    def _looks_like_low_signal_line(self, low: str) -> bool:
        """Detect boilerplate/legal/navigation lines that should be excluded."""
        text = str(low or "").strip()
        if not text:
            return True
        menu_tokens = (
            "asx announcements",
            "quarterly reports",
            "annual reports",
            "research",
            "right to receive documents",
            "media",
            "presentations",
            "procurement",
            "careers",
            "contact",
        )
        menu_hits = sum(1 for token in menu_tokens if token in text)
        if menu_hits >= 3:
            return True
        legal_patterns = (
            "708a cleansing notice",
            "cleansing notice",
            "application for quotation of securities",
            "appendix 2a",
            "appendix 3b",
            "appendix 3c",
            "part 6d.2",
            "chapter 2m",
            "sections 674 and 674a",
            "corporations act 2001",
            "without disclosure to investors",
            "this notice is given under paragraph 5(e)",
        )
        nav_patterns = (
            "skip to content",
            "privacy policy",
            "terms and conditions",
            "cookie",
            "sign in",
            "log in",
            "menu",
            "navigation",
            "footer",
            "header",
        )
        if any(token in text for token in nav_patterns):
            return True
        if any(token in text for token in legal_patterns):
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
            )
            if any(token in text for token in override_tokens):
                return False
            return True
        return False

    def _classify_excerpt_chunk_bucket(self, chunk: str) -> str:
        """Assign chunk to a signal bucket so selection covers multiple themes."""
        low = str(chunk or "").lower()
        if any(token in low for token in ("first gold", "gold pour", "milestone", "timeline", "q1 ", "q2 ", "q3 ", "q4 ")):
            return "timeline"
        if any(token in low for token in ("npv", "irr", "aisc", "capex", "resource", "reserve", "grade", "production", "mine life")):
            return "economics"
        if any(token in low for token in ("funding", "facility", "loan", "debt", "cash", "placement", "raise", "runway")):
            return "funding"
        if any(token in low for token in ("market cap", "shares", "enterprise value", "ev/oz", "valuation", "price target")):
            return "market"
        if any(token in low for token in ("management", "board", "director", "ceo", "governance", "jurisdiction", "permit")):
            return "governance"
        return "other"

    def _split_text_chunks(self, text: str, chunk_chars: int = 820, overlap: int = 140) -> List[str]:
        """Split long decoded text into paragraph/sentence-aligned chunks."""
        source = (text or "").strip()
        if not source:
            return []
        size = max(300, int(chunk_chars))
        # Avoid character-window slicing that can create broken leading tokens
        # ("ommence", "ernal", etc.) in downstream fact extraction.
        _ = overlap  # Retained in signature for compatibility.

        paragraphs = [part.strip() for part in re.split(r"\n{2,}", source) if part.strip()]
        if not paragraphs:
            paragraphs = [source]

        chunks: List[str] = []
        current_parts: List[str] = []
        current_len = 0

        def _flush_current() -> None:
            nonlocal current_parts, current_len
            if not current_parts:
                return
            merged = "\n\n".join(current_parts).strip()
            if merged:
                chunks.append(merged)
            current_parts = []
            current_len = 0

        def _emit_piece(piece: str) -> None:
            nonlocal current_parts, current_len
            value = str(piece or "").strip()
            if not value:
                return
            if not current_parts:
                current_parts = [value]
                current_len = len(value)
                return
            projected = current_len + 2 + len(value)
            if projected <= size:
                current_parts.append(value)
                current_len = projected
                return
            _flush_current()
            current_parts = [value]
            current_len = len(value)

        for paragraph in paragraphs:
            # Keep sentence boundaries for long paragraphs.
            paragraph_pieces: List[str] = []
            if len(paragraph) <= size:
                paragraph_pieces = [paragraph]
            else:
                sentences = [
                    sent.strip()
                    for sent in re.split(r"(?<=[\.\!\?;:])\s+", paragraph)
                    if sent and sent.strip()
                ]
                if not sentences:
                    sentences = [paragraph]

                sentence_buffer: List[str] = []
                sentence_len = 0
                for sentence in sentences:
                    if not sentence_buffer:
                        sentence_buffer = [sentence]
                        sentence_len = len(sentence)
                        continue
                    projected = sentence_len + 1 + len(sentence)
                    if projected <= size:
                        sentence_buffer.append(sentence)
                        sentence_len = projected
                        continue
                    paragraph_pieces.append(" ".join(sentence_buffer).strip())
                    sentence_buffer = [sentence]
                    sentence_len = len(sentence)
                if sentence_buffer:
                    paragraph_pieces.append(" ".join(sentence_buffer).strip())

            for piece in paragraph_pieces:
                _emit_piece(piece)

        _flush_current()

        deduped: List[str] = []
        seen = set()
        for piece in chunks:
            compact = re.sub(r"\s+", " ", piece).strip()
            if not compact:
                continue
            if len(compact) < 120 and deduped:
                continue
            key = compact.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(compact)
        return deduped

    def _extract_query_terms(self, query_context: str, max_terms: int = 48) -> List[str]:
        """Extract significant query/rubric terms for chunk scoring."""
        raw = re.sub(r"[^a-zA-Z0-9\s]", " ", str(query_context or "").lower())
        tokens = [tok for tok in raw.split() if len(tok) >= 3]
        if not tokens:
            return []
        stop = {
            "the",
            "and",
            "for",
            "with",
            "this",
            "that",
            "from",
            "into",
            "using",
            "under",
            "over",
            "analysis",
            "company",
            "score",
            "template",
            "output",
            "required",
            "provide",
            "include",
            "latest",
            "source",
            "sources",
            "market",
            "data",
            "quality",
            "value",
            "timeline",
            "price",
            "target",
            "stage",
        }
        ordered: List[str] = []
        seen = set()
        for token in tokens:
            if token in stop:
                continue
            if token in seen:
                continue
            seen.add(token)
            ordered.append(token)
            if len(ordered) >= max_terms:
                break
        return ordered

    def _score_excerpt_chunk(self, chunk: str, query_terms: List[str]) -> float:
        """Score decoded chunk relevance for financial analysis prompts."""
        text = str(chunk or "").strip()
        if not text:
            return -5.0
        low = text.lower()
        score = 0.0

        # Query/rubric lexical overlap.
        if query_terms:
            overlap = sum(1 for term in query_terms if term in low)
            score += min(12.0, overlap * 0.8)

        finance_terms = (
            "npv",
            "irr",
            "aisc",
            "capex",
            "opex",
            "resource",
            "reserve",
            "grade",
            "production",
            "gold",
            "first gold",
            "gold pour",
            "funding",
            "facility",
            "loan",
            "debt",
            "cash",
            "market cap",
            "shares",
            "enterprise value",
            "ev/oz",
            "price target",
            "quarterly",
            "investor presentation",
            "annual report",
            "dfs",
            "pfs",
            "development",
            "milestone",
            "timeline",
            "q1",
            "q2",
            "q3",
            "q4",
        )
        finance_hits = sum(1 for token in finance_terms if token in low)
        score += min(10.0, finance_hits * 0.65)

        # Quantitative richness.
        number_hits = len(re.findall(r"\b\d+(?:[\.,]\d+)?\b", low))
        score += min(4.0, number_hits * 0.20)
        if re.search(r"\b(?:aud|usd|a\$|us\$|moz|koz|g/t|oz|%)\b", low):
            score += 1.2

        # Dated milestone evidence is highly valuable.
        if re.search(r"\b(?:20\d{2})\b", low):
            score += 0.8
        if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", low):
            score += 0.4

        # Boilerplate/navigation penalty.
        boilerplate_tokens = (
            "skip to content",
            "cookie",
            "privacy policy",
            "terms and conditions",
            "menu",
            "navigation",
            "home ",
            "contact us",
            "search",
            "login",
            "sign in",
            "subscribe",
            "javascript",
        )
        boilerplate_hits = sum(1 for token in boilerplate_tokens if token in low)
        if boilerplate_hits > 0 and finance_hits <= 1 and number_hits <= 2:
            score -= min(4.0, boilerplate_hits * 1.2)

        if self._looks_like_heading_line(text):
            if finance_hits <= 1 and number_hits <= 2:
                score -= 3.0

        # Legal/admin notice penalty: these chunks are often non-material for valuation.
        legal_notice_tokens = (
            "708a cleansing notice",
            "cleansing notice",
            "appendix 2a",
            "appendix 3b",
            "appendix 3c",
            "application for quotation of securities",
            "part 6d.2",
            "chapter 2m",
            "sections 674 and 674a",
            "corporations act 2001",
        )
        legal_hits = sum(1 for token in legal_notice_tokens if token in low)
        if legal_hits > 0:
            if finance_hits <= 2 and number_hits <= 4:
                score -= min(10.0, legal_hits * 3.5)
            else:
                score -= min(2.0, legal_hits * 0.8)

        # Very short chunks are usually low-signal.
        if len(text) < 180:
            score -= 1.0

        return score

    def _looks_like_pdf_source(self, url: str, title: str, content_type: str = "") -> bool:
        """Identify PDF sources from URL/title/content-type hints."""
        url_lower = (url or "").lower()
        title_lower = (title or "").lower()
        content_type_lower = (content_type or "").lower()
        return (
            "pdf" in content_type_lower
            or url_lower.endswith(".pdf")
            or "/asxpdf/" in url_lower
            or "pdf" in title_lower
        )

    def _build_entity_terms(
        self,
        *,
        ticker: Optional[str],
        user_query: str,
        research_brief: str,
    ) -> List[str]:
        """Build compact ticker/company terms used to bias source ranking."""
        terms: set[str] = set()

        ticker_raw = str(ticker or "").strip().lower()
        if ticker_raw:
            terms.add(ticker_raw)
            parts = [part for part in re.split(r"[:/\s]+", ticker_raw) if part]
            if parts:
                symbol = parts[-1]
                if len(symbol) >= 2:
                    terms.add(symbol)
                if len(parts) >= 2:
                    exchange = parts[0]
                    terms.add(f"{exchange}:{symbol}")
                    terms.add(f"{exchange} {symbol}")

        company_name = ""
        brief_match = re.search(
            r"company name:\s*([^\n\.]+)",
            str(research_brief or ""),
            flags=re.IGNORECASE,
        )
        if brief_match:
            company_name = brief_match.group(1).strip()
        if not company_name:
            query_match = re.search(
                r"analysis on\s+([^\n]+?)\s+using this rubric",
                str(user_query or ""),
                flags=re.IGNORECASE,
            )
            if query_match:
                company_name = query_match.group(1).strip()

        if company_name:
            normalized = re.sub(r"[^a-z0-9 ]+", " ", company_name.lower())
            normalized = re.sub(r"\s+", " ", normalized).strip()
            if len(normalized) >= 3:
                terms.add(normalized)
            de_suffix = re.sub(
                r"\b(limited|ltd|inc|corp|corporation|plc)\b",
                "",
                normalized,
            )
            de_suffix = re.sub(r"\s+", " ", de_suffix).strip()
            if len(de_suffix) >= 3:
                terms.add(de_suffix)
            words = [word for word in de_suffix.split(" ") if len(word) >= 2]
            if len(words) >= 2:
                terms.add(" ".join(words[:2]))
            if len(words) >= 3:
                terms.add(" ".join(words[:3]))

        compact_terms = sorted(
            term for term in terms
            if term and len(term) >= 3 and term not in {"company", "analysis", "rubric"}
        )
        return compact_terms[:10]

    def _score_candidate(
        self,
        base_score: float,
        url: str,
        title: str,
        snippet: str,
        published_at: str,
        max_sources: int = 0,
        entity_terms: Optional[List[str]] = None,
    ) -> float:
        """Score sources so primary/recent filings rank above weak commentary."""
        domain = self._domain_of(url)
        text = f"{title} {snippet}".lower()
        score = float(base_score)

        low_signal_notice = self._is_low_signal_notice_doc(
            title=title,
            snippet=snippet,
            url=url,
        )
        high_signal_filing = self._is_high_signal_filing_doc(
            title=title,
            snippet=snippet,
            url=url,
        )

        if domain in {"announcements.asx.com.au", "www2.asx.com.au"}:
            score += 3.5
        elif domain.endswith("asx.com.au"):
            score += 2.8
        elif domain.endswith("sec.gov"):
            score += 3.2
        elif domain.endswith("wcsecure.weblink.com.au"):
            score += 2.0
        elif any(token in domain for token in ("investor", "ir.", "relations")):
            score += 1.4

        if any(token in text for token in ("annual report", "quarterly", "investor presentation", "results", "filing", "announcement")):
            score += 1.2
        if any(token in text for token in ("trading halt", "capital raising", "placement", "funding", "board")):
            score += 0.8
        if high_signal_filing:
            score += 2.2
        if low_signal_notice:
            # Heavy penalty for legal/administrative notices that rarely
            # add investment insight in template-driven analysis.
            score -= 9.0

        # Penalize low-signal prediction/commentary sources.
        if domain in {
            "walletinvestor.com",
            "stockinvest.us",
            "gov.capital",
            "coincodex.com",
            "youtube.com",
            "www.youtube.com",
            "cruxinvestor.com",
            "www.cruxinvestor.com",
            "tradingview.com",
            "www.tradingview.com",
            "marketscreener.com",
            "www.marketscreener.com",
        }:
            score -= 3.0
        if any(token in text for token in ("stock forecast", "price prediction", "should you buy")):
            score -= 2.5
        if any(token in domain for token in ("miningweekly", "intelligentinvestor")):
            score -= 1.2
        if "quarterly-reports" in url.lower() and not url.lower().endswith(".pdf"):
            # Penalize index/list pages versus direct report PDFs.
            score -= 1.6

        if published_at:
            try:
                year = int(published_at[:4])
                current_year = datetime.utcnow().year
                if year >= current_year:
                    score += 0.9
                elif year == current_year - 1:
                    score += 0.4
            except Exception:
                pass

        # Baseline age weighting for all source-window sizes.
        age_days = self._published_age_days(published_at)
        if age_days is not None:
            if age_days <= 120:
                score += 0.8
            elif age_days <= 365:
                score += 0.4
            elif age_days <= 720:
                score -= 0.6
            else:
                score -= 1.4

        # In larger source windows, aggressively favor fresher primary filings
        # to avoid filling marginal slots with stale secondary commentary.
        if int(max_sources or 0) >= 8:
            authority = self._source_authority_level(url)
            age_days = self._published_age_days(published_at)

            if authority >= 3:
                score += 1.1
            elif authority == 2:
                score += 0.7
            elif authority == 1:
                score += 0.15
            else:
                score -= 0.5

            if age_days is None:
                score -= 0.35
            elif age_days <= 45:
                score += 1.1
            elif age_days <= 120:
                score += 0.8
            elif age_days <= 270:
                score += 0.35
            elif age_days <= 450:
                score -= 0.4
            else:
                score -= 1.1

            # Extra penalty for stale secondary commentary in expanded windows.
            if authority <= 1 and age_days is not None and age_days > 180:
                score -= 0.9
            if authority <= 1 and any(
                token in domain
                for token in (
                    "miningweekly",
                    "intelligentinvestor",
                    "marketindex",
                    "simplywall",
                    "tradingview",
                )
            ):
                score -= 0.6

        # Entity relevance boost/penalty to reduce wrong-company filings.
        terms = [str(term).strip().lower() for term in (entity_terms or []) if str(term).strip()]
        if terms:
            text_blob = f"{title} {snippet} {url}".lower()
            hit_count = sum(1 for term in terms if term in text_blob)
            if hit_count > 0:
                score += min(3.0, 0.9 * hit_count)
            elif domain.endswith("asx.com.au") or domain.endswith("wcsecure.weblink.com.au"):
                # Generic filing pages are risky when they don't mention the target company/ticker.
                score -= 1.4

        return score

    def _is_low_signal_notice_doc(self, title: str, snippet: str, url: str) -> bool:
        """Detect legal/admin notices that are usually low signal for analysis."""
        text = " ".join([title or "", snippet or "", url or ""]).lower()
        low_patterns = (
            "708a cleansing notice",
            "cleansing notice",
            "appendix 2a",
            "application for quotation of securities",
            "appendix 3b",
            "appendix 3c",
            "quotation of securities",
            "corporations act 2001",
            "part 6d.2",
            "chapter 2m",
            "sections 674 and 674a",
        )
        return any(token in text for token in low_patterns)

    def _is_high_signal_filing_doc(self, title: str, snippet: str, url: str) -> bool:
        """Detect filings/materials that usually carry valuation signal."""
        text = " ".join([title or "", snippet or "", url or ""]).lower()
        high_patterns = (
            "definitive feasibility study",
            "dfs",
            "pre-feasibility study",
            "pfs",
            "feasibility study",
            "investor presentation",
            "corporate presentation",
            "quarterly activities report",
            "quarterly activity report",
            "quarterly report",
            "annual report",
            "cashflow report",
            "appendix 5b",
            "appendix 5c",
            "loan facility",
            "funding package",
            "resource update",
            "project update",
            "first gold",
            "gold pour",
        )
        return any(token in text for token in high_patterns)

    def _published_age_days(self, published_at: str) -> Optional[int]:
        """Convert ISO date to age-in-days; returns None when unavailable."""
        value = (published_at or "").strip()
        if len(value) < 10:
            return None
        try:
            parsed = datetime.strptime(value[:10], "%Y-%m-%d").date()
            age = (datetime.utcnow().date() - parsed).days
            return max(0, int(age))
        except Exception:
            return None

    def _domain_of(self, url: str) -> str:
        """Return normalized URL domain."""
        try:
            return urlparse(url).netloc.lower().replace("www.", "")
        except Exception:
            return ""

    def _source_authority_level(self, url: str) -> int:
        """Rank source authority for update-table prioritization."""
        domain = self._domain_of(url)
        if domain.endswith("asx.com.au") or domain.endswith("sec.gov"):
            return 3
        if domain.endswith("wcsecure.weblink.com.au") or domain.endswith("investorpa.com"):
            return 2
        if domain.endswith("marketindex.com.au") or domain.endswith("intelligentinvestor.com.au"):
            return 1
        if "investor" in domain or "ir." in domain:
            return 1
        return 0

    def _normalize_title(self, title: str, snippet: str, url: str) -> str:
        """Prefer informative titles; repair low-signal placeholders."""
        clean_title = re.sub(r"\s+", " ", title or "").strip()
        if clean_title and not self._is_low_signal_title(clean_title):
            return clean_title

        clean_snippet = re.sub(r"\s+", " ", snippet or "").strip()
        if clean_snippet and not self._is_low_signal_title(clean_snippet):
            if "." in clean_snippet:
                clean_snippet = clean_snippet.split(".", 1)[0].strip()
            if len(clean_snippet) > 110:
                clean_snippet = clean_snippet[:107].rstrip() + "..."
            if clean_snippet:
                return clean_snippet

        domain = self._domain_of(url)
        inferred_date = self._extract_iso_date(url)
        if domain == "announcements.asx.com.au":
            if inferred_date:
                return f"ASX announcement PDF ({inferred_date})"
            return "ASX announcement PDF"
        if domain == "wcsecure.weblink.com.au":
            if inferred_date:
                return f"Company announcement PDF ({inferred_date})"
            return "Company announcement PDF"

        return self._title_from_url(url)

    def _is_low_signal_title(self, title: str) -> bool:
        """Detect generic titles that add little meaning."""
        lowered = (title or "").strip().lower()
        if lowered in {
            "",
            "untitled",
            "for personal use only",
            "page i",
            "source",
        }:
            return True
        if "for personal use only" in lowered:
            return True
        if re.match(r"^page\s*\|?\s*[ivxlcdm0-9]+$", lowered):
            return True
        if re.match(r"^page\s+[ivxlcdm0-9]+$", lowered):
            return True
        return False

    def _extract_best_date(
        self,
        published_at: str,
        title: str,
        snippet: str,
        url: str,
    ) -> str:
        """Extract an ISO date from known fields and URL/title patterns."""
        # ASX PDF URLs contain stable filing dates and should dominate noisy metadata.
        url_text = str(url or "").lower()
        if "announcements.asx.com.au/asxpdf/" in url_text:
            url_date = self._extract_iso_date(url)
            if url_date:
                return url_date

        for candidate in (published_at, title, snippet, url):
            date_value = self._extract_iso_date(candidate)
            if date_value:
                return date_value
        return ""

    def _extract_iso_date(self, text: str) -> str:
        """Parse date formats commonly seen in finance source links/snippets."""
        if not text:
            return ""

        # YYYY-MM-DD or YYYY/MM/DD
        match = re.search(r"(?<!\d)(\d{4})[-/](\d{2})[-/](\d{2})(?!\d)", text)
        if match:
            candidate = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
            if self._is_plausible_date(candidate):
                return candidate

        # YYYYMMDD in paths
        match = re.search(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)", text)
        if match:
            candidate = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
            if self._is_plausible_date(candidate):
                return candidate

        # D Month YYYY
        month_map = {
            "jan": "01",
            "feb": "02",
            "mar": "03",
            "apr": "04",
            "may": "05",
            "jun": "06",
            "jul": "07",
            "aug": "08",
            "sep": "09",
            "oct": "10",
            "nov": "11",
            "dec": "12",
        }
        match = re.search(
            r"(?<!\d)(\d{1,2})\s+([A-Za-z]{3,9})\s+(20\d{2})(?!\d)",
            text,
        )
        if match:
            day = int(match.group(1))
            month_key = match.group(2)[:3].lower()
            year = match.group(3)
            month = month_map.get(month_key)
            if month and 1 <= day <= 31:
                candidate = f"{year}-{month}-{day:02d}"
                if self._is_plausible_date(candidate):
                    return candidate

        return ""

    def _is_plausible_date(self, iso_date: str) -> bool:
        """Reject malformed or clearly impossible publication dates."""
        try:
            parsed = datetime.strptime(iso_date, "%Y-%m-%d").date()
        except Exception:
            return False

        today = datetime.utcnow().date()
        if parsed.year < 2000:
            return False
        # Allow slight future skew from timezone/crawling noise.
        if parsed > today:
            return False
        return True

    def _build_latest_updates(
        self,
        results: List[Dict[str, Any]],
        max_rows: int = 8,
    ) -> List[Dict[str, str]]:
        """Build a normalized latest-updates table from ranked source results."""
        rows: List[Dict[str, Any]] = []
        for item in results:
            title = str(item.get("title", "")).strip() or "Corporate update"
            snippet = str(item.get("content", "")).strip()
            url = str(item.get("url", "")).strip()
            date_value = self._extract_best_date(
                str(item.get("published_at", "")).strip(),
                title,
                snippet,
                url,
            )
            update_kind = self._classify_update_kind(title, snippet)
            rows.append(
                {
                    "date": date_value or "Unknown",
                    "update": title,
                    "why_it_matters": self._why_update_matters(update_kind),
                    "source_url": url,
                    "_kind": update_kind,
                    "_score": float(item.get("score", 0.0)),
                    "_sort_date": date_value,
                    "_authority": self._source_authority_level(url),
                }
            )

        material_priority = {
            "funding": 6,
            "trading_halt": 6,
            "reporting": 5,
            "operations": 5,
            "governance": 4,
            "presentation": 4,
            "legal_notice": 0,
            "general": 1,
        }

        preferred_material_rows: List[Dict[str, Any]] = []
        preferred_general_rows: List[Dict[str, Any]] = []
        fallback_material_rows: List[Dict[str, Any]] = []
        fallback_general_rows: List[Dict[str, Any]] = []
        for row in rows:
            authority = int(row.get("_authority", 0))
            if row.get("_kind") == "general":
                if authority > 0:
                    preferred_general_rows.append(row)
                else:
                    fallback_general_rows.append(row)
            else:
                if authority > 0:
                    preferred_material_rows.append(row)
                else:
                    fallback_material_rows.append(row)

        sort_key = lambda row: (
            int(row.get("_authority", 0)),
            row.get("_sort_date", "") != "",
            row.get("_sort_date", ""),
            int(material_priority.get(str(row.get("_kind", "general")), 1)),
            float(row.get("_score", 0.0)),
        )
        preferred_material_rows.sort(key=sort_key, reverse=True)
        preferred_general_rows.sort(key=sort_key, reverse=True)
        fallback_material_rows.sort(key=sort_key, reverse=True)
        fallback_general_rows.sort(key=sort_key, reverse=True)
        rows = (
            preferred_material_rows
            + preferred_general_rows
            + fallback_material_rows
            + fallback_general_rows
        )

        # Keep legal/admin notices out of the update table unless required to
        # avoid empty rows.
        non_legal_rows = [row for row in rows if row.get("_kind") != "legal_notice"]
        legal_rows = [row for row in rows if row.get("_kind") == "legal_notice"]
        if non_legal_rows:
            legal_cap = 0
            if len(non_legal_rows) < max_rows:
                legal_cap = 1 if max_rows <= 8 else 2
                legal_cap = min(legal_cap, max_rows - len(non_legal_rows))
            rows = non_legal_rows + legal_rows[:legal_cap]

        pruned: List[Dict[str, str]] = []
        seen = set()
        for row in rows:
            key = row.get("source_url", "")
            if not key or key in seen:
                continue
            seen.add(key)
            pruned.append(
                {
                    "date": row["date"],
                    "update": row["update"],
                    "why_it_matters": row["why_it_matters"],
                    "source_url": row["source_url"],
                }
            )
            if len(pruned) >= max_rows:
                break

        return pruned

    def _classify_update_kind(self, title: str, snippet: str) -> str:
        """Classify update category from source title/snippet."""
        title_text = (title or "").lower()
        text = f"{title} {snippet}".lower()
        if self._is_low_signal_notice_doc(title=title, snippet=snippet, url=""):
            return "legal_notice"
        if "trading halt" in title_text or "trading halt" in text:
            return "trading_halt"
        if any(token in title_text for token in ("annual report", "quarterly", "results")):
            return "reporting"
        if any(token in title_text for token in ("capital raising", "placement", "financing", "raises", "loan")):
            return "funding"
        if any(token in text for token in ("capital raising", "placement", "financing", "raises", "loan")):
            return "funding"
        if any(token in text for token in ("annual report", "quarterly", "quarterly activity", "results")):
            return "reporting"
        if any(token in title_text for token in ("presentation", "investor deck", "indaba")):
            return "presentation"
        if any(token in text for token in ("presentation", "investor deck", "indaba")):
            return "presentation"
        if any(token in title_text for token in ("board", "director", "ceo", "chair")):
            return "governance"
        if any(token in text for token in ("board", "director", "ceo", "chair")):
            return "governance"
        if any(token in title_text for token in ("dfs", "feasibility", "resource", "reserve", "production")):
            return "operations"
        if any(token in text for token in ("dfs", "feasibility", "resource", "reserve", "production")):
            return "operations"
        return "general"

    def _why_update_matters(self, update_kind: str) -> str:
        """Map update category to an investment-relevance explanation."""
        if update_kind == "legal_notice":
            return "Low direct valuation signal; typically administrative unless it materially changes share structure."
        if update_kind == "funding":
            return "Affects cash runway, dilution risk, and ability to execute project milestones."
        if update_kind == "trading_halt":
            return "Often signals pending material news and short-term event risk."
        if update_kind == "reporting":
            return "Updates operating/financial metrics used in valuation and risk assessment."
        if update_kind == "presentation":
            return "Provides management assumptions, timeline guidance, and project narrative."
        if update_kind == "governance":
            return "Leadership changes can shift execution quality and strategic direction."
        if update_kind == "operations":
            return "Directly informs project economics, timeline, and production outlook."
        return "May contain market-sensitive developments; verify details in the source."

    def _normalize_research_summary(
        self,
        raw_summary: str,
        user_query: str,
        ticker: Optional[str],
        latest_updates: List[Dict[str, str]],
    ) -> str:
        """Ensure summary is always actionable, structured, and link-grounded."""
        clean_summary = (raw_summary or "").strip()
        has_useful_raw_summary = self._is_useful_summary(clean_summary)
        entity = ticker or "the company"

        lines: List[str] = []
        lines.append(f"Latest market-relevant updates for {entity} based on retrieved sources.")
        lines.append("")

        if has_useful_raw_summary:
            lines.append("### Synthesis")
            lines.append(clean_summary)
            lines.append("")
        elif clean_summary:
            lines.append("### Synthesis")
            lines.append(clean_summary)
            lines.append("")
            lines.append("### Structured fallback synthesis")
            lines.extend(self._build_generated_synthesis(entity, latest_updates))
            lines.append("")
        else:
            lines.append("### Synthesis")
            lines.extend(self._build_generated_synthesis(entity, latest_updates))
            lines.append("")

        if latest_updates:
            lines.append("### Latest Updates (with links)")
            lines.append("| Date | Update | Why it matters | Source |")
            lines.append("|---|---|---|---|")
            for row in latest_updates:
                date_value = self._escape_table_cell(row.get("date", "Unknown"))
                update = self._escape_table_cell(row.get("update", "Update"))
                why = self._escape_table_cell(row.get("why_it_matters", ""))
                url = row.get("source_url", "")
                if url:
                    source = f"[link]({url})"
                else:
                    source = "N/A"
                lines.append(f"| {date_value} | {update} | {why} | {source} |")
            lines.append("")
            lines.extend(self._build_summary_takeaways(latest_updates))
        else:
            lines.append("No high-confidence source updates were extracted.")

        return "\n".join(lines).strip()

    def _build_summary_takeaways(self, latest_updates: List[Dict[str, str]]) -> List[str]:
        """Create concise takeaways from extracted updates."""
        lines: List[str] = []
        dated = [row["date"] for row in latest_updates if row.get("date") and row["date"] != "Unknown"]
        if dated:
            lines.append(f"Most recent dated update in retrieved sources: {max(dated)}.")

        joined = " ".join([row.get("update", "").lower() for row in latest_updates])
        if any(token in joined for token in ("capital raising", "placement", "raises", "loan")):
            lines.append("Funding-related announcements are present and should be reviewed for dilution/cash-runway impact.")
        if any(token in joined for token in ("annual report", "quarterly", "results")):
            lines.append("Periodic reporting documents are included, supporting a stronger fact base for valuation checks.")
        if any(token in joined for token in ("trading halt",)):
            lines.append("Trading-halt references are present; correlate timing with the next material announcement.")
        return lines

    def _build_generated_synthesis(
        self,
        entity: str,
        latest_updates: List[Dict[str, str]],
    ) -> List[str]:
        """Create deterministic synthesis when model-generated prose is weak."""
        if not latest_updates:
            return [
                f"- Retrieved sources for {entity}, but no high-confidence update rows were extracted.",
                "- Validate ticker-specific filings manually before using this output for investment decisions.",
            ]

        dated_rows = [row for row in latest_updates if row.get("date") and row.get("date") != "Unknown"]
        lead_row = dated_rows[0] if dated_rows else latest_updates[0]
        lead_date = lead_row.get("date", "Unknown")
        lead_title = lead_row.get("update", "Latest filing")

        joined = " ".join([row.get("update", "").lower() for row in latest_updates])
        lines = [f"- Most recent dated item: **{lead_date}** ({lead_title})."]
        if any(token in joined for token in ("placement", "capital raising", "funding", "loan", "raise")):
            lines.append("- Funding-related items are present; assess dilution and cash-runway impact.")
        if any(token in joined for token in ("quarterly", "annual report", "results", "appendix 4c")):
            lines.append("- Periodic reporting documents are present; use them to refresh operational and valuation assumptions.")
        if any(token in joined for token in ("trading halt",)):
            lines.append("- Trading-halt references are present; check surrounding announcements for the catalyst.")
        if len(lines) == 1:
            lines.append("- Source set is announcement-heavy; review linked filings directly to extract quantitative changes.")
        return lines

    def _is_useful_summary(self, summary: str) -> bool:
        """Heuristic guard against low-signal or malformed model summaries."""
        text = re.sub(r"\s+", " ", summary or "").strip()
        if len(text) < 120:
            return False
        if text.lower().startswith(("and strategic overview", "referenced by perplexity research")):
            return False
        return True

    def _escape_table_cell(self, value: str) -> str:
        """Escape markdown table cell values."""
        return (value or "").replace("\n", " ").replace("|", "\\|").strip()

    def _clean_url(self, value: str) -> str:
        """Normalize URL strings and reject non-http links."""
        cleaned = value.strip().rstrip(".,);")
        if not cleaned.startswith(("http://", "https://")):
            return ""
        return cleaned

    def _title_from_url(self, url: str) -> str:
        """Generate a readable fallback title from URL."""
        try:
            parsed = urlparse(url)
            host = parsed.netloc.replace("www.", "")
            tail = parsed.path.split("/")[-1]
            if tail:
                return f"{host} / {tail}"
            return host or "Source"
        except Exception:
            return "Source"
