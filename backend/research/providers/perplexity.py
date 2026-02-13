"""Perplexity-backed research provider."""

import asyncio
import os
import re
import tempfile
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
        self._log(
            f"gather start model={selected_model} depth={depth} "
            f"max_sources={max_sources} max_steps={effective_max_steps} "
            f"max_output_tokens={effective_max_output_tokens} "
            f"reasoning_effort={effective_reasoning_effort or 'none'} "
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
        payload = self._build_payload(
            prompt,
            depth=depth,
            max_sources=max_sources,
            model_override=model_override,
            max_steps_override=effective_max_steps,
            max_output_tokens_override=effective_max_output_tokens,
            reasoning_effort_override=effective_reasoning_effort,
        )

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        reasoning_retry_applied = "none"
        request_attempts = 0

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

        def _is_invalid_request(status_code: Optional[int], body_text: str) -> bool:
            if status_code != 400:
                return False
            text = (body_text or "").lower()
            return "invalid request" in text or "\"invalid_request\"" in text

        request_start = perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                try:
                    data = await _post_once(client, payload)
                    self._log(
                        f"api success model={selected_model} status=200 "
                        f"elapsed={perf_counter() - request_start:.1f}s attempts={request_attempts}"
                    )
                except httpx.HTTPStatusError as first_exc:
                    first_status = first_exc.response.status_code if first_exc.response else None
                    first_body = first_exc.response.text if first_exc.response is not None else ""

                    # Per-model compatibility fallback:
                    # Some models reject medium/high reasoning effort with a generic 400 invalid request.
                    if _is_invalid_request(first_status, first_body):
                        retried = False
                        retry_payload = dict(payload)
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
                                data = await _post_once(client, retry_payload)
                                payload = retry_payload
                                retried = True
                                self._log(
                                    f"api retry success model={selected_model} status=200 "
                                    f"elapsed={perf_counter() - request_start:.1f}s attempts={request_attempts}"
                                )
                            except httpx.HTTPStatusError as retry_exc:
                                first_exc = retry_exc
                                first_status = retry_exc.response.status_code if retry_exc.response else None
                                first_body = retry_exc.response.text if retry_exc.response is not None else ""

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
                                data = await _post_once(client, retry_payload)
                                payload = retry_payload
                                retried = True
                                self._log(
                                    f"api retry success model={selected_model} status=200 "
                                    f"elapsed={perf_counter() - request_start:.1f}s attempts={request_attempts}"
                                )
                            except httpx.HTTPStatusError as retry_exc:
                                first_exc = retry_exc
                                first_status = retry_exc.response.status_code if retry_exc.response else None
                                first_body = retry_exc.response.text if retry_exc.response is not None else ""

                        if not retried:
                            raise first_exc
                    else:
                        raise first_exc
        except httpx.TimeoutException:
            self._log(
                f"api timeout model={selected_model} elapsed={perf_counter() - request_start:.1f}s"
            )
            return {
                "error": "Perplexity request timed out",
                "results": [],
                "result_count": 0,
                "provider": self.name,
            }
        except httpx.HTTPStatusError as exc:
            body = exc.response.text[:500] if exc.response is not None else ""
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
        results = self._candidates_to_results(source_candidates, max_sources)
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
            decoded_map, decode_report = await self._decode_ranked_sources(results)
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
                "tools": [tool.get("type", "unknown") for tool in payload.get("tools", [])],
                "max_steps": payload.get("max_steps"),
                "max_output_tokens": payload.get("max_output_tokens"),
                "max_sources": max_sources,
                "research_prompt_chars": len(prompt),
                "research_brief_chars": len(research_brief or ""),
                "raw_summary_chars": len(raw_summary or ""),
                "raw_summary_preview": (raw_summary or "")[:280],
                "source_decoding": decode_report,
                "request_attempts": request_attempts,
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
            "max_output_tokens": max_output_tokens,
            "parallel_tool_calls": True,
            "tools": self._build_tools(depth=depth, max_sources=max_sources),
        }

        chosen_preset = self._select_preset(depth)
        if chosen_preset:
            payload["preset"] = chosen_preset

        if reasoning_effort in {"low", "medium", "high"}:
            payload["reasoning"] = {"effort": reasoning_effort}

        return payload

    def _select_preset(self, depth: str) -> str:
        """Choose preset based on depth and env preference."""
        if self.preset:
            if depth == "deep":
                return self.preset
            if self.preset == "deep-research":
                return "search"
            return self.preset
        return "deep-research" if depth == "deep" else "search"

    def _build_tools(self, depth: str, max_sources: int) -> List[Dict[str, Any]]:
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

        # Some responses include output_text convenience field.
        if not text_parts:
            output_text = data.get("output_text")
            if isinstance(output_text, str):
                text_parts.append(output_text)
            elif isinstance(output_text, list):
                for item in output_text:
                    if isinstance(item, str):
                        text_parts.append(item)

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

        return ranked[:max_sources]

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
                return await self._decode_one_source(source["url"], source.get("title", ""))

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

    async def _decode_one_source(self, url: str, title: str) -> Dict[str, Any]:
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

        excerpt = self._make_excerpt(full_text)
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
        body = re.sub(r"(?is)<[^>]+>", " ", body)
        body = unescape(body)
        body = re.sub(r"\s+", " ", body).strip()
        if not body:
            return "", title, "No readable text extracted from HTML"
        return body, title, ""

    def _make_excerpt(self, full_text: str) -> str:
        """Normalize and trim decoded text for prompt-safe evidence injection."""
        text = re.sub(r"\s+", " ", full_text or "").strip()
        if not text:
            return ""
        max_chars = max(600, int(SOURCE_DECODING_MAX_CHARS))
        if len(text) <= max_chars:
            return text
        return text[: max_chars - 3].rstrip() + "..."

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

    def _score_candidate(
        self,
        base_score: float,
        url: str,
        title: str,
        snippet: str,
        published_at: str,
    ) -> float:
        """Score sources so primary/recent filings rank above weak commentary."""
        domain = self._domain_of(url)
        text = f"{title} {snippet}".lower()
        score = float(base_score)

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
        }:
            score -= 3.0
        if any(token in text for token in ("stock forecast", "price prediction", "should you buy")):
            score -= 2.5

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

        return score

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
