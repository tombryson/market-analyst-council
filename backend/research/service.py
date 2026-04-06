"""Research service with provider selection and evidence-pack normalization."""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

from .schemas import EvidencePack, EvidenceSource
from .providers import (
    PerplexityResearchProvider,
    TavilyResearchProvider,
    ResearchProvider,
)
from ..template_loader import get_template_loader
from ..config import (
    RESEARCH_PROVIDER,
    RESEARCH_DEPTH,
    MAX_SOURCES,
)


class ResearchService:
    """Main entry point for retrieval and evidence normalization."""

    def __init__(
        self,
        provider_name: str = RESEARCH_PROVIDER,
        depth: str = RESEARCH_DEPTH,
        max_sources: int = MAX_SOURCES,
    ):
        self.provider_name = (provider_name or "tavily").strip().lower()
        self.depth = (depth or "basic").strip().lower()
        self.max_sources = max(1, int(max_sources))

    async def gather_research(
        self,
        user_query: str,
        ticker: Optional[str] = None,
        model_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Gather research and normalize to include an evidence pack."""
        provider = self._create_provider(self.provider_name)
        provider_name = provider.name

        result = await provider.gather(
            user_query=user_query,
            ticker=ticker,
            depth=self.depth,
            max_sources=self.max_sources,
            model_override=model_override,
        )

        # Automatic fallback to Tavily if primary provider fails.
        if result.get("error") and provider_name != "tavily":
            fallback = TavilyResearchProvider()
            fallback_result = await fallback.gather(
                user_query=user_query,
                ticker=ticker,
                depth=self.depth,
                max_sources=self.max_sources,
                model_override=model_override,
            )
            fallback_result["provider_fallback"] = "tavily"
            fallback_result["fallback_reason"] = result["error"]
            result = fallback_result
            provider_name = fallback.name

        result.setdefault("query", user_query)
        result.setdefault("results", [])
        result.setdefault("performed_at", datetime.utcnow().isoformat())
        result.setdefault("provider", provider_name)

        ranked_results, ranking_meta = self._rank_and_select_results(
            search_results=result.get("results", []) or [],
            user_query=user_query,
            ticker=ticker,
            provider_name=provider_name,
            max_results=self.max_sources,
        )
        result["results"] = ranked_results
        result["result_count"] = len(ranked_results)
        provider_metadata = result.get("provider_metadata")
        if not isinstance(provider_metadata, dict):
            provider_metadata = {}
        provider_metadata["ranking"] = ranking_meta
        result["provider_metadata"] = provider_metadata

        if not result.get("evidence_pack"):
            result["evidence_pack"] = self._build_evidence_pack(
                user_query=user_query,
                ticker=ticker,
                provider=result["provider"],
                depth=self.depth,
                search_results=result.get("results", []),
                summary=result.get("research_summary", ""),
            )

        return result

    async def gather_mining_supplementary_facts(
        self,
        *,
        user_query: str = "",
        company: str,
        ticker: str,
        exchange: str,
        commodity: str,
        template_id: str = "",
        company_type: str = "",
        preset: Optional[str] = None,
        repair_preset: Optional[str] = None,
        model_override: Optional[str] = None,
        max_priority_sources: Optional[int] = None,
        enable_targeted_repairs: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Run the segmented Perplexity mining supplementary-facts workflow."""
        return await self.gather_supplementary_facts(
            pipeline_id="resources_supplementary",
            user_query=user_query,
            company=company,
            ticker=ticker,
            exchange=exchange,
            commodity=commodity,
            template_id=template_id,
            company_type=company_type,
            preset=preset,
            repair_preset=repair_preset,
            model_override=model_override,
            max_priority_sources=max_priority_sources,
            enable_targeted_repairs=enable_targeted_repairs,
        )

    async def gather_supplementary_facts(
        self,
        *,
        pipeline_id: str,
        user_query: str = "",
        company: str,
        ticker: str,
        exchange: str,
        commodity: str = "",
        template_id: str = "",
        company_type: str = "",
        preset: Optional[str] = None,
        repair_preset: Optional[str] = None,
        model_override: Optional[str] = None,
        max_priority_sources: Optional[int] = None,
        enable_targeted_repairs: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Run the segmented Perplexity supplementary-facts workflow for a resolved sector pipeline."""
        provider = PerplexityResearchProvider()
        return await provider.gather_supplementary_facts(
            pipeline_id=pipeline_id,
            user_query=user_query,
            company=company,
            ticker=ticker,
            exchange=exchange,
            commodity=commodity,
            template_id=template_id,
            company_type=company_type,
            preset=preset,
            repair_preset=repair_preset,
            model_override=model_override,
            max_priority_sources=max_priority_sources,
            enable_targeted_repairs=enable_targeted_repairs,
        )

    def _create_provider(self, provider_name: str) -> ResearchProvider:
        """Construct provider implementation from config name."""
        if provider_name == "perplexity":
            return PerplexityResearchProvider()
        if provider_name == "tavily":
            return TavilyResearchProvider()

        print(f"Unknown RESEARCH_PROVIDER '{provider_name}', defaulting to tavily")
        return TavilyResearchProvider()

    def _rank_and_select_results(
        self,
        *,
        search_results: List[Dict[str, Any]],
        user_query: str,
        ticker: Optional[str],
        provider_name: str,
        max_results: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Apply entity-aware scoring and exchange-sensitive quotas to result candidates."""
        target = max(1, int(max_results))
        exchange = _infer_exchange_from_ticker(ticker or "")
        entity_profile = _build_entity_profile(user_query=user_query, ticker=ticker)

        deduped: Dict[str, Dict[str, Any]] = {}
        for row in search_results:
            if not isinstance(row, dict):
                continue
            url = str(row.get("url", "") or "").strip()
            title = str(row.get("title", "") or "").strip()
            if not url and not title:
                continue
            key = url or f"title::{title[:120].lower()}"
            current = deduped.get(key)
            current_score = float(current.get("score", 0.0)) if current else -1e9
            row_score = float(row.get("score", 0.0) or 0.0)
            if current is None or row_score > current_score:
                deduped[key] = row

        candidates: List[Dict[str, Any]] = []
        for row in deduped.values():
            url = str(row.get("url", "") or "").strip()
            host = ""
            try:
                host = urlparse(url).netloc.lower()
            except Exception:
                host = ""
            title = str(row.get("title", "") or "").strip() or "Untitled"
            content = str(row.get("content", "") or "").strip()
            published_at = str(row.get("published_at", "") or "").strip()
            source_provider = str(row.get("provider", "") or provider_name).strip().lower()
            base_score = float(row.get("score", 0.0) or 0.0)

            source_bucket, source_boost = _source_tier_boost(
                host=host,
                title=title,
                url=url,
                content=content,
                exchange=exchange,
            )
            entity_boost, entity_match = _entity_match_boost(
                title=title,
                url=url,
                content=content,
                host=host,
                exchange=exchange,
                entity_profile=entity_profile,
            )
            recency = _recency_boost(published_at)
            content_boost = _content_density_boost(content)
            provider_boost = 0.0
            if source_provider == "perplexity":
                provider_boost = 0.12
            elif source_provider == "tavily":
                provider_boost = 0.05

            final_score = base_score + provider_boost + source_boost + entity_boost + recency + content_boost

            typed = {
                "title": title,
                "url": url,
                "content": content,
                "published_at": published_at,
                "provider": source_provider,
                "score": round(float(final_score), 6),
                "source_bucket": source_bucket,
                "source_type": _source_type_from_bucket(source_bucket, url=url, title=title),
                "entity_match": bool(entity_match),
            }
            candidates.append(typed)

        pre_gate_count = len(candidates)
        dropped_by_entity_gate = 0
        if ticker:
            entity_matched = [row for row in candidates if bool(row.get("entity_match"))]
            dropped_by_entity_gate = pre_gate_count - len(entity_matched)
            if entity_matched:
                candidates = entity_matched

        ranked = sorted(candidates, key=lambda r: float(r.get("score", 0.0)), reverse=True)
        selected = _select_results_with_quotas(
            candidates=ranked,
            target=target,
            exchange=exchange,
        )

        selected_sorted = sorted(selected, key=lambda r: float(r.get("score", 0.0)), reverse=True)
        # Hide internal ranking-only fields from downstream payload.
        for row in selected_sorted:
            row.pop("entity_match", None)

        return selected_sorted, {
            "exchange": exchange,
            "requested_top_n": target,
            "candidate_count_pre_gate": pre_gate_count,
            "candidate_count_post_gate": len(ranked),
            "entity_gate_applied": bool(ticker),
            "entity_gate_dropped": int(dropped_by_entity_gate),
            "selected_count": len(selected_sorted),
            "selected_bucket_counts": _count_source_buckets(selected_sorted),
        }

    def _build_evidence_pack(
        self,
        user_query: str,
        ticker: Optional[str],
        provider: str,
        depth: str,
        search_results: List[Dict[str, Any]],
        summary: str,
    ) -> EvidencePack:
        """Normalize search outputs into a single evidence-pack structure."""
        sources: List[EvidenceSource] = []
        for result in search_results:
            url = result.get("url", "")
            title = result.get("title", "Untitled")
            snippet = result.get("content", "")
            source_type = str(result.get("source_type", "")).strip()
            if not source_type:
                source_type = _guess_source_type(url, title)
            sources.append(
                {
                    "url": url,
                    "title": title,
                    "snippet": snippet,
                    "source_type": source_type,
                    "published_at": result.get("published_at", ""),
                    "score": float(result.get("score", 0.0)),
                    "provider": provider,
                }
            )

        key_facts = _extract_key_facts(summary)
        missing_data: List[str] = []
        if not sources:
            missing_data.append("No external web sources were retrieved.")
        if ticker:
            exchange = _infer_exchange_from_ticker(ticker)
            expected_domains = _expected_domains_for_exchange(exchange)
            if expected_domains and not _sources_include_expected_domains(sources, expected_domains):
                missing_data.append(
                    f"No expected primary-source domain found for {exchange.upper()} ticker-focused query."
                )

        return {
            "question": user_query,
            "ticker": ticker or "",
            "provider": provider,
            "depth": depth,
            "generated_at": datetime.utcnow().isoformat(),
            "sources": sources,
            "key_facts": key_facts,
            "semantic_analytics": _build_semantic_analytics(sources),
            "missing_data": missing_data,
        }


def format_evidence_pack_for_prompt(evidence_pack: Dict[str, Any]) -> str:
    """Format normalized evidence pack for direct prompt inclusion."""
    if not evidence_pack:
        return ""

    lines = [
        f"Provider: {evidence_pack.get('provider', 'unknown')}",
        f"Depth: {evidence_pack.get('depth', 'unknown')}",
    ]
    ticker = evidence_pack.get("ticker")
    if ticker:
        lines.append(f"Ticker: {ticker}")

    sources = evidence_pack.get("sources", [])
    lines.append(f"Sources collected: {len(sources)}")
    for idx, source in enumerate(sources, start=1):
        lines.append(f"{idx}. {source.get('title', 'Untitled')}")
        lines.append(f"   URL: {source.get('url', '')}")
        if source.get("source_type"):
            lines.append(f"   Type: {source['source_type']}")
        if source.get("published_at"):
            lines.append(f"   Published: {source['published_at']}")
        snippet = source.get("snippet", "")
        if snippet:
            if len(snippet) > 300:
                snippet = snippet[:297] + "..."
            lines.append(f"   Snippet: {snippet}")

    key_facts = evidence_pack.get("key_facts", [])
    if key_facts:
        lines.append("")
        lines.append("Key facts extracted:")
        for fact in key_facts[:10]:
            lines.append(f"- {fact}")

    semantic_analytics = evidence_pack.get("semantic_analytics")
    if isinstance(semantic_analytics, dict) and semantic_analytics:
        lines.append("")
        lines.append("Semantic analytics:")
        top_domains = semantic_analytics.get("top_domains", []) or []
        source_types = semantic_analytics.get("source_type_counts", {}) or {}
        recency = semantic_analytics.get("recency_buckets", {}) or {}
        if top_domains:
            lines.append(
                "- top_domains: "
                + ", ".join(f"{d.get('domain')}({d.get('count')})" for d in top_domains[:5])
            )
        if source_types:
            lines.append(
                "- source_types: "
                + ", ".join(f"{k}={v}" for k, v in source_types.items())
            )
        if recency:
            lines.append(
                "- recency: "
                + ", ".join(f"{k}={v}" for k, v in recency.items())
            )

    claim_ledger = evidence_pack.get("claim_ledger")
    if isinstance(claim_ledger, dict) and claim_ledger:
        lines.append("")
        lines.append("Verified Claim Ledger (reconciled before Stage 2):")
        counts = claim_ledger.get("counts", {}) or {}
        lines.append(
            f"- Raw claims: {int(counts.get('raw_claims', 0))}, "
            f"Resolved fields: {int(counts.get('resolved_fields', 0))}, "
            f"Conflicts: {int(counts.get('conflicts', 0))}"
        )

        resolved = claim_ledger.get("resolved_claims", {}) or {}
        preferred_fields = [
            "project_stage",
            "stage_multiplier",
            "post_tax_npv_aud_m",
            "post_tax_npv_usd_m",
            "aisc_usd_per_oz",
            "market_cap_aud_m",
            "shares_outstanding_b",
            "funding_status",
        ]
        for field in preferred_fields:
            row = resolved.get(field)
            if not isinstance(row, dict):
                continue
            value = row.get("value")
            unit = str(row.get("unit", "")).strip()
            source_id = str(row.get("source_id", "")).strip()
            published = str(row.get("published_at", "")).strip()
            url = str(row.get("url", "")).strip()
            suffix = f" {unit}" if unit else ""
            source_part = f" [{source_id}]" if source_id else ""
            date_part = f" ({published})" if published else ""
            lines.append(
                f"- {field}: {value}{suffix}{source_part}{date_part}"
            )
            if url:
                lines.append(f"  source: {url}")

        conflicts = claim_ledger.get("conflicts", []) or []
        for conflict in conflicts[:3]:
            if not isinstance(conflict, dict):
                continue
            field = str(conflict.get("field", "")).strip()
            selected = conflict.get("selected_value")
            candidates = conflict.get("candidates", []) or []
            lines.append(
                f"- conflict({field}): selected={selected}, alternatives={len(candidates)}"
            )

    deterministic_lane = evidence_pack.get("deterministic_finance_lane")
    if isinstance(deterministic_lane, dict) and deterministic_lane:
        lines.append("")
        lines.append("Deterministic Finance Lane (from verified fields):")
        lines.append(f"- status: {deterministic_lane.get('status', 'unknown')}")
        derived = deterministic_lane.get("derived_metrics", {}) or {}
        scores = deterministic_lane.get("score_components", {}) or {}
        risked_npv_aud = derived.get("risked_npv_aud_m")
        risked_npv_usd = derived.get("risked_npv_usd_m")
        ratio = derived.get("npv_market_cap_ratio")
        ratio_basis = derived.get("npv_market_cap_ratio_basis")
        if risked_npv_aud is not None:
            lines.append(f"- risked_npv_aud_m: {risked_npv_aud}")
        if risked_npv_usd is not None:
            lines.append(f"- risked_npv_usd_m: {risked_npv_usd}")
        if ratio is not None:
            lines.append(f"- npv_market_cap_ratio: {ratio} ({ratio_basis})")
        if scores.get("value_npv_vs_market_cap_score") is not None:
            lines.append(
                f"- value_npv_vs_market_cap_score: {scores.get('value_npv_vs_market_cap_score')}"
            )
        if scores.get("quality_stage_score_component") is not None:
            lines.append(
                f"- quality_stage_score_component: {scores.get('quality_stage_score_component')}"
            )
        missing_critical = deterministic_lane.get("missing_critical_fields", []) or []
        if missing_critical:
            lines.append(
                f"- missing_critical_fields: {', '.join(str(item) for item in missing_critical[:6])}"
            )

    missing = evidence_pack.get("missing_data", [])
    if missing:
        lines.append("")
        lines.append("Missing/weak data signals:")
        for item in missing:
            lines.append(f"- {item}")

    return "\n".join(lines)


_GENERIC_QUERY_TOKENS: Set[str] = {
    "latest",
    "metrics",
    "timeline",
    "update",
    "updates",
    "market",
    "cap",
    "shares",
    "outstanding",
    "company",
    "limited",
    "ltd",
    "inc",
    "corp",
    "plc",
    "first",
    "gold",
    "quarterly",
    "annual",
    "report",
    "reports",
    "cash",
    "funding",
    "valuation",
    "analysis",
    "rate",
    "quality",
    "deep",
    "research",
    "asx",
    "nyse",
    "nasdaq",
    "tsx",
    "tsxv",
}


def _guess_source_type(url: str, title: str) -> str:
    """Classify source based on URL/domain and title hints."""
    text = f"{url} {title}".lower()
    host = ""
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ""

    if any(token in text for token in ["asx", "sec.gov", "10-k", "10-q", "8-k", "filing"]):
        return "filing"
    if any(token in text for token in ["investor", "presentation", "deck"]):
        return "presentation"
    if any(token in host for token in ["yahoo.com", "investing.com", "marketwatch", "bloomberg"]):
        return "market_data"
    if any(token in host for token in ["marketindex.com.au", "intelligentinvestor.com.au", "stockanalysis.com"]):
        return "market_data"
    if any(token in text for token in ["quarterly", "annual report", "earnings", "results"]):
        return "company_report"
    return "web"


def _source_type_from_bucket(source_bucket: str, *, url: str, title: str) -> str:
    """Map ranking bucket into stable source type labels for evidence pack."""
    bucket = (source_bucket or "").strip().lower()
    if bucket in {"primary_exchange", "primary_company"}:
        return "filing"
    if bucket == "trusted_secondary":
        return "market_data"
    if bucket == "analyst_coverage":
        return "analyst"
    if bucket == "social_discussion":
        return "discussion"
    return _guess_source_type(url, title)


def _build_entity_profile(user_query: str, ticker: Optional[str]) -> Dict[str, Any]:
    """Extract symbol and company-like terms used for strict entity relevance gating."""
    ticker_text = str(ticker or "").strip().upper()
    query_text = str(user_query or "").strip()
    query_lower = query_text.lower()

    symbol_terms: Set[str] = set()
    if ticker_text:
        if ":" in ticker_text:
            _, symbol = ticker_text.split(":", 1)
        else:
            symbol = ticker_text
        symbol = symbol.strip()
        if symbol:
            symbol_terms.add(symbol.lower())
            symbol_terms.add(symbol.replace(".", "").lower())

    raw_tokens = re.findall(r"[A-Za-z][A-Za-z0-9&.-]{1,}", query_text)
    term_tokens: Set[str] = set()
    for token in raw_tokens:
        low = token.lower().strip(".:-_")
        if len(low) < 3:
            continue
        if low in _GENERIC_QUERY_TOKENS:
            continue
        # Suppress long numeric-ish tokens or obvious year artifacts.
        if low.isdigit():
            continue
        term_tokens.add(low)

    phrase_terms: Set[str] = set()
    words = [w.lower() for w in re.findall(r"[A-Za-z]{3,}", query_text)]
    for idx in range(len(words) - 1):
        a, b = words[idx], words[idx + 1]
        if a in _GENERIC_QUERY_TOKENS or b in _GENERIC_QUERY_TOKENS:
            continue
        phrase_terms.add(f"{a} {b}")

    return {
        "query_lower": query_lower,
        "symbols": sorted(symbol_terms),
        "terms": sorted(term_tokens),
        "phrases": sorted(phrase_terms),
    }


def _entity_match_boost(
    *,
    title: str,
    url: str,
    content: str,
    host: str,
    exchange: str,
    entity_profile: Dict[str, Any],
) -> Tuple[float, bool]:
    """Score entity relevance with strict title/url-first gating."""
    title_url = f"{title} {url}".lower()
    content_text = str(content or "").lower()[:1200]
    if not title_url and not content_text:
        return -0.90, False

    symbols = entity_profile.get("symbols", []) or []
    terms = entity_profile.get("terms", []) or []
    phrases = entity_profile.get("phrases", []) or []

    symbol_hit_title = False
    for symbol in symbols:
        if symbol and re.search(rf"\b{re.escape(symbol)}\b", title_url):
            symbol_hit_title = True
            break

    symbol_hit_content = False
    if not symbol_hit_title:
        for symbol in symbols:
            if symbol and re.search(rf"\b{re.escape(symbol)}\b", content_text):
                symbol_hit_content = True
                break

    phrase_hit_title = False
    for phrase in phrases:
        if phrase and phrase in title_url:
            phrase_hit_title = True
            break

    phrase_hit_content = False
    if not phrase_hit_title:
        for phrase in phrases:
            if phrase and phrase in content_text:
                phrase_hit_content = True
                break

    term_hits_title = 0
    for term in terms:
        if term and re.search(rf"\b{re.escape(term)}\b", title_url):
            term_hits_title += 1
            if term_hits_title >= 4:
                break

    term_hits_content = 0
    for term in terms:
        if term and re.search(rf"\b{re.escape(term)}\b", content_text):
            term_hits_content += 1
            if term_hits_content >= 4:
                break

    entity_match = bool(symbol_hit_title or phrase_hit_title or term_hits_title >= 2)
    if (not entity_match) and (symbol_hit_content or phrase_hit_content):
        # Allow content-only matches only for exchange-primary style hosts.
        host_l = (host or "").lower()
        exchange_domains = _expected_domains_for_exchange(exchange)
        if any(domain in host_l for domain in exchange_domains):
            entity_match = True

    if not entity_match:
        return -0.85, False

    score = 0.0
    if symbol_hit_title:
        score += 0.45
    elif symbol_hit_content:
        score += 0.20

    if phrase_hit_title:
        score += 0.25
    elif phrase_hit_content:
        score += 0.10

    if term_hits_title >= 2:
        score += 0.20
    elif term_hits_title == 1:
        score += 0.10
    elif term_hits_content >= 2:
        score += 0.08
    return score, True


def _source_tier_boost(
    *,
    host: str,
    title: str,
    url: str,
    content: str,
    exchange: str,
) -> Tuple[str, float]:
    """Assign source tier bucket with exchange-aware soft boosts."""
    h = (host or "").lower()
    t = (title or "").lower()
    u = (url or "").lower()
    c = (content or "").lower()
    text = f"{t} {u} {c[:500]}"
    exchange_primary = set(_expected_domains_for_exchange(exchange))

    if any(domain in h for domain in exchange_primary):
        return "primary_exchange", 0.62

    if _looks_like_primary_company_document(title=t, url=u):
        return "primary_company", 0.46

    trusted_secondary_domains = (
        "intelligentinvestor.com.au",
        "listcorp.com",
        "stockanalysis.com",
        "morningstar.com",
        "marketwatch.com",
        "marketscreener.com",
        "tradingview.com",
    )
    if any(domain in h for domain in trusted_secondary_domains):
        return "trusted_secondary", 0.28

    if _looks_like_analyst_coverage(host=h, title=t, content=text):
        return "analyst_coverage", 0.16

    if _looks_like_social_or_forum(host=h, title=t, content=text):
        return "social_discussion", -0.20

    return "other", 0.02


def _looks_like_primary_company_document(*, title: str, url: str) -> bool:
    """Heuristic for official company material mirrored outside exchange domains."""
    text = f"{title} {url}".lower()
    doc_tokens = (
        "quarterly activity report",
        "quarterly report",
        "annual report",
        "investor presentation",
        "corporate presentation",
        "definitive feasibility study",
        "dfs",
        "pfs",
        "feasibility study",
        "appendix 5b",
        "appendix 5c",
        "asx announcement",
    )
    if any(token in text for token in doc_tokens):
        return True
    if ".pdf" in text and any(token in text for token in ("quarterly", "annual", "presentation", "appendix")):
        return True
    return False


def _looks_like_analyst_coverage(*, host: str, title: str, content: str) -> bool:
    """Heuristic for broker/research coverage."""
    host_tokens = (
        "mining.com",
        "smallcaps.com.au",
        "streetwisereports.com",
        "cruxinvestor.com",
        "researchtree.com",
    )
    text = f"{title} {content}".lower()
    text_tokens = (
        "analyst",
        "broker note",
        "research report",
        "valuation",
        "price target",
        "buy rating",
    )
    if any(host == token or host.endswith(f".{token}") for token in host_tokens):
        return True
    return any(token in text for token in text_tokens)


def _looks_like_social_or_forum(*, host: str, title: str, content: str) -> bool:
    """Heuristic for lower-priority discussion sources."""
    host_tokens = (
        "twitter.com",
        "x.com",
        "reddit.com",
        "hotcopper.com.au",
        "stocktwits.com",
        "futunn.com",
    )
    text = f"{title} {content}".lower()
    if any(token in host for token in host_tokens):
        return True
    return any(token in text for token in ("forum", "thread", "post", "community discussion"))


def _content_density_boost(content: str) -> float:
    """Reward substantive excerpts while avoiding oversized influence."""
    size = len((content or "").strip())
    if size <= 80:
        return -0.05
    if size <= 300:
        return 0.02
    if size <= 1200:
        return 0.07
    if size <= 3000:
        return 0.11
    return 0.14


def _select_results_with_quotas(
    *,
    candidates: List[Dict[str, Any]],
    target: int,
    exchange: str,
) -> List[Dict[str, Any]]:
    """Select top-N while ensuring primary-document floor without hard allowlists."""
    if not candidates:
        return []
    target_n = max(1, int(target))

    primary_buckets = {"primary_exchange", "primary_company"}
    secondary_bucket = {"trusted_secondary"}
    ranked = sorted(candidates, key=lambda r: float(r.get("score", 0.0)), reverse=True)

    exchange_key = (exchange or "").upper()
    if exchange_key:
        primary_min = max(1, int(round(target_n * 0.5)))
    else:
        primary_min = max(1, target_n // 3)
    primary_min = min(primary_min, target_n)

    recent_primary_min = min(primary_min, 2 if target_n >= 6 else 1)
    secondary_min = 1 if target_n >= 6 else 0

    selected: List[Dict[str, Any]] = []
    seen_urls: Set[str] = set()

    def _add(row: Dict[str, Any]) -> bool:
        url = str(row.get("url", "") or "").strip()
        key = url or f"title::{str(row.get('title', ''))[:120].lower()}"
        if key in seen_urls:
            return False
        seen_urls.add(key)
        selected.append(row)
        return True

    recent_primary = [
        r for r in ranked
        if str(r.get("source_bucket", "")) in primary_buckets and _is_recent(r.get("published_at", ""), 180)
    ]
    for row in recent_primary:
        if len([x for x in selected if str(x.get("source_bucket", "")) in primary_buckets]) >= recent_primary_min:
            break
        if len(selected) >= target_n:
            break
        _add(row)

    primary_all = [r for r in ranked if str(r.get("source_bucket", "")) in primary_buckets]
    for row in primary_all:
        if len([x for x in selected if str(x.get("source_bucket", "")) in primary_buckets]) >= primary_min:
            break
        if len(selected) >= target_n:
            break
        _add(row)

    secondary_all = [r for r in ranked if str(r.get("source_bucket", "")) in secondary_bucket]
    for row in secondary_all:
        if len([x for x in selected if str(x.get("source_bucket", "")) in secondary_bucket]) >= secondary_min:
            break
        if len(selected) >= target_n:
            break
        _add(row)

    for row in ranked:
        if len(selected) >= target_n:
            break
        _add(row)

    return selected[:target_n]


def _is_recent(published_at: str, days_threshold: int) -> bool:
    """Return True when source date is within threshold days."""
    text = str(published_at or "").strip()
    if not text:
        return False
    try:
        ts = datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        try:
            ts = datetime.strptime(text[:10], "%Y-%m-%d")
        except Exception:
            return False
    age_days = (datetime.utcnow() - ts).days
    return age_days <= int(days_threshold)


def _count_source_buckets(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count selected source buckets for observability."""
    counts: Dict[str, int] = {}
    for row in rows:
        bucket = str(row.get("source_bucket", "other")).strip() or "other"
        counts[bucket] = counts.get(bucket, 0) + 1
    return counts


def _extract_key_facts(summary: str) -> List[str]:
    """Extract lightweight fact bullets from provider summary text."""
    if not summary:
        return []

    facts: List[str] = []
    for raw_line in summary.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("-", "*")):
            line = line[1:].strip()
        if len(line) < 20:
            continue
        facts.append(line)
        if len(facts) >= 8:
            break

    # Fallback: use the first sentence-like chunk if no bullet-ish lines found.
    if not facts:
        compact = " ".join(summary.split())
        if compact:
            facts.append(compact[:220] + ("..." if len(compact) > 220 else ""))
    return facts


def _infer_exchange_from_ticker(ticker: str) -> str:
    """Infer exchange code from prefix/suffix ticker notation."""
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
    """Primary filing/reference domains expected for a given exchange."""
    fallback_map = {
        "ASX": ["asx.com.au", "marketindex.com.au", "wcsecure.weblink.com.au"],
        "NYSE": ["sec.gov"],
        "NASDAQ": ["sec.gov"],
        "TSX": ["globenewswire.com"],
        "TSXV": ["globenewswire.com"],
        "LSE": ["londonstockexchange.com", "investegate.co.uk"],
        "AIM": ["londonstockexchange.com", "investegate.co.uk"],
    }

    key = (exchange or "").strip()
    key_upper = key.upper()

    try:
        loader = get_template_loader()
        normalized = loader.normalize_exchange(key) or loader.normalize_exchange(key_upper)
        if not normalized and key_upper:
            alias_map = {
                "ASX": "asx",
                "NYSE": "nyse",
                "NASDAQ": "nasdaq",
                "TSX": "tsx",
                "TSXV": "tsxv",
                "LSE": "lse",
                "AIM": "aim",
            }
            normalized = alias_map.get(key_upper)
        if normalized:
            params = loader.get_exchange_retrieval_params(normalized)
            suffixes = [
                str(item).strip().lower()
                for item in (params.get("allowed_domain_suffixes", []) or [])
                if str(item).strip()
            ]
            deduped: List[str] = []
            for item in suffixes:
                if item not in deduped:
                    deduped.append(item)
            if deduped:
                return deduped
    except Exception:
        pass

    return fallback_map.get(key_upper, [])


def _sources_include_expected_domains(
    sources: List[Dict[str, Any]],
    expected_domains: List[str],
) -> bool:
    """Return True when at least one source URL matches expected exchange domains."""
    expected = [domain.lower() for domain in expected_domains if domain]
    if not expected:
        return True
    for source in sources:
        url = str(source.get("url", "")).strip()
        if not url:
            continue
        try:
            host = urlparse(url).netloc.lower()
        except Exception:
            continue
        if any(domain in host for domain in expected):
            return True
    return False


def _recency_boost(published_at: str) -> float:
    """Recency score with stronger bias toward last 6 months."""
    text = (published_at or "").strip()
    if not text:
        return -0.02
    try:
        ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        try:
            ts = datetime.strptime(text[:10], "%Y-%m-%d")
        except Exception:
            return -0.02
    now = datetime.utcnow()
    age_days = (now - ts.replace(tzinfo=None)).days
    if age_days <= 30:
        return 0.25
    if age_days <= 90:
        return 0.18
    if age_days <= 180:
        return 0.10
    if age_days <= 365:
        return 0.03
    return -0.08


def _build_semantic_analytics(sources: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute compact analytics for merged semantic+web retrieval."""
    domain_counts: Dict[str, int] = {}
    source_type_counts: Dict[str, int] = {}
    recency_buckets = {"0_30d": 0, "31_90d": 0, "91_365d": 0, "over_365d": 0, "unknown": 0}

    now = datetime.utcnow()
    for source in sources:
        url = str(source.get("url", "")).strip()
        source_type = str(source.get("source_type", "web")).strip() or "web"
        source_type_counts[source_type] = source_type_counts.get(source_type, 0) + 1

        try:
            host = urlparse(url).netloc.lower()
        except Exception:
            host = ""
        if host:
            domain_counts[host] = domain_counts.get(host, 0) + 1

        published = str(source.get("published_at", "")).strip()
        if not published:
            recency_buckets["unknown"] += 1
            continue
        dt_obj: Optional[datetime] = None
        try:
            dt_obj = datetime.fromisoformat(published.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            try:
                dt_obj = datetime.strptime(published[:10], "%Y-%m-%d")
            except Exception:
                dt_obj = None
        if dt_obj is None:
            recency_buckets["unknown"] += 1
            continue
        age_days = (now - dt_obj).days
        if age_days <= 30:
            recency_buckets["0_30d"] += 1
        elif age_days <= 90:
            recency_buckets["31_90d"] += 1
        elif age_days <= 365:
            recency_buckets["91_365d"] += 1
        else:
            recency_buckets["over_365d"] += 1

    top_domains = sorted(
        [{"domain": domain, "count": count} for domain, count in domain_counts.items()],
        key=lambda x: x["count"],
        reverse=True,
    )
    return {
        "top_domains": top_domains[:10],
        "source_type_counts": source_type_counts,
        "recency_buckets": recency_buckets,
    }
