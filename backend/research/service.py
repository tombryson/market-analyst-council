"""Research service with provider selection and evidence-pack normalization."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .schemas import EvidencePack, EvidenceSource
from .providers import (
    PerplexityResearchProvider,
    TavilyResearchProvider,
    ResearchProvider,
)
from ..config import RESEARCH_PROVIDER, RESEARCH_DEPTH, MAX_SOURCES


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
        result["results"] = result["results"][: self.max_sources]
        result.setdefault("result_count", len(result["results"]))
        result.setdefault("performed_at", datetime.utcnow().isoformat())
        result.setdefault("provider", provider_name)

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

    def _create_provider(self, provider_name: str) -> ResearchProvider:
        """Construct provider implementation from config name."""
        if provider_name == "perplexity":
            return PerplexityResearchProvider()
        if provider_name == "tavily":
            return TavilyResearchProvider()

        print(f"Unknown RESEARCH_PROVIDER '{provider_name}', defaulting to tavily")
        return TavilyResearchProvider()

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
            sources.append(
                {
                    "url": url,
                    "title": title,
                    "snippet": snippet,
                    "source_type": _guess_source_type(url, title),
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

    missing = evidence_pack.get("missing_data", [])
    if missing:
        lines.append("")
        lines.append("Missing/weak data signals:")
        for item in missing:
            lines.append(f"- {item}")

    return "\n".join(lines)


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
    if any(token in text for token in ["quarterly", "annual report", "earnings", "results"]):
        return "company_report"
    return "web"


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
