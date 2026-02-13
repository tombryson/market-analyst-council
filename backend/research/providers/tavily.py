"""Legacy Tavily-backed provider adapter."""

from datetime import datetime
from typing import Any, Dict, Optional

from .base import ResearchProvider
from ...search import (
    perform_search,
    reformulate_query_for_search,
    perform_financial_search,
)


class TavilyResearchProvider(ResearchProvider):
    """Adapter to keep existing Tavily behavior behind provider interface."""

    name = "tavily"

    async def gather(
        self,
        user_query: str,
        ticker: Optional[str] = None,
        depth: str = "basic",
        max_sources: int = 10,
        model_override: Optional[str] = None,
        research_brief: str = "",
    ) -> Dict[str, Any]:
        """Run legacy search logic with configurable limits."""
        _ = model_override  # This provider does not support model-specific overrides.
        _ = research_brief  # This provider does not use template-guided research prompts.
        if ticker:
            result = await perform_financial_search(ticker)
        else:
            search_query = await reformulate_query_for_search(user_query)
            search_depth = "advanced" if depth == "deep" else "basic"
            result = await perform_search(
                search_query,
                max_results=max_sources,
                search_depth=search_depth,
            )

        if result is None:
            result = {
                "error": "Tavily provider returned no result",
                "results": [],
                "result_count": 0,
            }

        result.setdefault("query", user_query)
        result.setdefault("results", [])
        result.setdefault("result_count", len(result["results"]))
        result.setdefault("performed_at", datetime.utcnow().isoformat())
        result["provider"] = self.name
        result["search_depth"] = depth
        return result
