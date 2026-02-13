"""Provider interface for retrieval backends."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class ResearchProvider(ABC):
    """Abstract provider used by ResearchService."""

    name: str = "unknown"

    @abstractmethod
    async def gather(
        self,
        user_query: str,
        ticker: Optional[str] = None,
        depth: str = "basic",
        max_sources: int = 10,
        model_override: Optional[str] = None,
        research_brief: str = "",
    ) -> Dict[str, Any]:
        """
        Gather research results in a search-like structure.

        Expected keys:
        - query
        - results (list of {title, url, content, score})
        - result_count
        - optional: evidence_pack, research_summary, error
        """
        raise NotImplementedError
