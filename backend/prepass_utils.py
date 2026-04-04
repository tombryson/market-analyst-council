import re
from typing import Any


def tail_text(value: Any, max_chars: int = 1200) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def normalize_retrieval_query_seed(
    *,
    company_name: str = "",
    query_hint: str = "",
    ticker: str = "",
) -> str:
    preferred = str(company_name or "").strip()
    if preferred:
        return re.sub(r"\s+", " ", preferred).strip()[:120]

    text = str(query_hint or "").strip() or str(ticker or "").strip()
    text = re.sub(r"^\s*run\s+full\s+analysis\s+on\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*\([A-Z]{2,6}:[A-Z0-9.\-]+\)\s*$", "", text).strip()
    return re.sub(r"\s+", " ", text).strip()[:120]
