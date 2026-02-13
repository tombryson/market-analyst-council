"""Typed contracts for normalized research evidence."""

from typing import List, TypedDict


class EvidenceSource(TypedDict, total=False):
    """Single normalized source used by the council."""

    url: str
    title: str
    snippet: str
    source_type: str
    published_at: str
    score: float
    provider: str


class EvidencePack(TypedDict, total=False):
    """Normalized evidence bundle passed into prompts and UI."""

    question: str
    ticker: str
    provider: str
    depth: str
    generated_at: str
    sources: List[EvidenceSource]
    key_facts: List[str]
    missing_data: List[str]
