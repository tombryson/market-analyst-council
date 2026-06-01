"""Render second-pass source rows and fact packs for downstream council stages."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _clean_text(value: Any, *, max_chars: int = 0) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if max_chars > 0 and len(text) > max_chars:
        return text[: max_chars - 3].rstrip() + "..."
    return text


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _iter_candidate_results(evidence_pack: Optional[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    if not isinstance(evidence_pack, dict):
        return

    # Stage 1 metadata from the emulated Perplexity lane.
    for model_run in _as_list(evidence_pack.get("per_model_research_runs")):
        result = _as_dict(_as_dict(model_run).get("result"))
        if result:
            yield result

    # Some callers attach the Stage 1 metadata under the UI evidence pack.
    for nested_key in ("stage1_emulated_metadata", "stage1_metadata"):
        nested = _as_dict(evidence_pack.get(nested_key))
        for model_run in _as_list(nested.get("per_model_research_runs")):
            result = _as_dict(_as_dict(model_run).get("result"))
            if result:
                yield result

    # Allow direct use with a single model result in tests or audit scripts.
    if any(
        key in evidence_pack
        for key in (
            "stage1_second_pass_source_rows",
            "stage1_second_pass_fact_pack",
            "stage1_second_pass_fact_digest_v2",
            "stage1_second_pass_compact_fact_bundle",
            "stage1_second_pass_mandatory_fact_ledger",
        )
    ):
        yield evidence_pack


def _fact_count(packet: Dict[str, Any]) -> int:
    sections = _as_dict(packet.get("sections") or packet.get("categories"))
    total = 0
    for values in sections.values():
        total += len(_as_list(values))
    return total


def _score_result(result: Dict[str, Any]) -> Tuple[int, int, int]:
    source_rows = _as_list(result.get("stage1_second_pass_source_rows"))
    fact_pack = _as_dict(result.get("stage1_second_pass_fact_pack"))
    digest = _as_dict(result.get("stage1_second_pass_fact_digest_v2"))
    compact = _as_dict(result.get("stage1_second_pass_compact_fact_bundle"))
    ledger = _as_dict(result.get("stage1_second_pass_mandatory_fact_ledger"))
    return (
        _fact_count(fact_pack)
        + _fact_count(digest)
        + _fact_count(compact)
        + len(_as_list(ledger.get("facts"))),
        len(source_rows),
        len(str(result.get("stage1_second_pass") or "")),
    )


def _select_best_second_pass_result(evidence_pack: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    candidates = list(_iter_candidate_results(evidence_pack))
    if not candidates:
        return {}
    return max(candidates, key=_score_result)


def _source_id(row: Dict[str, Any], index: int) -> str:
    raw = str(row.get("source_id") or "").strip()
    return raw or f"S{index}"


def _source_rows_from_result(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = _as_list(result.get("stage1_second_pass_source_rows"))
    if rows:
        return [_as_dict(row) for row in rows if isinstance(row, dict)]

    # Fall back to source indexes inside fact packets when rows are not present.
    for key in (
        "stage1_second_pass_fact_pack",
        "stage1_second_pass_fact_digest_v2",
        "stage1_second_pass_compact_fact_bundle",
    ):
        packet = _as_dict(result.get(key))
        source_index = _as_list(packet.get("source_index"))
        if source_index:
            return [_as_dict(row) for row in source_index if isinstance(row, dict)]
    return []


def _packet_from_result(result: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    for key in ("stage1_second_pass_fact_pack", "stage1_second_pass_fact_digest_v2"):
        packet = _as_dict(result.get(key))
        if _fact_count(packet) > 0:
            return key, packet
    packet = _as_dict(result.get("stage1_second_pass_compact_fact_bundle"))
    if _fact_count(packet) > 0:
        return "stage1_second_pass_compact_fact_bundle", packet
    return "", {}


def _render_mandatory_fact_ledger(result: Dict[str, Any], *, max_facts: int) -> List[str]:
    ledger = _as_dict(result.get("stage1_second_pass_mandatory_fact_ledger"))
    facts = _as_list(ledger.get("facts"))
    if not facts:
        return []
    rendered = [
        "Mandatory source fact ledger:",
        "- Instruction: these facts were pinned into Stage 1 and must not be treated as absent or unknown.",
    ]
    for item in facts[:max_facts]:
        row = _as_dict(item)
        fact = _clean_text(row.get("fact"), max_chars=420)
        if not fact:
            continue
        sid = _clean_text(row.get("source_id"), max_chars=24)
        family = _clean_text(row.get("family"), max_chars=60).replace("_", " ")
        published = _clean_text(row.get("published_at"), max_chars=40)
        prefix_bits = []
        if sid:
            prefix_bits.append(f"[{sid}]")
        if family:
            prefix_bits.append(family)
        if published:
            prefix_bits.append(f"({published})")
        prefix = " ".join(prefix_bits)
        rendered.append(f"- {prefix}: {fact}" if prefix else f"- {fact}")
    return rendered


def _render_source_index(source_rows: List[Dict[str, Any]], *, max_rows: int) -> List[str]:
    rendered: List[str] = []
    for index, row in enumerate(source_rows[:max_rows], start=1):
        sid = _source_id(row, index)
        title = _clean_text(row.get("title") or "Untitled", max_chars=120)
        published = _clean_text(row.get("published_at") or row.get("date"), max_chars=40)
        url = _clean_text(row.get("url"), max_chars=180)
        decoded = row.get("decoded")
        status_bits = []
        if published:
            status_bits.append(published)
        if decoded is not None:
            status_bits.append("decoded" if bool(decoded) else "not decoded")
        status = f" ({'; '.join(status_bits)})" if status_bits else ""
        line = f"- [{sid}] {title}{status}"
        if url:
            line += f" | {url}"
        rendered.append(line)
    return rendered


def _render_source_excerpts(source_rows: List[Dict[str, Any]], *, max_rows: int) -> List[str]:
    rendered: List[str] = []
    for index, row in enumerate(source_rows[:max_rows], start=1):
        excerpt = _clean_text(row.get("excerpt") or row.get("snippet") or row.get("content"), max_chars=360)
        if not excerpt:
            continue
        sid = _source_id(row, index)
        title = _clean_text(row.get("title") or "Untitled", max_chars=80)
        rendered.append(f"- [{sid}] {title}: {excerpt}")
    return rendered


def _render_fact_sections(
    packet: Dict[str, Any],
    *,
    max_sections: int,
    max_facts_per_section: int,
) -> List[str]:
    sections = _as_dict(packet.get("sections") or packet.get("categories"))
    rendered: List[str] = []
    seen: set[str] = set()

    for section_name, values in list(sections.items())[:max_sections]:
        facts: List[str] = []
        for item in _as_list(values):
            row = _as_dict(item)
            if row:
                fact = _clean_text(
                    row.get("fact")
                    or row.get("claim")
                    or row.get("text")
                    or row.get("summary"),
                    max_chars=420,
                )
                sid = _clean_text(row.get("source_id"), max_chars=24)
                published = _clean_text(row.get("published_at"), max_chars=40)
            else:
                fact = _clean_text(item, max_chars=420)
                sid = ""
                published = ""
            if not fact:
                continue
            key = re.sub(r"[^a-z0-9]+", " ", f"{section_name} {sid} {fact}".lower()).strip()
            if not key or key in seen:
                continue
            seen.add(key)
            prefix_bits = []
            if sid:
                prefix_bits.append(f"[{sid}]")
            if published:
                prefix_bits.append(f"({published})")
            prefix = " ".join(prefix_bits)
            facts.append(f"- {prefix} {fact}".strip())
            if len(facts) >= max_facts_per_section:
                break
        if facts:
            label = _clean_text(section_name).replace("_", " ").title()
            rendered.append(f"{label}:\n" + "\n".join(facts))
    return rendered


def build_source_fact_context(
    evidence_pack: Optional[Dict[str, Any]],
    *,
    max_source_rows: int = 15,
    max_excerpt_rows: int = 6,
    max_sections: int = 10,
    max_facts_per_section: int = 8,
    max_chars: int = 12000,
) -> str:
    """Render the authoritative Stage 1 second-pass packet for Stage 2.5/3.

    The renderer is architecture-wide: it preserves the fact-pack sections
    already produced by Stage 1 instead of using sector-specific keyword
    filters.
    """
    result = _select_best_second_pass_result(evidence_pack)
    if not result:
        return ""

    source_rows = _source_rows_from_result(result)
    packet_key, fact_packet = _packet_from_result(result)
    ledger = _as_dict(result.get("stage1_second_pass_mandatory_fact_ledger"))
    ledger_count = len(_as_list(ledger.get("facts")))
    if not source_rows and not fact_packet and not ledger_count:
        return ""

    sections: List[str] = []
    fact_total = _fact_count(fact_packet)
    schema = _clean_text(fact_packet.get("schema"), max_chars=80)
    sections.append(
        "PRIMARY/PREPASS SOURCE FACT PACKET\n"
        "Source packet status:\n"
        f"- second_pass_source_rows: {len(source_rows)}\n"
        f"- mandatory_fact_ledger_facts: {ledger_count}\n"
        f"- fact_packet: {packet_key or 'none'}\n"
        f"- fact_packet_schema: {schema or 'unknown'}\n"
        f"- rendered_fact_count: {fact_total}\n"
        "- Instruction: treat the rows and facts below as injected primary/prepass evidence. "
        "Do not claim a data point is absent, unknown, or unverified when this packet contains it."
    )

    ledger_lines = _render_mandatory_fact_ledger(result, max_facts=max_facts_per_section)
    if ledger_lines:
        sections.append("\n".join(ledger_lines))

    fact_sections = _render_fact_sections(
        fact_packet,
        max_sections=max_sections,
        max_facts_per_section=max_facts_per_section,
    )
    if fact_sections:
        sections.append("Facts present by source-pack section:\n" + "\n\n".join(fact_sections))

    source_index = _render_source_index(source_rows, max_rows=max_source_rows)
    if source_index:
        sections.append("Source index:\n" + "\n".join(source_index))

    excerpts = _render_source_excerpts(source_rows, max_rows=max_excerpt_rows)
    if excerpts:
        sections.append("Decoded source excerpts:\n" + "\n".join(excerpts))

    rendered = "\n\n".join(sections).strip()
    if max_chars > 0 and len(rendered) > max_chars:
        rendered = rendered[: max_chars - 3].rstrip() + "..."
    return rendered
