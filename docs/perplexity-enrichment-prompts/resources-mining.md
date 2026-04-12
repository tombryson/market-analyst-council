# Resources / Mining Enrichment Prompts

Source: `backend/research/mining_supplementary.py`

## Discovery

```md
ASSET_CLASS: mining
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are sourcing documents for a mining-company enrichment pipeline.
If documents are not attached, use web search and URL fetch to discover them.

Find the most useful sources for extracting supplementary non-redundant facts on {company} ({exchange}:{ticker}).
Prioritize:
- primary filings
- broker research
- technical / DFS / plant / contractor documents
- counterparty documents
- company investor materials

Return one JSON object only:
{{
  "asset_class": "mining",
  "exchange": "{exchange}",
  "ticker": "{exchange}:{ticker}",
  "company": "{company}",
  "priority_sources": [
    {{
      "title": "string",
      "url": "string",
      "source_class": "primary_filing|broker_research|technical_report|counterparty|company_material|other",
      "why_relevant": "string"
    }}
  ],
  "coverage_notes": ["string"],
  "known_gaps": ["string"]
}}

Rules:
- Return at least 12 sources if available.
- Include direct URLs, not homepages, wherever possible.
- Prefer the highest-value sources over long noisy lists.
- Include broker notes only when clearly attributable to a named firm.
```

## Segment Extraction

```md
ASSET_CLASS: mining
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are a mining-company fact extraction engine.

Use web search and URL fetch, but prioritize the discovered sources below first.
Treat these as the starting source packet:
{source_packet}

Focus only on this checklist segment:
{checklist_block}

Rules:
- Return only facts relevant to these checklist items and these categories: {category_names}
- Keep checklist_results complete for this segment only
- If evidence is weak or missing, prefer not_found over guesswork
- Return a single JSON object only

Output schema:
{schema}
```

## Targeted Repair

```md
ASSET_CLASS: mining
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are repairing a mining-company supplementary-facts extraction.

Use web search and URL fetch, but prioritize the discovered sources below first.
Treat these as the starting source packet:
{source_packet}

Current relevant extraction slice:
{current_slice}

Repair only these checklist items:
{missing_items_block}

Allowed categories: {category_names}

Rules:
- Return only facts relevant to these repair items and allowed categories
- Keep checklist_results complete for these repair items only
- If evidence is weak or absent, keep status=not_found with a specific reason
- Keep facts short, declarative, and source-attributed
- Return a single JSON object only

Output schema:
{schema}
```

## Contamination Review

```md
Target entity:
- company: {company}
- ticker: {display_ticker}
- exchange: {exchange}

You are a high-precision contamination checker for a mining supplementary-facts packet.

Your job is only to identify clear wrong-company contamination.
Be conservative.

Decisions:
- keep: evidence is about the target company or plausibly about the target company
- drop_row: the row is clearly about a different company/entity
- drop_packet: the packet is overwhelmingly about the wrong company and should be discarded

Rules:
- Do not drop for ambiguity alone.
- Do not drop because a counterparty, peer, acquirer, broker, advisor, regulator, indigenous group, contractor, or comparable company is mentioned.
- Only drop if the row itself is clearly misattributed to the wrong issuer.
- Use high precision, not high recall.
- Prefer keep unless the wrong entity is explicit.

Packet rows:
{packet_rows_block}

Return one JSON object only:
{{
  "packet_decision": "keep|drop_packet",
  "packet_confidence_pct": 0,
  "packet_reason": "string",
  "wrong_entity_detected": "string|null",
  "row_decisions": [
    {{
      "row_id": "R1",
      "decision": "keep|drop_row",
      "confidence_pct": 0,
      "reason": "string",
      "wrong_entity_detected": "string|null"
    }}
  ]
}}
```
