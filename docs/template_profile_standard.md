# Template Profile Standard

This repo supports template-specific behavior via YAML, not Python hardcoding.

## Required Components Per Template

1. `rubric`
- Full Stage 1 analysis instruction body.

2. `verification_schema`
- `fact_digest.sections` keyword buckets.
- `fact_digest.timeline_terms` and `fact_digest.timeline_focus_terms`.
- `compliance.section_markers` and `compliance.required_sections`.

3. `template_behavior`
- `commodity_profile`: `gold|copper|lithium|silver|uranium|oil_gas|none`
- `stage1_research_lanes`: list of extra retrieval lanes (e.g., sector macro lane).
- `stage3_scoring_factors`:
  - `quality`: exact factor labels Stage 3 chairman must use
  - `value`: exact factor labels Stage 3 chairman must use

4. `output_schema`
- Structured JSON shape expected by Stage 3 parser/UI.

## How It Is Used

- Stage 1 research brief uses `template_behavior.stage1_research_lanes`.
- Market facts prepass resolves commodity injection from `template_behavior.commodity_profile` first.
- Stage 3 chairman scoring guidance resolves from `template_behavior.stage3_scoring_factors`.
- Stage 1 digest/compliance uses `verification_schema`.
- Stage 3 normalization/UI mapping uses `output_schema`.

## Validation

Run:

```bash
.venv/bin/python scripts/validate_template_profiles.py
```

This checks profile completeness across all templates.
