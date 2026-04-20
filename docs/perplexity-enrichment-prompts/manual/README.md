# Manual Perplexity Enrichment Prompts

These markdown files are Stage 1 retrieval briefs for Perplexity Deep Research or equivalent active research UIs.

Use these prompts to make Perplexity retrieve and organise source material. Then attach the returned source packet/PDF/markdown/text to the normal Alpha Edge / llm-council enrichment or council flow.

Rules for this folder:
- The root `*.md` files are retrieval-first prompts.
- They should not ask Perplexity to enforce the full extraction schema.
- They should return a source packet, not an investment memo.
- They should exclude recommendations, price targets, NAV, NPV, rNPV, DCF, fair value, and analyst opinions.
- Replace all bracketed placeholders before running.
- Preserve wrong-company or unsupported material under `rejected_sources`.

Advanced extractor prompts are preserved in:

`extraction/`

Those Stage 2 files are for manual extraction or backend schema enforcement after a source packet exists.
