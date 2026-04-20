# Perplexity Enrichment Prompts

This directory has two layers of enrichment prompts.

## Manual Stage 1 Retrieval Briefs

Use these when running Perplexity Deep Research or another active research UI manually:

`manual/`

The root manual prompts are retrieval-first. They tell Perplexity what source material to find and organise, then return a source packet suitable for attachment to the normal Alpha Edge / llm-council enrichment flow.

Start here:

`manual/README.md`

## Advanced Stage 2 Extractor Prompts

The previous detailed extraction/schema prompts are preserved here:

`manual/extraction/`

Use those only after a source packet already exists, or for backend/manual schema enforcement.

## Backend Pipeline Snippets

The files in this directory root are exported from backend supplementary pipeline code and are useful for inspecting the segmented API flow:

- `resources-mining.md`
- `pharma-biotech.md`
- `software-saas.md`
- `energy-oil-gas.md`

Those root files are not the manual web-UI prompts. For manual Perplexity runs, use `manual/`.
