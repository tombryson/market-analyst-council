# Supplementary API Pipelines Shelved (2026-04-06)

We prototyped sector-specific Perplexity supplementary pipelines as a way to generate prepass-ready enrichment packets automatically.

Current decision:
- keep the code in-repo
- disable the feature by default
- remove the Alpha Edge UI entry point for now

Why we parked it:
- benchmark quality did not beat the manual Perplexity web UI exports we were already using
- benchmark cost was too high for a supplementary-only step
- the cost/quality tradeoff was not commercially sensible in the current design

Practical outcome:
- use uploaded supplementary documents or manual Perplexity web exports for now
- do not enable API supplementary generation unless we intentionally re-open the experiment

Current gate:
- `SUPPLEMENTARY_API_PIPELINES_ENABLED=false`

If we revisit this later, the bar should be:
- materially lower cost than the current prototype
- clear quality advantage over manual web UI exports, or at least equivalent quality with much better automation value
