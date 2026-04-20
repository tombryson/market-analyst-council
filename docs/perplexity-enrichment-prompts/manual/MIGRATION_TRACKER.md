# Manual Enrichment Prompt Migration Tracker

Current state: root manual prompts are Stage 1 retrieval briefs. Previous detailed Stage 2 extractor prompts are preserved in `extraction/`.

| Asset Class | Retrieval Brief | Extractor Preserved | UI Copy Ready | Fixture Tested | Status |
|---|---|---:|---:|---:|---|
| Bank Financials | `bank-financials.md` | yes | yes | no | ready |
| Consumer Retail | `consumer-retail.md` | yes | yes | no | ready |
| Consumer Staples | `consumer-staples.md` | yes | yes | no | ready |
| Datacentres | `datacentres.md` | yes | yes | no | ready |
| Defence / Aerospace | `defence-aerospace.md` | yes | yes | no | ready |
| Energy / Oil and Gas | `energy-oil-gas.md` | yes | yes | no | ready |
| Financials / Bank and Insurance | `financials-bank-insurance.md` | yes | yes | no | ready |
| Fixed Income / Credit | `fixed-income-credit.md` | yes | yes | no | ready |
| Gambling / Wagering | `gambling-wagering.md` | yes | yes | no | ready |
| Gaming Interactive | `gaming-interactive.md` | yes | yes | no | ready |
| General Equity | `general-equity.md` | yes | yes | no | ready |
| Healthcare Services | `healthcare-services.md` | yes | yes | no | ready |
| Industrials / Consumer / REIT Umbrella | `industrials-consumer-reit.md` | yes | yes | no | ready |
| Industrials | `industrials.md` | yes | yes | no | ready |
| Insurance | `insurance.md` | yes | yes | no | ready |
| Medtech | `medtech.md` | yes | yes | no | ready |
| Pharma / Biotech | `pharma-biotech.md` | yes | yes | no | ready |
| Real Estate / REIT | `real-estate-reit.md` | yes | yes | no | ready |
| Resources / Mining | `resources-mining.md` | yes | yes | no | ready |
| Semiconductors | `semiconductors.md` | yes | yes | no | ready |
| Software / SaaS | `software-saas.md` | yes | yes | no | ready |
| Technology Platforms | `technology-platforms.md` | yes | yes | no | ready |

## Manual Workflow

1. Open Alpha Edge Analysis tab.
2. Open Enrichment Templates.
3. Pick the asset class.
4. Replace placeholders in the copied prompt if needed.
5. Paste the retrieval brief into Perplexity Deep Research.
6. Export or copy the returned source packet.
7. Attach the source packet via DOC in Alpha Edge.
8. Run the normal council/enrichment flow.

## Fixture Testing Still Needed

For each retrieval brief, test:
- A clean same-company run.
- A sparse/no-source run.
- A wrong-company contamination run.
- A source-rich run with primary filings and third-party documents.
