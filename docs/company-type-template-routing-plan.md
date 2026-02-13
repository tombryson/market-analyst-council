# Company-Type and Exchange Routing Plan

## Goal
Route each financial analysis run through the correct analysis template using:
1. Manual company-type selection when provided.
2. Automatic company-type detection when not provided.
3. Template auto-selection tied to company type before Stage 1 retrieval.
4. Manual exchange selection when provided.
5. Automatic exchange detection when not provided.
6. Exchange-specific assumptions injected into Stage 1 prompts.

## Current State (Implemented)
- Added a predefined company-type catalog in `/Users/Toms_Macbook/Projects/llm-council/backend/template_loader.py`.
- Added template routing resolver:
  - explicit `template_id` wins
  - else explicit `company_type`
  - else auto-detect company type
  - else keyword template detection + ticker financial fallback
- Added API endpoint:
  - `GET /api/company-types`
- Added API endpoint:
  - `GET /api/exchanges`
- Added request-level company-type support in streaming endpoint:
  - `/api/conversations/{id}/message/stream` now accepts `company_type` and `exchange`
- Added template/company-type metadata to stored user messages.
- Stage 1 Perplexity retrieval now accepts a `research_brief` generated from selected template + company type + exchange assumptions.
- Added first dedicated gold-miner template:
  - `/Users/Toms_Macbook/Projects/llm-council/backend/templates/gold_miner.yaml`
- Added company-type metadata to templates:
  - `/Users/Toms_Macbook/Projects/llm-council/backend/templates/financial_quality_mvp.yaml`
  - `/Users/Toms_Macbook/Projects/llm-council/backend/templates/gold_miner.yaml`
  - `/Users/Toms_Macbook/Projects/llm-council/backend/templates/resources_gold_monometallic.yaml`
  - `/Users/Toms_Macbook/Projects/llm-council/backend/templates/pharma_biotech.yaml`
  - `/Users/Toms_Macbook/Projects/llm-council/backend/templates/general.yaml`
- Added CLI support for company-type testing:
  - `/Users/Toms_Macbook/Projects/llm-council/test_emulated_council.py`
  - `/Users/Toms_Macbook/Projects/llm-council/test_quality_mvp.py`

## Predefined Company Types
- `gold_miner`
- `copper_miner`
- `lithium_miner`
- `diversified_miner`
- `pharma_biotech`
- `medtech`
- `energy_oil_gas`
- `bank_financials`
- `insurance`
- `software_saas`
- `industrials`
- `consumer_retail`
- `real_estate_reit`
- `general_equity`

## Predefined Exchanges
- `asx`
- `nyse`
- `nasdaq`
- `tsx`
- `tsxv`
- `lse`
- `aim`
- `unknown`

## Mapping Model
- Type-to-template mapping is many-to-few today:
  - `gold_miner` -> `gold_miner` (fallback `resources_gold_monometallic`)
  - other mining-like types -> `resources_gold_monometallic` (fallback `financial_quality_mvp`)
  - pharma/biotech -> `pharma_biotech` (fallback `financial_quality_mvp`)
  - all other financial company types -> `financial_quality_mvp`
  - non-match fallback -> `general` or `financial_quality_mvp` when ticker is present

## Next Steps
1. Add specialized templates for additional types:
   - e.g. `resources_copper`, `resources_lithium`, `banking`, `software_saas`.
2. Add optional ticker-to-company metadata enrichment:
   - determine probable company type using exchange profile + latest filings.
3. Add frontend controls:
  - company-type selector (`auto` + predefined list).
  - exchange selector (`auto` + predefined list).
  - display template/company-type/exchange selection source in UI.
4. Add regression tests:
   - routing tests for explicit/auto/fallback behavior.
   - Stage 1 prompt-shaping tests validating `research_brief` inclusion.
