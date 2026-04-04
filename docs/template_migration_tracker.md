# Template Migration Tracker

This tracker records migration from shared fallback templates to template-specific profiles using the standardized structure:
- `rubric`
- `verification_schema`
- `template_behavior` (`commodity_profile`, `stage1_research_lanes`, `stage3_scoring_factors`)
- `output_schema`

## Completed in this batch

### Resource templates
- `gold_miner` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/resources_gold_monometallic.yaml`)
- `copper_miner` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/resources_copper_monometallic.yaml`)
- `lithium_miner` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/resources_lithium_monometallic.yaml`)
- `silver_miner` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/resources_silver_monometallic.yaml`)
- `uranium_miner` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/resources_uranium_monometallic.yaml`)

### Sector templates
- `energy_oil_gas` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/energy_oil_gas.yaml`)
- `pharma_biotech` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/pharma_biotech.yaml`)
- `medtech` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/medtech.yaml`)
- `financials_bank_insurance` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/financials_bank_insurance.yaml`)
- `bank_financials` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/bank_financials.yaml`)
- `insurance` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/insurance.yaml`)
- `software_saas` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/software_saas.yaml`)
- `datacentres` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/datacentres.yaml`)
- `industrials_consumer_reit` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/industrials_consumer_reit.yaml`)
- `industrials` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/industrials.yaml`)
- `consumer_retail` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/consumer_retail.yaml`)
- `real_estate_reit` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/real_estate_reit.yaml`)
- `general_equity` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/general_equity.yaml`)

### Additional resource templates
- `diversified_miner` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/diversified_miner.yaml`)
- `bauxite_miner` (`/Users/Toms_Macbook/Projects/llm-council/backend/templates/resources_bauxite_monometallic.yaml`)

## Current company type routing

- `gold_miner` -> `gold_miner`
- `silver_miner` -> `silver_miner`
- `uranium_miner` -> `uranium_miner`
- `copper_miner` -> `copper_miner`
- `lithium_miner` -> `lithium_miner`
- `bauxite_miner` -> `bauxite_miner`
- `diversified_miner` -> `diversified_miner`
- `pharma_biotech` -> `pharma_biotech`
- `medtech` -> `medtech`
- `energy_oil_gas` -> `energy_oil_gas`
- `bank_financials` -> `bank_financials`
- `insurance` -> `insurance`
- `software_saas` -> `software_saas`
- `datacentres` -> `datacentres`
- `industrials` -> `industrials`
- `consumer_retail` -> `consumer_retail`
- `real_estate_reit` -> `real_estate_reit`
- `general_equity` -> `general_equity`

## Next sequential migration items

1. Add exchange-specific retrieval profile tuning for datacentre-heavy disclosures (e.g., lease announcements, capacity reservations, power procurement updates) if you want stricter source targeting by exchange.
2. Add alumina/aluminium commodity-profile feed for `bauxite_miner` if you want template-specific commodity spot injection (currently sector-macro lane + company filings).
3. Expand exchange retrieval profiles for non-ASX sectors if you want stricter source lane control by template + exchange combination.
