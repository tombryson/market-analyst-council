# Manual Web UI Analysis Prompt YAMLs

Generated from the live `llm-council` template loader, not hand-written copies.

Use the `copy_paste_prompt` field for manual Web UI runs. If a template defines `copy_paste_rubric`, that Web UI-specific rubric is used. Runtime Stage 1 continues to use `rubric` / `stage1_focus_prompt` through `TemplateLoader.get_stage1_research_brief(...)`.

Regenerate with:

```bash
python3 scripts/export_stage1_analysis_prompts.py
```

## Files

- `energy_oil_gas.yaml`
- `consumer_retail.yaml`
- `consumer_staples.yaml`
- `gambling.yaml`
- `industrials.yaml`
- `industrials_consumer_reit.yaml`
- `real_estate_reit.yaml`
- `bank_financials.yaml`
- `financials_bank_insurance.yaml`
- `insurance.yaml`
- `fixed_income.yaml`
- `general_equity.yaml`
- `healthcare_services.yaml`
- `medtech.yaml`
- `defence.yaml`
- `datacentres.yaml`
- `pharma_biotech.yaml`
- `bauxite_miner.yaml`
- `copper_miner.yaml`
- `diversified_miner.yaml`
- `gold_miner.yaml`
- `lithium_miner.yaml`
- `silver_miner.yaml`
- `uranium_miner.yaml`
- `gaming_interactive.yaml`
- `semiconductors.yaml`
- `software_saas.yaml`
- `technology_platforms.yaml`
