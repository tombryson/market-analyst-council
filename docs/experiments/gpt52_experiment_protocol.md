# GPT-5.2 Stage-1 Reliability Protocol

## Objective
Establish stable, repeatable operating parameters for GPT-5.2 Stage-1 analysis using statistically defensible criteria instead of anecdotal runs.

## Design
- Experimental unit: one full Stage-1 diagnostic run (`test_quality_mvp.py --stage1-only --diagnostic-mode`).
- Factors encoded as arms in `/Users/Toms_Macbook/Projects/llm-council/docs/experiments/gpt52_arms_v1.json`.
- Randomized blocked schedule:
  - For each replicate, arm order is shuffled.
  - This controls for time-varying backend conditions.
- Default initial sample size: 3 replicates per arm (pilot).
- Confirmation sample size: 6-10 replicates for top 2 arms.

## Failure Taxonomy
- Hard failures (binary):
  - subprocess timeout/non-zero
  - provider error present
  - zero retrieved results
  - empty Stage-1 response
- Soft conformance failures:
  - second-pass failure with surviving response
  - template non-conformance
  - citation gate fail
  - timeline guard fail

## Primary and Secondary Metrics
- Primary stability metric:
  - Hard-success rate with Wilson 95% CI.
  - Decision quantity: CI lower bound.
- Secondary quality metrics (on hard-success runs):
  - composite quality score (weighted, transparent in script)
  - compliance score (bootstrap mean CI)
  - soft-failure rate
- Operational metrics:
  - median and p90 runtime
  - timeout incidence
  - HTTP 402/429/500/504 incidence

## Decision Rules
- Stable arm gate:
  - hard-success CI lower bound >= 0.70
  - soft-failure rate <= 0.40
  - median runtime on hard-success runs <= 1200s
- Quality gate:
  - stable gate passed
  - quality composite mean >= 0.55
  - compliance mean >= 0.60
- Promotion rule:
  - `most_stable` arm chosen by max hard-success CI lower bound.
  - `most_optimized` chosen among stable arms by quality, then compliance, then reliability.

## Run Commands
Pilot:

```bash
./.venv/bin/python /Users/Toms_Macbook/Projects/llm-council/test_gpt52_experiment.py \
  --arms-file /Users/Toms_Macbook/Projects/llm-council/docs/experiments/gpt52_arms_v1.json \
  --ticker WWI \
  --template-id gold_miner \
  --exchange asx \
  --query-mode template_only \
  --replicates 3 \
  --seed 42 \
  --run-timeout-seconds 2400 \
  --out-dir /tmp/gpt52_exp_pilot
```

Confirmation:

```bash
./.venv/bin/python /Users/Toms_Macbook/Projects/llm-council/test_gpt52_experiment.py \
  --arms-file /Users/Toms_Macbook/Projects/llm-council/docs/experiments/gpt52_arms_v1.json \
  --ticker WWI \
  --template-id gold_miner \
  --exchange asx \
  --query-mode template_only \
  --replicates 8 \
  --seed 42 \
  --run-timeout-seconds 2400 \
  --out-dir /tmp/gpt52_exp_confirm
```

## Outputs
- `manifest.json`: full experimental setup + schedule.
- `runs.jsonl`: one JSON object per run.
- `summary.json`: arm-level statistics and recommendations.
- `report.md`: readable research summary.

## Practical Notes
- Backend variance is high; do not trust single-run outcomes.
- If all arms fail the stable gate, reduce design complexity:
  - force `PERPLEXITY_PRESET_STRATEGY=deep_only`
  - force `PERPLEXITY_STREAM_ENABLED=false`
  - reduce `MAX_SOURCES`, `MAX_STEPS`
  - rerun pilot.
