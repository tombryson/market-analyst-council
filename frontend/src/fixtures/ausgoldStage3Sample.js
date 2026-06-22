export const ausgoldStage3Sample = {
  analysis_type: 'resources',
  ticker: 'AUC',
  company_name: 'Ausgold Limited',
  analysis_date: '2026-02-19T03:03:58.187113',
  current_development_stage: 'financing/permitting phase',
  certainty_pct_24m: 55,
  development_timeline: [
    {
      milestone: 'Permitting & Environmental Approvals',
      target_period: 'H1 2026',
      status: 'planned',
      confidence_pct: 85,
    },
    {
      milestone: 'Project Financing Package (Debt/Equity)',
      target_period: 'Mid-2026',
      status: 'planned',
      confidence_pct: 60,
    },
    {
      milestone: 'Final Investment Decision (FID)',
      target_period: 'Q3 2026',
      status: 'planned',
      confidence_pct: 60,
    },
    {
      milestone: 'Construction Start',
      target_period: 'Q4 2026',
      status: 'planned',
      confidence_pct: 55,
    },
    {
      milestone: 'Commissioning / First Gold',
      target_period: 'H1 2028',
      status: 'planned',
      confidence_pct: 40,
    },
  ],
  price_targets: {
    current_price: 1.01,
    target_12m: 1.25,
    target_24m: 1.75,
    upside_12m_pct: 23.8,
    upside_24m_pct: 73.8,
    scenarios: {
      base: 1.25,
      bull: 1.85,
      bear: 0.6,
    },
    scenario_targets: {
      '12m': {
        base: 1.25,
        bull: 1.85,
        bear: 0.6,
      },
      '24m': {
        base: 1.75,
        bull: 2.5,
        bear: 0.45,
      },
    },
    scenario_drivers: {
      '12m': {
        base: [
          "Successful debt/equity financing mix (FID approved) with moderate dilution (~20-30%).",
          "Construction commences, re-rating the project to 'Development' stage.",
          'Gold price holds >A$3,800/oz.',
        ],
        bull: [
          'Driven by M&A activity (takeover premium) or gold price spike >A$4,200/oz expanding margins.',
          'Requires non-dilutive financing (e.g., royalty/stream or strategic partner).',
        ],
        bear: [
          'Financing delays force highly dilutive equity raises or project deferral.',
          'AISC inflation or capex blowout >20% erodes margins.',
        ],
      },
      '24m': {
        base: [
          'First gold pour achieved or imminent.',
          'Operational risk decreases; cash flow visibility improves.',
        ],
        bull: [
          'Aggressive production ramp-up hits nameplate (136koz pa) early.',
          'Resource expansion extends mine life.',
        ],
        bear: [
          'Operational failure during ramp-up or sustained gold price collapse <A$3,000/oz making debt service difficult.',
        ],
      },
    },
  },
  investment_recommendation: {
    rating: 'HOLD / SPECULATIVE BUY',
    conviction: 'MEDIUM',
    summary:
      "While the asset is high-quality and the Jurisdiction is Tier 1, the valuation is not 'cheap' on a risked basis.",
  },
  investment_verdict: {
    top_reasons: [
      'Leverage to Spot Gold: Economics are robust at A$4,000/oz+; payback period drops to <18 months, attracting debt providers.',
      'Strategic Asset: +3Moz Resource in WA is a prime takeover target for mills running low on ore.',
      'Optimization: DFS 1.28Moz Reserve is only a portion of the 3.04Moz Resource; mine life extension is highly probable.',
    ],
    failure_conditions: [
      "Funding Spiral: Inability to secure debt forces a 'highly dilutive' equity event or delays FID, crushing sentiment.",
      'Cost Escalation: WA labor/materials inflation pushes AISC >A$2,000/oz, compressing margins and breaching debt covenants.',
      'Gold Price Correction: A drop below A$3,000/oz renders the project marginal relative to the risk assumed.',
    ],
  },
  market_data_provenance: {
    prepass_currency: 'AUD',
  },
  extended_analysis: {
    next_major_catalysts: [
      'Final Investment Decision (FID)',
      'Project financing package completion',
    ],
  },
};

