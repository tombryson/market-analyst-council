"""
Investment analysis templates for different sectors.
These define the structured JSON output format for the chairman's synthesis.
"""

RESOURCES_TEMPLATE = {
    "analysis_type": "resources",
    "ticker": "",
    "company_name": "",
    "analysis_date": "",
    "market_data": {
        "current_price": None,
        "market_cap_aud_m": None,
        "shares_outstanding_m": None,
        "enterprise_value_aud_m": None,
        "cash_aud_m": None,
        "debt_aud_m": None
    },
    "projects": [
        {
            "project_name": "",
            "resource_tonnes_mt": None,
            "grade_g_t": None,
            "recovery_fraction": None,
            "mine_life_years": None,
            "aisc_aud_oz": None,
            "capex_aud_m": None,
            "npv_aud_m": None,
            "stage": "",  # "Scoping", "PFS", "DFS", "Development", etc.
            "stage_multiplier": None,
            "risked_npv_aud_m": None
        }
    ],
    "total_risked_npv_aud_m": None,
    "quality_score": {
        "total": None,  # 0-100
        "breakdown": {
            "jurisdiction": {"score": None, "weight": 0.20, "rationale": ""},
            "infrastructure": {"score": None, "weight": 0.10, "rationale": ""},
            "management": {"score": None, "weight": 0.15, "rationale": ""},
            "development_stage": {"score": None, "weight": 0.15, "rationale": ""},
            "funding": {"score": None, "weight": 0.20, "rationale": ""},
            "certainty_12m": {"score": None, "weight": 0.10, "rationale": ""},
            "esg": {"score": None, "weight": 0.10, "rationale": ""}
        }
    },
    "value_score": {
        "total": None,  # 0-100
        "breakdown": {
            "npv_vs_market_cap": {"score": None, "weight": 0.30, "ratio": None, "rationale": ""},
            "ev_per_resource_oz": {"score": None, "weight": 0.20, "value": None, "rationale": ""},
            "exploration_upside": {"score": None, "weight": 0.20, "potential_pct": None, "rationale": ""},
            "cost_competitiveness": {"score": None, "weight": 0.15, "rationale": ""},
            "ma_strategic_value": {"score": None, "weight": 0.15, "rationale": ""}
        }
    },
    "price_targets": {
        "current_price": None,
        "target_12m": None,
        "target_24m": None,
        "upside_12m_pct": None,
        "upside_24m_pct": None,
        "methodology": ""
    },
    "development_timeline": {
        "current_stage": "",
        "key_milestones": [
            {
                "milestone": "",
                "expected_date": "",
                "probability_pct": None
            }
        ]
    },
    "headwinds_tailwinds": {
        "quantitative": [
            {
                "factor": "",
                "type": "headwind/tailwind",
                "threshold": "",
                "impact": ""
            }
        ],
        "qualitative": [
            {
                "factor": "",
                "type": "headwind/tailwind",
                "impact": ""
            }
        ]
    },
    "investment_recommendation": {
        "rating": "",  # "BUY", "HOLD", "SELL"
        "conviction": "",  # "HIGH", "MEDIUM", "LOW"
        "summary": "",
        "key_risks": [],
        "key_opportunities": [],
        "dissenting_views": []
    },
    "council_metadata": {
        "consensus_level": "",  # "Strong", "Moderate", "Weak"
        "top_ranked_models": [],
        "key_disagreements": []
    }
}


PHARMA_TEMPLATE = {
    "analysis_type": "pharma",
    "ticker": "",
    "company_name": "",
    "analysis_date": "",
    "market_data": {
        "current_price": None,
        "market_cap_usd_m": None,
        "shares_outstanding_m": None,
        "enterprise_value_usd_m": None,
        "cash_usd_m": None,
        "debt_usd_m": None
    },
    "pipeline": [
        {
            "drug_name": "",
            "indication": "",
            "target_population": None,
            "current_stage": "",  # "Pre-Clinical", "Phase 1", "Phase 2", "Phase 3", "Submitted", "Approved"
            "pos_multiplier": None,  # Probability of Success
            "peak_market_share_pct": None,
            "gross_annual_price_usd": None,
            "net_price_after_rebates_pct": None,
            "effective_patent_life_years": None,
            "remaining_rd_cost_usd_m": None,
            "npv_usd_m": None,
            "rnpv_usd_m": None  # Risk-adjusted NPV
        }
    ],
    "total_rnpv_usd_m": None,
    "quality_score": {
        "total": None,  # 0-100
        "breakdown": {
            "regulatory_environment": {"score": None, "weight": 0.20, "rationale": ""},
            "scientific_manufacturing": {"score": None, "weight": 0.10, "rationale": ""},
            "management": {"score": None, "weight": 0.15, "rationale": ""},
            "pipeline_maturity": {"score": None, "weight": 0.15, "rationale": ""},
            "cash_runway_funding": {"score": None, "weight": 0.20, "rationale": ""},
            "certainty_12m": {"score": None, "weight": 0.10, "rationale": ""},
            "clinical_ethical_standards": {"score": None, "weight": 0.10, "rationale": ""}
        }
    },
    "value_score": {
        "total": None,  # 0-100
        "breakdown": {
            "rnpv_vs_market_cap": {"score": None, "weight": 0.30, "ratio": None, "rationale": ""},
            "ev_per_risk_adj_peak_sales": {"score": None, "weight": 0.20, "value": None, "rationale": ""},
            "pipeline_platform_potential": {"score": None, "weight": 0.20, "rationale": ""},
            "market_positioning_moat": {"score": None, "weight": 0.15, "rationale": ""},
            "ma_strategic_value": {"score": None, "weight": 0.15, "rationale": ""}
        }
    },
    "price_targets": {
        "current_price": None,
        "target_12m": None,
        "target_24m": None,
        "upside_12m_pct": None,
        "upside_24m_pct": None,
        "methodology": ""
    },
    "development_timeline": {
        "current_stage": "",
        "key_milestones": [
            {
                "milestone": "",
                "drug_name": "",
                "expected_date": "",
                "probability_pct": None
            }
        ]
    },
    "headwinds_tailwinds": {
        "quantitative": [
            {
                "factor": "",
                "type": "headwind/tailwind",
                "threshold": "",
                "impact": ""
            }
        ],
        "qualitative": [
            {
                "factor": "",
                "type": "headwind/tailwind",
                "impact": ""
            }
        ]
    },
    "investment_recommendation": {
        "rating": "",  # "BUY", "HOLD", "SELL"
        "conviction": "",  # "HIGH", "MEDIUM", "LOW"
        "summary": "",
        "key_risks": [],
        "key_opportunities": [],
        "dissenting_views": []
    },
    "council_metadata": {
        "consensus_level": "",  # "Strong", "Moderate", "Weak"
        "top_ranked_models": [],
        "key_disagreements": []
    }
}


def get_template_for_sector(sector: str) -> dict:
    """
    Get the appropriate analysis template for a given sector.

    Args:
        sector: "resources", "pharma", or "general"

    Returns:
        Template dictionary
    """
    if sector.lower() in ["resources", "mining", "gold", "metals"]:
        return RESOURCES_TEMPLATE.copy()
    elif sector.lower() in ["pharma", "biotech", "pharmaceutical"]:
        return PHARMA_TEMPLATE.copy()
    else:
        # Default to resources for now
        return RESOURCES_TEMPLATE.copy()


def detect_sector_from_context(ticker: str, user_query: str, search_results: dict = None) -> str:
    """
    Detect the sector from available context.

    Args:
        ticker: Stock ticker (if provided)
        user_query: User's question
        search_results: Search results metadata

    Returns:
        Detected sector: "resources", "pharma", or "general"
    """
    query_lower = user_query.lower()

    # Keywords for pharma/biotech
    pharma_keywords = [
        "drug", "clinical trial", "phase 1", "phase 2", "phase 3",
        "fda", "ema", "pipeline", "pharmaceutical", "biotech",
        "indication", "patient", "therapy", "treatment"
    ]

    # Keywords for resources/mining
    resources_keywords = [
        "gold", "mineral", "resource", "reserve", "mining", "ore",
        "jorc", "pfs", "dfs", "feasibility", "drill", "grade",
        "aisc", "ounce", "oz", "tonne", "deposit", "exploration"
    ]

    # Check pharma keywords
    pharma_score = sum(1 for kw in pharma_keywords if kw in query_lower)

    # Check resources keywords
    resources_score = sum(1 for kw in resources_keywords if kw in query_lower)

    if pharma_score > resources_score and pharma_score > 2:
        return "pharma"
    elif resources_score > pharma_score and resources_score > 2:
        return "resources"
    else:
        # Default to resources for ASX tickers
        return "resources"
