"""
Template loader for investment analysis rubrics.
Loads templates from YAML files in the templates/ directory.
"""

import copy
import re
import yaml
from typing import Any, Dict, List, Optional
from pathlib import Path

from .template_prompt_library import get_template_prompt_fallback


PREALLOCATED_COMPANY_TYPES: List[Dict[str, Any]] = [
    {
        "id": "gold_miner",
        "name": "Gold Miner",
        "description": "Gold-focused mining, development, or exploration company.",
        "default_template_id": "gold_miner",
        "template_candidates": ["gold_miner"],
        "aliases": ["gold", "gold_mining", "gold_miner", "precious_metals"],
        "detection_keywords": [
            "gold",
            "au",
            "ounce",
            "oz",
            "aisc",
            "jorc",
            "mine",
            "mining",
            "resource",
        ],
    },
    {
        "id": "silver_miner",
        "name": "Silver Miner",
        "description": "Silver-focused mining, development, or exploration company.",
        "default_template_id": "silver_miner",
        "template_candidates": ["silver_miner", "gold_miner"],
        "aliases": ["silver", "silver_mining", "silver_miner", "ag", "precious_metals"],
        "detection_keywords": [
            "silver",
            "ag",
            "ageq",
            "ag_eq",
            "silver-zinc",
            "silver-lead",
            "zinc",
            "lead",
            "polymetallic",
            "ounce",
            "oz",
            "aisc",
            "jorc",
            "mine",
            "mining",
            "resource",
        ],
    },
    {
        "id": "uranium_miner",
        "name": "Uranium Miner",
        "description": "Uranium-focused mining, development, or exploration company.",
        "default_template_id": "uranium_miner",
        "template_candidates": ["uranium_miner", "gold_miner"],
        "aliases": ["uranium", "u3o8", "yellowcake", "uranium_mining", "nuclear_fuel"],
        "detection_keywords": [
            "uranium",
            "u3o8",
            "yellowcake",
            "lbs",
            "million pounds",
            "aisc",
            "cash cost",
            "resource",
            "reserve",
            "mine",
            "mining",
        ],
    },
    {
        "id": "copper_miner",
        "name": "Copper Miner",
        "description": "Copper-focused mining, development, or exploration company.",
        "default_template_id": "copper_miner",
        "template_candidates": ["copper_miner", "general_equity"],
        "aliases": ["copper", "cu", "copper_mining", "base_metals"],
        "detection_keywords": [
            "copper",
            "cu",
            "concentrate",
            "mine",
            "mining",
            "resource",
            "jorc",
            "feasibility",
        ],
    },
    {
        "id": "lithium_miner",
        "name": "Lithium Miner",
        "description": "Lithium producer or explorer across hard-rock or brine assets.",
        "default_template_id": "lithium_miner",
        "template_candidates": ["lithium_miner", "general_equity"],
        "aliases": ["lithium", "li", "battery_metals"],
        "detection_keywords": [
            "lithium",
            "spodumene",
            "brine",
            "carbonate",
            "hydroxide",
            "mine",
            "mining",
            "resource",
        ],
    },
    {
        "id": "bauxite_miner",
        "name": "Bauxite Miner",
        "description": "Bauxite and alumina producer or explorer.",
        "default_template_id": "bauxite_miner",
        "template_candidates": ["bauxite_miner", "diversified_miner", "general_equity"],
        "aliases": ["bauxite", "alumina", "aluminium", "aluminum"],
        "detection_keywords": [
            "bauxite",
            "alumina",
            "aluminium",
            "aluminum",
            "refinery",
            "resource",
            "reserve",
            "mine",
            "mining",
        ],
    },
    {
        "id": "diversified_miner",
        "name": "Diversified Miner",
        "description": "Diversified mining company with multiple commodities.",
        "default_template_id": "diversified_miner",
        "template_candidates": ["diversified_miner", "general_equity"],
        "aliases": ["diversified_mining", "minerals", "metals"],
        "detection_keywords": [
            "commodity",
            "mine",
            "mining",
            "minerals",
            "resource",
            "reserve",
            "production",
        ],
    },
    {
        "id": "pharma_biotech",
        "name": "Pharma/Biotech",
        "description": "Drug development, biotech platform, or specialty pharma company.",
        "default_template_id": "pharma_biotech",
        "template_candidates": ["pharma_biotech"],
        "aliases": ["pharma", "biotech", "pharmaceutical", "drug_developer"],
        "detection_keywords": [
            "drug",
            "trial",
            "phase 1",
            "phase 2",
            "phase 3",
            "fda",
            "ema",
            "pipeline",
            "biotech",
        ],
    },
    {
        "id": "medtech",
        "name": "Medtech",
        "description": "Medical device, diagnostics, or medtech platform company.",
        "default_template_id": "medtech",
        "template_candidates": ["medtech", "pharma_biotech"],
        "aliases": ["medical_device", "diagnostics", "medtech"],
        "detection_keywords": [
            "medical device",
            "diagnostic",
            "medtech",
            "fda clearance",
            "device",
            "clinical",
        ],
    },
    {
        "id": "energy_oil_gas",
        "name": "Energy (Oil/Gas)",
        "description": "Oil and gas exploration, production, or services company.",
        "default_template_id": "energy_oil_gas",
        "template_candidates": ["energy_oil_gas"],
        "aliases": ["energy", "oil", "gas", "o&g"],
        "detection_keywords": [
            "oil",
            "gas",
            "barrel",
            "boe",
            "lng",
            "upstream",
            "production",
        ],
    },
    {
        "id": "bank_financials",
        "name": "Bank/Financials",
        "description": "Banking, lending, asset management, or diversified financials.",
        "default_template_id": "bank_financials",
        "template_candidates": ["bank_financials", "financials_bank_insurance", "general_equity"],
        "aliases": ["bank", "financials", "lender", "asset_manager"],
        "detection_keywords": [
            "bank",
            "lending",
            "loan",
            "deposit",
            "net interest margin",
            "capital ratio",
        ],
    },
    {
        "id": "insurance",
        "name": "Insurance",
        "description": "Insurers, brokers, and insurance-adjacent financial companies.",
        "default_template_id": "insurance",
        "template_candidates": ["insurance", "financials_bank_insurance", "general_equity"],
        "aliases": ["insurer", "insurance"],
        "detection_keywords": [
            "insurance",
            "premium",
            "combined ratio",
            "underwriting",
            "claims",
        ],
    },
    {
        "id": "software_saas",
        "name": "Software/SaaS",
        "description": "Software and SaaS companies.",
        "default_template_id": "software_saas",
        "template_candidates": ["software_saas", "general_equity"],
        "aliases": ["software", "saas", "cloud"],
        "detection_keywords": [
            "saas",
            "arr",
            "software",
            "cloud",
            "churn",
            "retention",
            "enterprise software",
        ],
    },
    {
        "id": "datacentres",
        "name": "Datacentres",
        "description": "Data centre developers/operators and digital infrastructure REITs.",
        "default_template_id": "datacentres",
        "template_candidates": ["datacentres", "industrials", "general_equity"],
        "aliases": ["data_center", "data_centre", "datacenter", "datacentre", "colo", "colocation"],
        "detection_keywords": [
            "data center",
            "data centre",
            "datacenter",
            "datacentre",
            "colocation",
            "hyperscale",
            "it load",
            "megawatt",
            "mw",
            "pue",
            "lease-up",
            "capacity online",
        ],
    },
    {
        "id": "industrials",
        "name": "Industrials",
        "description": "Industrials, manufacturing, and engineering companies.",
        "default_template_id": "industrials",
        "template_candidates": ["industrials", "industrials_consumer_reit", "general_equity"],
        "aliases": ["industrial", "manufacturing", "engineering"],
        "detection_keywords": [
            "industrial",
            "manufacturing",
            "order book",
            "backlog",
            "engineering",
        ],
    },
    {
        "id": "consumer_retail",
        "name": "Consumer/Retail",
        "description": "Consumer brands, retail, and discretionary spending businesses.",
        "default_template_id": "consumer_retail",
        "template_candidates": ["consumer_retail", "industrials_consumer_reit", "general_equity"],
        "aliases": ["consumer", "retail", "ecommerce"],
        "detection_keywords": [
            "retail",
            "consumer",
            "same store sales",
            "basket size",
            "ecommerce",
        ],
    },
    {
        "id": "consumer_staples",
        "name": "Consumer Staples",
        "description": "Staples, food, beverage, and other demand-resilient consumer businesses.",
        "default_template_id": "consumer_staples",
        "template_candidates": ["consumer_staples", "consumer_retail", "general_equity"],
        "aliases": ["staples", "consumer_staples", "food", "beverage"],
        "detection_keywords": [
            "staples",
            "food",
            "beverage",
            "grocery",
            "pricing power",
            "private label",
            "distribution",
        ],
    },
    {
        "id": "gambling",
        "name": "Gambling",
        "description": "Wagering, lotteries, gaming machines, casinos, and gambling technology businesses.",
        "default_template_id": "gambling",
        "template_candidates": ["gambling", "consumer_retail", "general_equity"],
        "aliases": ["gambling", "wagering", "lottery", "casino"],
        "detection_keywords": [
            "wagering",
            "sportsbook",
            "lottery",
            "casino",
            "gross gaming revenue",
            "hold",
            "turnover",
        ],
    },
    {
        "id": "gaming_interactive",
        "name": "Gaming Interactive",
        "description": "Video game publishers, interactive content platforms, and digital entertainment studios.",
        "default_template_id": "gaming_interactive",
        "template_candidates": ["gaming_interactive", "technology_platforms", "general_equity"],
        "aliases": ["gaming", "interactive_gaming", "video_games"],
        "detection_keywords": [
            "gaming",
            "video game",
            "bookings",
            "live ops",
            "engagement",
            "dau",
            "mau",
        ],
    },
    {
        "id": "technology_platforms",
        "name": "Technology Platforms",
        "description": "Platform, internet, payments, ad-tech, and broader technology businesses outside pure SaaS.",
        "default_template_id": "technology_platforms",
        "template_candidates": ["technology_platforms", "software_saas", "general_equity"],
        "aliases": ["technology", "platform", "internet", "fintech", "adtech"],
        "detection_keywords": [
            "platform",
            "marketplace",
            "payments",
            "ad tech",
            "take rate",
            "network effects",
            "transaction volume",
        ],
    },
    {
        "id": "semiconductors",
        "name": "Semiconductors",
        "description": "Semiconductor designers, manufacturers, equipment suppliers, and related hardware businesses.",
        "default_template_id": "semiconductors",
        "template_candidates": ["semiconductors", "technology_platforms", "general_equity"],
        "aliases": ["semiconductor", "semiconductors", "chip", "foundry", "fabless"],
        "detection_keywords": [
            "semiconductor",
            "chip",
            "foundry",
            "wafer",
            "fabless",
            "asic",
            "gpu",
        ],
    },
    {
        "id": "defence",
        "name": "Defence",
        "description": "Defence, aerospace, mission systems, and sovereign capability businesses.",
        "default_template_id": "defence",
        "template_candidates": ["defence", "industrials", "general_equity"],
        "aliases": ["defence", "defense", "aerospace", "mission_systems"],
        "detection_keywords": [
            "defence",
            "defense",
            "aerospace",
            "backlog",
            "contract award",
            "tender",
            "sovereign",
        ],
    },
    {
        "id": "healthcare_services",
        "name": "Healthcare Services",
        "description": "Healthcare providers, diagnostics, services, and care-delivery businesses.",
        "default_template_id": "healthcare_services",
        "template_candidates": ["healthcare_services", "medtech", "general_equity"],
        "aliases": ["healthcare", "healthcare_services", "provider", "diagnostics"],
        "detection_keywords": [
            "healthcare services",
            "diagnostics",
            "provider",
            "reimbursement",
            "payer mix",
            "utilization",
        ],
    },
    {
        "id": "fixed_income",
        "name": "Fixed Income",
        "description": "Bond funds, credit vehicles, and listed fixed-income exposures.",
        "default_template_id": "fixed_income",
        "template_candidates": ["fixed_income", "general_equity"],
        "aliases": ["bonds", "bond", "credit", "fixed_income", "fixed income"],
        "detection_keywords": [
            "bond",
            "credit",
            "fixed income",
            "duration",
            "spread",
            "yield",
            "floating rate",
        ],
    },
    {
        "id": "real_estate_reit",
        "name": "Real Estate/REIT",
        "description": "Real estate developers, property trusts, and REITs.",
        "default_template_id": "real_estate_reit",
        "template_candidates": ["real_estate_reit", "industrials_consumer_reit", "general_equity"],
        "aliases": ["reit", "real_estate", "property"],
        "detection_keywords": [
            "reit",
            "property",
            "occupancy",
            "cap rate",
            "rent roll",
            "real estate",
        ],
    },
    {
        "id": "general_equity",
        "name": "General Equity",
        "description": "Default company profile when no sector template is strongly matched.",
        "default_template_id": "general_equity",
        "template_candidates": ["general_equity"],
        "aliases": ["general", "equity", "company"],
        "detection_keywords": ["quality", "valuation", "fundamentals", "company"],
    },
]

GENERIC_COMPANY_TYPE_KEYWORDS = {
    "energy",
    "mine",
    "mining",
    "minerals",
    "metals",
    "resource",
    "resources",
    "reserve",
    "reserves",
    "project",
    "projects",
    "production",
    "producer",
    "jorc",
    "aisc",
    "cash cost",
    "commodity",
}

COMMODITY_MINER_TYPES = {
    "gold_miner",
    "silver_miner",
    "uranium_miner",
    "copper_miner",
    "lithium_miner",
    "bauxite_miner",
}

# Canonical template aliases to prevent rubric drift across duplicate ids.
TEMPLATE_ID_ALIASES: Dict[str, str] = {
    # Legacy id -> canonical id
    "resources_gold_monometallic": "gold_miner",
    "resources_copper_monometallic": "copper_miner",
    "resources_lithium_monometallic": "lithium_miner",
    "resources_silver_monometallic": "silver_miner",
    "resources_uranium_monometallic": "uranium_miner",
    "resources_bauxite_monometallic": "bauxite_miner",
    "financial_quality_mvp": "general_equity",
    "general": "general_equity",
}


PREFERRED_FALLBACK_TEMPLATE_IDS: List[str] = [
    "general_equity",
    "energy_oil_gas",
    "bank_financials",
    "insurance",
    "financials_bank_insurance",
    "software_saas",
    "datacentres",
    "industrials",
    "consumer_retail",
    "consumer_staples",
    "gambling",
    "gaming_interactive",
    "technology_platforms",
    "semiconductors",
    "defence",
    "healthcare_services",
    "fixed_income",
    "real_estate_reit",
    "industrials_consumer_reit",
    "gold_miner",
    "copper_miner",
    "lithium_miner",
    "silver_miner",
    "uranium_miner",
    "bauxite_miner",
    "diversified_miner",
    "medtech",
    "pharma_biotech",
]


# Optional direct company-to-type assignments used by auto-detection.
# This avoids relying purely on keyword matching for known issuers.
PREALLOCATED_COMPANY_TYPE_ASSIGNMENTS: List[Dict[str, Any]] = [
    {
        "company_type": "gold_miner",
        "tickers": ["ASX:WWI", "WWI"],
        "company_names": ["West Wits Mining Limited", "West Wits Mining"],
    },
    {
        "company_type": "gold_miner",
        "tickers": ["ASX:BTR", "BTR", "BTR.AX"],
        "company_names": ["Brightstar Resources Limited", "Brightstar Resources"],
    },
    {
        "company_type": "silver_miner",
        "tickers": ["ASX:POL", "POL", "POL.AX"],
        "company_names": ["Polymetals Resources Limited", "Polymetals Resources"],
    },
    {
        "company_type": "uranium_miner",
        "tickers": ["ASX:PEN", "PEN", "PEN.AX"],
        "company_names": ["Peninsula Energy Ltd", "Peninsula Energy Limited", "Peninsula Energy"],
    },
    {
        "company_type": "gold_miner",
        "tickers": ["ASX:AUC", "AUC", "AUC.AX"],
        "company_names": ["Ausgold Limited", "Ausgold"],
    },
]


# Optional direct company-to-exchange assignments for bare tickers and fuzzy names.
PREALLOCATED_EXCHANGE_ASSIGNMENTS: List[Dict[str, Any]] = [
    {
        "exchange": "asx",
        "tickers": ["ASX:WWI", "WWI", "WWI.AX"],
        "company_names": ["West Wits Mining Limited", "West Wits Mining"],
    },
    {
        "exchange": "asx",
        "tickers": ["ASX:BTR", "BTR", "BTR.AX"],
        "company_names": ["Brightstar Resources Limited", "Brightstar Resources"],
    },
    {
        "exchange": "asx",
        "tickers": ["ASX:POL", "POL", "POL.AX"],
        "company_names": ["Polymetals Resources Limited", "Polymetals Resources"],
    },
]


PREALLOCATED_EXCHANGES: List[Dict[str, Any]] = [
    {
        "id": "asx",
        "name": "ASX",
        "description": "Australian Securities Exchange",
        "aliases": ["asx", "australian securities exchange", "australia"],
        "ticker_prefixes": ["ASX:"],
        "ticker_suffixes": [".AX"],
        "detection_keywords": ["asx", "australia", "australian"],
        "assumption_template": (
            "Exchange profile: ASX (Australia). Prefer ASX announcements, quarterly/annual reports, "
            "Appendix 4D/4E/5B/5C, and investor presentations. Market data in AUD by default unless the "
            "company reports otherwise."
        ),
        # Prepass retrieval profile for PDF dump / injection-bundle workers.
        "retrieval_params": {
            "max_sources_default": 40,
            "allowed_domain_suffixes": [
                "asx.com.au",
                "marketindex.com.au",
                "intelligentinvestor.com.au",
                "wcsecure.weblink.com.au",
                "aspecthuntley.com.au",
            ],
            "lookback_days_default": 365,
            "target_price_sensitive_default": 20,
            "target_non_price_sensitive_default": 20,
            "price_sensitive_strategy": "asx_deterministic_latest",
            "material_filing_tokens": [
                "price-sensitive asx announcement",
                "appendix 4c",
                "appendix 4d",
                "appendix 4e",
                "appendix 5b",
                "appendix 5c",
                "quarterly",
                "half-year",
                "half yearly",
                "interim report",
                "financial report",
                "annual report",
                "investor presentation",
                "feasibility study",
                "dfs",
                "pfs",
                "funding",
                "loan facility",
                "resource update",
                "project update",
                "first gold",
                "gold pour",
            ],
            "low_signal_notice_tokens": [
                "cleansing notice",
                "appendix 2a",
                "appendix 3b",
                "appendix 3c",
                "application for quotation of securities",
                "notice of annual general meeting",
            ],
        },
    },
    {
        "id": "nyse",
        "name": "NYSE",
        "description": "New York Stock Exchange",
        "aliases": ["nyse", "new york stock exchange", "us", "usa"],
        "ticker_prefixes": ["NYSE:"],
        "ticker_suffixes": [".N"],
        "detection_keywords": ["nyse", "new york", "us listed", "u.s. listed"],
        "assumption_template": (
            "Exchange profile: NYSE (United States). Prefer SEC filings (10-K/10-Q/8-K), "
            "official earnings releases, and investor presentations. Market data in USD by default."
        ),
        # Preliminary WIP profile for NYSE source collection/injection.
        "retrieval_params": {
            "max_sources_default": 40,
            "allowed_domain_suffixes": [
                "sec.gov",
                "sec.report",
                "nyse.com",
                "businesswire.com",
                "globenewswire.com",
                "prnewswire.com",
                "stockanalysis.com",
                "marketwatch.com",
            ],
            "lookback_days_default": 365,
            "target_price_sensitive_default": 10,
            "target_non_price_sensitive_default": 10,
            "price_sensitive_strategy": "sec_material_filing_tokens",
            "material_filing_tokens": [
                "form 8-k",
                "8-k",
                "form 10-q",
                "10-q",
                "form 10-k",
                "10-k",
                "form 20-f",
                "20-f",
                "form 6-k",
                "6-k",
                "def 14a",
                "earnings release",
                "investor presentation",
                "guidance",
                "material definitive agreement",
            ],
            "low_signal_notice_tokens": [
                "form 3",
                "form 4",
                "form 5",
                "schedule 13g",
                "13g/a",
                "section 16 filing",
            ],
        },
    },
    {
        "id": "nasdaq",
        "name": "NASDAQ",
        "description": "NASDAQ Stock Market",
        "aliases": ["nasdaq", "nasdaqgs", "nasdaqgm", "us", "usa"],
        "ticker_prefixes": ["NASDAQ:"],
        "ticker_suffixes": [".O", ".Q"],
        "detection_keywords": ["nasdaq", "us listed", "u.s. listed"],
        "assumption_template": (
            "Exchange profile: NASDAQ (United States). Prefer SEC filings and official investor "
            "materials. Market data in USD by default."
        ),
        # Preliminary WIP profile for NASDAQ source collection/injection.
        "retrieval_params": {
            "max_sources_default": 40,
            "allowed_domain_suffixes": [
                "sec.gov",
                "sec.report",
                "nasdaq.com",
                "businesswire.com",
                "globenewswire.com",
                "prnewswire.com",
                "stockanalysis.com",
                "marketwatch.com",
            ],
            "lookback_days_default": 365,
            "target_price_sensitive_default": 10,
            "target_non_price_sensitive_default": 10,
            "price_sensitive_strategy": "sec_material_filing_tokens",
            "material_filing_tokens": [
                "form 8-k",
                "8-k",
                "form 10-q",
                "10-q",
                "form 10-k",
                "10-k",
                "form 20-f",
                "20-f",
                "form 6-k",
                "6-k",
                "def 14a",
                "earnings release",
                "investor presentation",
                "guidance",
                "material definitive agreement",
            ],
            "low_signal_notice_tokens": [
                "form 3",
                "form 4",
                "form 5",
                "schedule 13g",
                "13g/a",
                "section 16 filing",
            ],
        },
    },
    {
        "id": "tsx",
        "name": "TSX",
        "description": "Toronto Stock Exchange",
        "aliases": ["tsx", "toronto stock exchange", "canada"],
        "ticker_prefixes": ["TSX:"],
        "ticker_suffixes": [".TO"],
        "detection_keywords": ["tsx", "toronto", "canada"],
        "assumption_template": (
            "Exchange profile: TSX (Canada). Deterministic primary-source mode: prioritize "
            "official issuer releases, TSX/TMX pages, and approved Canadian wire services. "
            "Market data in CAD by default."
        ),
        "retrieval_params": {
            "max_sources_default": 40,
            "allowed_domain_suffixes": [
                "newsfilecorp.com",
                "globenewswire.com",
                "businesswire.com",
                "newswire.ca",
                "money.tmx.com",
                "tmx.com",
                "tsx.com",
                "sedarplus.ca",
            ],
            "lookback_days_default": 365,
            "target_price_sensitive_default": 15,
            "target_non_price_sensitive_default": 15,
            "price_sensitive_strategy": "canadian_material_filing_tokens",
            "source_quality_priority": {
                "newsfilecorp.com": 100,
                "globenewswire.com": 100,
                "businesswire.com": 95,
                "newswire.ca": 95,
                "money.tmx.com": 90,
                "tmx.com": 88,
                "tsx.com": 88,
                "sedarplus.ca": 85,
            },
            "material_filing_tokens": [
                "ni 43-101",
                "technical report",
                "resource estimate",
                "reserve estimate",
                "preliminary economic assessment",
                "pea",
                "pfs",
                "dfs",
                "md&a",
                "financial statements",
                "news release",
                "funding",
                "private placement",
                "debt facility",
                "offtake",
                "project update",
            ],
            "low_signal_notice_tokens": [
                "stock option grant",
                "warrant exercise",
                "insider report",
                "early warning report",
                "notice of annual meeting",
                "agm notice",
            ],
        },
    },
    {
        "id": "tsxv",
        "name": "TSXV",
        "description": "TSX Venture Exchange",
        "aliases": ["tsxv", "tsx venture", "venture exchange", "canada"],
        "ticker_prefixes": ["TSXV:"],
        "ticker_suffixes": [".V"],
        "detection_keywords": ["tsxv", "venture exchange", "canada"],
        "assumption_template": (
            "Exchange profile: TSXV (Canada). Dual-primary source mode: use both "
            "wire releases and issuer official-site disclosures, with overlap de-duplicated. "
            "Market data in CAD by default."
        ),
        "retrieval_params": {
            "max_sources_default": 40,
            "allowed_domain_suffixes": [
                "globenewswire.com",
            ],
            "lookback_days_default": 365,
            "target_price_sensitive_default": 15,
            "target_non_price_sensitive_default": 15,
            "price_sensitive_strategy": "canadian_material_filing_tokens",
            "source_quality_priority": {
                "globenewswire.com": 100,
            },
            "material_filing_tokens": [
                "ni 43-101",
                "technical report",
                "resource estimate",
                "reserve estimate",
                "drill results",
                "assay",
                "metallurgy",
                "preliminary economic assessment",
                "pea",
                "pfs",
                "dfs",
                "md&a",
                "financial statements",
                "news release",
                "funding",
                "private placement",
                "flow-through",
                "debt facility",
                "offtake",
                "project update",
            ],
            "low_signal_notice_tokens": [
                "stock option grant",
                "grant of options",
                "warrant exercise",
                "insider report",
                "early warning report",
                "notice of annual meeting",
                "agm notice",
            ],
        },
    },
    {
        "id": "cse",
        "name": "CSE",
        "description": "Canadian Securities Exchange",
        "aliases": ["cse", "canadian securities exchange", "cnsx", "canada"],
        "ticker_prefixes": ["CSE:"],
        "ticker_suffixes": [".CN"],
        "detection_keywords": ["cse", "cnsx", "canadian securities exchange"],
        "assumption_template": (
            "Exchange profile: CSE (Canada). Deterministic primary-source mode: prioritize "
            "CSE issuer postings, official issuer releases, and approved Canadian wire services. "
            "Market data in CAD by default."
        ),
        "retrieval_params": {
            "max_sources_default": 40,
            "allowed_domain_suffixes": [
                "thecse.com",
                "cms.thecse.com",
                "newsfilecorp.com",
                "globenewswire.com",
                "businesswire.com",
                "newswire.ca",
                "sedarplus.ca",
                "money.tmx.com",
            ],
            "lookback_days_default": 365,
            "target_price_sensitive_default": 15,
            "target_non_price_sensitive_default": 15,
            "price_sensitive_strategy": "canadian_material_filing_tokens",
            "source_quality_priority": {
                "thecse.com": 100,
                "cms.thecse.com": 100,
                "newsfilecorp.com": 98,
                "globenewswire.com": 96,
                "businesswire.com": 94,
                "newswire.ca": 94,
                "sedarplus.ca": 88,
                "money.tmx.com": 82,
            },
            "material_filing_tokens": [
                "ni 43-101",
                "technical report",
                "resource estimate",
                "reserve estimate",
                "preliminary economic assessment",
                "pea",
                "pfs",
                "dfs",
                "md&a",
                "financial statements",
                "news release",
                "funding",
                "private placement",
                "debt facility",
                "offtake",
                "project update",
            ],
            "low_signal_notice_tokens": [
                "stock option grant",
                "warrant exercise",
                "insider report",
                "early warning report",
                "notice of annual meeting",
                "agm notice",
            ],
        },
    },
    {
        "id": "lse",
        "name": "LSE",
        "description": "London Stock Exchange",
        "aliases": ["lse", "london stock exchange", "uk"],
        "ticker_prefixes": ["LSE:"],
        "ticker_suffixes": [".L"],
        "detection_keywords": ["lse", "london", "uk listed"],
        "assumption_template": (
            "Exchange profile: LSE (United Kingdom). Prefer RNS announcements, annual/interim reports, "
            "and competent person reports where available. Market data in GBP by default."
        ),
    },
    {
        "id": "aim",
        "name": "AIM",
        "description": "Alternative Investment Market (LSE AIM)",
        "aliases": ["aim", "lse aim"],
        "ticker_prefixes": ["AIM:"],
        "ticker_suffixes": [],
        "detection_keywords": ["aim listed", "lse aim"],
        "assumption_template": (
            "Exchange profile: AIM (UK). Prefer RNS announcements, annual/interim filings, "
            "and project technical updates. Market data in GBP by default."
        ),
    },
    {
        "id": "jse",
        "name": "JSE",
        "description": "Johannesburg Stock Exchange",
        "aliases": ["jse", "johannesburg stock exchange", "south africa", "za"],
        "ticker_prefixes": ["JSE:"],
        "ticker_suffixes": [".JO"],
        "detection_keywords": ["jse", "johannesburg", "south africa", "senz", "sens"],
        "assumption_template": (
            "Exchange profile: JSE (South Africa). Prefer SENS announcements, annual/interim "
            "reports, production updates, and competent persons reports (SAMREC where applicable). "
            "Market data in ZAR by default."
        ),
        "retrieval_params": {
            "max_sources_default": 40,
            "allowed_domain_suffixes": [
                "jse.co.za",
                "senspdf.jse.co.za",
                "sharenet.co.za",
                "moneyweb.co.za",
                "miningweekly.com",
                "stockanalysis.com",
            ],
            "lookback_days_default": 365,
            "target_price_sensitive_default": 10,
            "target_non_price_sensitive_default": 10,
            "price_sensitive_strategy": "jse_sens_material_tokens",
            "material_filing_tokens": [
                "sens",
                "trading statement",
                "results for the year",
                "interim results",
                "resource statement",
                "reserve statement",
                "samrec",
                "production update",
                "funding",
                "debt facility",
                "project update",
                "feasibility study",
                "dfs",
                "pfs",
            ],
            "low_signal_notice_tokens": [
                "dealing in securities",
                "director dealing",
                "notice of annual general meeting",
                "change to the board",
                "change of company secretary",
            ],
        },
    },
    {
        "id": "unknown",
        "name": "Unknown/Other",
        "description": "Fallback when exchange is not explicitly known",
        "aliases": ["unknown", "other", "auto"],
        "ticker_prefixes": [],
        "ticker_suffixes": [],
        "detection_keywords": [],
        "assumption_template": (
            "Exchange profile: unknown. Infer filing sources from company domicile and exchange references "
            "in primary documents; do not assume ASX-specific formats by default."
        ),
        "retrieval_params": {
            "allowed_domain_suffixes": [],
            "lookback_days_default": 365,
            "target_price_sensitive_default": 0,
            "target_non_price_sensitive_default": 20,
            "price_sensitive_strategy": "none",
            "material_filing_tokens": [],
            "low_signal_notice_tokens": [],
        },
    },
]

# Prompt-level substitutions used to keep templates exchange/currency agnostic.
# Placeholders supported in rubrics/prompts:
# - [MARKET_DATA_SOURCES]
# - [PRIMARY_FILING_SOURCES]
# - [DEFAULT_CURRENCY]
# - [FX_CONVERSION_GUIDANCE]
EXCHANGE_PROMPT_SUBSTITUTIONS: Dict[str, Dict[str, str]] = {
    "asx": {
        "MARKET_DATA_SOURCES": "asx.com.au and/or marketindex.com.au",
        "PRIMARY_FILING_SOURCES": (
            "ASX announcements, quarterly/annual reports, investor presentations, "
            "and technical studies (PFS/FS/DFS)"
        ),
        "DEFAULT_CURRENCY": "AUD",
        "FX_CONVERSION_GUIDANCE": (
            "current AUD/USD conversion rate (or project-local FX where applicable)"
        ),
    },
    "nyse": {
        "MARKET_DATA_SOURCES": "nyse.com, sec.gov, and major market-data providers",
        "PRIMARY_FILING_SOURCES": (
            "SEC filings (10-K/10-Q/8-K/20-F/6-K), official earnings releases, "
            "and investor presentations"
        ),
        "DEFAULT_CURRENCY": "USD",
        "FX_CONVERSION_GUIDANCE": (
            "USD-denominated analysis by default; include FX conversion only where "
            "project economics are reported in non-USD currencies"
        ),
    },
    "nasdaq": {
        "MARKET_DATA_SOURCES": "nasdaq.com, sec.gov, and major market-data providers",
        "PRIMARY_FILING_SOURCES": (
            "SEC filings (10-K/10-Q/8-K/20-F/6-K), official earnings releases, "
            "and investor presentations"
        ),
        "DEFAULT_CURRENCY": "USD",
        "FX_CONVERSION_GUIDANCE": (
            "USD-denominated analysis by default; include FX conversion only where "
            "project economics are reported in non-USD currencies"
        ),
    },
    "tsx": {
        "MARKET_DATA_SOURCES": "tsx.com, sedarplus.ca, and major Canadian market-data providers",
        "PRIMARY_FILING_SOURCES": (
            "TSX/TMX issuer pages, SEDAR+ filings, NI 43-101 technical reports, and approved wire releases "
            "(Newsfile, GlobeNewswire, Business Wire, CNW)"
        ),
        "DEFAULT_CURRENCY": "CAD",
        "FX_CONVERSION_GUIDANCE": (
            "current CAD/USD conversion rate (or project-local FX where applicable)"
        ),
    },
    "tsxv": {
        "MARKET_DATA_SOURCES": "tsx.com, sedarplus.ca, and major Canadian market-data providers",
        "PRIMARY_FILING_SOURCES": (
            "GlobeNewswire company releases first, then SEDAR+ filings and issuer disclosures where available"
        ),
        "DEFAULT_CURRENCY": "CAD",
        "FX_CONVERSION_GUIDANCE": (
            "current CAD/USD conversion rate (or project-local FX where applicable)"
        ),
    },
    "cse": {
        "MARKET_DATA_SOURCES": "thecse.com, sedarplus.ca, and major Canadian market-data providers",
        "PRIMARY_FILING_SOURCES": (
            "CSE issuer postings, approved wire releases (Newsfile/GlobeNewswire/Business Wire/CNW), "
            "and SEDAR+ filings where available"
        ),
        "DEFAULT_CURRENCY": "CAD",
        "FX_CONVERSION_GUIDANCE": (
            "current CAD/USD conversion rate (or project-local FX where applicable)"
        ),
    },
    "lse": {
        "MARKET_DATA_SOURCES": "londonstockexchange.com, RNS feeds, and major UK market-data providers",
        "PRIMARY_FILING_SOURCES": (
            "RNS announcements, annual/interim reports, and technical reports where available"
        ),
        "DEFAULT_CURRENCY": "GBP",
        "FX_CONVERSION_GUIDANCE": (
            "current GBP/USD conversion rate (or project-local FX where applicable)"
        ),
    },
    "aim": {
        "MARKET_DATA_SOURCES": "londonstockexchange.com, RNS feeds, and major UK market-data providers",
        "PRIMARY_FILING_SOURCES": (
            "RNS announcements, annual/interim filings, and project technical updates"
        ),
        "DEFAULT_CURRENCY": "GBP",
        "FX_CONVERSION_GUIDANCE": (
            "current GBP/USD conversion rate (or project-local FX where applicable)"
        ),
    },
    "jse": {
        "MARKET_DATA_SOURCES": "jse.co.za, SENS feeds, and major South African market-data providers",
        "PRIMARY_FILING_SOURCES": (
            "SENS announcements, annual/interim reports, production updates, and SAMREC-style technical disclosures where available"
        ),
        "DEFAULT_CURRENCY": "ZAR",
        "FX_CONVERSION_GUIDANCE": (
            "current ZAR/USD conversion rate (or project-local FX where applicable)"
        ),
    },
    "unknown": {
        "MARKET_DATA_SOURCES": (
            "official exchange sources, regulator filings, and reliable market-data providers "
            "for the listing venue"
        ),
        "PRIMARY_FILING_SOURCES": (
            "primary regulator filings, official exchange announcements, and company investor materials"
        ),
        "DEFAULT_CURRENCY": "listing currency",
        "FX_CONVERSION_GUIDANCE": (
            "relevant base-currency FX conversion where project economics are reported in a different currency"
        ),
    },
}


class TemplateLoader:
    """Loads and manages analysis templates from YAML files."""

    def __init__(self, templates_dir: str = None):
        """
        Initialize template loader.

        Args:
            templates_dir: Path to templates directory (defaults to backend/templates)
        """
        if templates_dir is None:
            # Default to templates/ directory relative to this file
            templates_dir = Path(__file__).parent / "templates"

        self.templates_dir = Path(templates_dir)
        self._templates_cache: Dict[str, dict] = {}
        self.contracts_path = Path(__file__).parent / "template_contracts" / "contracts.yaml"
        self._contracts_cache: Dict[str, Any] = {}
        self._load_all_templates()
        self._load_template_contracts()

    def _canonical_template_id(self, template_id: Optional[str]) -> Optional[str]:
        """Resolve alias template ids to canonical ids when available."""
        if not template_id:
            return template_id
        key = str(template_id).strip()
        canonical = TEMPLATE_ID_ALIASES.get(key, key)
        if canonical in self._templates_cache:
            return canonical
        # Fallback: preserve original id if canonical target is unavailable.
        return key

    def _load_all_templates(self):
        """Load all YAML template files from the templates directory."""
        if not self.templates_dir.exists():
            print(f"Warning: Templates directory not found: {self.templates_dir}")
            return

        for yaml_file in self.templates_dir.glob("*.yaml"):
            try:
                with open(yaml_file, 'r') as f:
                    template_data = yaml.safe_load(f)

                if template_data and 'id' in template_data:
                    template_id = template_data['id']
                    self._templates_cache[template_id] = template_data
                    print(f"Loaded template: {template_id}")
                else:
                    print(f"Warning: Template missing 'id' field: {yaml_file}")

            except Exception as e:
                print(f"Error loading template {yaml_file}: {e}")

    def _load_template_contracts(self):
        """Load template contract assets used to preserve industry-specific Stage 3 detail."""
        if not self.contracts_path.exists():
            return

        try:
            with open(self.contracts_path, "r") as f:
                contract_data = yaml.safe_load(f) or {}
            if isinstance(contract_data, dict):
                self._contracts_cache = contract_data
        except Exception as e:
            print(f"Error loading template contracts {self.contracts_path}: {e}")

    def _deep_merge_dicts(self, base: Any, override: Any) -> Any:
        """Recursively merge contract defaults/families/template overrides."""
        if not isinstance(base, dict) or not isinstance(override, dict):
            return copy.deepcopy(override)

        merged = copy.deepcopy(base)
        for key, value in override.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = self._deep_merge_dicts(merged[key], value)
            else:
                merged[key] = copy.deepcopy(value)
        return merged

    def _resolve_fallback_template_id(self, requested: Optional[str] = None) -> str:
        """Resolve a safe fallback template from active templates only."""
        candidates: List[str] = []
        if requested:
            candidates.append(self._canonical_template_id(requested))
        candidates.extend(PREFERRED_FALLBACK_TEMPLATE_IDS)

        seen: set[str] = set()
        for template_id in candidates:
            tid = str(template_id or "").strip()
            if not tid or tid in seen:
                continue
            seen.add(tid)
            if tid in self._templates_cache:
                return tid

        return next(iter(self._templates_cache), "")

    def list_templates(self) -> List[Dict[str, Any]]:
        """
        List all available templates with metadata.

        Returns:
            List of dicts with id, name, description, category
        """
        templates_list = []
        for template_id, template_data in self._templates_cache.items():
            templates_list.append({
                "id": template_id,
                "name": template_data.get("name", template_id),
                "description": template_data.get("description", ""),
                "category": template_data.get("category", "general"),
                "company_types": template_data.get("company_types", []),
            })

        # Sort by category, then name
        templates_list.sort(key=lambda t: (t['category'], t['name']))
        return templates_list

    def list_company_types(self) -> List[Dict[str, Any]]:
        """Return predefined company types and their mapped templates."""
        company_types: List[Dict[str, Any]] = []
        for entry in PREALLOCATED_COMPANY_TYPES:
            default_template_id = self._resolve_template_for_company_type(entry)
            template_candidates = [
                template_id
                for template_id in entry.get("template_candidates", [])
                if template_id in self._templates_cache
            ]
            if default_template_id and default_template_id not in template_candidates:
                template_candidates.insert(0, default_template_id)
            company_types.append(
                {
                    "id": entry["id"],
                    "name": entry["name"],
                    "description": entry["description"],
                    "default_template_id": default_template_id,
                    "template_candidates": template_candidates,
                }
            )
        return company_types

    def list_exchanges(self) -> List[Dict[str, str]]:
        """Return predefined exchanges and their descriptions."""
        exchanges: List[Dict[str, str]] = []
        for entry in PREALLOCATED_EXCHANGES:
            exchanges.append(
                {
                    "id": entry["id"],
                    "name": entry["name"],
                    "description": entry["description"],
                }
            )
        return exchanges

    def get_exchange_retrieval_params(self, exchange: Optional[str]) -> Dict[str, Any]:
        """Return retrieval-profile params for the selected exchange."""
        normalized = self.normalize_exchange(exchange) or "unknown"
        fallback_entry = next(
            (item for item in PREALLOCATED_EXCHANGES if item.get("id") == "unknown"),
            {},
        )
        fallback = dict(fallback_entry.get("retrieval_params", {}) or {})
        entry = next(
            (item for item in PREALLOCATED_EXCHANGES if item.get("id") == normalized),
            None,
        )
        selected = dict((entry or {}).get("retrieval_params", {}) or {})
        merged = dict(fallback)
        merged.update(selected)
        merged["exchange"] = normalized
        return merged

    def normalize_company_type(self, company_type: Optional[str]) -> Optional[str]:
        """Normalize company type input into a known company type id."""
        if not company_type:
            return None
        normalized = company_type.strip().lower().replace("-", "_").replace(" ", "_")
        for entry in PREALLOCATED_COMPANY_TYPES:
            aliases = set(entry.get("aliases", []))
            aliases.add(entry["id"])
            if normalized in aliases:
                return entry["id"]
        return None

    def normalize_exchange(self, exchange: Optional[str]) -> Optional[str]:
        """Normalize exchange input into a known exchange id."""
        if not exchange:
            return None
        normalized = exchange.strip().lower().replace("-", "_")
        for entry in PREALLOCATED_EXCHANGES:
            aliases = set(alias.lower() for alias in entry.get("aliases", []))
            aliases.add(entry["id"])
            if normalized in aliases:
                return entry["id"]
        return None

    def detect_company_type(
        self,
        user_query: str,
        ticker: str = None,
        minimum_score: float = 2.0,
    ) -> Optional[str]:
        """Detect company type deterministically from query/ticker context."""
        assigned_type = self._detect_assigned_company_type(user_query=user_query, ticker=ticker)
        if assigned_type:
            return assigned_type

        inferred_name = self.infer_company_name(user_query or "", ticker=ticker)
        text = "\n".join(
            part for part in [user_query or "", ticker or "", inferred_name or ""] if str(part).strip()
        ).lower()
        ranked = self._rank_company_types(text)
        if not ranked:
            return None

        best = ranked[0]
        best_type = str(best.get("company_type") or "").strip()
        best_score = float(best.get("score") or 0.0)
        if not best_type or best_score < float(minimum_score):
            return None

        if len(ranked) >= 2:
            runner_up = ranked[1]
            runner_type = str(runner_up.get("company_type") or "").strip()
            runner_score = float(runner_up.get("score") or 0.0)
            if (
                best_type in COMMODITY_MINER_TYPES
                and runner_type in COMMODITY_MINER_TYPES
                and (best_score - runner_score) < 1.0
            ):
                return None

        return best_type
        
    def _rank_company_types(self, text: str) -> List[Dict[str, Any]]:
        corpus = str(text or "").lower()
        ranked: List[Dict[str, Any]] = []

        def _keyword_present(keyword: str) -> bool:
            kw = str(keyword or "").strip().lower()
            if not kw:
                return False
            normalized = re.sub(r"\s+", " ", kw)
            if normalized in {"au", "ag", "cu", "li"}:
                return bool(re.search(rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])", corpus))
            pattern = re.escape(normalized).replace(r"\ ", r"\s+")
            return bool(re.search(rf"(?<![a-z0-9]){pattern}(?![a-z0-9])", corpus))

        for entry in PREALLOCATED_COMPANY_TYPES:
            company_type_id = str(entry.get("id") or "").strip()
            if not company_type_id or company_type_id == "general_equity":
                continue

            score = 0.0
            specific_score = 0.0
            generic_score = 0.0
            matches: List[str] = []
            keywords = list(entry.get("detection_keywords", []) or [])
            keywords.extend(entry.get("aliases", []) or [])
            keywords.append(company_type_id.replace("_", " "))

            seen: set[str] = set()
            for raw in keywords:
                kw = str(raw or "").strip().lower()
                if not kw or kw in seen:
                    continue
                seen.add(kw)
                if not _keyword_present(kw):
                    continue

                normalized_kw = re.sub(r"\s+", " ", kw)
                if normalized_kw in GENERIC_COMPANY_TYPE_KEYWORDS:
                    weight = 0.4
                    generic_score += weight
                else:
                    weight = 2.5 if (" " in normalized_kw or len(normalized_kw) > 5) else 1.5
                    specific_score += weight
                score += weight
                matches.append(kw)

            if score <= 0:
                continue

            ranked.append(
                {
                    "company_type": company_type_id,
                    "score": round(score, 3),
                    "specific_score": round(specific_score, 3),
                    "generic_score": round(generic_score, 3),
                    "matched_keywords": matches,
                }
            )

        ranked.sort(
            key=lambda item: (
                float(item.get("score") or 0.0),
                float(item.get("specific_score") or 0.0),
                -float(item.get("generic_score") or 0.0),
            ),
            reverse=True,
        )
        return ranked

    def detect_exchange(
        self,
        user_query: str,
        ticker: str = None,
    ) -> Optional[str]:
        """Detect exchange from ticker formatting and query keywords."""
        from_ticker = self._match_exchange_from_ticker(ticker or "")
        if from_ticker:
            return from_ticker

        from_assignment = self._detect_assigned_exchange(user_query, ticker=ticker)
        if from_assignment:
            return from_assignment

        text = f"{user_query or ''} {ticker or ''}".lower()
        best_exchange: Optional[str] = None
        best_score = 0

        for entry in PREALLOCATED_EXCHANGES:
            if entry["id"] == "unknown":
                continue
            score = 0
            for keyword in entry.get("detection_keywords", []):
                kw = keyword.lower().strip()
                if kw and kw in text:
                    score += 2 if " " in kw else 1
            if score > best_score:
                best_score = score
                best_exchange = entry["id"]

        if best_score >= 1:
            return best_exchange

        return "unknown"

    def _detect_assigned_exchange(
        self,
        user_query: str,
        ticker: Optional[str] = None,
    ) -> Optional[str]:
        """
        Resolve exchange from curated assignments before keyword heuristics.
        """
        ticker_key = self._ticker_match_keys(ticker)
        inferred_name = self.infer_company_name(user_query or "", ticker=ticker).strip().lower()

        for assignment in PREALLOCATED_EXCHANGE_ASSIGNMENTS:
            exchange_id = self.normalize_exchange(assignment.get("exchange"))
            if not exchange_id or exchange_id == "unknown":
                continue

            for raw_ticker in assignment.get("tickers", []):
                normalized_ticker = str(raw_ticker or "").strip().upper()
                if not normalized_ticker:
                    continue
                if normalized_ticker in ticker_key:
                    return exchange_id
                if ":" in normalized_ticker:
                    bare = normalized_ticker.split(":", 1)[1]
                    if bare and bare in ticker_key:
                        return exchange_id

            for raw_name in assignment.get("company_names", []):
                normalized_name = str(raw_name or "").strip().lower()
                if normalized_name and normalized_name in inferred_name:
                    return exchange_id

        return None

    def infer_company_name(self, user_query: str, ticker: str = None) -> str:
        """
        Infer company name from question/context with ticker fallback.
        """
        text = (user_query or "").strip()
        if not text:
            return (ticker or "the company").strip() or "the company"

        # Pattern: "West Wits Mining Limited (ASX:WWI)"
        paren_match = re.search(
            r"\b([A-Z][A-Za-z0-9&.,'\-\s]{2,100})\s*\((?:ASX|NYSE|NASDAQ|TSXV?|LSE|AIM|CSE|JSE)\s*:\s*[A-Z0-9.\-]{1,12}\)",
            text,
        )
        if paren_match:
            return self._clean_company_name(paren_match.group(1))

        # Pattern: "ASX:West Wits Mining Limited" (your format)
        exchange_name_match = re.search(
            r"\b(?:ASX|NYSE|NASDAQ|TSXV?|LSE|AIM|CSE|JSE)\s*:\s*([A-Z][A-Za-z0-9&.,'\-\s]{2,100})",
            text,
        )
        if exchange_name_match:
            candidate = exchange_name_match.group(1).strip(" .,:;-")
            # Avoid returning plain ticker-like strings.
            if not re.fullmatch(r"[A-Z0-9.\-]{1,12}", candidate):
                return self._clean_company_name(candidate)

        # Name-like entity ending in common corporate suffixes.
        suffix_match = re.search(
            r"\b([A-Z][A-Za-z0-9&.,'\-\s]{2,100}\b(?:Limited|Ltd|Inc\.?|Corp\.?|Corporation|PLC|Plc))\b",
            text,
        )
        if suffix_match:
            return self._clean_company_name(suffix_match.group(1))

        # Fall back to ticker symbol.
        if ticker:
            cleaned_ticker = re.sub(r"^[A-Z]+:", "", ticker.strip().upper())
            if cleaned_ticker:
                return cleaned_ticker

        return "the company"

    def _clean_company_name(self, value: str) -> str:
        """Clean inferred company name by trimming obvious trailing prompt fragments."""
        cleaned = (value or "").strip(" .,:;-")
        leading_prompt_patterns = (
            r"^(?:run\s+)?(?:a\s+)?(?:full\s+)?(?:investment\s+)?analysis\s+(?:on|for)\s+",
            r"^(?:run\s+)?(?:full\s+)?research\s+(?:on|for)\s+",
            r"^(?:provide|give|create|write|generate)\s+(?:a\s+)?(?:full\s+)?(?:investment\s+)?(?:analysis|report|brief|review)\s+(?:on|for)\s+",
            r"^(?:analyse|analyze|research|review)\s+",
        )
        for pattern in leading_prompt_patterns:
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip(" .,:;-")
        cleaned = re.sub(r"\s+out of\s+100\b.*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+over\s+\d+\s*(?:months?|years?)\b.*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+for\s+the\s+next\s+\d+\s*(?:months?|years?)\b.*$", "", cleaned, flags=re.IGNORECASE)
        return cleaned.strip(" .,:;-")

    def resolve_template_selection(
        self,
        user_query: str,
        ticker: str = None,
        explicit_template_id: str = None,
        company_type: str = None,
        exchange: str = None,
        fallback_id: str = "general_equity",
    ) -> Dict[str, Any]:
        """
        Resolve template + company type + exchange with explicit settings first.

        Returns:
            Dict containing selected template and selection metadata.
        """
        selection_source = "auto"
        resolved_fallback_id = self._resolve_fallback_template_id(fallback_id)
        selected_company_type = self.normalize_company_type(company_type)
        selected_exchange = self.normalize_exchange(exchange)
        selected_company_name = self.infer_company_name(user_query, ticker=ticker)
        assigned_company_name = self._lookup_assigned_company_name(ticker)
        if assigned_company_name:
            lowered = (selected_company_name or "").strip().lower()
            ticker_keys_lower = {key.lower() for key in self._ticker_match_keys(ticker)}
            if (
                not lowered
                or lowered == "the company"
                or lowered == (ticker or "").strip().lower()
                or lowered in ticker_keys_lower
            ):
                selected_company_name = assigned_company_name
        exchange_selection_source = "explicit_exchange" if selected_exchange else "auto_exchange"

        if not selected_exchange:
            selected_exchange = self.detect_exchange(user_query=user_query, ticker=ticker)
            exchange_selection_source = (
                "auto_detected_exchange"
                if selected_exchange and selected_exchange != "unknown"
                else "exchange_unknown_fallback"
            )

        canonical_explicit = self._canonical_template_id(explicit_template_id) if explicit_template_id else None
        if canonical_explicit and canonical_explicit in self._templates_cache:
            selected_template_id = canonical_explicit
            selection_source = "explicit_template"
        else:
            selected_template_id = None
            if explicit_template_id and (not canonical_explicit or canonical_explicit not in self._templates_cache):
                selection_source = "invalid_explicit_template_fallback"

            if selected_company_type:
                entry = self._company_type_entry(selected_company_type)
                selected_template_id = self._resolve_template_for_company_type(entry) if entry else None
                selection_source = "explicit_company_type"
            else:
                detected_company_type = self.detect_company_type(user_query, ticker=ticker)
                if detected_company_type:
                    selected_company_type = detected_company_type
                    selected_template_id = self._choose_template_for_company_type(
                        detected_company_type,
                        user_query,
                    )
                    selection_source = "deterministic_company_type"

            if not selected_template_id:
                selected_template_id = self.auto_detect_template(
                    user_query=user_query,
                    ticker=ticker,
                    fallback_id=resolved_fallback_id,
                )
                selection_source = f"{selection_source}+template_keyword_match"

        # Canonicalize id before final safety checks.
        selected_template_id = self._canonical_template_id(selected_template_id)

        # Final safety fallback.
        if selected_template_id not in self._templates_cache:
            selected_template_id = resolved_fallback_id or selected_template_id
            selection_source = f"{selection_source}+template_fallback"

        if not selected_company_type:
            selected_company_type = self._default_company_type_for_template(selected_template_id)

        template_data = self.get_template(selected_template_id) or {}
        exchange_assumptions = self.get_exchange_assumptions(selected_exchange)
        return {
            "template_id": selected_template_id,
            "template_name": template_data.get("name", selected_template_id),
            "company_type": selected_company_type or "general_equity",
            "company_name": selected_company_name,
            "exchange": selected_exchange or "unknown",
            "exchange_selection_source": exchange_selection_source,
            "exchange_assumptions": exchange_assumptions,
            "selection_source": selection_source,
        }

    def get_template(self, template_id: str) -> Optional[dict]:
        """
        Get a specific template by ID.

        Args:
            template_id: The template ID (e.g., "gold_miner")

        Returns:
            Template dict or None if not found
        """
        canonical_id = self._canonical_template_id(template_id)
        return self._templates_cache.get(canonical_id)

    def get_template_contract(self, template_id: str) -> Dict[str, Any]:
        """
        Resolve the template-specific contract asset for a canonical template id.

        Contracts merge three layers:
        1) global defaults
        2) family defaults (resources/software/pharma/etc.)
        3) template-specific overrides
        """
        canonical_id = self._canonical_template_id(template_id)
        if not canonical_id:
            return {}

        contracts_root = self._contracts_cache or {}
        defaults = contracts_root.get("defaults", {}) or {}
        families = contracts_root.get("families", {}) or {}
        templates = contracts_root.get("templates", {}) or {}
        template_contract = templates.get(canonical_id, {}) or {}
        family_key = str(template_contract.get("family", "") or "").strip()
        family_contract = families.get(family_key, {}) if family_key else {}

        merged = self._deep_merge_dicts(defaults, family_contract)
        merged = self._deep_merge_dicts(merged, template_contract)
        merged["id"] = canonical_id
        if family_key:
            merged["family"] = family_key

        template = self.get_template(canonical_id) or {}
        behavior = template.get("template_behavior", {}) or {}
        output_schema = template.get("output_schema", {}) or {}

        chairman_contract = merged.setdefault("chairman_contract", {})
        if not chairman_contract.get("scoring_factors") and behavior.get("stage3_scoring_factors"):
            chairman_contract["scoring_factors"] = copy.deepcopy(behavior.get("stage3_scoring_factors", {}))

        analysis_contract = merged.setdefault("analysis_contract", {})
        if not analysis_contract.get("required_output_schema_fields"):
            analysis_contract["required_output_schema_fields"] = list(output_schema.get("required_fields", []) or [])
        if not analysis_contract.get("analysis_type"):
            analysis_contract["analysis_type"] = str((output_schema.get("structure", {}) or {}).get("analysis_type", "") or "")

        return merged

    def get_template_contract_section(self, template_id: str, section: str) -> Dict[str, Any]:
        """Return one named section from a template contract, if present."""
        contract = self.get_template_contract(template_id)
        value = contract.get(section, {})
        return value if isinstance(value, dict) else {}

    def list_template_contracts(self) -> List[Dict[str, str]]:
        """List all template contracts keyed by canonical template id."""
        templates = (self._contracts_cache or {}).get("templates", {}) or {}
        out: List[Dict[str, str]] = []
        for template_id, contract in templates.items():
            out.append(
                {
                    "id": str(template_id),
                    "family": str((contract or {}).get("family", "") or ""),
                    "industry_label": str((contract or {}).get("industry_label", "") or ""),
                }
            )
        out.sort(key=lambda item: item["id"])
        return out

    def get_rubric(self, template_id: str) -> Optional[str]:
        """
        Get just the rubric text from a template.

        Args:
            template_id: The template ID

        Returns:
            Rubric text or None if not found
        """
        template = self.get_template(template_id) or {}
        rubric = str(template.get('rubric') or '').strip()
        if rubric:
            return rubric
        fallback = get_template_prompt_fallback(template_id, template)
        return fallback or None

    def _exchange_prompt_substitutions(self, exchange: Optional[str]) -> Dict[str, str]:
        """Return exchange-aware prompt substitutions with safe fallback defaults."""
        normalized = self.normalize_exchange(exchange) or "unknown"
        merged = dict(EXCHANGE_PROMPT_SUBSTITUTIONS.get("unknown", {}))
        merged.update(EXCHANGE_PROMPT_SUBSTITUTIONS.get(normalized, {}))
        return merged

    def apply_prompt_substitutions(
        self,
        text: str,
        company_name: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> str:
        """Apply company/exchange placeholder substitutions to prompt text."""
        rendered = str(text or "")
        if not rendered:
            return ""

        normalized_exchange = self.normalize_exchange(exchange) or "unknown"
        if company_name:
            rendered = rendered.replace("[Company Name]", company_name)
        rendered = rendered.replace("[Exchange]", normalized_exchange.upper())

        subs = self._exchange_prompt_substitutions(normalized_exchange)
        for key, value in subs.items():
            rendered = rendered.replace(f"[{key}]", str(value))

        return rendered

    def render_template_rubric(
        self,
        template_id: str,
        company_name: Optional[str] = None,
        exchange: Optional[str] = None,
        max_chars: int = 0,
    ) -> str:
        """
        Render rubric text with placeholder substitutions applied.

        Placeholders supported:
        - [Company Name]
        - [Exchange]
        """
        template = self.get_template(template_id) or {}
        rubric = str(template.get("rubric") or "").strip()
        if not rubric:
            rubric = get_template_prompt_fallback(template_id, template)
        if not rubric:
            return ""
        rubric = self.apply_prompt_substitutions(
            rubric,
            company_name=company_name,
            exchange=exchange,
        )
        if max_chars > 0:
            rubric = rubric[:max_chars]
        return rubric.strip()

    def render_copy_paste_rubric(
        self,
        template_id: str,
        company_name: Optional[str] = None,
        exchange: Optional[str] = None,
        max_chars: int = 0,
    ) -> str:
        """
        Render the manual Web UI rubric with placeholder substitutions applied.

        Priority:
        1) `copy_paste_rubric` (manual Web UI / external-model workflow)
        2) `rubric` / prompt fallback (for templates not yet split)

        Runtime Stage 1 does not call this method.
        """
        template = self.get_template(template_id) or {}
        rubric = str(template.get("copy_paste_rubric") or template.get("rubric") or "").strip()
        if not rubric:
            rubric = get_template_prompt_fallback(template_id, template)
        if not rubric:
            return ""
        rubric = self.apply_prompt_substitutions(
            rubric,
            company_name=company_name,
            exchange=exchange,
        )
        if max_chars > 0:
            rubric = rubric[:max_chars]
        return rubric.strip()

    def render_stage1_query_prompt(
        self,
        template_id: str,
        company_name: Optional[str] = None,
        exchange: Optional[str] = None,
        max_chars: int = 0,
    ) -> str:
        """
        Render the Stage 1 task prompt with placeholder substitutions applied.

        Priority:
        1) `stage1_focus_prompt` (preferred concise retrieval prompt)
        2) `rubric` (fallback for templates without a dedicated Stage 1 prompt)
        """
        template = self.get_template(template_id) or {}
        prompt = str(template.get("stage1_focus_prompt") or template.get("rubric") or "").strip()
        if not prompt:
            prompt = get_template_prompt_fallback(template_id, template)
        if not prompt:
            return ""
        prompt = self.apply_prompt_substitutions(
            prompt,
            company_name=company_name,
            exchange=exchange,
        )
        if max_chars > 0:
            prompt = prompt[:max_chars]
        return prompt.strip()

    def auto_detect_template(
        self,
        user_query: str,
        ticker: str = None,
        fallback_id: str = "general_equity"
    ) -> str:
        """
        Auto-detect which template to use based on query keywords.

        Args:
            user_query: User's question
            ticker: Stock ticker if provided
            fallback_id: Template ID to use if no match found

        Returns:
            Template ID (best match or fallback)
        """
        query_lower = user_query.lower()

        # Score each template based on keyword matches
        best_score = 0
        resolved_fallback = self._resolve_fallback_template_id(fallback_id)
        best_template_id = resolved_fallback

        for template_id, template_data in self._templates_cache.items():
            keywords = template_data.get('auto_detect_keywords', [])
            if not keywords:
                continue

            # Count keyword matches
            score = sum(1 for keyword in keywords if keyword.lower() in query_lower)

            if score > best_score:
                best_score = score
                best_template_id = template_id

        # Require at least 2 keyword matches to override fallback
        if best_score < 2:
            return resolved_fallback

        return best_template_id

    def get_stage1_research_brief(
        self,
        template_id: str,
        company_type: Optional[str] = None,
        exchange: Optional[str] = None,
        company_name: Optional[str] = None,
        include_rubric: bool = True,
        max_chars: int = 0,
    ) -> str:
        """
        Build a bounded research brief for Stage 1 retrieval-oriented model calls.
        """
        template = self.get_template(template_id) or {}
        rubric = ""
        if include_rubric:
            rubric = (template.get("stage1_focus_prompt") or template.get("rubric") or "").strip()
            if not rubric:
                rubric = get_template_prompt_fallback(template_id, template)
            rubric = self.apply_prompt_substitutions(
                rubric,
                company_name=company_name,
                exchange=exchange,
            )
            if max_chars > 0:
                rubric = rubric[:max_chars]

        company_type_line = (
            f"Company type: {company_type}."
            if company_type
            else "Company type: unspecified."
        )
        exchange_line = (
            f"Exchange: {exchange}."
            if exchange
            else "Exchange: unknown."
        )
        exchange_assumptions = self.get_exchange_assumptions(exchange)
        behavior = self.get_template_behavior(template_id)
        extra_lane_lines: List[str] = []
        for lane in (behavior.get("stage1_research_lanes") or []):
            lane_text = str(lane or "").strip()
            if not lane_text:
                continue
            lane_text = self.apply_prompt_substitutions(
                lane_text,
                company_name=company_name,
                exchange=exchange,
            )
            if not lane_text:
                continue
            if lane_text.startswith("-"):
                extra_lane_lines.append(f"{lane_text}\n")
            else:
                extra_lane_lines.append(f"- {lane_text}\n")
        extra_research_lanes = "".join(extra_lane_lines)
        return (
            "Financial analysis framing:\n"
            f"- Template: {template_id}\n"
            f"- Company name: {company_name or 'unknown'}.\n"
            f"- {company_type_line}\n"
            f"- {exchange_line}\n"
            "- Gather evidence needed for scoring and investment judgment, not generic summaries.\n"
            "- Prioritize recent primary documents with quantitative data.\n"
            "- Run a dedicated management/governance evidence lane: board and executive bios, prior operating track record, insider ownership/alignment, leadership changes, and governance red flags.\n\n"
            f"{extra_research_lanes}"
            "Exchange assumptions:\n"
            f"{exchange_assumptions}\n\n"
            + (
                "Template rubric:\n"
                f"{rubric}"
                if include_rubric and rubric
                else "Template rubric:\n(omitted in brief; expected in main query)"
            )
        ).strip()

    def get_copy_paste_research_brief(
        self,
        template_id: str,
        company_type: Optional[str] = None,
        exchange: Optional[str] = None,
        company_name: Optional[str] = None,
        include_rubric: bool = True,
        max_chars: int = 0,
    ) -> str:
        """
        Build the manual Web UI copy/paste prompt.

        This deliberately prefers `copy_paste_rubric` so external Web UI lanes can
        ask for standalone document/source review without changing runtime Stage 1.
        """
        template = self.get_template(template_id) or {}
        rubric = ""
        if include_rubric:
            rubric = str(template.get("copy_paste_rubric") or template.get("rubric") or "").strip()
            if not rubric:
                rubric = get_template_prompt_fallback(template_id, template)
            rubric = self.apply_prompt_substitutions(
                rubric,
                company_name=company_name,
                exchange=exchange,
            )
            if max_chars > 0:
                rubric = rubric[:max_chars]

        company_type_line = (
            f"Company type: {company_type}."
            if company_type
            else "Company type: unspecified."
        )
        exchange_line = (
            f"Exchange: {exchange}."
            if exchange
            else "Exchange: unknown."
        )
        exchange_assumptions = self.get_exchange_assumptions(exchange)
        behavior = self.get_template_behavior(template_id)
        extra_lane_lines: List[str] = []
        for lane in (behavior.get("stage1_research_lanes") or []):
            lane_text = str(lane or "").strip()
            if not lane_text:
                continue
            lane_text = self.apply_prompt_substitutions(
                lane_text,
                company_name=company_name,
                exchange=exchange,
            )
            if not lane_text:
                continue
            if lane_text.startswith("-"):
                extra_lane_lines.append(f"{lane_text}\n")
            else:
                extra_lane_lines.append(f"- {lane_text}\n")
        extra_research_lanes = "".join(extra_lane_lines)
        return (
            "Manual Web UI analysis prompt:\n"
            f"- Template: {template_id}\n"
            f"- Company name: {company_name or 'unknown'}.\n"
            f"- {company_type_line}\n"
            f"- {exchange_line}\n"
            "- Use this prompt when pasting into external model Web UIs.\n"
            "- Perform standalone source/document research if documents are not already attached.\n"
            "- Keep outputs decision-grade, evidence-linked, and explicit about assumptions.\n\n"
            f"{extra_research_lanes}"
            "Exchange assumptions:\n"
            f"{exchange_assumptions}\n\n"
            + (
                "Copy/paste rubric:\n"
                f"{rubric}"
                if include_rubric and rubric
                else "Copy/paste rubric:\n(omitted)"
            )
        ).strip()

    def _lookup_assigned_company_name(self, ticker: Optional[str]) -> Optional[str]:
        """Return assigned company name for known tickers, if configured."""
        ticker_keys = self._ticker_match_keys(ticker)
        if not ticker_keys:
            return None
        for assignment in PREALLOCATED_COMPANY_TYPE_ASSIGNMENTS:
            names = assignment.get("company_names") or []
            if not names:
                continue
            for raw_ticker in assignment.get("tickers", []):
                normalized_ticker = str(raw_ticker or "").strip().upper()
                if normalized_ticker and normalized_ticker in ticker_keys:
                    name = str(names[0]).strip()
                    if name:
                        return name
        return None

    def get_exchange_assumptions(self, exchange: Optional[str], max_chars: int = 1200) -> str:
        """Return exchange-specific assumptions text used for prompt substitution."""
        normalized = self.normalize_exchange(exchange) or "unknown"
        entry = self._exchange_entry(normalized)
        text = entry.get("assumption_template", "") if entry else ""
        if max_chars > 0:
            text = text[:max_chars]
        return text

    def _company_type_entry(self, company_type_id: str) -> Optional[Dict[str, Any]]:
        for entry in PREALLOCATED_COMPANY_TYPES:
            if entry["id"] == company_type_id:
                return entry
        return None

    def _exchange_entry(self, exchange_id: str) -> Optional[Dict[str, Any]]:
        for entry in PREALLOCATED_EXCHANGES:
            if entry["id"] == exchange_id:
                return entry
        return None

    def _match_exchange_from_ticker(self, ticker: str) -> Optional[str]:
        normalized = (ticker or "").strip().upper()
        if not normalized:
            return None
        for entry in PREALLOCATED_EXCHANGES:
            for prefix in entry.get("ticker_prefixes", []):
                if normalized.startswith(prefix.upper()):
                    return entry["id"]
            for suffix in entry.get("ticker_suffixes", []):
                if suffix and normalized.endswith(suffix.upper()):
                    return entry["id"]
        return None

    def _detect_assigned_company_type(
        self,
        user_query: str,
        ticker: Optional[str] = None,
    ) -> Optional[str]:
        """
        Resolve company type from a curated assignment list before keyword heuristics.
        """
        ticker_key = self._ticker_match_keys(ticker)
        inferred_name = self.infer_company_name(user_query or "", ticker=ticker).strip().lower()

        for assignment in PREALLOCATED_COMPANY_TYPE_ASSIGNMENTS:
            company_type_id = assignment.get("company_type")
            if not company_type_id:
                continue

            for raw_ticker in assignment.get("tickers", []):
                normalized_ticker = str(raw_ticker or "").strip().upper()
                if normalized_ticker and normalized_ticker in ticker_key:
                    return company_type_id

            for raw_name in assignment.get("company_names", []):
                normalized_name = str(raw_name or "").strip().lower()
                if normalized_name and normalized_name in inferred_name:
                    return company_type_id

        return None

    def _ticker_match_keys(self, ticker: Optional[str]) -> set[str]:
        """
        Build normalized ticker keys with and without exchange prefix.
        """
        normalized = str(ticker or "").strip().upper()
        if not normalized:
            return set()

        keys = {normalized}
        if ":" in normalized:
            keys.add(normalized.split(":", 1)[1])
        return keys

    def _resolve_template_for_company_type(self, entry: Dict[str, Any]) -> str:
        preferred = entry.get("default_template_id")
        if preferred in self._templates_cache:
            return preferred
        for template_id in entry.get("template_candidates", []):
            if template_id in self._templates_cache:
                return template_id
        return self._resolve_fallback_template_id()

    def _score_template_keywords(self, template_id: str, user_query: str) -> int:
        template_data = self._templates_cache.get(template_id, {})
        keywords = template_data.get("auto_detect_keywords", [])
        query_lower = (user_query or "").lower()
        return sum(1 for keyword in keywords if keyword.lower() in query_lower)

    def _choose_template_for_company_type(self, company_type_id: str, user_query: str) -> str:
        entry = self._company_type_entry(company_type_id)
        if not entry:
            return self._resolve_fallback_template_id()

        default_template = self._resolve_template_for_company_type(entry)
        candidates = [
            template_id
            for template_id in entry.get("template_candidates", [])
            if template_id in self._templates_cache
        ]
        if not candidates:
            return default_template

        best_candidate = None
        best_score = 0
        for template_id in candidates:
            score = self._score_template_keywords(template_id, user_query)
            if score > best_score:
                best_score = score
                best_candidate = template_id

        if best_candidate and best_score >= 1:
            # For sector-specific company types, keep the default sector template
            # unless a strong template-keyword signal explicitly indicates another one.
            if best_candidate != default_template and company_type_id != "general_equity":
                default_score = self._score_template_keywords(default_template, user_query)
                if best_score < 3 or best_score <= default_score:
                    return default_template
            return best_candidate

        return default_template

    def _default_company_type_for_template(self, template_id: str) -> str:
        template_data = self.get_template(template_id) or {}
        declared_types = template_data.get("company_types", [])
        if declared_types:
            normalized = self.normalize_company_type(str(declared_types[0]))
            if normalized:
                return normalized

        for entry in PREALLOCATED_COMPANY_TYPES:
            default_template = entry.get("default_template_id")
            if default_template == template_id:
                return entry["id"]

        return "general_equity"

    def get_output_schema(self, template_id: str) -> Optional[dict]:
        """
        Get the output schema for a template.

        Args:
            template_id: The template ID

        Returns:
            Output schema dict or None
        """
        template = self.get_template(template_id)
        if template:
            return template.get('output_schema')
        return None

    def get_verification_schema(self, template_id: str) -> Dict[str, Any]:
        """
        Get optional verification schema for Stage 1 digest/compliance checks.

        The schema is expected under `verification_schema` in template YAML.
        Returns an empty dict when absent or invalid.
        """
        template = self.get_template(template_id) or {}
        schema = template.get("verification_schema")
        if isinstance(schema, dict):
            return schema
        return {}

    def get_template_behavior(self, template_id: str) -> Dict[str, Any]:
        """
        Get optional template behavior settings.

        Expected under `template_behavior` in template YAML.
        Returns empty dict when absent/invalid.
        """
        template = self.get_template(template_id) or {}
        behavior = template.get("template_behavior")
        if isinstance(behavior, dict):
            return behavior
        return {}

    def is_structured_template(self, template_id: str) -> bool:
        """
        Check if a template requires structured JSON output.

        Args:
            template_id: The template ID

        Returns:
            True if template requires structured output
        """
        schema = self.get_output_schema(template_id)
        if not schema:
            return False

        schema_type = schema.get('type', '')
        return schema_type != 'freeform'

    def reload(self):
        """Reload all templates from disk (useful for development)."""
        self._templates_cache.clear()
        self._load_all_templates()


# Global instance
_loader = None


def get_template_loader() -> TemplateLoader:
    """Get the global template loader instance (singleton pattern)."""
    global _loader
    if _loader is None:
        _loader = TemplateLoader()
    return _loader


def list_available_templates() -> List[Dict[str, Any]]:
    """Convenience function to list all templates."""
    return get_template_loader().list_templates()


def load_template(template_id: str) -> Optional[dict]:
    """Convenience function to load a template."""
    return get_template_loader().get_template(template_id)


def get_rubric_text(template_id: str) -> Optional[str]:
    """Convenience function to get rubric text."""
    return get_template_loader().get_rubric(template_id)


def get_verification_schema(template_id: str) -> Dict[str, Any]:
    """Convenience function to get optional verification schema."""
    return get_template_loader().get_verification_schema(template_id)


def get_template_behavior(template_id: str) -> Dict[str, Any]:
    """Convenience function to get optional template behavior settings."""
    return get_template_loader().get_template_behavior(template_id)


def get_template_contract(template_id: str) -> Dict[str, Any]:
    """Convenience function to get a merged template contract."""
    return get_template_loader().get_template_contract(template_id)


def get_template_contract_section(template_id: str, section: str) -> Dict[str, Any]:
    """Convenience function to get one section from a merged template contract."""
    return get_template_loader().get_template_contract_section(template_id, section)


def auto_detect_template(user_query: str, ticker: str = None) -> str:
    """Convenience function for auto-detection."""
    return get_template_loader().auto_detect_template(user_query, ticker)


def list_company_types() -> List[Dict[str, Any]]:
    """Convenience function to list supported company types."""
    return get_template_loader().list_company_types()


def list_exchanges() -> List[Dict[str, str]]:
    """Convenience function to list supported exchanges."""
    return get_template_loader().list_exchanges()


def resolve_template_selection(
    user_query: str,
    ticker: str = None,
    explicit_template_id: str = None,
    company_type: str = None,
    exchange: str = None,
    fallback_id: str = "general_equity",
) -> Dict[str, Any]:
    """Convenience function to resolve template/company type/exchange selection."""
    return get_template_loader().resolve_template_selection(
        user_query=user_query,
        ticker=ticker,
        explicit_template_id=explicit_template_id,
        company_type=company_type,
        exchange=exchange,
        fallback_id=fallback_id,
    )
