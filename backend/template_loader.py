"""
Template loader for investment analysis rubrics.
Loads templates from YAML files in the templates/ directory.
"""

import re
import yaml
from typing import Any, Dict, List, Optional
from pathlib import Path


PREALLOCATED_COMPANY_TYPES: List[Dict[str, Any]] = [
    {
        "id": "gold_miner",
        "name": "Gold Miner",
        "description": "Gold-focused mining, development, or exploration company.",
        "default_template_id": "gold_miner",
        "template_candidates": ["gold_miner", "resources_gold_monometallic", "financial_quality_mvp"],
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
        "id": "copper_miner",
        "name": "Copper Miner",
        "description": "Copper-focused mining, development, or exploration company.",
        "default_template_id": "resources_gold_monometallic",
        "template_candidates": ["resources_gold_monometallic", "financial_quality_mvp"],
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
        "default_template_id": "resources_gold_monometallic",
        "template_candidates": ["resources_gold_monometallic", "financial_quality_mvp"],
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
        "id": "diversified_miner",
        "name": "Diversified Miner",
        "description": "Diversified mining company with multiple commodities.",
        "default_template_id": "resources_gold_monometallic",
        "template_candidates": ["resources_gold_monometallic", "financial_quality_mvp"],
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
        "template_candidates": ["pharma_biotech", "financial_quality_mvp"],
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
        "default_template_id": "financial_quality_mvp",
        "template_candidates": ["financial_quality_mvp", "pharma_biotech"],
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
        "default_template_id": "financial_quality_mvp",
        "template_candidates": ["financial_quality_mvp"],
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
        "default_template_id": "financial_quality_mvp",
        "template_candidates": ["financial_quality_mvp"],
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
        "default_template_id": "financial_quality_mvp",
        "template_candidates": ["financial_quality_mvp"],
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
        "default_template_id": "financial_quality_mvp",
        "template_candidates": ["financial_quality_mvp"],
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
        "id": "industrials",
        "name": "Industrials",
        "description": "Industrials, manufacturing, and engineering companies.",
        "default_template_id": "financial_quality_mvp",
        "template_candidates": ["financial_quality_mvp"],
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
        "default_template_id": "financial_quality_mvp",
        "template_candidates": ["financial_quality_mvp"],
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
        "id": "real_estate_reit",
        "name": "Real Estate/REIT",
        "description": "Real estate developers, property trusts, and REITs.",
        "default_template_id": "financial_quality_mvp",
        "template_candidates": ["financial_quality_mvp"],
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
        "default_template_id": "financial_quality_mvp",
        "template_candidates": ["financial_quality_mvp", "general"],
        "aliases": ["general", "equity", "company"],
        "detection_keywords": ["quality", "valuation", "fundamentals", "company"],
    },
]


# Optional direct company-to-type assignments used by auto-detection.
# This avoids relying purely on keyword matching for known issuers.
PREALLOCATED_COMPANY_TYPE_ASSIGNMENTS: List[Dict[str, Any]] = [
    {
        "company_type": "gold_miner",
        "tickers": ["ASX:WWI", "WWI"],
        "company_names": ["West Wits Mining Limited", "West Wits Mining"],
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
            "Appendix 5B/5C, and investor presentations. Market data in AUD by default unless the "
            "company reports otherwise."
        ),
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
            "Exchange profile: TSX (Canada). Prefer SEDAR+ filings, NI 43-101 technical reports, "
            "MD&A, and company releases. Market data in CAD by default."
        ),
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
            "Exchange profile: TSXV (Canada). Prefer SEDAR+ filings, NI 43-101 disclosures, "
            "exploration updates, and financing announcements. Market data in CAD by default."
        ),
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
    },
]


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
        self._load_all_templates()

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
        minimum_score: int = 2,
    ) -> Optional[str]:
        """Detect company type from query/ticker context."""
        assigned_type = self._detect_assigned_company_type(user_query=user_query, ticker=ticker)
        if assigned_type:
            return assigned_type

        text = f"{user_query or ''} {ticker or ''}".lower()
        best_type: Optional[str] = None
        best_score = 0

        for entry in PREALLOCATED_COMPANY_TYPES:
            if entry["id"] == "general_equity":
                continue
            score = 0
            for keyword in entry.get("detection_keywords", []):
                kw = keyword.lower().strip()
                if not kw:
                    continue
                if kw in text:
                    # Longer phrases are a bit more informative.
                    score += 2 if " " in kw else 1
            if score > best_score:
                best_score = score
                best_type = entry["id"]

        if best_score >= minimum_score:
            return best_type
        return None

    def detect_exchange(
        self,
        user_query: str,
        ticker: str = None,
    ) -> Optional[str]:
        """Detect exchange from ticker formatting and query keywords."""
        from_ticker = self._match_exchange_from_ticker(ticker or "")
        if from_ticker:
            return from_ticker

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

    def infer_company_name(self, user_query: str, ticker: str = None) -> str:
        """
        Infer company name from question/context with ticker fallback.
        """
        text = (user_query or "").strip()
        if not text:
            return (ticker or "the company").strip() or "the company"

        # Pattern: "West Wits Mining Limited (ASX:WWI)"
        paren_match = re.search(
            r"\b([A-Z][A-Za-z0-9&.,'\-\s]{2,100})\s*\((?:ASX|NYSE|NASDAQ|TSXV?|LSE|AIM)\s*:\s*[A-Z0-9.\-]{1,12}\)",
            text,
        )
        if paren_match:
            return self._clean_company_name(paren_match.group(1))

        # Pattern: "ASX:West Wits Mining Limited" (your format)
        exchange_name_match = re.search(
            r"\b(?:ASX|NYSE|NASDAQ|TSXV?|LSE|AIM)\s*:\s*([A-Z][A-Za-z0-9&.,'\-\s]{2,100})",
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
        fallback_id: str = "general",
    ) -> Dict[str, Any]:
        """
        Resolve template + company type + exchange with explicit settings first.

        Returns:
            Dict containing selected template and selection metadata.
        """
        selection_source = "auto"
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

        if explicit_template_id and explicit_template_id in self._templates_cache:
            selected_template_id = explicit_template_id
            selection_source = "explicit_template"
        else:
            selected_template_id = None
            if explicit_template_id and explicit_template_id not in self._templates_cache:
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
                    selection_source = "auto_company_type"

            if not selected_template_id:
                selected_template_id = self.auto_detect_template(
                    user_query=user_query,
                    ticker=ticker,
                    fallback_id=fallback_id,
                )
                selection_source = f"{selection_source}+template_keyword_match"

            # Company analysis default: if ticker exists and auto-detect picked "general",
            # use financial_quality_mvp when available.
            if ticker and selected_template_id == "general" and "financial_quality_mvp" in self._templates_cache:
                selected_template_id = "financial_quality_mvp"
                selection_source = f"{selection_source}+ticker_financial_default"

        # Final safety fallback.
        if selected_template_id not in self._templates_cache:
            if "financial_quality_mvp" in self._templates_cache:
                selected_template_id = "financial_quality_mvp"
            else:
                selected_template_id = fallback_id if fallback_id in self._templates_cache else "general"
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
            template_id: The template ID (e.g., "resources_gold_monometallic")

        Returns:
            Template dict or None if not found
        """
        return self._templates_cache.get(template_id)

    def get_rubric(self, template_id: str) -> Optional[str]:
        """
        Get just the rubric text from a template.

        Args:
            template_id: The template ID

        Returns:
            Rubric text or None if not found
        """
        template = self.get_template(template_id)
        if template:
            return template.get('rubric', '')
        return None

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
            return ""
        if company_name:
            rubric = rubric.replace("[Company Name]", company_name)
        if exchange:
            rubric = rubric.replace("[Exchange]", exchange.upper())
        if max_chars > 0:
            rubric = rubric[:max_chars]
        return rubric.strip()

    def auto_detect_template(
        self,
        user_query: str,
        ticker: str = None,
        fallback_id: str = "general"
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
        best_template_id = fallback_id

        for template_id, template_data in self._templates_cache.items():
            # Skip general template in scoring (it's the fallback)
            if template_id == "general":
                continue

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
            return fallback_id

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
            if company_name:
                rubric = rubric.replace("[Company Name]", company_name)
            if exchange:
                rubric = rubric.replace("[Exchange]", exchange.upper())
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
        return (
            "Financial analysis framing:\n"
            f"- Template: {template_id}\n"
            f"- Company name: {company_name or 'unknown'}.\n"
            f"- {company_type_line}\n"
            f"- {exchange_line}\n"
            "- Gather evidence needed for scoring and investment judgment, not generic summaries.\n"
            "- Prioritize recent primary documents with quantitative data.\n\n"
            "Exchange assumptions:\n"
            f"{exchange_assumptions}\n\n"
            + (
                "Template rubric:\n"
                f"{rubric}"
                if include_rubric and rubric
                else "Template rubric:\n(omitted in brief; expected in main query)"
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
        if "financial_quality_mvp" in self._templates_cache:
            return "financial_quality_mvp"
        if "general" in self._templates_cache:
            return "general"
        return next(iter(self._templates_cache), "general")

    def _score_template_keywords(self, template_id: str, user_query: str) -> int:
        template_data = self._templates_cache.get(template_id, {})
        keywords = template_data.get("auto_detect_keywords", [])
        query_lower = (user_query or "").lower()
        return sum(1 for keyword in keywords if keyword.lower() in query_lower)

    def _choose_template_for_company_type(self, company_type_id: str, user_query: str) -> str:
        entry = self._company_type_entry(company_type_id)
        if not entry:
            return "financial_quality_mvp" if "financial_quality_mvp" in self._templates_cache else "general"

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
    fallback_id: str = "general",
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
