# Announcement Summaries

- generated_at_utc: 2026-02-18T09:16:33.373647+00:00
- worker_model: openai/gpt-4o-mini
- output_min_importance_score: 80
- output_include_numeric_facts: False
- hybrid_vision_enabled: True
- hybrid_vision_model: openai/gpt-4o-mini
- hybrid_vision_max_pages: 50 (0=all; default soft cap=50)
- dump_dir: /Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg
- total_processed: 20
- kept_for_injection: 4
- dropped_as_unimportant: 16

## Kept Documents (JSON Elements)

### 01_2026-02-10_sec_10_q_filing_2026_02_10.md
```json
{
  "doc_id": "01_2026-02-10_sec_10_q_filing_2026_02_10.md",
  "price_sensitive": {
    "is_price_sensitive": true,
    "confidence": 0.95,
    "reason": "The document contains financial results, capital structure changes, and management's discussion that significantly impact the company's valuation."
  },
  "importance": {
    "is_important": true,
    "importance_score": 85,
    "tier": "high",
    "keep_for_injection": true,
    "reason": "The document provides crucial updates on financial performance, capital needs, and project timelines, which are critical for investment decision-making."
  },
  "summary": {
    "one_line": "Paramount Gold Nevada Corp. reports significant financial changes in Q2 2026 and updates on its projects.",
    "key_points": [
      "Cash and equivalents increased to $3.54 million as of Dec 31, 2025, up from $1.35 million.",
      "Total assets rose to $53.86 million compared to $52.40 million in June 2025.",
      "Total liabilities increased to $25.38 million, up from $18.83 million.",
      "Net loss for Q2 2026 was $4.43 million, a sharp increase of 118% from $2.03 million in Q2 2025.",
      "Total expenses in Q2 2026 were $2.15 million, up from $1.22 million in Q2 2025.",
      "Exploration and development costs were $739,808 for Q2 2026, nearly double from $377,112 in Q2 2025.",
      "The company has issued 2.85 million shares under the At-the-Market (ATM) program, raising $2.71 million.",
      "Secured $15 million in convertible debenture financing with a 10% interest rate, convertible into 4.75% gross revenue royalty.",
      "Management reported compliance with loan covenants as of Dec 31, 2025.",
      "Company expects to fund operations through existing cash, ATM program, insurance proceeds, and potential royalty sales.",
      "Expected cash expenditures over the next twelve months are approximately $3.4 million for corporate expenses and $1.5 million for project activities.",
      "Impairment reviews indicated no impairment of mineral properties as of Dec 31, 2025.",
      "Management expects to incur continuing operational losses due to ongoing exploration and development activities.",
      "The regulatory approval for the Grass Mountain project is progressing, with various permits secured.",
      "Key risks include the ability to generate positive cash flows and secure additional financing for operations."
    ],
    "numeric_facts": [],
    "timeline_milestones": [
      {
        "milestone": "Completion of environmental impact statement for Grass Mountain",
        "target_window": "2026",
        "direction": "new",
        "source_snippet": "The Bu eau o Land Managemen..."
      },
      {
        "milestone": "Projected earnings from operational activities",
        "target_window": "unknown",
        "direction": "unclear",
        "source_snippet": "The Company does no expec..."
      },
      {
        "milestone": "Cash funding strategy implementation",
        "target_window": "upcoming months",
        "direction": "new",
        "source_snippet": "The Company expects to fund..."
      }
    ],
    "capital_structure": [
      "200 million common shares authorized, with 83,813,242 shares outstanding as of Feb 9, 2026.",
      "Convertible debenture of $15 million issued with a 10% annual interest rate."
    ],
    "catalysts_next_12m": [
      "Completion of necessary permits for Grass Mountain project.",
      "Execution of additional financing arrangements, including potential equity raises.",
      "Progress in exploration activities for Nevada and Oregon projects."
    ],
    "risks_headwinds": [
      "Ongoing operational losses due to lack of revenue from mining activities.",
      "Challenges in securing additional financing for exploration and development.",
      "Dependencies on market conditions and regulatory approvals for project advancement."
    ],
    "market_impact_assessment": "The financial results indicate a significant operational loss and increased liabilities, raising concerns for investors regarding the company's future profitability and liquidity. The ability to secure additional funding will be critical in sustaining operations and project developments."
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "high",
    "notes": [
      "The document covers comprehensive financial and operational updates that are critical for investment evaluation."
    ]
  },
  "source_meta": {
    "file_name": "01_2026-02-10_sec_10_q_filing_2026_02_10.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/01_2026-02-10_sec_10_q_filing_2026_02_10.md",
    "title": "SEC 10-Q filing (2026-02-10)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312526044779/pzg-20251231.htm",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312526044779/pzg-20251231.htm",
    "domain": "www.sec.gov",
    "published_at": "2026-02-10",
    "decoded_chars_in_file": 98715,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312526044779/pzg-20251231.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  }
}
```

### 06_2025-11-20_sec_8_k_filing_2025_11_20.md
```json
{
  "doc_id": "06_2025-11-20_sec_8_k_filing_2025_11_20.md",
  "price_sensitive": {
    "is_price_sensitive": true,
    "confidence": 0.9,
    "reason": "The filing involves a significant equity raise that could impact stock price."
  },
  "importance": {
    "is_important": true,
    "importance_score": 85,
    "tier": "high",
    "keep_for_injection": true,
    "reason": "Discussion of a prospectus supplement for equity offering, impacting capital structure and liquidity."
  },
  "summary": {
    "one_line": "Prospectus supplement filed for an equity offering of up to $14.9 million.",
    "key_points": [
      "Company filed prospectus supplement on November 20, 2025.",
      "Equity offering amounting to $14.9 million announced.",
      "Offering is pursuant to the Controlled Equity Offering SM Sales Agreement.",
      "Prior prospectus supplements dated March 22, 2024, and May 16, 2024, are relevant.",
      "The sales agreement involves Canaccord Genuity and A.G.P./Alliance Global Partners.",
      "As of the date of the supplement, $5.9 million has been sold under the sales agreement.",
      "Supplement amends previous prospectus information regarding the equity offering.",
      "Legal opinion on the validity of the common stock included as an exhibit.",
      "Potential impact on the company's liquidity and capital structure."
    ],
    "numeric_facts": [],
    "timeline_milestones": [],
    "capital_structure": [
      "Equity raise of up to $14.9 million.",
      "Sale of $5.9 million already executed under the agreement."
    ],
    "catalysts_next_12m": [
      "Further sales under the equity offering may continue.",
      "Financial performance post-funding could influence stock price."
    ],
    "risks_headwinds": [
      "Dilution risk for existing shareholders due to the equity raise.",
      "Market reception of the offering may affect stock price."
    ],
    "market_impact_assessment": "The announcement of the equity offering is likely to affect investor perception and the company's stock price, depending on market conditions and investor sentiment."
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "high",
    "notes": [
      "attempt_1:json_parse_failed"
    ]
  },
  "source_meta": {
    "file_name": "06_2025-11-20_sec_8_k_filing_2025_11_20.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/06_2025-11-20_sec_8_k_filing_2025_11_20.md",
    "title": "SEC 8-K filing (2025-11-20)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525289809/d94365d8k.htm",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525289809/d94365d8k.htm",
    "domain": "www.sec.gov",
    "published_at": "2025-11-20",
    "decoded_chars_in_file": 4471,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525289809/d94365d8k.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  }
}
```

### 08_2025-11-14_sec_10_q_filing_2025_11_14.md
```json
{
  "doc_id": "08_2025-11-14_sec_10_q_filing_2025_11_14.md",
  "price_sensitive": {
    "is_price_sensitive": true,
    "confidence": 0.9,
    "reason": "Contains critical financial data, funding changes and operational updates."
  },
  "importance": {
    "is_important": true,
    "importance_score": 85,
    "tier": "high",
    "keep_for_injection": true,
    "reason": "Includes significant financial results, operational losses, and details on funding and capital structure."
  },
  "summary": {
    "one_line": "Paramount Gold Nevada Corp. reported a net loss of $4.3 million for the quarter ended September 30, 2025, while also detailing significant capital structure and operational developments.",
    "key_points": [
      "Net loss of $4,324,338 for the three months ended September 30, 2025, compared to $1,572,138 in the same period of 2024.",
      "Total current assets increased to $5,125,732 from $2,707,350 as of June 30, 2025.",
      "Total liabilities rose to $23,266,910 from $18,834,909.",
      "Cash and cash equivalents were $4,165,894 at the end of the reporting period.",
      "Total mineral properties valued at $49,137,478 remained unchanged.",
      "Expenditures on exploration and development surged to $566,096 from $395,298 in the prior year.",
      "Company generated no revenue from mining operations.",
      "Expenses related to reclamation decreased by 30% from the prior year.",
      "Cash used in operating activities totalled $1,081,039, primarily for permitting and exploration.",
      "Cash provided by financing activities was $3,895,932 from sales under the ATM program and warrant issuance.",
      "Capital raised through equity was $1,895,932, issuing 2,146,561 shares.",
      "Debentures of $15,000,000 secured against the company’s assets carry an interest rate of 10% per annum.",
      "The debentures can be repaid in cash or converted to a royalty of 4.75% on gold and silver produced.",
      "Incurred $870,111 in debenture issuance costs.",
      "Non-cash interest expense of $426,839 incurred.",
      "Company remains dependent on equity financing for future operations due to ongoing losses.",
      "The company faces substantial doubt about its ability to continue as a going concern.",
      "Plans to fund operations through existing cash, ATM programs, and potential equity financing."
    ],
    "numeric_facts": [],
    "timeline_milestones": [
      {
        "milestone": "Cash position maintained",
        "target_window": "Quarterly",
        "direction": "reconfirmed",
        "source_snippet": "cash on hand and working capital."
      },
      {
        "milestone": "Funding through Equity",
        "target_window": "Next 12 months",
        "direction": "unclear",
        "source_snippet": "Equity financings...in the near future."
      }
    ],
    "capital_structure": [
      "Secured Royalty Convertible Debentures of $15,000,000 with 10% interest.",
      "Total common stock outstanding as of September 30, 2025, was 77,933,356 shares.",
      "Additional paid-in capital increased to $126,518,883."
    ],
    "catalysts_next_12m": [
      "Completion of necessary permitting for the Glassy Mountain Project.",
      "Further developments in equity financing to sustain operations."
    ],
    "risks_headwinds": [
      "Substantial doubt about the company's ability to continue as a going concern.",
      "Reliance on future capital raising efforts amidst ongoing losses.",
      "Potential regulatory hurdles in securing permits for operational projects."
    ],
    "market_impact_assessment": "The financial health of Paramount Gold Nevada Corp. shows significant operational losses, raising concerns about its ability to fund future development projects. The dependency on capital markets places its stock at risk, while potential developments in fiscal strategies could mitigate those risks if successful."
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "high",
    "notes": [
      "Comprehensive financial details provided with clarity on liabilities and capital structure."
    ]
  },
  "source_meta": {
    "file_name": "08_2025-11-14_sec_10_q_filing_2025_11_14.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/08_2025-11-14_sec_10_q_filing_2025_11_14.md",
    "title": "SEC 10-Q filing (2025-11-14)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525281737/pzg-20250930.htm",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525281737/pzg-20250930.htm",
    "domain": "www.sec.gov",
    "published_at": "2025-11-14",
    "decoded_chars_in_file": 84353,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525281737/pzg-20250930.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  }
}
```

### 18_2025-05-12_sec_10_q_filing_2025_05_12.md
```json
{
  "doc_id": "18_2025-05-12_sec_10_q_filing_2025_05_12.md",
  "price_sensitive": {
    "is_price_sensitive": true,
    "confidence": 0.85,
    "reason": "The document includes significant financial disclosures regarding the company’s operational losses, capital structure changes, and upcoming liquidity risks."
  },
  "importance": {
    "is_important": true,
    "importance_score": 90,
    "tier": "critical",
    "keep_for_injection": true,
    "reason": "Includes crucial financial details which impact investor decisions, such as cash liquidity, debt arrangements, operational losses, and equity financing."
  },
  "summary": {
    "one_line": "Paramount Gold Nevada Corp. reported significant financial losses and liquidity challenges in its SEC 10-Q filing for Q3 2025.",
    "key_points": [
      "Net loss for the three months ended March 31, 2025, was $2,618,307, compared to $1,814,045 in Q3 2024.",
      "Total nine-month loss increased to $6,221,934 from $5,462,764 year-over-year.",
      "Current cash balance as of March 31, 2025, is $2,139,516, down from $5,423,059 in June 2024.",
      "Exploration and development expenses were $733,906 for Q3 2025, a 3% increase from $713,404 in Q3 2024.",
      "Reclamation expenses dropped to $14,193 from $252,534 year-over-year due to previous one-time costs.",
      "Company incurred $439,564 from equity financing under an ATM program, issuing a total of 1,158,309 shares.",
      "Total assets decreased from $56,361,612 in June 2024 to $52,619,457 in March 2025.",
      "Current liabilities include accounts payable of $541,422 and accrued liabilities of $120,000.",
      "Non-current liabilities primarily consist of $11,587,039 in secured royalty convertible debentures.",
      "Management anticipates upcoming cash needs of approximately $3 million for operations within the next twelve months."
    ],
    "numeric_facts": [],
    "timeline_milestones": [
      {
        "milestone": "Cash needs estimate for next 12 months",
        "target_window": "Next 12 months",
        "direction": "unclear",
        "source_snippet": "We anticipate our twelve-month cash expenditures to be as follows..."
      }
    ],
    "capital_structure": [
      "Current cash balance of $2,139,516.",
      "Total liabilities include $11,587,039 in secured royalty convertible debentures.",
      "ATM program established for $7 million with Canaccord Genuity LLC and A.G.P./Alliance Global Partners."
    ],
    "catalysts_next_12m": [
      "Anticipated exploration and permitting activities for the Grass Mountain Project.",
      "Potential additional equity financing to meet liquidity needs."
    ],
    "risks_headwinds": [
      "Continuous operating losses causing liquidity concerns.",
      "Dependence on equity financing and royalty sales for funding."
    ],
    "market_impact_assessment": "The financial disclosures highlight the company's ongoing operational struggles, potential liquidity issues, and reliance on capital markets, which could affect investor confidence and stock performance."
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "high",
    "notes": [
      "Document provides substantial financial data relevant for investment analysis.",
      "attempt_1:json_parse_failed"
    ]
  },
  "source_meta": {
    "file_name": "18_2025-05-12_sec_10_q_filing_2025_05_12.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/18_2025-05-12_sec_10_q_filing_2025_05_12.md",
    "title": "SEC 10-Q filing (2025-05-12)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000095017025069293/pzg-20250331.htm",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000095017025069293/pzg-20250331.htm",
    "domain": "www.sec.gov",
    "published_at": "2025-05-12",
    "decoded_chars_in_file": 91253,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000095017025069293/pzg-20250331.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  }
}
```

## Dropped Documents (Classification Only)

### 02_2026-02-10_sec_10_q_filing_2026_02_10.md
```json
{
  "doc_id": "02_2026-02-10_sec_10_q_filing_2026_02_10.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.7,
    "reason": "Document lacks quantitative financial data and material updates."
  },
  "importance": {
    "is_important": false,
    "importance_score": 0,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "Contains generic filing information with no significant financial or operational updates."
  },
  "source_meta": {
    "file_name": "02_2026-02-10_sec_10_q_filing_2026_02_10.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/02_2026-02-10_sec_10_q_filing_2026_02_10.md",
    "title": "SEC 10-Q filing (2026-02-10)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312526044779/0001193125-26-044779-index.html",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312526044779/0001193125-26-044779-index.html",
    "domain": "www.sec.gov",
    "published_at": "2026-02-10",
    "decoded_chars_in_file": 1666,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312526044779/0001193125-26-044779-index.html'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document primarily consists of metadata and default boilerplate text from SEC filing."
    ]
  }
}
```

### 03_2025-12-15_sec_8_k_filing_2025_12_15.md
```json
{
  "doc_id": "03_2025-12-15_sec_8_k_filing_2025_12_15.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.8,
    "reason": "The document primarily discusses shareholder meeting results and routine corporate governance matters."
  },
  "importance": {
    "is_important": false,
    "importance_score": 20,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "The content lacks significant financial or operational implications."
  },
  "source_meta": {
    "file_name": "03_2025-12-15_sec_8_k_filing_2025_12_15.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/03_2025-12-15_sec_8_k_filing_2025_12_15.md",
    "title": "SEC 8-K filing (2025-12-15)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525319223/pzg-20251211.htm",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525319223/pzg-20251211.htm",
    "domain": "www.sec.gov",
    "published_at": "2025-12-15",
    "decoded_chars_in_file": 6824,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525319223/pzg-20251211.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document primarily concerns annual meeting proceedings and director elections."
    ]
  }
}
```

### 04_2025-12-15_sec_8_k_filing_2025_12_15.md
```json
{
  "doc_id": "04_2025-12-15_sec_8_k_filing_2025_12_15.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.8,
    "reason": "The document primarily consists of boilerplate content and procedural information without significant financial impact."
  },
  "importance": {
    "is_important": false,
    "importance_score": 20,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "The filing details do not include essential updates on funding, project economics, or strategic changes."
  },
  "source_meta": {
    "file_name": "04_2025-12-15_sec_8_k_filing_2025_12_15.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/04_2025-12-15_sec_8_k_filing_2025_12_15.md",
    "title": "SEC 8-K filing (2025-12-15)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525319223/0001193125-25-319223-index.html",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525319223/0001193125-25-319223-index.html",
    "domain": "www.sec.gov",
    "published_at": "2025-12-15",
    "decoded_chars_in_file": 1737,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525319223/0001193125-25-319223-index.html'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document lacks substantive financial or strategic information."
    ]
  }
}
```

### 05_2025-12-15_edgar_filing_documents_for_0001193125_25_319223_sec_gov.md
```json
{
  "doc_id": "05_2025-12-15_edgar_filing_documents_for_0001193125_25_319223_sec_gov.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.8,
    "reason": "Document primarily contains administrative details and procedural elements."
  },
  "importance": {
    "is_important": false,
    "importance_score": 10,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "No substantive financial or strategic content relevant to investment analysis."
  },
  "source_meta": {
    "file_name": "05_2025-12-15_edgar_filing_documents_for_0001193125_25_319223_sec_gov.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/05_2025-12-15_edgar_filing_documents_for_0001193125_25_319223_sec_gov.md",
    "title": "EDGAR Filing Documents for 0001193125-25-319223 - SEC.gov",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/0001193125-25-319223-index.html",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/0001193125-25-319223-index.html",
    "domain": "www.sec.gov",
    "published_at": "2025-12-15",
    "decoded_chars_in_file": 1737,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/0001193125-25-319223-index.html'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document contains mostly boilerplate language and administrative information without material impact on company valuation."
    ]
  }
}
```

### 07_2025-11-20_sec_8_k_filing_2025_11_20.md
```json
{
  "doc_id": "07_2025-11-20_sec_8_k_filing_2025_11_20.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.9,
    "reason": "Document lacks specific financial or operational updates impacting stock value."
  },
  "importance": {
    "is_important": false,
    "importance_score": 25,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "Content is largely procedural and does not convey material information relevant for investment decisions."
  },
  "source_meta": {
    "file_name": "07_2025-11-20_sec_8_k_filing_2025_11_20.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/07_2025-11-20_sec_8_k_filing_2025_11_20.md",
    "title": "SEC 8-K filing (2025-11-20)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525289809/0001193125-25-289809-index.html",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525289809/0001193125-25-289809-index.html",
    "domain": "www.sec.gov",
    "published_at": "2025-11-20",
    "decoded_chars_in_file": 1832,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525289809/0001193125-25-289809-index.html'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document content lacks substantive financial or operational information."
    ]
  }
}
```

### 09_2025-11-14_sec_10_q_filing_2025_11_14.md
```json
{
  "doc_id": "09_2025-11-14_sec_10_q_filing_2025_11_14.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.9,
    "reason": "Document contains procedural admin details with no valuation impact."
  },
  "importance": {
    "is_important": false,
    "importance_score": 10,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "Filing is a standard quarterly report lacking significant new information."
  },
  "source_meta": {
    "file_name": "09_2025-11-14_sec_10_q_filing_2025_11_14.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/09_2025-11-14_sec_10_q_filing_2025_11_14.md",
    "title": "SEC 10-Q filing (2025-11-14)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525281737/0001193125-25-281737-index.html",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525281737/0001193125-25-281737-index.html",
    "domain": "www.sec.gov",
    "published_at": "2025-11-14",
    "decoded_chars_in_file": 1667,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525281737/0001193125-25-281737-index.html'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document consists mainly of filing details and lacks substantive content."
    ]
  }
}
```

### 10_2025-10-28_sec_def_14a_filing_2025_10_28.md
```json
{
  "doc_id": "10_2025-10-28_sec_def_14a_filing_2025_10_28.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.8,
    "reason": "The document primarily contains procedural information regarding a proxy statement and does not provide material information impacting stock price."
  },
  "importance": {
    "is_important": false,
    "importance_score": 25,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "Contains primarily administrative details for the annual meeting and proxy voting without material financial implications."
  },
  "source_meta": {
    "file_name": "10_2025-10-28_sec_def_14a_filing_2025_10_28.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/10_2025-10-28_sec_def_14a_filing_2025_10_28.md",
    "title": "SEC DEF 14A filing (2025-10-28)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525253604/pzg-20251028.htm",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525253604/pzg-20251028.htm",
    "domain": "www.sec.gov",
    "published_at": "2025-10-28",
    "decoded_chars_in_file": 200710,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525253604/pzg-20251028.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": true,
    "signal_quality": "medium",
    "notes": [
      "Document primarily consists of standard proxy statement disclosures without significant new information impacting financials."
    ]
  }
}
```

### 11_2025-10-28_sec_def_14a_filing_2025_10_28.md
```json
{
  "doc_id": "11_2025-10-28_sec_def_14a_filing_2025_10_28.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.8,
    "reason": "The document is a DEF 14A filing, which typically contains proxy statements and is usually not price-sensitive by itself."
  },
  "importance": {
    "is_important": false,
    "importance_score": 20,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "The document does not detail significant updates regarding funding, project economics, management changes, or any material risks."
  },
  "source_meta": {
    "file_name": "11_2025-10-28_sec_def_14a_filing_2025_10_28.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/11_2025-10-28_sec_def_14a_filing_2025_10_28.md",
    "title": "SEC DEF 14A filing (2025-10-28)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525253604/0001193125-25-253604-index.html",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525253604/0001193125-25-253604-index.html",
    "domain": "www.sec.gov",
    "published_at": "2025-10-28",
    "decoded_chars_in_file": 1636,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525253604/0001193125-25-253604-index.html'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document primarily consists of administrative filing information with no substantive financial or operational updates."
    ]
  }
}
```

### 12_2025-09-25_sec_10_k_filing_2025_09_25.md
```json
{
  "doc_id": "12_2025-09-25_sec_10_k_filing_2025_09_25.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.9,
    "reason": "Insufficient detail provided in the document to assess price sensitivity."
  },
  "importance": {
    "is_important": false,
    "importance_score": 10,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "Document lacks specific financial details or announcements impacting investment analysis."
  },
  "source_meta": {
    "file_name": "12_2025-09-25_sec_10_k_filing_2025_09_25.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/12_2025-09-25_sec_10_k_filing_2025_09_25.md",
    "title": "SEC 10-K filing (2025-09-25)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525217759/0001193125-25-217759-index.html",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525217759/0001193125-25-217759-index.html",
    "domain": "www.sec.gov",
    "published_at": "2025-09-25",
    "decoded_chars_in_file": 2776,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525217759/0001193125-25-217759-index.html'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document primarily consists of file description without substantive content."
    ]
  }
}
```

### 13_2025-09-25_edgar_filing_documents_for_0001193125_25_217759_sec_gov.md
```json
{
  "doc_id": "13_2025-09-25_edgar_filing_documents_for_0001193125_25_217759_sec_gov.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.9,
    "reason": "Contains filing information without substantive financial data."
  },
  "importance": {
    "is_important": false,
    "importance_score": 10,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "Document is primarily an SEC filing announcement without significant financial implications."
  },
  "source_meta": {
    "file_name": "13_2025-09-25_edgar_filing_documents_for_0001193125_25_217759_sec_gov.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/13_2025-09-25_edgar_filing_documents_for_0001193125_25_217759_sec_gov.md",
    "title": "EDGAR Filing Documents for 0001193125-25-217759 - SEC.gov",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/0001193125-25-217759-index.htm",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/0001193125-25-217759-index.htm",
    "domain": "www.sec.gov",
    "published_at": "2025-09-25",
    "decoded_chars_in_file": 2776,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/0001193125-25-217759-index.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "No substantive financial information extracted from document."
    ]
  }
}
```

### 14_2025-07-31_edgar_entity_landing_page_sec_gov.md
```json
{
  "doc_id": "14_2025-07-31_edgar_entity_landing_page_sec_gov.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.9,
    "reason": "The document does not contain specific financial, operational, or regulatory information relevant to investment analysis."
  },
  "importance": {
    "is_important": false,
    "importance_score": 10,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "The content is largely administrative and does not impact financial valuation or investment decisions."
  },
  "source_meta": {
    "file_name": "14_2025-07-31_edgar_entity_landing_page_sec_gov.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/14_2025-07-31_edgar_entity_landing_page_sec_gov.md",
    "title": "EDGAR Entity Landing Page - SEC.gov",
    "source_url": "https://www.sec.gov/edgar/browse/?CIK=1050446&owner=exclude",
    "pdf_url": "https://www.sec.gov/edgar/browse/?CIK=1050446&owner=exclude",
    "domain": "www.sec.gov",
    "published_at": "2025-07-31",
    "decoded_chars_in_file": 3446,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/edgar/browse/?CIK=1050446&owner=exclude'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document contains mostly administrative information with no actionable insights."
    ]
  }
}
```

### 15_2025-07-18_edgar_filing_documents_for_0001193125_25_160903.md
```json
{
  "doc_id": "15_2025-07-18_edgar_filing_documents_for_0001193125_25_160903.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.9,
    "reason": "Document contains administrative and procedural content with no immediate financial impact."
  },
  "importance": {
    "is_important": false,
    "importance_score": 10,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "Document primarily includes legal and administrative filings, lacking significant economic or strategic information."
  },
  "source_meta": {
    "file_name": "15_2025-07-18_edgar_filing_documents_for_0001193125_25_160903.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/15_2025-07-18_edgar_filing_documents_for_0001193125_25_160903.md",
    "title": "EDGAR Filing Documents for 0001193125-25-160903",
    "source_url": "https://sec.gov/Archives/edgar/data/1551950/0001193125-25-160903-index.htm",
    "pdf_url": "https://sec.gov/Archives/edgar/data/1551950/0001193125-25-160903-index.htm",
    "domain": "sec.gov",
    "published_at": "2025-07-18",
    "decoded_chars_in_file": 5353,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://sec.gov/Archives/edgar/data/1551950/0001193125-25-160903-index.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document lacks relevant financial information; mainly contains legal filing details."
    ]
  }
}
```

### 16_2025-06-10_sec_8_k_filing_2025_06_10.md
```json
{
  "doc_id": "16_2025-06-10_sec_8_k_filing_2025_06_10.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.8,
    "reason": "Document discusses an accounting firm change but lacks financial implications."
  },
  "importance": {
    "is_important": false,
    "importance_score": 10,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "The content mainly relates to a change in accounting firm without significant impact on financials or operations."
  },
  "source_meta": {
    "file_name": "16_2025-06-10_sec_8_k_filing_2025_06_10.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/16_2025-06-10_sec_8_k_filing_2025_06_10.md",
    "title": "SEC 8-K filing (2025-06-10)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000095017025084433/pzg-20250606.htm",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000095017025084433/pzg-20250606.htm",
    "domain": "www.sec.gov",
    "published_at": "2025-06-10",
    "decoded_chars_in_file": 5509,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000095017025084433/pzg-20250606.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "medium",
    "notes": [
      "Mainly discusses change in auditing firm; low relevance for investors."
    ]
  }
}
```

### 17_2025-06-10_edgar_filing_documents_for_0000950170_25_084433_sec_gov.md
```json
{
  "doc_id": "17_2025-06-10_edgar_filing_documents_for_0000950170_25_084433_sec_gov.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.8,
    "reason": "Document does not contain specific financial or operational updates that impact investment analysis."
  },
  "importance": {
    "is_important": false,
    "importance_score": 0,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "The content primarily consists of generic filing information without material financial updates."
  },
  "source_meta": {
    "file_name": "17_2025-06-10_edgar_filing_documents_for_0000950170_25_084433_sec_gov.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/17_2025-06-10_edgar_filing_documents_for_0000950170_25_084433_sec_gov.md",
    "title": "EDGAR Filing Documents for 0000950170-25-084433 - SEC.gov",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000095017025084433/0000950170-25-084433-index.html",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000095017025084433/0000950170-25-084433-index.html",
    "domain": "www.sec.gov",
    "published_at": "2025-06-10",
    "decoded_chars_in_file": 1586,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000095017025084433/0000950170-25-084433-index.html'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document lacks substantive financial content relevant to investment decisions."
    ]
  }
}
```

### 19_2025-05-12_sec_10_q_filing_2025_05_12.md
```json
{
  "doc_id": "19_2025-05-12_sec_10_q_filing_2025_05_12.md",
  "price_sensitive": {
    "is_price_sensitive": false,
    "confidence": 0.8,
    "reason": "Lacks specific financial details or updates impacting investment decisions."
  },
  "importance": {
    "is_important": false,
    "importance_score": 10,
    "tier": "ignore",
    "keep_for_injection": false,
    "reason": "Document appears to be a routine filing with no significant updates on financials or strategy."
  },
  "source_meta": {
    "file_name": "19_2025-05-12_sec_10_q_filing_2025_05_12.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/19_2025-05-12_sec_10_q_filing_2025_05_12.md",
    "title": "SEC 10-Q filing (2025-05-12)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000095017025069293/0000950170-25-069293-index.html",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000095017025069293/0000950170-25-069293-index.html",
    "domain": "www.sec.gov",
    "published_at": "2025-05-12",
    "decoded_chars_in_file": 1666,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000095017025069293/0000950170-25-069293-index.html'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "low",
    "notes": [
      "Document contains general filing information without substantive content."
    ]
  }
}
```

### 20_2025-05-01_sec_8_k_filing_2025_05_01.md
```json
{
  "doc_id": "20_2025-05-01_sec_8_k_filing_2025_05_01.md",
  "price_sensitive": {
    "is_price_sensitive": true,
    "confidence": 0.8,
    "reason": "Executive resignation and associated agreements may impact company operations."
  },
  "importance": {
    "is_important": true,
    "importance_score": 75,
    "tier": "medium",
    "keep_for_injection": false,
    "reason": "Management change could affect operational strategy and execution.; below_injection_threshold_80"
  },
  "source_meta": {
    "file_name": "20_2025-05-01_sec_8_k_filing_2025_05_01.md",
    "file": "/Users/Toms_Macbook/Projects/llm-council/outputs/pdf_dump/20260218_091423_nyse_pzg/20_2025-05-01_sec_8_k_filing_2025_05_01.md",
    "title": "SEC 8-K filing (2025-05-01)",
    "source_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525110136/d795551d8k.htm",
    "pdf_url": "https://www.sec.gov/Archives/edgar/data/1629210/000119312525110136/d795551d8k.htm",
    "domain": "www.sec.gov",
    "published_at": "2025-05-01",
    "decoded_chars_in_file": 4056,
    "vision_meta": {
      "enabled": true,
      "status": "failed",
      "reason": "pdf_download_failed:HTTPStatusError:Client error '403 Forbidden' for url 'https://www.sec.gov/Archives/edgar/data/1629210/000119312525110136/d795551d8k.htm'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403",
      "total_pages": 0,
      "pages_processed": 0,
      "page_cap": 50,
      "page_cap_applied": false,
      "relevant_pages": 0
    }
  },
  "extraction_quality": {
    "text_truncated_for_model": false,
    "signal_quality": "high",
    "notes": []
  }
}
```
