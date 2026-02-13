"""Internet search integration using Tavily API."""

import httpx
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from .config import TAVILY_API_KEY, MAX_SEARCH_RESULTS
from .openrouter import query_model
from .pdf_processor import process_pdf_attachment, save_attachment
import uuid


EXCHANGE_PREFIXES = ("ASX", "NYSE", "NASDAQ", "TSX", "TSXV", "LSE", "AIM")
SUFFIX_TO_EXCHANGE = {
    "AX": "ASX",
    "N": "NYSE",
    "O": "NASDAQ",
    "Q": "NASDAQ",
    "TO": "TSX",
    "V": "TSXV",
    "L": "LSE",
}
EXCHANGE_TO_YAHOO_SUFFIX = {
    "ASX": ".AX",
    "NYSE": ".N",
    "NASDAQ": ".O",
    "TSX": ".TO",
    "TSXV": ".V",
    "LSE": ".L",
    "AIM": ".L",
}


async def perform_search(
    query: str,
    max_results: int = MAX_SEARCH_RESULTS,
    search_depth: str = "advanced"
) -> Optional[Dict[str, Any]]:
    """
    Perform internet search using Tavily API.

    Args:
        query: Search query (can be reformulated from user question)
        max_results: Number of results to return (default from config)
        search_depth: "basic" or "advanced" (advanced is more thorough)

    Returns:
        Dict with 'query', 'results' list, 'performed_at', 'result_count'
        Returns dict with 'error' key if search fails
    """
    if not TAVILY_API_KEY:
        return {
            "error": "Tavily API key not configured",
            "results": [],
            "result_count": 0
        }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": TAVILY_API_KEY,
                    "query": query,
                    "search_depth": search_depth,
                    "max_results": max_results,
                    "include_answer": False,  # We'll let LLMs synthesize
                    "include_raw_content": False  # We just need summaries
                },
                headers={"Content-Type": "application/json"}
            )

            if response.status_code != 200:
                return {
                    "error": f"Tavily API error: {response.status_code}",
                    "results": [],
                    "result_count": 0
                }

            data = response.json()
            results = data.get("results", [])

            # Format results consistently
            formatted_results = []
            for result in results:
                formatted_results.append({
                    "title": result.get("title", "Untitled"),
                    "url": result.get("url", ""),
                    "content": result.get("content", ""),
                    "score": result.get("score", 0.0)
                })

            return {
                "query": query,
                "results": formatted_results,
                "performed_at": datetime.utcnow().isoformat(),
                "result_count": len(formatted_results)
            }

    except httpx.TimeoutException:
        return {
            "error": "Search request timed out",
            "results": [],
            "result_count": 0
        }
    except Exception as e:
        # Log error but don't block the council process
        print(f"Search error: {str(e)}")
        return {
            "error": f"Search unavailable: {str(e)}",
            "results": [],
            "result_count": 0
        }


async def reformulate_query_for_search(user_query: str) -> str:
    """
    Use a fast LLM to reformulate user question into optimal search query.

    Args:
        user_query: Original user question

    Returns:
        Optimized search query string
    """
    reformulation_prompt = f"""You are a search query optimizer. Given a user's question, extract the key search terms that would yield the best search engine results.

Guidelines:
- Remove conversational elements ("Can you tell me", "I'd like to know", etc.)
- Focus on factual information needs
- Keep it concise (typically 3-8 words)
- Include relevant time periods if mentioned (e.g., "2025", "latest", "recent")
- Preserve important technical terms and proper nouns

User Question: {user_query}

Optimized Search Query:"""

    messages = [{"role": "user", "content": reformulation_prompt}]

    try:
        # Use fast, cheap model for reformulation
        response = await query_model("google/gemini-2.5-flash", messages, timeout=15.0)

        if response and response.get('content'):
            query = response['content'].strip().strip('"\'')
            # If reformulation fails or is too long, fall back to original
            if len(query) > 0 and len(query) < 200:
                return query
    except Exception as e:
        print(f"Query reformulation error: {str(e)}")

    # Fallback: use original query
    return user_query


def classify_asx_announcement(title: str, url: str) -> tuple[str, int]:
    """
    Classify ASX announcement by importance.

    Returns:
        (category, priority) where priority: 1=critical, 2=important, 3=routine, 4=ignore
    """
    title_lower = title.lower()

    # CRITICAL (Priority 1) - Must download
    critical_keywords = [
        'investor presentation', 'agm presentation', 'company presentation',
        'feasibility study', 'dfs', 'definitive feasibility', 'pfs', 'pre-feasibility',
        'scoping study', 'bankable feasibility',
        'annual report', 'financial report',
        'resource estimate', 'reserve estimate', 'jorc', 'mineral resource',
        'quarterly report', 'quarterly activities', 'quarterly cashflow'
    ]

    # IMPORTANT (Priority 2) - Should download
    important_keywords = [
        'drilling results', 'assay results', 'exploration update',
        'metallurgical', 'met test', 'offtake agreement', 'finance', 'funding',
        'acquisition', 'strategic', 'partnership', 'jv', 'joint venture',
        'production update', 'operational update'
    ]

    # ROUTINE IGNORE (Priority 4) - Skip these
    ignore_keywords = [
        'appendix 3b', 'appendix 3y', 'appendix 3z', 'appendix 2a',
        'change of director', 'change in director', 'director interest',
        'notice of annual general meeting', 'notice of agm',  # Just the notice, not presentation
        'trading halt', 'pause in trading', 'voluntary suspension',
        'cleansing notice', 'section 708a', 's708a',
        'becoming a substantial holder', 'ceasing to be substantial',
        'change to substantial', 'notification of interest',
        'disclosure of interest', 'initial director',
        'security purchase plan', 'spp results'
    ]

    # Check ignore first
    for keyword in ignore_keywords:
        if keyword in title_lower:
            return ('ignore', 4)

    # Check critical
    for keyword in critical_keywords:
        if keyword in title_lower:
            return ('critical', 1)

    # Check important
    for keyword in important_keywords:
        if keyword in title_lower:
            return ('important', 2)

    # Default: routine
    return ('routine', 3)


async def scrape_marketindex_announcements(ticker: str) -> List[Dict[str, Any]]:
    """
    Scrape announcements from marketindex.com.au for a given ticker.

    Args:
        ticker: ASX ticker code (e.g., "BML")

    Returns:
        List of announcement dicts with title, url, date, priority
    """
    url = f"https://www.marketindex.com.au/asx/{ticker.lower()}"
    print(f"Scraping announcements from: {url}")

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers) as client:
            response = await client.get(url)

            if response.status_code != 200:
                print(f"Failed to fetch marketindex page: {response.status_code}")
                return []

            html = response.text

            # Parse HTML to extract announcements
            # Look for ASX announcement links (they typically point to asx.com.au PDFs)
            announcements = []

            # Simple regex-based extraction (could use BeautifulSoup for robustness)
            # Look for links to ASX PDFs
            import re

            # Pattern for ASX PDF links
            pdf_pattern = r'href="(https://www\.asx\.com\.au/asxpdf/[^"]+\.pdf)"[^>]*>([^<]+)</a>'
            matches = re.findall(pdf_pattern, html)

            for pdf_url, title in matches:
                title = title.strip()
                category, priority = classify_asx_announcement(title, pdf_url)

                announcements.append({
                    'title': title,
                    'url': pdf_url,
                    'category': category,
                    'priority': priority
                })

            # Sort by priority (1 is highest)
            announcements.sort(key=lambda x: x['priority'])

            print(f"Found {len(announcements)} announcements for {ticker}")
            print(f"  - Critical: {sum(1 for a in announcements if a['priority'] == 1)}")
            print(f"  - Important: {sum(1 for a in announcements if a['priority'] == 2)}")
            print(f"  - Routine: {sum(1 for a in announcements if a['priority'] == 3)}")
            print(f"  - Ignored: {sum(1 for a in announcements if a['priority'] == 4)}")

            return announcements

    except Exception as e:
        print(f"Error scraping marketindex for {ticker}: {e}")
        return []


def extract_ticker_from_query(query: str) -> Optional[str]:
    """
    Extract exchange-prefixed ticker code from user query.
    Supports ASX/NYSE/NASDAQ/TSX/TSXV/LSE/AIM patterns.

    Args:
        query: User query text

    Returns:
        Exchange-prefixed ticker (e.g., "ASX:WWI", "NYSE:NEM") or bare ticker if detected.
    """
    text = (query or "").strip()
    if not text:
        return None

    # Highest priority: PREFIX:SYMBOL, no spaces in symbol.
    prefixed = re.search(
        r"\b(ASX|NYSE|NASDAQ|TSXV?|LSE|AIM)\s*:\s*([A-Z0-9.\-]{1,12})\b",
        text,
        re.IGNORECASE,
    )
    if prefixed:
        exchange = prefixed.group(1).upper()
        raw_symbol = prefixed.group(2).strip()
        # Guard against "ASX:West Wits Mining Limited" style company-name strings.
        if raw_symbol[:1].isupper() and raw_symbol[1:].islower():
            raw_symbol = ""
        symbol = raw_symbol.upper()
        # Ignore cases like "ASX:West" that look like names not symbols.
        if re.fullmatch(r"[A-Z0-9.\-]{1,12}", symbol):
            return f"{exchange}:{symbol}"

    # Secondary: SYMBOL.SUFFIX notation.
    with_suffix = re.search(r"\b([A-Z][A-Z0-9]{0,10})\.(AX|TO|V|N|O|Q|L)\b", text, re.IGNORECASE)
    if with_suffix:
        symbol = with_suffix.group(1).upper()
        suffix = with_suffix.group(2).upper()
        exchange = SUFFIX_TO_EXCHANGE.get(suffix)
        if exchange:
            return f"{exchange}:{symbol}"

    # Tertiary: PREFIX SYMBOL (without colon)
    spaced = re.search(
        r"\b(ASX|NYSE|NASDAQ|TSXV?|LSE|AIM)\s+([A-Z0-9]{1,6})\b",
        text,
        re.IGNORECASE,
    )
    if spaced:
        exchange = spaced.group(1).upper()
        symbol = spaced.group(2).upper()
        if re.fullmatch(r"[A-Z0-9]{1,6}", symbol):
            return f"{exchange}:{symbol}"

    # Final fallback: explicit "ticker XXX" pattern.
    ticker_keyword = re.search(r"\bticker\s*[:\-]?\s*([A-Z0-9]{1,6})\b", text, re.IGNORECASE)
    if ticker_keyword:
        return ticker_keyword.group(1).upper()

    return None


async def perform_financial_search(ticker: str) -> Dict[str, Any]:
    """
    Get market data for a ticker using exchange-aware Tavily search.
    Supports prefixed or bare tickers and adjusts filing sources by exchange.

    NOTE: Does NOT download PDFs - user should upload those manually.

    Args:
        ticker: Ticker code (e.g., "ASX:WWI", "NYSE:NEM", "WWI")

    Returns:
        Search results with market data
    """
    parsed = _parse_ticker(ticker)
    symbol = parsed["symbol"]
    exchange = parsed["exchange"]
    display_ticker = parsed["display_ticker"]
    yahoo_ticker = parsed["yahoo_ticker"]
    exchange_label = exchange or "UNKNOWN"

    if not symbol:
        return {
            "error": "No valid ticker symbol provided",
            "results": [],
            "pdfs_processed": [],
            "performed_at": datetime.utcnow().isoformat(),
            "result_count": 0,
            "search_type": "exchange_finance_search",
            "ticker": ticker,
            "yahoo_ticker": "",
            "exchange": exchange_label,
        }

    all_results = []

    print(f"Fetching market data for {display_ticker} (exchange={exchange_label})")

    # Targeted searches for market/fundamental data.
    search_queries = [
        f"site:finance.yahoo.com {yahoo_ticker} stock price statistics",
        f"{yahoo_ticker} market cap shares outstanding enterprise value",
        f"{symbol} {exchange_label} latest investor presentation",
    ]
    if exchange == "ASX":
        search_queries.extend(
            [
                f"site:asx.com.au {symbol} announcement",
                f"site:marketindex.com.au ASX:{symbol} market cap shares outstanding",
            ]
        )
    elif exchange in {"NYSE", "NASDAQ"}:
        search_queries.extend(
            [
                f"site:sec.gov {symbol} 10-K 10-Q 8-K",
                f"{symbol} {exchange_label} earnings release investor relations",
            ]
        )
    elif exchange in {"TSX", "TSXV"}:
        search_queries.extend(
            [
                f"site:sedarplus.ca {symbol} filing",
                f"{symbol} {exchange_label} NI 43-101 technical report",
            ]
        )
    elif exchange in {"LSE", "AIM"}:
        search_queries.extend(
            [
                f"site:londonstockexchange.com {symbol} RNS",
                f"site:investegate.co.uk {symbol} RNS announcement",
            ]
        )
    else:
        search_queries.extend(
            [
                f"{symbol} company annual report latest",
                f"{symbol} stock exchange filings investor relations",
            ]
        )

    for query in search_queries:
        try:
            print(f"Search: {query}")
            result = await perform_search(query, max_results=2, search_depth="basic")

            if result and not result.get('error'):
                results_list = result.get('results', [])
                all_results.extend(results_list)

        except Exception as e:
            print(f"Search error for '{query}': {e}")
            continue

    # Deduplicate by URL
    seen_urls = set()
    unique_results = []
    for r in all_results:
        url = r.get('url', '')
        if url not in seen_urls:
            seen_urls.add(url)
            unique_results.append(r)

    return {
        "query": f"Market data for {display_ticker} (Yahoo: {yahoo_ticker})",
        "results": unique_results[:10],
        "pdfs_processed": [],  # No PDFs - user uploads manually
        "performed_at": datetime.utcnow().isoformat(),
        "result_count": len(unique_results),
        "search_type": "exchange_finance_search",
        "ticker": display_ticker,
        "yahoo_ticker": yahoo_ticker,
        "exchange": exchange_label,
        "symbol": symbol,
    }


def _parse_ticker(ticker: str) -> Dict[str, str]:
    """Parse prefixed/suffixed ticker formats into normalized search identifiers."""
    raw = str(ticker or "").strip().upper()
    exchange = ""
    symbol = ""

    if ":" in raw:
        prefix, rest = raw.split(":", 1)
        prefix = prefix.strip().upper()
        rest = rest.strip().upper()
        if prefix in EXCHANGE_PREFIXES:
            exchange = prefix
            symbol = rest
    elif "." in raw:
        match = re.fullmatch(r"([A-Z][A-Z0-9]{0,10})\.(AX|TO|V|N|O|Q|L)", raw)
        if match:
            symbol = match.group(1).upper()
            exchange = SUFFIX_TO_EXCHANGE.get(match.group(2).upper(), "")
        else:
            symbol = raw
    else:
        symbol = raw

    if exchange and symbol:
        yahoo_suffix = EXCHANGE_TO_YAHOO_SUFFIX.get(exchange, "")
        yahoo_ticker = f"{symbol}{yahoo_suffix}" if yahoo_suffix else symbol
        display_ticker = f"{exchange}:{symbol}"
    else:
        yahoo_ticker = symbol
        display_ticker = symbol

    return {
        "raw": raw,
        "exchange": exchange,
        "symbol": symbol,
        "display_ticker": display_ticker,
        "yahoo_ticker": yahoo_ticker,
    }


async def download_and_process_pdf(url: str, context: str) -> Optional[Dict[str, Any]]:
    """
    Download a PDF from URL and process it.

    Args:
        url: URL to PDF file
        context: Context for the PDF (company name, etc.)

    Returns:
        Processed PDF information
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)

            if response.status_code != 200:
                return None

            # Save to temp location
            temp_id = str(uuid.uuid4())
            filename = url.split('/')[-1] or f"{context.replace(' ', '_')}.pdf"

            file_path = await save_attachment(
                response.content,
                "temp_search",
                temp_id,
                filename
            )

            # Process PDF
            processed = await process_pdf_attachment(file_path, filename)
            processed['source_url'] = url

            return processed

    except Exception as e:
        print(f"PDF download error: {e}")
        return None




def format_search_results_for_prompt(search_results: Dict[str, Any]) -> str:
    """
    Format search results into readable text for inclusion in prompts.

    Args:
        search_results: Results from perform_search() or perform_financial_search()

    Returns:
        Formatted markdown string with results
    """
    if search_results.get('error'):
        return f"Note: Internet search was attempted but failed ({search_results['error']}). Please answer based on your knowledge."

    results = search_results.get('results', [])
    if not results:
        return "Note: Internet search returned no results. Please answer based on your knowledge."

    query = search_results.get('query', 'Unknown query')
    search_type = search_results.get('search_type', 'standard')

    lines = []

    if search_type == 'financial_multi_query':
        lines.append(f"**Financial Research Conducted: {query}**")
        lines.append(f"Multiple targeted searches performed across ASX and market data sources")
        lines.append(f"Found {len(results)} relevant documents and sources:\n")
    else:
        lines.append(f"Search query used: \"{query}\"")
        lines.append(f"Found {len(results)} relevant sources:\n")

    for i, result in enumerate(results, 1):
        lines.append(f"{i}. **{result['title']}**")
        lines.append(f"   URL: {result['url']}")
        if result.get('content'):
            # Truncate very long content
            content = result['content']
            if len(content) > 500:
                content = content[:497] + "..."
            lines.append(f"   Summary: {content}")
        lines.append("")  # Blank line between results

    # Add information about downloaded PDFs
    pdfs = search_results.get('pdfs_processed', [])
    if pdfs:
        lines.append(f"\n**{len(pdfs)} PDF document(s) automatically downloaded and parsed:**\n")
        for pdf in pdfs:
            lines.append(f"- {pdf.get('filename', 'Unknown')} ({pdf.get('page_count', 0)} pages)")
            if pdf.get('source_url'):
                lines.append(f"  Source: {pdf['source_url']}")
            if pdf.get('summary'):
                lines.append(f"  Summary: {pdf['summary'][:200]}...")
            lines.append("")

    return "\n".join(lines)
