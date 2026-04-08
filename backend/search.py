"""Internet search integration using Tavily API and deterministic ASX retrieval."""

import httpx
import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
from html import unescape
from urllib.parse import urljoin
from .config import TAVILY_API_KEY, MAX_SEARCH_RESULTS
from .openrouter import query_model


EXCHANGE_PREFIXES = ("ASX", "NYSE", "NASDAQ", "TSX", "TSXV", "LSE", "AIM", "CSE", "JSE")
SUFFIX_TO_EXCHANGE = {
    "AX": "ASX",
    "N": "NYSE",
    "O": "NASDAQ",
    "Q": "NASDAQ",
    "TO": "TSX",
    "V": "TSXV",
    "L": "LSE",
    "CN": "CSE",
    "JO": "JSE",
}
EXCHANGE_TO_YAHOO_SUFFIX = {
    "ASX": ".AX",
    "NYSE": ".N",
    "NASDAQ": ".O",
    "TSX": ".TO",
    "TSXV": ".V",
    "LSE": ".L",
    "AIM": ".L",
    "CSE": ".CN",
    "JSE": ".JO",
}

_ASX_ANNOUNCEMENT_SEARCH_URL = "https://www.asx.com.au/asx/v2/statistics/announcements.do"


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
        'becoming a substantial holder', 'ceasing to be substantial', 'ceasing to be a substantial holder',
        'change to substantial', 'change in substantial holding', 'notification of interest',
        'disclosure of interest', 'initial director',
        'security purchase plan', 'spp results',
        'application for quotation of securities',
        'notification regarding unquoted securities',
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


def _clean_html_fragment(value: str) -> str:
    text = re.sub(r"(?is)<br\s*/?>", " ", str(value or ""))
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_asx_datetime(date_text: str, time_text: str) -> Optional[datetime]:
    date_value = str(date_text or "").strip()
    if not date_value:
        return None
    text = f"{date_value} {str(time_text or '').strip()}".strip()
    for fmt in ("%d/%m/%Y %I:%M %p", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        return datetime.strptime(date_value, "%d/%m/%Y").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_asx_announcement_rows(html_text: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not html_text:
        return rows

    row_chunks = re.findall(r"(?is)<tr>(.*?)</tr>", html_text)
    for chunk in row_chunks:
        if "displayannouncement.do" not in chunk.lower():
            continue
        date_match = re.search(
            r"(?is)(\d{2}/\d{2}/\d{4})\s*<br>\s*(?:<span[^>]*>([^<]+)</span>)?",
            chunk,
        )
        if not date_match:
            continue
        date_text = str(date_match.group(1) or "").strip()
        time_text = str(date_match.group(2) or "").strip()

        link_match = re.search(
            r'(?is)<a[^>]+href="([^"]*displayAnnouncement\.do[^"]+)"[^>]*>',
            chunk,
        )
        if not link_match:
            continue
        display_url = urljoin("https://www.asx.com.au", unescape(link_match.group(1)))

        title_match = re.search(
            r'(?is)<a[^>]+href="[^"]*displayAnnouncement\.do[^"]+"[^>]*>\s*(.*?)<br',
            chunk,
        )
        title = _clean_html_fragment(title_match.group(1) if title_match else "")
        if not title:
            title = "ASX Announcement"

        published_dt = _parse_asx_datetime(date_text, time_text)
        category, priority = classify_asx_announcement(title, display_url)
        rows.append(
            {
                "display_url": display_url,
                "title": title,
                "published_dt": published_dt,
                "published_at": (
                    published_dt.astimezone(timezone.utc).date().isoformat() if published_dt else ""
                ),
                "category": category,
                "priority": priority,
            }
        )
    return rows


async def _resolve_asx_display_to_pdf_url(
    client: httpx.AsyncClient,
    display_url: str,
) -> str:
    try:
        response = await client.get(display_url)
    except Exception:
        return ""
    if response.status_code >= 400:
        return ""

    html_text = str(response.text or "")
    hidden = re.search(r'(?is)name="pdfURL"\s+value="([^"]+)"', html_text)
    if hidden:
        return unescape(hidden.group(1)).strip()

    direct = re.search(
        r"(https://announcements\.asx\.com\.au/asxpdf/[^\s\"']+\.pdf)",
        html_text,
        flags=re.IGNORECASE,
    )
    if direct:
        return unescape(direct.group(1)).strip()
    return ""


def _clean_html_text(fragment: str) -> str:
    text = unescape(str(fragment or ""))
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_marketindex_row_date(fragment: str) -> str:
    text = _clean_html_text(fragment)
    if not text:
        return ""
    patterns = [
        r"\b(\d{4}-\d{2}-\d{2})\b",
        r"\b(\d{2}/\d{2}/\d{4})\b",
        r"\b(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        raw = match.group(1).strip()
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except Exception:
                continue
    return ""


async def scrape_marketindex_announcements(ticker: str, max_results: int = 80) -> List[Dict[str, Any]]:
    """
    Scrape ASX announcement PDFs from the Market Index ticker page.

    This restores the legacy deterministic ASX lane:
    Market Index ticker page -> direct ASX PDF links -> announcement ranking.
    """
    parsed = _parse_ticker(ticker)
    symbol = str(parsed.get("symbol", "") or "").strip().upper()
    if not symbol:
        symbol = str(ticker or "").strip().upper().replace("ASX:", "")
    if not symbol:
        return []

    url = f"https://www.marketindex.com.au/asx/{symbol.lower()}"
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            )
        }
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers) as client:
            response = await client.get(url)
            if response.status_code != 200:
                print(f"Failed to fetch Market Index page for {symbol}: {response.status_code}")
                return []

            html = str(response.text or "")
            if not html.strip():
                return []

            matches = list(
                re.finditer(
                    r'(?is)<a[^>]+href=["\'](https://(?:www\.)?asx\.com\.au/asxpdf/[^"\']+\.pdf)["\'][^>]*>(.*?)</a>',
                    html,
                )
            )
            if not matches:
                matches = list(
                    re.finditer(
                        r'(?is)<a[^>]+href=["\'](https://announcements\.asx\.com\.au/asxpdf/[^"\']+\.pdf)["\'][^>]*>(.*?)</a>',
                        html,
                    )
                )

            announcements: List[Dict[str, Any]] = []
            seen_urls: set[str] = set()
            for match in matches:
                pdf_url = str(match.group(1) or "").strip()
                if not pdf_url or pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                title = _clean_html_text(match.group(2))
                if not title:
                    title = pdf_url.rsplit("/", 1)[-1]

                window_start = max(0, match.start() - 600)
                window_end = min(len(html), match.end() + 600)
                context_window = html[window_start:window_end]
                published_at = _extract_marketindex_row_date(context_window)
                category, priority = classify_asx_announcement(title, pdf_url)

                announcements.append(
                    {
                        "title": title,
                        "url": pdf_url,
                        "published_at": published_at,
                        "category": category,
                        "priority": priority,
                    }
                )

            announcements.sort(
                key=lambda item: (
                    1 if str(item.get("published_at", "")).strip() else 0,
                    str(item.get("published_at", "")).strip(),
                    -int(item.get("priority", 99) or 99),
                ),
                reverse=True,
            )
            return announcements[: max(1, int(max_results))]
    except Exception as e:
        print(f"Error scraping Market Index for {symbol}: {e}")
        return []


async def search_asx_announcements(
    ticker: str,
    max_results: int = 20,
    lookback_years: int = 2,
) -> List[Dict[str, Any]]:
    """
    Deterministically fetch ASX announcement PDFs from the official ASX search page.

    This is the same core retrieval lane used in full analysis:
    ASX search page -> displayAnnouncement -> canonical announcements PDF.
    """
    parsed = _parse_ticker(ticker)
    symbol = str(parsed.get("symbol", "") or "").strip().upper()
    if not symbol:
        symbol = str(ticker or "").strip().upper().replace("ASX:", "")
    if not symbol:
        return []

    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )
    years = [datetime.utcnow().year - idx for idx in range(max(1, int(lookback_years)))]
    rows: List[Dict[str, Any]] = []

    try:
        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": user_agent},
        ) as client:
            for year in years:
                response = await client.get(
                    _ASX_ANNOUNCEMENT_SEARCH_URL,
                    params={
                        "by": "asxCode",
                        "asxCode": symbol,
                        "timeframe": "Y",
                        "year": str(year),
                    },
                )
                if response.status_code >= 400:
                    continue
                rows.extend(_parse_asx_announcement_rows(str(response.text or "")))

            deduped: List[Dict[str, Any]] = []
            seen_displays = set()
            for row in rows:
                display_url = str(row.get("display_url") or "").strip()
                if not display_url or display_url in seen_displays:
                    continue
                seen_displays.add(display_url)
                pdf_url = await _resolve_asx_display_to_pdf_url(client, display_url)
                if not pdf_url:
                    continue
                item = dict(row)
                item["url"] = pdf_url
                deduped.append(item)

        deduped.sort(
            key=lambda item: (
                str(item.get("published_at", "")).strip(),
                -int(item.get("priority", 99) or 99),
            ),
            reverse=True,
        )
        return deduped[: max(1, int(max_results))]
    except Exception as e:
        print(f"Error searching official ASX announcements for {symbol}: {e}")
        return []


def extract_ticker_from_query(query: str) -> Optional[str]:
    """
    Extract exchange-prefixed ticker code from user query.
    Supports ASX/NYSE/NASDAQ/TSX/TSXV/LSE/AIM/CSE/JSE patterns.

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
        r"\b(ASX|NYSE|NASDAQ|TSXV?|LSE|AIM|CSE|JSE)\s*:\s*([A-Z0-9.\-]{1,12})\b",
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
    with_suffix = re.search(r"\b([A-Z][A-Z0-9]{0,10})\.(AX|TO|V|N|O|Q|L|CN|JO)\b", text, re.IGNORECASE)
    if with_suffix:
        symbol = with_suffix.group(1).upper()
        suffix = with_suffix.group(2).upper()
        exchange = SUFFIX_TO_EXCHANGE.get(suffix)
        if exchange:
            return f"{exchange}:{symbol}"

    # Tertiary: PREFIX SYMBOL (without colon)
    spaced = re.search(
        r"\b(ASX|NYSE|NASDAQ|TSXV?|LSE|AIM|CSE|JSE)\s+([A-Z0-9]{1,6})\b",
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
    elif exchange == "CSE":
        search_queries.extend(
            [
                f"site:thecse.com {symbol} issuer filings",
                f"site:sedarplus.ca {symbol} filing NI 43-101",
            ]
        )
    elif exchange in {"LSE", "AIM"}:
        search_queries.extend(
            [
                f"site:londonstockexchange.com {symbol} RNS",
                f"site:investegate.co.uk {symbol} RNS announcement",
            ]
        )
    elif exchange == "JSE":
        search_queries.extend(
            [
                f"site:jse.co.za {symbol} SENS announcement",
                f"{symbol} JSE annual report production update",
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
        match = re.fullmatch(r"([A-Z][A-Z0-9]{0,10})\.(AX|TO|V|N|O|Q|L|CN|JO)", raw)
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
