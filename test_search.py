"""Test script to check what data we get for ASX ticker search."""

import asyncio
from backend.search import perform_financial_search


async def test_ticker_search():
    ticker = "WWI"

    print("=" * 80)
    print(f"TESTING MARKET DATA SEARCH FOR: {ticker}")
    print("=" * 80)
    print()

    # Perform the search
    result = await perform_financial_search(ticker)

    # Print basic info
    print(f"Query: {result.get('query')}")
    print(f"Search Type: {result.get('search_type')}")
    print(f"Ticker: {result.get('ticker')}")
    print(f"Result Count: {result.get('result_count')}")
    print(f"Performed At: {result.get('performed_at')}")

    if result.get('error'):
        print(f"\n❌ ERROR: {result['error']}")
        return

    print()
    print("=" * 80)
    print("SEARCH RESULTS DETAILS:")
    print("=" * 80)
    print()

    # Print each result in detail
    for i, r in enumerate(result.get('results', []), 1):
        print(f"RESULT #{i}")
        print(f"  Title: {r.get('title', 'N/A')}")
        print(f"  URL: {r.get('url', 'N/A')}")
        print(f"  Content/Snippet:")
        content = r.get('content', 'N/A')
        # Print content with indentation
        for line in content.split('\n'):
            print(f"    {line}")
        print(f"  Score: {r.get('score', 'N/A')}")
        print()

    print("=" * 80)
    print("WHAT WOULD BE SENT TO COUNCIL:")
    print("=" * 80)
    print()

    # Show what the council would receive
    from backend.search import format_search_results_for_prompt
    formatted = format_search_results_for_prompt(result)
    print(formatted)
    print()
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_ticker_search())
