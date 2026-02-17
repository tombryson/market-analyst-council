#!/usr/bin/env python3
"""
Probe Intelligent Investor announcements page for explicit price-sensitive markers.
"""

from __future__ import annotations

import argparse
import json
import re
from html import unescape
from typing import Any, Dict, List, Optional

import httpx


def _clean_html_fragment(value: str) -> str:
    text = re.sub(r"(?is)<[^>]+>", " ", str(value or ""))
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_popover_blocks(html: str) -> Dict[str, Dict[str, Any]]:
    blocks: Dict[str, Dict[str, Any]] = {}
    for match in re.finditer(r'id="summary_popover_content_(\d+)"', html):
        pop_id = match.group(1)
        start = match.start()
        next_match = re.search(r'id="summary_popover_content_\d+"', html[match.end() :])
        if next_match:
            end = match.end() + next_match.start()
        else:
            end = min(len(html), start + 12000)
        block = html[start:end]
        is_price_sensitive = "Price-sensitive ASX Announcement" in block
        key_points_raw = re.findall(r"(?is)<li[^>]*class=\"mt-2\"[^>]*>(.*?)</li>", block)
        key_points = [_clean_html_fragment(item) for item in key_points_raw if _clean_html_fragment(item)]
        blocks[pop_id] = {
            "is_price_sensitive": bool(is_price_sensitive),
            "key_points": key_points,
        }
    return blocks


def _extract_date_from_row(row_html: str) -> str:
    cells = re.findall(r'(?is)<td class="text-left">(.*?)</td>', row_html)
    for cell in cells:
        text = _clean_html_fragment(cell)
        if re.search(r"\b\d{1,2}\s+[A-Za-z]{3}\s+20\d{2}\b", text):
            return text
    return ""


def _extract_title_for_pdf(row_html: str, pdf_url: str) -> str:
    escaped = re.escape(pdf_url)
    matches = re.findall(rf'(?is)<a[^>]+href="{escaped}"[^>]*>(.*?)</a>', row_html)
    best = ""
    for item in matches:
        text = _clean_html_fragment(item)
        if len(text) > len(best):
            best = text
    return best


def _extract_announcements(html: str) -> List[Dict[str, Any]]:
    popovers = _extract_popover_blocks(html)
    rows = re.findall(r"(?is)<tr>.*?</tr>", html)
    items: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        pdf_urls = re.findall(
            r"https://www\.aspecthuntley\.com\.au/asxdata/\d{8}/pdf/\d+\.pdf",
            row,
            flags=re.IGNORECASE,
        )
        if not pdf_urls:
            continue
        pdf_url = str(pdf_urls[0]).strip()
        if not pdf_url:
            continue

        summary_match = re.search(r'id="summary_popover_(\d+)"', row)
        summary_id = summary_match.group(1) if summary_match else ""
        pop = popovers.get(summary_id, {})

        item = items.get(pdf_url, {})
        item.update(
            {
                "pdf_url": pdf_url,
                "title": _extract_title_for_pdf(row, pdf_url),
                "published_at_raw": _extract_date_from_row(row),
                "summary_popover_id": summary_id,
                "has_summary_popover": bool(summary_id),
                "ii_price_sensitive_marker": bool(pop.get("is_price_sensitive", False)),
                "ii_key_points": list(pop.get("key_points", []) or []),
            }
        )
        items[pdf_url] = item

    return list(items.values())


async def _fetch(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        )
    }
    timeout = httpx.Timeout(45.0, connect=20.0, read=45.0, write=20.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        response = await client.get(url)
        response.raise_for_status()
        return str(response.text or "")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Probe II price-sensitive markers")
    parser.add_argument("--url", required=True, help="Intelligent Investor announcements URL")
    parser.add_argument("--output-json", default="", help="Optional output file path")
    args = parser.parse_args()

    html = await _fetch(args.url)
    rows = _extract_announcements(html)
    rows.sort(
        key=lambda row: (
            1 if row.get("ii_price_sensitive_marker") else 0,
            row.get("published_at_raw", ""),
            row.get("title", ""),
        ),
        reverse=True,
    )

    flagged = [row for row in rows if row.get("ii_price_sensitive_marker")]
    out = {
        "url": args.url,
        "total_rows_with_pdf": len(rows),
        "flagged_price_sensitive_rows": len(flagged),
        "sample_flagged": flagged[:8],
        "sample_unflagged": [row for row in rows if not row.get("ii_price_sensitive_marker")][:8],
        "rows": rows,
    }

    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"wrote: {args.output_json}")

    print(json.dumps(
        {
            "total_rows_with_pdf": out["total_rows_with_pdf"],
            "flagged_price_sensitive_rows": out["flagged_price_sensitive_rows"],
            "sample_flagged_titles": [item.get("title", "") for item in out["sample_flagged"][:5]],
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

