#!/usr/bin/env python3
"""
Perplexity PDF dump utility.

Flow:
1) Run Perplexity retrieval.
2) Keep ASX / MarketIndex / Intelligent Investor sources within last 12 months.
3) Resolve PDF URLs.
4) Decode full PDF text (no excerpt truncation).
5) Dump one markdown file per PDF + manifest/index.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from dotenv import load_dotenv

from backend.pdf_processor import extract_text_from_pdf
from backend.research.providers.perplexity import PerplexityResearchProvider


ALLOWED_DOMAIN_SUFFIXES = (
    "asx.com.au",
    "marketindex.com.au",
    "intelligentinvestor.com.au",
)


def _parse_iso_date(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        try:
            return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _is_allowed_domain(url: str) -> bool:
    host = urlparse(str(url or "")).netloc.lower()
    if not host:
        return False
    for suffix in ALLOWED_DOMAIN_SUFFIXES:
        if host == suffix or host.endswith(f".{suffix}"):
            return True
    return False


def _looks_like_pdf_url(url: str) -> bool:
    lower = str(url or "").lower()
    return lower.endswith(".pdf") or ".pdf?" in lower or "/asxpdf/" in lower


def _slugify(value: str, max_len: int = 80) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    if not text:
        return "document"
    return text[:max_len].strip("_") or "document"


def _parse_date_from_pdf_url(url: str) -> Optional[datetime]:
    text = str(url or "").strip()
    if not text:
        return None
    asx_match = re.search(r"/asxpdf/(\d{8})/", text, flags=re.IGNORECASE)
    if asx_match:
        stamp = asx_match.group(1)
        try:
            return datetime.strptime(stamp, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


async def _extract_pdf_urls_from_page(
    client: httpx.AsyncClient,
    source_url: str,
) -> List[str]:
    source_url = str(source_url or "").strip()
    if not source_url:
        return []
    if _looks_like_pdf_url(source_url):
        return [source_url]

    try:
        response = await client.get(source_url)
    except Exception:
        return []
    if response.status_code >= 400:
        return []

    html = str(response.text or "")
    if not html.strip():
        return []

    found: List[str] = []
    found_norm = set()

    def _add(candidate: str) -> None:
        resolved = urljoin(source_url, str(candidate or "").strip())
        if not resolved:
            return
        norm = resolved.strip()
        if not norm or norm in found_norm:
            return
        found_norm.add(norm)
        found.append(norm)

    # ASX display pages often embed a hidden pdfURL.
    hidden = re.search(r'(?is)name="pdfURL"\s+value="([^"]+)"', html)
    if hidden:
        _add(hidden.group(1).strip())

    # Any direct ASX pdf link in page.
    asxpdf_matches = re.findall(
        r"(https://announcements\.asx\.com\.au/asxpdf/[^\s\"']+\.pdf)",
        html,
        flags=re.IGNORECASE,
    )
    for item in asxpdf_matches:
        _add(item.strip())

    # Generic href PDF extraction.
    hrefs = re.findall(r'(?is)href=["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']', html)
    for href in hrefs:
        _add(href.strip())

    scored: List[tuple[float, str]] = []
    for resolved in found:
        if not resolved:
            continue
        score = 0.0
        low = resolved.lower()
        if "announcements.asx.com.au/asxpdf/" in low:
            score += 4.0
        if _is_allowed_domain(resolved):
            score += 1.0
        if "/pdf/" in low or low.endswith(".pdf"):
            score += 0.8
        scored.append((score, resolved))

    if not scored:
        return []
    scored.sort(key=lambda row: row[0], reverse=True)
    return [row[1] for row in scored]


async def _decode_pdf_to_text(
    client: httpx.AsyncClient,
    pdf_url: str,
) -> Dict[str, Any]:
    try:
        response = await client.get(pdf_url)
    except Exception as exc:
        return {"ok": False, "error": f"download_failed: {exc}"}

    if response.status_code >= 400:
        return {"ok": False, "error": f"http_{response.status_code}"}

    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        extracted = await extract_text_from_pdf(tmp_path)
        error = str(extracted.get("error", "")).strip()
        text = str(extracted.get("text", "")).strip()
        metadata = extracted.get("metadata", {}) or {}
        if error:
            return {"ok": False, "error": error}
        if not text:
            return {"ok": False, "error": "empty_decoded_text"}
        return {
            "ok": True,
            "text": text,
            "page_count": int(metadata.get("page_count", 0) or 0),
            "title": str(metadata.get("title", "")).strip(),
            "content_type": str(response.headers.get("content-type", "")),
            "bytes": len(response.content),
        }
    finally:
        if tmp_path:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass


def _write_dump_markdown(path: Path, row: Dict[str, Any], decoded: Dict[str, Any]) -> None:
    lines = [
        f"# PDF Dump: {row.get('title', 'Untitled')}",
        "",
        f"- source_url: {row.get('source_url', '')}",
        f"- pdf_url: {row.get('pdf_url', '')}",
        f"- domain: {row.get('domain', '')}",
        f"- published_at: {row.get('published_at', '')}",
        f"- score: {row.get('score', 0.0)}",
        f"- downloaded_at_utc: {datetime.now(timezone.utc).isoformat()}",
        f"- page_count: {decoded.get('page_count', 0)}",
        f"- decoded_chars: {len(str(decoded.get('text', '')))}",
        f"- content_type: {decoded.get('content_type', '')}",
        "",
        "---",
        "",
        "## Full Decoded Text",
        "",
        str(decoded.get("text", "")),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Perplexity recent-PDF full-text dump")
    parser.add_argument("--query", required=True, help="User query for Perplexity retrieval")
    parser.add_argument("--ticker", default="", help="Ticker like ASX:WWI")
    parser.add_argument("--depth", default="deep", choices=["basic", "deep"], help="Research depth")
    parser.add_argument("--max-sources", type=int, default=30, help="Perplexity retrieval source window")
    parser.add_argument("--top", type=int, default=10, help="Number of PDFs to dump")
    parser.add_argument("--lookback-days", type=int, default=365, help="Recency window")
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional output dir. Default: outputs/pdf_dump/<timestamp>_<ticker>",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    load_dotenv()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max(1, int(args.lookback_days)))

    provider = PerplexityResearchProvider()
    result = await provider.gather(
        user_query=str(args.query),
        ticker=str(args.ticker or ""),
        depth=str(args.depth),
        max_sources=max(1, int(args.max_sources)),
        research_brief="",
    )

    sources = list(result.get("results", []) or [])
    if not sources:
        print("No retrieval sources returned by Perplexity.")
        return 1

    candidate_rows: List[Dict[str, Any]] = []
    for source in sources:
        source_url = str(source.get("url", "")).strip()
        if not source_url or not _is_allowed_domain(source_url):
            continue
        published_at = str(source.get("published_at", "")).strip()
        parsed = _parse_iso_date(published_at)
        candidate_rows.append(
            {
                "source_url": source_url,
                "title": str(source.get("title", "")).strip() or "Untitled",
                "published_at": published_at,
                "published_dt": parsed,
                "score": float(source.get("score", 0.0) or 0.0),
                "domain": urlparse(source_url).netloc.lower(),
            }
        )

    if not candidate_rows:
        print("No allowed-domain sources in lookback window.")
        return 1

    candidate_rows.sort(
        key=lambda row: (
            row["published_dt"] or datetime(1970, 1, 1, tzinfo=timezone.utc),
            row["score"],
        ),
        reverse=True,
    )

    ticker_slug = _slugify(args.ticker or "noticker", max_len=24)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.output_dir) if args.output_dir else Path("outputs") / "pdf_dump" / f"{ts}_{ticker_slug}"
    out_dir.mkdir(parents=True, exist_ok=True)

    selected: List[Dict[str, Any]] = []
    target_files = max(1, int(args.top))

    timeout = httpx.Timeout(45.0, connect=20.0, read=45.0, write=20.0)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        )
    }
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        pdf_pool: List[Dict[str, Any]] = []
        seen_pool_urls = set()
        for row in candidate_rows:
            discovered = await _extract_pdf_urls_from_page(client, row["source_url"])
            for resolved_pdf in discovered:
                if resolved_pdf in seen_pool_urls:
                    continue
                seen_pool_urls.add(resolved_pdf)
                published_dt = _parse_date_from_pdf_url(resolved_pdf) or row.get("published_dt")
                if not published_dt:
                    continue
                if published_dt < cutoff:
                    continue
                published_at = published_dt.strftime("%Y-%m-%d")
                pdf_pool.append(
                    {
                        **row,
                        "pdf_url": resolved_pdf,
                        "published_dt": published_dt,
                        "published_at": published_at,
                    }
                )

        if not pdf_pool:
            print("No resolvable PDF URLs found from filtered candidates.")
            return 1

        pdf_pool.sort(
            key=lambda row: (row["published_dt"], row["score"]),
            reverse=True,
        )

        seen_selected = set()
        selection_budget = max(target_files, target_files * 3)
        for row in pdf_pool:
            if len(selected) >= selection_budget:
                break
            resolved_pdf = row.get("pdf_url")
            if not resolved_pdf or resolved_pdf in seen_selected:
                continue
            seen_selected.add(resolved_pdf)
            selected.append(row)

        if not selected:
            print("No resolvable PDF URLs found from filtered candidates.")
            return 1

        written: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []

        for idx, row in enumerate(selected, start=1):
            if len(written) >= target_files:
                break
            decoded = await _decode_pdf_to_text(client, row["pdf_url"])
            if not decoded.get("ok"):
                failed.append(
                    {
                        "index": idx,
                        "title": row["title"],
                        "source_url": row["source_url"],
                        "pdf_url": row["pdf_url"],
                        "error": decoded.get("error", "decode_failed"),
                    }
                )
                continue

            date_part = row["published_dt"].strftime("%Y-%m-%d")
            name = _slugify(row["title"], max_len=72)
            file_index = len(written) + 1
            file_name = f"{file_index:02d}_{date_part}_{name}.md"
            file_path = out_dir / file_name
            _write_dump_markdown(file_path, row, decoded)

            written.append(
                {
                    "index": file_index,
                    "file": str(file_path),
                    "title": row["title"],
                    "source_url": row["source_url"],
                    "pdf_url": row["pdf_url"],
                    "published_at": row["published_at"],
                    "score": row["score"],
                    "decoded_chars": len(str(decoded.get("text", ""))),
                    "page_count": int(decoded.get("page_count", 0) or 0),
                }
            )

    manifest = {
        "query": args.query,
        "ticker": args.ticker,
        "depth": args.depth,
        "lookback_days": int(args.lookback_days),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "allowed_domains": list(ALLOWED_DOMAIN_SUFFIXES),
        "retrieved_sources": len(sources),
        "target_files": target_files,
        "candidate_sources_considered": len(candidate_rows),
        "candidate_pdfs_in_window": len(selected),
        "selected_pdf_candidates": len(selected),
        "written_files": written,
        "failed_files": failed,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    index_lines = [
        "# PDF Dump Index",
        "",
        f"- query: {args.query}",
        f"- ticker: {args.ticker}",
        f"- depth: {args.depth}",
        f"- lookback_days: {args.lookback_days}",
        f"- retrieved_sources: {len(sources)}",
        f"- candidate_sources_considered: {len(candidate_rows)}",
        f"- selected_pdf_candidates: {len(selected)}",
        f"- written_files: {len(written)}",
        f"- failed_files: {len(failed)}",
        "",
        "## Written Files",
        "",
    ]
    for row in written:
        index_lines.append(
            f"- {row['index']:02d}. `{Path(row['file']).name}` | {row['published_at']} | chars={row['decoded_chars']} | pages={row['page_count']} | {row['pdf_url']}"
        )
    if failed:
        index_lines.extend(["", "## Failures", ""])
        for row in failed:
            index_lines.append(
                f"- {row['index']:02d}. {row['title']} | {row['error']} | {row['pdf_url']}"
            )
    (out_dir / "index.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")

    print(f"Output directory: {out_dir}")
    print(f"Retrieved sources: {len(sources)}")
    print(f"Candidate sources considered: {len(candidate_rows)}")
    print(f"Selected PDF candidates: {len(selected)}")
    print(f"Written markdown dumps: {len(written)}")
    print(f"Failed decodes: {len(failed)}")
    print(f"Manifest: {out_dir / 'manifest.json'}")
    print(f"Index: {out_dir / 'index.md'}")
    return 0 if written else 1


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
