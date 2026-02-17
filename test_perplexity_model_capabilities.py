"""Quick Perplexity model capability preflight for configured Stage 1 models."""

import argparse
import asyncio
import json
from typing import Dict, List

import httpx

from backend.config import (
    PERPLEXITY_API_KEY,
    PERPLEXITY_API_URL,
    PERPLEXITY_COUNCIL_MODELS,
)


def _parse_models(raw: str) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


async def _probe_model(model: str, timeout: float) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "input": "Reply with exactly OK.",
        "max_output_tokens": 32,
    }
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(PERPLEXITY_API_URL, headers=headers, json=payload)
        status_code = response.status_code
        body = response.text[:300].replace("\n", " ")
        if status_code == 200:
            return {
                "model": model,
                "status": "ok",
                "http_status": str(status_code),
                "details": "supported",
            }
        return {
            "model": model,
            "status": "fail",
            "http_status": str(status_code),
            "details": body or "request_failed",
        }
    except Exception as exc:
        return {
            "model": model,
            "status": "error",
            "http_status": "",
            "details": f"{type(exc).__name__}: {exc}",
        }


async def _run(models: List[str], timeout: float) -> int:
    if not PERPLEXITY_API_KEY:
        print("PERPLEXITY_API_KEY is missing.")
        return 2
    if not models:
        print("No models configured/provided.")
        return 2

    tasks = [_probe_model(model, timeout=timeout) for model in models]
    results = await asyncio.gather(*tasks)

    print("Perplexity model capability preflight")
    print("=" * 80)
    for row in results:
        print(
            f"{row['model']}: status={row['status']} "
            f"http={row['http_status'] or '-'} "
            f"details={row['details']}"
        )
    print("=" * 80)
    print(json.dumps(results, indent=2))

    failed = [row for row in results if row["status"] != "ok"]
    return 1 if failed else 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe Perplexity Responses API support for model IDs.",
    )
    parser.add_argument(
        "--models",
        type=str,
        default="",
        help="Comma-separated model IDs. Defaults to PERPLEXITY_COUNCIL_MODELS.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Request timeout per model in seconds.",
    )
    return parser


if __name__ == "__main__":
    parser = _build_parser()
    args = parser.parse_args()
    models = _parse_models(args.models) if args.models else list(PERPLEXITY_COUNCIL_MODELS or [])
    raise SystemExit(asyncio.run(_run(models, timeout=args.timeout)))
