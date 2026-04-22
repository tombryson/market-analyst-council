#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.template_loader import get_template_loader, resolve_template_selection


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render the copy/paste analysis prompt for a template/company."
    )
    parser.add_argument("--query", default="", help="User query or company description to help template resolution")
    parser.add_argument("--ticker", default="", help="Ticker, e.g. ASX:BTR")
    parser.add_argument("--company", default="", help="Explicit company name override")
    parser.add_argument("--exchange", default="", help="Explicit exchange override")
    parser.add_argument("--template-id", default="", help="Explicit template id override")
    parser.add_argument(
        "--core-rubric-only",
        action="store_true",
        help="Render only the core rubric instead of the full copy/paste wrapper.",
    )
    args = parser.parse_args()

    with contextlib.redirect_stdout(io.StringIO()):
        selection = resolve_template_selection(
            user_query=args.query or args.company or args.ticker,
            ticker=args.ticker or None,
            explicit_template_id=args.template_id or None,
            exchange=args.exchange or None,
        )
        loader = get_template_loader()

    template_id = selection["template_id"]
    company_name = args.company or selection.get("company_name") or "[COMPANY_NAME]"
    exchange = args.exchange or selection.get("exchange") or "ASX"
    company_type = selection.get("company_type") or template_id

    if args.core_rubric_only:
        prompt = loader.render_copy_paste_rubric(
            template_id,
            company_name=company_name,
            exchange=exchange,
        ).strip()
    else:
        prompt = loader.get_copy_paste_research_brief(
            template_id,
            company_type=company_type,
            exchange=exchange,
            company_name=company_name,
            include_rubric=True,
        ).strip()

    if not prompt:
        raise SystemExit(f"No prompt found for template '{template_id}'")

    print(prompt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
