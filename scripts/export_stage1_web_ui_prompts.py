#!/usr/bin/env python3
"""Export runtime Stage 1 template prompts as copy/paste Web UI YAML files."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.template_loader import get_template_loader  # noqa: E402

OUT_DIR = REPO_ROOT / "docs" / "stage1-web-ui-prompts"
TEMPLATES_DIR = REPO_ROOT / "backend" / "templates"
COMPANY_PLACEHOLDER = "[COMPANY_NAME]"
TICKER_PLACEHOLDER = "[EXCHANGE_CODE]:[TICKER]"
DEFAULT_EXCHANGE = "asx"


class LiteralString(str):
    pass


def _literal_presenter(dumper: yaml.Dumper, data: LiteralString):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(LiteralString, _literal_presenter, Dumper=yaml.SafeDumper)


def _template_source_paths() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for path in sorted(TEMPLATES_DIR.glob("*.yaml")):
        data = yaml.safe_load(path.read_text()) or {}
        template_id = str(data.get("id") or "").strip()
        if template_id:
            out[template_id] = str(path.relative_to(REPO_ROOT))
    return out


def _export_payload(template: Dict[str, Any], source_path: str) -> Dict[str, Any]:
    loader = get_template_loader()
    template_id = str(template.get("id") or "").strip()
    name = str(template.get("name") or template_id).strip()
    category = str(template.get("category") or "general").strip()
    company_types = list(template.get("company_types") or [])
    behavior = template.get("template_behavior") or {}
    scoring = behavior.get("stage3_scoring_factors") or {}

    stage1_query_prompt = loader.render_template_rubric(
        template_id,
        company_name=COMPANY_PLACEHOLDER,
        exchange=DEFAULT_EXCHANGE,
    )
    combined_web_ui_prompt = loader.get_stage1_research_brief(
        template_id,
        company_type=company_types[0] if company_types else template_id,
        exchange=DEFAULT_EXCHANGE.upper(),
        company_name=COMPANY_PLACEHOLDER,
        include_rubric=True,
    )

    return {
        "template_id": template_id,
        "template_name": name,
        "category": category,
        "source_yaml": source_path,
        "generated_from_runtime": {
            "stage1_query_prompt": "TemplateLoader.render_template_rubric(...) used by structured Stage 1 query assembly",
            "combined_web_ui_prompt": "TemplateLoader.get_stage1_research_brief(..., include_rubric=True) for manual Web UI use",
        },
        "placeholders": {
            "company_name": COMPANY_PLACEHOLDER,
            "ticker": TICKER_PLACEHOLDER,
            "exchange": "ASX",
            "instruction": "Replace placeholders before pasting into ChatGPT, Claude, Gemini, Perplexity, Manus, or another Web UI.",
        },
        "company_types": company_types,
        "scoring_factors": {
            "quality": list(scoring.get("quality") or []),
            "value": list(scoring.get("value") or []),
        },
        "web_ui_usage": {
            "recommended_block_to_copy": "combined_web_ui_prompt",
            "stage1_query_prompt_note": "This is the exact core prompt sent as the structured Stage 1 query in llm-council. Use it when another UI already has its own retrieval/source wrapper.",
            "combined_web_ui_prompt_note": "This wraps the core rubric with the same Stage 1 framing, exchange assumptions, governance lane, and sector research lane.",
        },
        "copy_paste_prompt": LiteralString(combined_web_ui_prompt),
        "stage1_query_prompt": LiteralString(stage1_query_prompt),
        "combined_web_ui_prompt": LiteralString(combined_web_ui_prompt),
    }


def main() -> int:
    loader = get_template_loader()
    source_paths = _template_source_paths()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    old_files = list(OUT_DIR.glob("*.yaml"))
    for old in old_files:
        old.unlink()

    templates = loader.list_templates()
    written = []
    for template in templates:
        template_id = str(template.get("id") or "").strip()
        if not template_id:
            continue
        full_template = loader.get_template(template_id) or template
        payload = _export_payload(full_template, source_paths.get(template_id, ""))
        out_path = OUT_DIR / f"{template_id}.yaml"
        out_path.write_text(
            yaml.safe_dump(
                payload,
                sort_keys=False,
                allow_unicode=True,
                width=120,
                default_flow_style=False,
            )
        )
        written.append(out_path)

    index_lines = [
        "# Stage 1 Web UI Prompt YAMLs",
        "",
        "Generated from the live `llm-council` YAML template loader, not hand-written copies.",
        "",
        "Use `combined_web_ui_prompt` for manual Web UI runs. Use `stage1_query_prompt` only when the target UI already supplies its own retrieval/source wrapper.",
        "",
        "Regenerate with:",
        "",
        "```bash",
        "python3 scripts/export_stage1_web_ui_prompts.py",
        "```",
        "",
        "## Files",
        "",
    ]
    for path in written:
        index_lines.append(f"- `{path.name}`")
    (OUT_DIR / "README.md").write_text("\n".join(index_lines) + "\n")

    print(f"Exported {len(written)} Stage 1 Web UI prompt YAML files to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
