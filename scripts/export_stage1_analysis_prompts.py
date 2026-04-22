#!/usr/bin/env python3
"""Export manual Web UI analysis prompts as copy/paste YAML files."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.template_loader import get_template_loader  # noqa: E402

OUT_DIR = REPO_ROOT / "docs" / "stage1-analysis-prompts"
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

    copy_paste_prompt = loader.get_copy_paste_research_brief(
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
        "placeholders": {
            "company_name": COMPANY_PLACEHOLDER,
            "ticker": TICKER_PLACEHOLDER,
            "exchange": "ASX",
            "instruction": "Replace placeholders before pasting into an external model interface.",
        },
        "generation_source": {
            "generated_by": "scripts/export_stage1_analysis_prompts.py",
            "source_of_truth": "backend/templates/*.yaml plus backend/template_loader.py",
            "copy_paste_prompt_renderer": "TemplateLoader.get_copy_paste_research_brief(..., include_rubric=True)",
            "core_rubric_renderer_used_inside_prompt": "TemplateLoader.render_copy_paste_rubric(...) / fallback rubric builder",
        },
        "company_types": company_types,
        "scoring_factors": {
            "quality": list(scoring.get("quality") or []),
            "value": list(scoring.get("value") or []),
        },
        "usage": {
            "copy_this_field": "copy_paste_prompt",
            "note": "Generated manual-use copy/paste prompt.",
        },
        "copy_paste_prompt": LiteralString(copy_paste_prompt),
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
        "# Copy/Paste Analysis Prompt YAMLs",
        "",
        "Generated from the live `llm-council` template loader, not hand-written copies.",
        "",
        "Use the `copy_paste_prompt` field for external model interfaces. If a template defines `copy_paste_rubric`, that copy/paste-specific rubric is used.",
        "",
        "Regenerate with:",
        "",
        "```bash",
        "python3 scripts/export_stage1_analysis_prompts.py",
        "```",
        "",
        "## Files",
        "",
    ]
    for path in written:
        index_lines.append(f"- `{path.name}`")
    (OUT_DIR / "README.md").write_text("\n".join(index_lines) + "\n")

    print(f"Exported {len(written)} Stage 1 analysis prompt YAML files to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
