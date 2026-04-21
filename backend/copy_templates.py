from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class CopyTemplateSource:
    kind: str
    label: str
    directory: Path
    pattern: str = "*.md"


SOURCES = (
    CopyTemplateSource(
        kind="enrichment_retrieval",
        label="Enrichment Retrieval Prompt",
        directory=PROJECT_ROOT / "docs" / "perplexity-enrichment-prompts" / "manual",
    ),
    CopyTemplateSource(
        kind="enrichment_extraction",
        label="Enrichment Extraction Prompt",
        directory=PROJECT_ROOT / "docs" / "perplexity-enrichment-prompts" / "manual" / "extraction",
    ),
    CopyTemplateSource(
        kind="analysis_yaml",
        label="Analysis YAML Template",
        directory=PROJECT_ROOT / "backend" / "templates",
        pattern="*.yaml",
    ),
)


def list_copy_templates() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for source in SOURCES:
        for path in _source_files(source):
            meta = _metadata_for_path(source, path)
            rows.append(
                {
                    "id": _template_id(source, path),
                    "kind": source.kind,
                    "kind_label": source.label,
                    "name": meta["name"],
                    "description": meta["description"],
                    "filename": path.name,
                    "relative_path": str(path.relative_to(PROJECT_ROOT)),
                }
            )
    return sorted(rows, key=lambda item: (str(item["kind"]), str(item["name"]).lower()))


def get_copy_template(template_id: str) -> Dict[str, Any]:
    wanted = str(template_id or "").strip()
    for source in SOURCES:
        for path in _source_files(source):
            if _template_id(source, path) != wanted:
                continue
            meta = _metadata_for_path(source, path)
            return {
                "id": wanted,
                "kind": source.kind,
                "kind_label": source.label,
                "name": meta["name"],
                "description": meta["description"],
                "filename": path.name,
                "relative_path": str(path.relative_to(PROJECT_ROOT)),
                "content": path.read_text(encoding="utf-8"),
            }
    raise KeyError(wanted)


def _source_files(source: CopyTemplateSource) -> List[Path]:
    if not source.directory.exists() or not source.directory.is_dir():
        return []
    ignored = {"README.md", "MIGRATION_TRACKER.md"}
    return [
        path
        for path in sorted(source.directory.glob(source.pattern))
        if path.is_file() and path.name not in ignored
    ]


def _template_id(source: CopyTemplateSource, path: Path) -> str:
    return f"{source.kind}:{path.stem}"


def _metadata_for_path(source: CopyTemplateSource, path: Path) -> Dict[str, str]:
    if source.kind == "analysis_yaml":
        return _yaml_metadata(path)
    return {
        "name": _titleize_slug(path.stem),
        "description": "Manual copy/paste prompt for external research or extraction UI use.",
    }


def _yaml_metadata(path: Path) -> Dict[str, str]:
    try:
        import yaml

        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        payload = {}
    name = str(payload.get("name") or payload.get("template_id") or payload.get("id") or path.stem).strip()
    description = str(payload.get("description") or "Structured council analysis template YAML.").strip()
    return {
        "name": name or _titleize_slug(path.stem),
        "description": description,
    }


def _titleize_slug(value: str) -> str:
    return " ".join(part.capitalize() for part in str(value or "").replace("_", "-").split("-") if part)
