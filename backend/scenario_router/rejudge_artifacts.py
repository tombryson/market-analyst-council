from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .action_judge import ActionJudge
from .artifact_replay import _coerce_baseline, _coerce_facts
from .lab_scribe import SCENARIO_ROUTER_EVENTS_DIR
from .model_thesis_judge import ModelAnnouncementThesisJudge
from .thesis_comparator import ThesisComparator


def needs_model_rejudge(payload: Dict[str, Any]) -> bool:
    """Return true for legacy artifacts that lack a valid model thesis judgement."""
    if not isinstance(payload, dict):
        return False
    facts = payload.get("announcement_facts") if isinstance(payload.get("announcement_facts"), dict) else {}
    baseline = payload.get("baseline_run") if isinstance(payload.get("baseline_run"), dict) else {}
    if not facts or not baseline:
        return False
    judgement = facts.get("model_judgement") if isinstance(facts.get("model_judgement"), dict) else {}
    return str(judgement.get("status") or "").strip().lower() != "valid"


def iter_router_artifacts(base_dir: Path) -> Iterable[Path]:
    base = Path(base_dir)
    if not base.exists():
        return []
    return (
        path
        for path in base.rglob("*.json")
        if path.name != "latest.json"
        and "/by_run/" not in path.as_posix()
        and "/reviews/" not in path.as_posix()
        and "/dedupe/" not in path.as_posix()
    )


async def rejudge_payload(payload: Dict[str, Any], judge: ModelAnnouncementThesisJudge | None = None) -> Dict[str, Any]:
    facts_payload = payload.get("announcement_facts") if isinstance(payload.get("announcement_facts"), dict) else {}
    baseline_payload = payload.get("baseline_run") if isinstance(payload.get("baseline_run"), dict) else {}
    facts = _coerce_facts(facts_payload)
    baseline = _coerce_baseline(baseline_payload)
    interpreted = await (judge or ModelAnnouncementThesisJudge()).interpret(facts, baseline)
    report = ThesisComparator().compare(interpreted, baseline)
    action = ActionJudge().judge(report)
    updated = dict(payload)
    updated["announcement_facts"] = interpreted.to_dict()
    updated["comparison_report"] = report.to_dict()
    updated["action_decision"] = action.to_dict()
    updated["rejudged_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    updated["rejudged_with"] = "model_thesis_judge"
    return updated


async def scan_and_rejudge(base_dir: Path, *, write: bool = False, limit: int = 0, force: bool = False) -> Dict[str, Any]:
    paths = list(iter_router_artifacts(base_dir))
    candidates: List[Path] = []
    rewritten: List[str] = []
    errors: List[Dict[str, str]] = []

    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append({"path": str(path), "error": f"read_error:{type(exc).__name__}"})
            continue
        if force or needs_model_rejudge(payload):
            candidates.append(path)
        if limit and len(candidates) >= limit:
            break

    if write:
        judge = ModelAnnouncementThesisJudge()
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                updated = await rejudge_payload(payload, judge)
                path.write_text(json.dumps(updated, indent=2, ensure_ascii=True), encoding="utf-8")
                rewritten.append(str(path))
            except Exception as exc:
                errors.append({"path": str(path), "error": f"rejudge_error:{type(exc).__name__}"})

    return {
        "status": "ok",
        "base_dir": str(base_dir),
        "scanned": len(paths),
        "candidates": len(candidates),
        "candidate_paths": [str(path) for path in candidates[:50]],
        "write": bool(write),
        "force": bool(force),
        "rewritten": len(rewritten),
        "rewritten_paths": rewritten[:50],
        "errors": errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Find or rejudge legacy scenario-router artifacts with the model thesis judge.")
    parser.add_argument("--base-dir", default=str(SCENARIO_ROUTER_EVENTS_DIR), help="Scenario router events directory.")
    parser.add_argument("--write", action="store_true", help="Rewrite candidate artifacts after model rejudgement.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum candidate artifacts to process.")
    parser.add_argument("--force", action="store_true", help="Rejudge every router artifact, including valid existing model judgements.")
    args = parser.parse_args()
    result = asyncio.run(
        scan_and_rejudge(
            Path(args.base_dir),
            write=bool(args.write),
            limit=max(0, int(args.limit or 0)),
            force=bool(args.force),
        )
    )
    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
