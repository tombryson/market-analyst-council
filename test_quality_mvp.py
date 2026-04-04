"""End-to-end MVP test for out-of-100 financial quality scoring.

Runs:
1) Perplexity-emulated Stage 1 deep research (with decoding if enabled)
2) Stage 2 peer ranking
3) Stage 3 structured synthesis using financial_quality_mvp template
"""

import argparse
import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

from backend.prepass_utils import (
    normalize_retrieval_query_seed as _normalize_retrieval_query_seed,
)
from backend.prepass_utils import tail_text as _tail_text


def _progress(message: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}][test_quality_mvp] {message}", flush=True)


def _ensure_pymupdf_runtime() -> None:
    """Re-exec with uv-managed environment if current Python cannot import PyMuPDF."""
    if os.environ.get("LLM_COUNCIL_NO_REEXEC") == "1":
        return

    try:
        import pymupdf  # noqa: F401
        return
    except Exception as e:
        uv = shutil.which("uv")
        if uv:
            print(
                "PyMuPDF unavailable in current interpreter; re-running with uv environment. "
                f"Reason: {type(e).__name__}: {e}"
            )
            os.environ["LLM_COUNCIL_NO_REEXEC"] = "1"
            os.execvpe(uv, [uv, "run", "python", *sys.argv], os.environ)
        print(
            "PyMuPDF unavailable and uv was not found. "
            "Run with: uv run python test_quality_mvp.py ..."
        )
        sys.exit(1)


def _sanitize_ticker_for_dir(ticker: str) -> str:
    text = str(ticker or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_") or "unknown"


SUPPLEMENTARY_CONTEXT_MAX_CHARS = 12000
SUPPLEMENTARY_CONTEXT_ALLOWED_EXTENSIONS = {".pdf", ".md", ".txt", ".json"}


async def _load_supplementary_context(path_value: str) -> str:
    path = Path(str(path_value or "").strip())
    if not path.exists() or not path.is_file():
        raise RuntimeError(f"Supplementary context file not found: {path}")

    suffix = path.suffix.lower()
    if suffix not in SUPPLEMENTARY_CONTEXT_ALLOWED_EXTENSIONS:
        return ""

    extracted_text = ""
    if suffix == ".pdf":
        from backend.pdf_processor import process_pdf_attachment

        processed = await process_pdf_attachment(str(path), path.name)
        if str(processed.get("status") or "").strip().lower() != "success":
            raise RuntimeError(
                f"Failed to process supplementary PDF: {path} ({processed.get('error') or 'unknown error'})"
            )
        extracted_text = str(processed.get("full_text") or processed.get("summary") or "").strip()
    else:
        try:
            extracted_text = path.read_text(encoding="utf-8").strip()
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read supplementary context file: {path} ({exc})"
            ) from exc
        if suffix == ".json" and extracted_text:
            try:
                extracted_text = json.dumps(
                    json.loads(extracted_text),
                    indent=2,
                    ensure_ascii=False,
                )
            except Exception:
                pass

    if not extracted_text:
        return ""

    bounded_text = extracted_text[:SUPPLEMENTARY_CONTEXT_MAX_CHARS].strip()
    if len(extracted_text) > SUPPLEMENTARY_CONTEXT_MAX_CHARS:
        bounded_text += "\n\n[Supplementary document truncated]"

    return (
        "SUPPLEMENTARY USER-PROVIDED DOCUMENT\n"
        "Use this as optional additional context only.\n"
        "Do not treat it as higher priority than filings, market facts, or company announcements.\n"
        f"Filename: {path.name}\n\n"
        f"{bounded_text}"
    )


def _manifest_count(value: Any) -> int:
    """Accept either a count scalar or a detailed collection in manifest fields."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        try:
            return int(float(text))
        except Exception:
            return 0
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    return 0


def _flatten_bundle_milestone(item: Any) -> str:
    if isinstance(item, dict):
        milestone = str(item.get("milestone", "")).strip()
        target_window = str(item.get("target_window", "")).strip()
        direction = str(item.get("direction", "")).strip()
        parts = [part for part in [milestone, target_window, direction] if part]
        return " | ".join(parts).strip()
    return str(item or "").strip()


def _build_source_rows_from_injection_bundle(
    bundle_path: Path,
    *,
    max_sources: int = 24,
    max_chars_per_source: int = 1600,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    docs = list(payload.get("docs", []) or [])
    docs_sorted = sorted(
        [doc for doc in docs if isinstance(doc, dict)],
        key=lambda row: (
            1 if bool(row.get("price_sensitive", False)) else 0,
            int(row.get("importance_score", 0) or 0),
            str(row.get("published_at", "")),
        ),
        reverse=True,
    )
    rows: List[Dict[str, Any]] = []
    max_sources_safe = max(1, int(max_sources))
    max_chars_safe = max(300, int(max_chars_per_source))
    for idx, doc in enumerate(docs_sorted[:max_sources_safe], 1):
        lines: List[str] = []
        one_line = str(doc.get("one_line", "")).strip()
        if one_line:
            lines.append(one_line)
        key_facts_paragraph = str(doc.get("key_facts_paragraph", "")).strip()
        if key_facts_paragraph:
            lines.append(key_facts_paragraph)
        for point in list(doc.get("key_points", []) or [])[:20]:
            text = str(point or "").strip()
            if text:
                lines.append(f"- {text}")
        for point in list(doc.get("timeline_milestones", []) or [])[:10]:
            text = _flatten_bundle_milestone(point)
            if text:
                lines.append(f"- Timeline: {text}")
        for point in list(doc.get("catalysts_next_12m", []) or [])[:8]:
            text = str(point or "").strip()
            if text:
                lines.append(f"- Catalyst: {text}")
        for point in list(doc.get("capital_structure", []) or [])[:8]:
            text = str(point or "").strip()
            if text:
                lines.append(f"- Capital: {text}")
        for point in list(doc.get("risks_headwinds", []) or [])[:8]:
            text = str(point or "").strip()
            if text:
                lines.append(f"- Risk: {text}")

        excerpt = "\n".join(lines).strip()
        if not excerpt:
            continue
        if len(excerpt) > max_chars_safe:
            excerpt = excerpt[: max_chars_safe - 3].rstrip() + "..."

        importance_score = int(doc.get("importance_score", 0) or 0)
        # Convert worker importance into the same rough signal range used by
        # second-pass material scoring.
        material_signal_score = max(0, min(8, int(round(importance_score / 12.5))))
        rows.append(
            {
                "source_id": f"S{len(rows) + 1}",
                "title": str(doc.get("title", "")).strip() or f"Bundled Source {idx}",
                "url": str(doc.get("pdf_url", "")).strip() or str(doc.get("url", "")).strip(),
                "published_at": str(doc.get("published_at", "")).strip(),
                "decode_status": "prepass_bundle",
                "decoded": True,
                "excerpt": excerpt,
                "material_signal_score": material_signal_score,
                "bundle_importance_score": importance_score,
                "bundle_price_sensitive": bool(doc.get("price_sensitive", False)),
            }
        )

    meta = {
        "bundle_path": str(bundle_path),
        "generated_at_utc": str(payload.get("generated_at_utc", "")),
        "docs_in_bundle": int(len(docs)),
        "rows_built": int(len(rows)),
        "min_importance_score": int(
            ((payload.get("injection_policy", {}) or {}).get("min_importance_score", 0) or 0)
        ),
        "kept_for_injection": int(payload.get("kept_for_injection", 0) or 0),
        "dropped_as_unimportant": int(payload.get("dropped_as_unimportant", 0) or 0),
        "dropped_below_importance_threshold": int(
            payload.get("dropped_below_importance_threshold", 0) or 0
        ),
        "selection_counts": payload.get("selection_counts", {}) or {},
        "selection_audit_high_importance_dropped_count": int(
            len(
                (
                    (payload.get("selection_audit", {}) or {}).get(
                        "high_importance_dropped",
                        [],
                    )
                    or []
                )
            )
        ),
    }
    return rows, meta


def _find_recent_bundle_path(
    *,
    repo_root: Path,
    ticker: str,
    max_age_hours: int = 24,
) -> Optional[Path]:
    ticker_norm = str(ticker or "").strip().upper()
    if not ticker_norm:
        return None
    cutoff = datetime.utcnow() - timedelta(hours=max(1, int(max_age_hours)))
    candidates: List[Tuple[datetime, Path]] = []

    roots: List[Path] = []
    prepass_root_raw = str(os.getenv("ANALYSIS_PREPASS_DIR", "")).strip()
    jobs_root_raw = str(os.getenv("ANALYSIS_JOBS_DIR", "")).strip()
    if prepass_root_raw:
        roots.append(Path(prepass_root_raw))
    if jobs_root_raw:
        roots.append(Path(jobs_root_raw) / "prepass")
    roots.append(repo_root / "outputs" / "pdf_dump")

    seen_roots = set()
    for pdf_dump_root in roots:
        try:
            resolved_root = pdf_dump_root.resolve()
        except Exception:
            resolved_root = pdf_dump_root
        root_key = str(resolved_root)
        if root_key in seen_roots or not resolved_root.exists():
            continue
        seen_roots.add(root_key)
        for manifest_path in resolved_root.glob("*/manifest.json"):
            try:
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            manifest_ticker = str(payload.get("ticker", "")).strip().upper()
            if manifest_ticker != ticker_norm:
                continue
            generated_raw = str(payload.get("generated_at_utc", "")).strip()
            if not generated_raw:
                continue
            try:
                generated = datetime.fromisoformat(generated_raw.replace("Z", "+00:00")).replace(
                    tzinfo=None
                )
            except Exception:
                continue
            if generated < cutoff:
                continue
            bundle_path_raw = str(payload.get("injection_bundle_json", "")).strip()
            if bundle_path_raw:
                bundle_path_candidate = Path(bundle_path_raw)
                bundle_path = (
                    bundle_path_candidate.resolve()
                    if bundle_path_candidate.is_absolute()
                    else (repo_root / bundle_path_candidate).resolve()
                )
            else:
                bundle_path = (manifest_path.parent / "injection_bundle.json").resolve()
            if bundle_path.exists() and bundle_path.is_file():
                candidates.append((generated, bundle_path))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _prepass_output_root(repo_root: Path) -> Path:
    prepass_root_raw = str(os.getenv("ANALYSIS_PREPASS_DIR", "")).strip()
    if prepass_root_raw:
        root = Path(prepass_root_raw)
    else:
        jobs_root_raw = str(os.getenv("ANALYSIS_JOBS_DIR", "")).strip()
        if jobs_root_raw:
            root = Path(jobs_root_raw) / "prepass"
        else:
            root = repo_root / "outputs" / "pdf_dump"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _prepare_primary_injection_bundle(
    *,
    repo_root: Path,
    ticker: str,
    company_name: str,
    query_hint: str,
    exchange: str,
    exchange_retrieval_params: Optional[Dict[str, Any]] = None,
    reuse_recent_bundle: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if reuse_recent_bundle:
        recent_bundle = _find_recent_bundle_path(
            repo_root=repo_root,
            ticker=ticker,
            max_age_hours=24,
        )
        if recent_bundle is not None:
            rows, meta = _build_source_rows_from_injection_bundle(recent_bundle)
            meta["strategy"] = "reused_recent_bundle"
            return rows, meta

    uv = shutil.which("uv")
    if not uv:
        raise RuntimeError(
            "Required bundle prepass needs `uv`, but it was not found in PATH."
        )
    output_dir = (
        _prepass_output_root(repo_root)
        / f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{_sanitize_ticker_for_dir(ticker)}_auto"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    query_seed = _normalize_retrieval_query_seed(
        company_name=company_name,
        query_hint=query_hint,
        ticker=ticker,
    )
    retrieval_query = (
        f"Latest material filings, announcements, and investor updates for {query_seed}"
    )
    retrieval_params = dict(exchange_retrieval_params or {})
    normalized_exchange = str(exchange or "").strip().lower()
    target_price_sensitive = int(retrieval_params.get("target_price_sensitive_default", 10) or 10)
    target_non_price_sensitive = int(
        retrieval_params.get("target_non_price_sensitive_default", 10) or 10
    )
    if (target_price_sensitive + target_non_price_sensitive) < 20:
        target_non_price_sensitive = max(
            target_non_price_sensitive,
            20 - max(0, target_price_sensitive),
        )
    # Canadian exchanges require a wider filing net due weaker broad-search coverage.
    if normalized_exchange in {"tsx", "tsxv", "cse"}:
        target_price_sensitive = max(target_price_sensitive, 15)
        target_non_price_sensitive = max(target_non_price_sensitive, 15)
    top_default = max(1, target_price_sensitive + target_non_price_sensitive)
    max_sources_default = int(retrieval_params.get("max_sources_default", 0) or 0)
    lookback_days_default = int(retrieval_params.get("lookback_days_default", 0) or 0)

    cmd = [
        uv,
        "run",
        "python",
        "test_perplexity_pdf_dump.py",
        "--query",
        retrieval_query,
        "--ticker",
        str(ticker),
        "--output-dir",
        str(output_dir),
        "--depth",
        "deep",
        "--top",
        str(top_default),
        "--target-price-sensitive",
        str(target_price_sensitive),
        "--target-non-price-sensitive",
        str(target_non_price_sensitive),
    ]
    if max_sources_default > 0:
        cmd.extend(["--max-sources", str(max_sources_default)])
    if lookback_days_default > 0:
        cmd.extend(["--lookback-days", str(lookback_days_default)])
    if str(exchange or "").strip():
        cmd.extend(["--exchange", str(exchange).strip().lower()])
    _progress("Primary injection prepass start (pdf dump + worker summaries)")
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )
    stdout = str(proc.stdout or "")
    stderr = str(proc.stderr or "")
    (output_dir / "prepass_subprocess.stdout.log").write_text(stdout, encoding="utf-8")
    (output_dir / "prepass_subprocess.stderr.log").write_text(stderr, encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(
            "Primary injection prepass failed. "
            f"rc={proc.returncode} "
            f"output_dir={output_dir} "
            f"stderr_tail={_tail_text(stderr)} "
            f"stdout_tail={_tail_text(stdout)}"
        )
    manifest_path = output_dir / "manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(
            f"Primary injection prepass completed but manifest not found: {manifest_path}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    bundle_path_raw = str(manifest.get("injection_bundle_json", "")).strip()
    if bundle_path_raw:
        bundle_path_candidate = Path(bundle_path_raw)
        bundle_path = (
            bundle_path_candidate.resolve()
            if bundle_path_candidate.is_absolute()
            else (repo_root / bundle_path_candidate).resolve()
        )
    else:
        bundle_path = (output_dir / "injection_bundle.json").resolve()
    if not bundle_path.exists() or not bundle_path.is_file():
        raise RuntimeError(
            f"Primary injection prepass completed but injection bundle missing: {bundle_path}"
        )
    rows, meta = _build_source_rows_from_injection_bundle(bundle_path)
    meta["strategy"] = "built_fresh_bundle"
    meta["output_dir"] = str(output_dir)
    meta["prepass_top"] = top_default
    meta["prepass_target_price_sensitive"] = target_price_sensitive
    meta["prepass_target_non_price_sensitive"] = target_non_price_sensitive
    meta["prepass_max_sources"] = max_sources_default
    meta["prepass_lookback_days"] = lookback_days_default
    meta["prepass_retrieved_sources"] = _manifest_count(
        manifest.get("retrieved_sources", 0)
    )
    meta["prepass_candidate_sources_considered"] = _manifest_count(
        manifest.get("candidate_sources_considered", 0)
    )
    meta["prepass_candidate_allowlisted_sources"] = _manifest_count(
        manifest.get("candidate_allowlisted_sources", 0)
    )
    meta["prepass_candidate_pdfs_in_window"] = _manifest_count(
        manifest.get("candidate_pdfs_in_window", 0)
    )
    meta["prepass_selected_primary_candidates"] = _manifest_count(
        manifest.get("selected_primary_candidates", 0)
    )
    meta["prepass_written_files"] = _manifest_count(manifest.get("written_files", 0))
    return rows, meta


def _print_header(args: argparse.Namespace, selection: Dict[str, Any]) -> None:
    print("=" * 90)
    print("FINANCIAL QUALITY MVP TEST")
    print("=" * 90)
    print("Query mode: template (fixed)")
    print(f"User query hint: {args.query or '(none)'}")
    print(f"Ticker: {args.ticker or '(none)'}")
    print(f"Requested Template Override: {args.template_id or '(auto)'}")
    print(f"Requested Company Type: {args.company_type or '(auto)'}")
    print(f"Requested Exchange: {args.exchange or '(auto)'}")
    print(
        "Resolved Selection: "
        f"template={selection.get('template_id')} "
        f"company={selection.get('company_name')} "
        f"company_type={selection.get('company_type')} "
        f"exchange={selection.get('exchange')} "
        f"source={selection.get('selection_source')}"
    )
    company_type_detection = selection.get("company_type_detection", {})
    if isinstance(company_type_detection, dict) and company_type_detection:
        print(
            "Company-type detection: "
            f"provider={company_type_detection.get('provider')} "
            f"status={company_type_detection.get('status')} "
            f"selected={company_type_detection.get('selected_company_type')} "
            f"candidate={company_type_detection.get('candidate_company_type')} "
            f"confidence={company_type_detection.get('confidence')}"
        )
    print("Runtime config source: .env (no CLI overrides)")
    print("-" * 90)


def _print_stage1(metadata: Dict[str, Any]) -> None:
    print("\nSTAGE 1")
    print("-" * 90)
    attempted = metadata.get("models_attempted", [])
    succeeded = metadata.get("models_succeeded", [])
    print(f"Models attempted: {len(attempted)}")
    print(f"Models succeeded: {len(succeeded)}")
    if metadata.get("stage1_execution_mode") is not None:
        print(
            "Stage1 routing: "
            f"execution_mode={metadata.get('stage1_execution_mode')} "
            f"mixed_mode={metadata.get('stage1_mixed_mode_enabled')} "
            f"perplexity_pool={metadata.get('stage1_mixed_mode_perplexity_pool')} "
            f"openrouter_pool={metadata.get('stage1_mixed_mode_openrouter_pool')}"
        )
    if metadata.get("stage1_second_pass_enabled") is not None:
        print(
            "Second-pass settings: "
            f"enabled={metadata.get('stage1_second_pass_enabled')} "
            f"max_sources={metadata.get('stage1_second_pass_max_sources')} "
            f"max_chars_per_source={metadata.get('stage1_second_pass_max_chars_per_source')} "
            f"appendix_max_sources={metadata.get('stage1_second_pass_appendix_max_sources')} "
            f"max_output_tokens={metadata.get('stage1_second_pass_max_output_tokens')} "
            f"reasoning_effort={metadata.get('stage1_second_pass_reasoning_effort')}"
        )
        prepass_supplied = metadata.get("stage1_prepass_source_rows_supplied")
        if prepass_supplied is None:
            prepass_supplied = metadata.get("stage1_source_rows_override_supplied")
        prepass_count = metadata.get("stage1_prepass_source_rows_count")
        if prepass_count is None:
            prepass_count = metadata.get("stage1_source_rows_override_count")
        if prepass_supplied is not None:
            print(
                "Second-pass prepass source rows: "
                f"supplied={prepass_supplied} "
                f"count={prepass_count}"
            )
        if metadata.get("stage1_timeline_guard_enabled") is not None:
            print(
                "Timeline guard settings: "
                f"enabled={metadata.get('stage1_timeline_guard_enabled')} "
                f"hard_fail={metadata.get('stage1_timeline_guard_hard_fail')} "
                f"digest_max_items={metadata.get('stage1_timeline_digest_max_items')}"
            )
        if metadata.get("stage1_fact_digest_v2_enabled") is not None:
            print(
                "Fact digest v2 settings: "
                f"enabled={metadata.get('stage1_fact_digest_v2_enabled')} "
                "max_facts_per_section="
                f"{metadata.get('stage1_fact_digest_v2_max_facts_per_section')} "
                "max_summary_bullets="
                f"{metadata.get('stage1_fact_digest_v2_max_summary_bullets')} "
                "max_narrative_words="
                f"{metadata.get('stage1_fact_digest_v2_max_narrative_words')}"
            )
        if metadata.get("stage1_second_pass_citation_gate_enabled") is not None:
            print(
                "Second-pass citation gate: "
                f"enabled={metadata.get('stage1_second_pass_citation_gate_enabled')} "
                f"min_count={metadata.get('stage1_second_pass_citation_min_count')} "
                "max_uncited_numeric_lines="
                f"{metadata.get('stage1_second_pass_citation_max_uncited_numeric_lines')}"
            )
        if metadata.get("stage1_second_pass_compliance_min_score") is not None:
            print(
                "Second-pass compliance thresholds: "
                f"min_score={metadata.get('stage1_second_pass_compliance_min_score')} "
                "min_rubric_coverage="
                f"{metadata.get('stage1_second_pass_compliance_min_rubric_coverage_pct')} "
                "min_numeric_citation="
                f"{metadata.get('stage1_second_pass_compliance_min_numeric_citation_pct')} "
                "catastrophic_score="
                f"{metadata.get('stage1_second_pass_compliance_catastrophic_score')}"
            )
    if metadata.get("stage1_asx_deterministic_announcements_enabled") is not None:
        print(
            "Deterministic ASX ingest: "
            f"enabled={metadata.get('stage1_asx_deterministic_announcements_enabled')} "
            f"target={metadata.get('stage1_asx_deterministic_target_announcements')} "
            f"lookback_years={metadata.get('stage1_asx_deterministic_lookback_years')} "
            "price_sensitive_only="
            f"{metadata.get('stage1_asx_deterministic_price_sensitive_only')} "
            "fill_non_sensitive="
            f"{metadata.get('stage1_asx_deterministic_include_non_sensitive_fill')} "
            f"max_decode={metadata.get('stage1_asx_deterministic_max_decode')}"
        )
    if metadata.get("stage1_shared_retrieval_requested") is not None:
        print(
            "Shared retrieval: "
            f"requested={metadata.get('stage1_shared_retrieval_requested')} "
            f"used={metadata.get('stage1_shared_retrieval_used')} "
            f"model={metadata.get('stage1_shared_retrieval_model')}"
        )
        if metadata.get("stage1_shared_retrieval_error"):
            print(f"Shared retrieval fallback reason: {metadata.get('stage1_shared_retrieval_error')}")
    if metadata.get("stage1_prepass_authoritative_mode") is not None:
        print(
            "Authoritative prepass mode: "
            f"enabled={metadata.get('stage1_prepass_authoritative_mode')} "
            f"prepass_rows={metadata.get('stage1_prepass_source_rows_count')}"
        )
    if metadata.get("stage1_openai_guardrails_enabled") is not None:
        print(
            "OpenAI pass-1 guardrails: "
            f"enabled={metadata.get('stage1_openai_guardrails_enabled')} "
            f"max_sources={metadata.get('stage1_openai_base_max_sources')} "
            f"max_steps={metadata.get('stage1_openai_base_max_steps')} "
            f"reasoning={metadata.get('stage1_openai_base_reasoning_effort')}"
        )
    if metadata.get("stage1_verification_compliance_markers") is not None:
        print(
            "Verification profile: "
            f"template={metadata.get('stage1_verification_template_id') or 'none'} "
            "digest_sections="
            f"{metadata.get('stage1_verification_digest_sections')} "
            "compliance_markers="
            f"{metadata.get('stage1_verification_compliance_markers')} "
            "critical_sections="
            f"{metadata.get('stage1_verification_critical_sections')}"
        )
        if metadata.get("stage1_verification_required_sections") is not None:
            print(
                "Verification required sections: "
                f"{metadata.get('stage1_verification_required_sections')}"
            )
    for run in metadata.get("per_model_research_runs", []):
        model = run.get("model", "unknown")
        result = run.get("result", {})
        provider_meta = result.get("provider_metadata", {}) or {}
        decode_meta = provider_meta.get("source_decoding", {}) or {}
        status = "OK" if not result.get("error") else "FAILED"
        print(f"[{status}] {model}")
        print(f"  result_count: {result.get('result_count', 0)}")
        if provider_meta.get("stage1_attempts") is not None:
            print(f"  stage1_attempts: {provider_meta.get('stage1_attempts')}")
        if decode_meta:
            print(
                "  decoded_sources: "
                f"{decode_meta.get('decoded', 0)}/{decode_meta.get('attempted', 0)}"
            )
        if provider_meta.get("stage1_second_pass_enabled") is not None:
            print(
                "  second_pass: "
                f"enabled={provider_meta.get('stage1_second_pass_enabled')} "
                f"success={provider_meta.get('stage1_second_pass_success')} "
                f"attempts={provider_meta.get('stage1_second_pass_attempts')}"
            )
            if provider_meta.get("stage1_second_pass_citation_gate_passed") is not None:
                print(
                    "  second_pass_citation_gate: "
                    f"passed={provider_meta.get('stage1_second_pass_citation_gate_passed')} "
                    f"reason={provider_meta.get('stage1_second_pass_citation_gate_reason')} "
                    f"citations={provider_meta.get('stage1_second_pass_citation_count')} "
                    "uncited_numeric_lines="
                    f"{provider_meta.get('stage1_second_pass_citation_uncited_numeric_lines')}"
                )
                if provider_meta.get("stage1_second_pass_compliance_score") is not None:
                    print(
                        "  second_pass_compliance: "
                        f"score={provider_meta.get('stage1_second_pass_compliance_score'):.3f} "
                        f"rating={provider_meta.get('stage1_second_pass_compliance_rating')} "
                        "rubric_coverage_pct="
                        f"{provider_meta.get('stage1_second_pass_rubric_coverage_pct')} "
                        "numeric_citation_pct="
                        f"{provider_meta.get('stage1_second_pass_citation_numeric_citation_pct')} "
                        "retry_recommended="
                        f"{provider_meta.get('stage1_second_pass_compliance_retry_recommended')}"
                    )
                    hard_fail_reasons = (
                        provider_meta.get("stage1_second_pass_compliance_hard_fail_reasons") or []
                    )
                    soft_fail_reasons = (
                        provider_meta.get("stage1_second_pass_compliance_soft_fail_reasons") or []
                    )
                    warning_reasons = (
                        provider_meta.get("stage1_second_pass_compliance_warning_reasons") or []
                    )
                    if hard_fail_reasons:
                        print(
                            "  second_pass_compliance_hard_fail_reasons: "
                            + " | ".join(hard_fail_reasons[:6])
                        )
                    if soft_fail_reasons:
                        print(
                            "  second_pass_compliance_soft_fail_reasons: "
                            + " | ".join(soft_fail_reasons[:6])
                        )
                    if warning_reasons:
                        print(
                            "  second_pass_compliance_warning_reasons: "
                            + " | ".join(warning_reasons[:6])
                        )
            if provider_meta.get("stage1_second_pass_timeline_guard_passed") is not None:
                print(
                    "  second_pass_timeline_guard: "
                    f"passed={provider_meta.get('stage1_second_pass_timeline_guard_passed')} "
                    f"reason={provider_meta.get('stage1_second_pass_timeline_guard_reason')} "
                    "shifted_quarters="
                    f"{provider_meta.get('stage1_second_pass_timeline_guard_shifted_quarters')} "
                    "evidence_windows="
                    f"{provider_meta.get('stage1_second_pass_timeline_guard_evidence_windows')} "
                    "response_windows="
                    f"{provider_meta.get('stage1_second_pass_timeline_guard_response_windows')}"
                )
            if provider_meta.get("stage1_second_pass_source_rows_count") is not None:
                print(
                    "  second_pass_injection: "
                    f"source_rows={provider_meta.get('stage1_second_pass_source_rows_count')} "
                    "timeline_evidence_rows="
                    f"{provider_meta.get('stage1_second_pass_timeline_evidence_count')} "
                    "timeline_digest_chars="
                    f"{provider_meta.get('stage1_second_pass_timeline_digest_chars')}"
                )
            if provider_meta.get("stage1_second_pass_fact_digest_v2_total_facts") is not None:
                print(
                    "  second_pass_fact_digest_v2: "
                    f"enabled={provider_meta.get('stage1_second_pass_fact_digest_v2_enabled')} "
                    f"facts={provider_meta.get('stage1_second_pass_fact_digest_v2_total_facts')} "
                    "sections="
                    f"{provider_meta.get('stage1_second_pass_fact_digest_v2_sections_with_facts')} "
                    "summary_bullets="
                    f"{provider_meta.get('stage1_second_pass_fact_digest_v2_summary_bullets')} "
                    "conflicts="
                    f"{provider_meta.get('stage1_second_pass_fact_digest_v2_conflicts')} "
                    "chars="
                    f"{provider_meta.get('stage1_second_pass_fact_digest_v2_chars')}"
                )
            if provider_meta.get("stage1_second_pass_verification_template_id") is not None:
                print(
                    "  second_pass_verification: "
                    f"template={provider_meta.get('stage1_second_pass_verification_template_id') or 'none'} "
                    "digest_sections="
                    f"{provider_meta.get('stage1_second_pass_verification_digest_sections')} "
                    "compliance_markers="
                    f"{provider_meta.get('stage1_second_pass_verification_compliance_markers')} "
                    "critical_sections="
                    f"{provider_meta.get('stage1_second_pass_verification_critical_sections')}"
                )
            if provider_meta.get("stage1_second_pass_prompt_chars") is not None:
                print(
                    "  second_pass_prompt_chars: "
                    f"{provider_meta.get('stage1_second_pass_prompt_chars')}"
                )
            if provider_meta.get("stage1_second_pass_response_chars") is not None:
                print(
                    "  second_pass_response_chars: "
                    f"{provider_meta.get('stage1_second_pass_response_chars')}"
                )
            if provider_meta.get("stage1_second_pass_fact_pack_chars") is not None:
                print(
                    "  second_pass_fact_pack: "
                    f"chars={provider_meta.get('stage1_second_pass_fact_pack_chars')} "
                    f"facts={provider_meta.get('stage1_second_pass_fact_pack_total_facts')} "
                    f"sections={provider_meta.get('stage1_second_pass_fact_pack_sections_with_facts')}"
                )
            if provider_meta.get("stage1_second_pass_asx_deterministic_enabled") is not None:
                print(
                    "  second_pass_asx_deterministic: "
                    f"enabled={provider_meta.get('stage1_second_pass_asx_deterministic_enabled')} "
                    f"used={provider_meta.get('stage1_second_pass_asx_deterministic_used')} "
                    f"symbol={provider_meta.get('stage1_second_pass_asx_deterministic_symbol')} "
                    f"reason={provider_meta.get('stage1_second_pass_asx_deterministic_reason')} "
                    f"cache_hit={provider_meta.get('stage1_second_pass_asx_deterministic_cache_hit')} "
                    "selected_rows="
                    f"{provider_meta.get('stage1_second_pass_asx_deterministic_selected_rows')} "
                    "decoded_rows="
                    f"{provider_meta.get('stage1_second_pass_asx_deterministic_decoded_rows')}"
                )
            if provider_meta.get("stage1_final_template_compliant") is not None:
                print(
                    "  second_pass_template_compliant: "
                    f"{provider_meta.get('stage1_final_template_compliant')} "
                    f"reason={provider_meta.get('stage1_final_template_reason')}"
                )
            if provider_meta.get("stage1_second_pass_error"):
                print(f"  second_pass_error: {provider_meta.get('stage1_second_pass_error')}")
            if provider_meta.get("stage1_second_pass_warning"):
                print(f"  second_pass_warning: {provider_meta.get('stage1_second_pass_warning')}")
        if provider_meta.get("stage1_shared_retrieval_used") is not None:
            print(
                "  shared_retrieval: "
                f"used={provider_meta.get('stage1_shared_retrieval_used')} "
                f"source_model={provider_meta.get('stage1_shared_retrieval_model')}"
            )
        prompt = result.get("research_prompt", "")
        if prompt:
            print(f"  research_prompt_chars: {len(prompt)}")
            print("  --- research_prompt_start ---")
            print(prompt)
            print("  --- research_prompt_end ---")
        second_pass_prompt = result.get("stage1_second_pass_prompt", "")
        if second_pass_prompt:
            print(f"  second_pass_prompt_chars(full): {len(second_pass_prompt)}")
            print("  --- second_pass_prompt_start ---")
            print(second_pass_prompt)
            print("  --- second_pass_prompt_end ---")
        second_pass_rows = result.get("stage1_second_pass_source_rows", []) or []
        if second_pass_rows:
            print(f"  second_pass_source_rows_count(full): {len(second_pass_rows)}")
            print("  --- second_pass_source_rows_top3 ---")
            print(json.dumps(second_pass_rows[:3], indent=2))
            print("  --- second_pass_source_rows_end ---")
        timeline_rows = result.get("stage1_second_pass_timeline_evidence", []) or []
        if timeline_rows:
            print(f"  second_pass_timeline_rows_count(full): {len(timeline_rows)}")
            print("  --- second_pass_timeline_rows_top5 ---")
            print(json.dumps(timeline_rows[:5], indent=2))
            print("  --- second_pass_timeline_rows_end ---")
        fact_digest_v2 = result.get("stage1_second_pass_fact_digest_v2", {}) or {}
        if fact_digest_v2:
            print("  --- second_pass_fact_digest_v2_summary ---")
            print(
                json.dumps(
                    {
                        "counts": fact_digest_v2.get("counts", {}),
                        "summary_bullets": (fact_digest_v2.get("summary_bullets", []) or [])[:6],
                        "conflicts": (fact_digest_v2.get("conflicts", []) or [])[:3],
                    },
                    indent=2,
                )
            )
            print("  --- second_pass_fact_digest_v2_end ---")
        if result.get("error"):
            print(f"  error: {result.get('error')}")


def _classify_response_type(response: str) -> str:
    text = (response or "").strip()
    if not text:
        return "empty"
    if text.startswith("Perplexity Deep Research Run for model"):
        return "fallback_research_log"
    json_candidate = text
    fence_match = re.match(r"^```(?:json)?\s*\n([\s\S]+?)\n```$", text, flags=re.IGNORECASE)
    if fence_match:
        json_candidate = fence_match.group(1).strip()
    if json_candidate.startswith("{"):
        try:
            json.loads(json_candidate)
            return "json_analysis"
        except Exception:
            return "json_malformed"
    if text.startswith("#") or text.startswith("**"):
        return "markdown_analysis"
    return "plain_text"


def _extract_named_score(response: str, label: str) -> Optional[float]:
    text = response or ""
    candidates: List[float] = []

    # Code-fenced JSON fallback.
    fence_match = re.match(r"^```(?:json)?\s*\n([\s\S]+?)\n```$", text.strip(), flags=re.IGNORECASE)
    if fence_match:
        fenced = fence_match.group(1).strip()
        if fenced:
            text = fenced

    # Structured JSON key forms.
    for m in re.finditer(
        rf'(?i)"{label}[_\s-]*score"\s*:\s*([0-9]{{1,3}}(?:\.[0-9]+)?)',
        text,
    ):
        try:
            value = float(m.group(1))
            if 0 <= value <= 100:
                candidates.append(value)
        except (TypeError, ValueError):
            continue

    # Nested JSON form like "quality_score": {"score": 88, ...}
    for m in re.finditer(
        rf'(?is)"{label}[_\s-]*score"\s*:\s*\{{[^{{}}]{{0,400}}?"score"\s*:\s*([0-9]{{1,3}}(?:\.[0-9]+)?)',
        text,
    ):
        try:
            value = float(m.group(1))
            if 0 <= value <= 100:
                candidates.append(value)
        except (TypeError, ValueError):
            continue

    # Common markdown/table forms like "Quality Score ... 66.5 / 100".
    for m in re.finditer(
        rf"(?i)\b{label}\s*score\b[^\n]{{0,240}}?([0-9]{{1,3}}(?:\.[0-9]+)?)\s*/\s*100\b",
        text,
    ):
        try:
            value = float(m.group(1))
            if 0 <= value <= 100:
                candidates.append(value)
        except (TypeError, ValueError):
            continue

    # Direct label assignment forms like "Quality Score: 63.75".
    for m in re.finditer(
        rf"(?i)\b{label}\s*score\b\s*[:=]\s*\*{{0,2}}\s*([0-9]{{1,3}}(?:\.[0-9]+)?)\b",
        text,
    ):
        try:
            value = float(m.group(1))
            if 0 <= value <= 100:
                candidates.append(value)
        except (TypeError, ValueError):
            continue

    # Formula-line fallback: pick last RHS value in lines mentioning "{label} score".
    for line in text.splitlines():
        if not re.search(rf"(?i)\b{label}\s*score\b", line):
            continue
        rhs = re.findall(r"=\s*\*{0,2}\s*([0-9]{1,3}(?:\.[0-9]+)?)", line)
        if rhs:
            try:
                value = float(rhs[-1])
                if 0 <= value <= 100:
                    candidates.append(value)
            except (TypeError, ValueError):
                continue

    # Heading + nearby section fallback:
    # Handles formats like:
    #   "## 1) Quality Score" + next line "**Score: 69 / 100**"
    #   table totals with "Rounded: 77/100"
    lines = text.splitlines()
    heading_re = re.compile(rf"(?i)\b{label}\s*score\b")
    other_label = "value" if label.lower() == "quality" else "quality"
    other_heading_re = re.compile(rf"(?i)\b{other_label}\s*score\b")
    nearby_score_re = re.compile(
        r"(?i)\bscore\b\s*[:=]\s*\*{0,2}\s*([0-9]{1,3}(?:\.[0-9]+)?)\s*/\s*100\b"
    )
    rounded_re = re.compile(
        r"(?i)\brounded\b\s*[:=]?\s*\*{0,2}\s*([0-9]{1,3}(?:\.[0-9]+)?)\s*/\s*100\b"
    )
    for idx, line in enumerate(lines):
        if not heading_re.search(line):
            continue
        end_idx = min(len(lines), idx + 40)
        for probe in range(idx + 1, end_idx):
            if other_heading_re.search(lines[probe]):
                end_idx = probe
                break
        section = "\n".join(lines[idx:end_idx])
        for regex in (nearby_score_re, rounded_re):
            for m in regex.finditer(section):
                try:
                    value = float(m.group(1))
                    if 0 <= value <= 100:
                        candidates.append(value)
                except (TypeError, ValueError):
                    continue

    if candidates:
        # Prefer the latest mention in the response (often the final roll-up total).
        return candidates[-1]
    return None


def _merge_supplementary_source_rows(
    primary_rows: List[Dict[str, Any]],
    supplementary_rows: List[Dict[str, Any]],
    *,
    insert_after: int = 10,
) -> List[Dict[str, Any]]:
    if not supplementary_rows:
        return list(primary_rows or [])
    primary = list(primary_rows or [])
    cutoff = max(0, int(insert_after))
    return primary[:cutoff] + list(supplementary_rows) + primary[cutoff:]


_STAGE1_SECTION_PATTERNS: Dict[str, re.Pattern] = {
    "quality_score": re.compile(r"(?i)\bquality[_\s-]*score\b"),
    "value_score": re.compile(r"(?i)\bvalue[_\s-]*score\b"),
    "price_targets": re.compile(
        r"(?i)\b12[-\s]*month\b|\b24[-\s]*month\b|\bprice[_\s-]*target"
    ),
    "development_timeline": re.compile(
        r"(?i)\bdevelopment[_\s-]*timeline\b|\btimeline\b|\bmilestone"
    ),
    "certainty": re.compile(r"(?i)\bcertainty\b|\bconfidence\b"),
    "headwinds_tailwinds": re.compile(r"(?i)\bheadwind\b|\btailwind"),
    "thesis_map": re.compile(r"(?i)\bthesis[_\s-]*map\b|\bbull\b|\bbase\b|\bbear\b"),
    "investment_verdict": re.compile(r"(?i)\binvestment[_\s-]*verdict\b|\brating\b|\bconviction\b"),
}


def _resolve_required_stage1_sections(metadata: Dict[str, Any]) -> List[str]:
    configured = metadata.get("stage1_verification_critical_sections", []) or []
    normalized = [
        str(item or "").strip().lower()
        for item in configured
        if str(item or "").strip()
    ]
    if normalized:
        return list(dict.fromkeys(normalized))
    # Conservative fallback aligned with current gold_miner compliance.
    return [
        "quality_score",
        "value_score",
        "price_targets",
        "development_timeline",
        "certainty",
        "headwinds_tailwinds",
        "thesis_map",
        "investment_verdict",
    ]


def _detect_stage1_section_presence(response: str) -> Dict[str, bool]:
    text = response or ""
    detected: Dict[str, bool] = {}
    for section, pattern in _STAGE1_SECTION_PATTERNS.items():
        detected[section] = bool(pattern.search(text))
    return detected


def _build_stage1_model_audit(
    stage1_results: List[Dict[str, Any]],
    metadata: Dict[str, Any],
) -> List[Dict[str, Any]]:
    required_sections = _resolve_required_stage1_sections(metadata)
    by_model = {item.get("model"): item for item in stage1_results}
    audits: List[Dict[str, Any]] = []
    for run in metadata.get("per_model_research_runs", []):
        model = run.get("model", "unknown")
        result = run.get("result", {}) or {}
        provider_meta = result.get("provider_metadata", {}) or {}
        response = (by_model.get(model, {}) or {}).get("response", "") or ""
        response_type = _classify_response_type(response)
        section_presence = _detect_stage1_section_presence(response)

        quality_score = _extract_named_score(response, "quality")
        value_score = _extract_named_score(response, "value")
        has_price_targets = bool(section_presence.get("price_targets"))
        has_timeline = bool(section_presence.get("development_timeline"))
        has_certainty = bool(section_presence.get("certainty"))
        has_thesis_map = bool(section_presence.get("thesis_map"))
        has_headwinds_tailwinds = bool(section_presence.get("headwinds_tailwinds"))
        has_investment_verdict = bool(section_presence.get("investment_verdict"))

        second_pass_success = provider_meta.get("stage1_second_pass_success")
        compliance_rating = (provider_meta.get("stage1_second_pass_compliance_rating") or "").lower()
        compliance_score = provider_meta.get("stage1_second_pass_compliance_score")
        citation_gate_passed = provider_meta.get("stage1_second_pass_citation_gate_passed")
        compliance_fail_reasons = list(
            provider_meta.get("stage1_second_pass_compliance_fail_reasons", []) or []
        )
        compliance_warning_reasons = list(
            provider_meta.get("stage1_second_pass_compliance_warning_reasons", []) or []
        )
        compliance_hard_fail_reasons = list(
            provider_meta.get("stage1_second_pass_compliance_hard_fail_reasons", []) or []
        )
        compliance_soft_fail_reasons = list(
            provider_meta.get("stage1_second_pass_compliance_soft_fail_reasons", []) or []
        )
        gate_critical_missing_sections = [
            str(item or "").strip().lower()
            for item in (provider_meta.get("stage1_second_pass_rubric_critical_missing_sections", []) or [])
            if str(item or "").strip()
        ]
        second_pass_error = provider_meta.get("stage1_second_pass_error")
        second_pass_warning = provider_meta.get("stage1_second_pass_warning")
        missing_required_sections = [
            section for section in required_sections if not section_presence.get(section, False)
        ]
        for section in gate_critical_missing_sections:
            if section not in missing_required_sections:
                missing_required_sections.append(section)

        reasons: List[str] = []
        if response_type in {"empty", "fallback_research_log", "json_malformed"}:
            reasons.append(f"response_type={response_type}")
        if second_pass_success is False:
            reasons.append("second_pass_failed")
        if compliance_rating == "red":
            reasons.append("compliance_rating=red")
        elif compliance_rating == "amber":
            reasons.append("compliance_rating=amber")
        if citation_gate_passed is False:
            reasons.append("citation_gate_failed")
        if missing_required_sections:
            reasons.append(
                "missing_required_sections=" + ",".join(sorted(set(missing_required_sections)))
            )
        if quality_score is None and "quality_score" in required_sections:
            reasons.append("missing_quality_score")
        if value_score is None and "value_score" in required_sections:
            reasons.append("missing_value_score")
        if not has_price_targets and "price_targets" in required_sections:
            reasons.append("missing_price_targets")
        if not has_timeline and "development_timeline" in required_sections:
            reasons.append("missing_timeline")
        if not has_certainty and "certainty" in required_sections:
            reasons.append("missing_certainty")
        if not has_thesis_map and "thesis_map" in required_sections:
            reasons.append("missing_thesis_map")
        if not has_headwinds_tailwinds and "headwinds_tailwinds" in required_sections:
            reasons.append("missing_headwinds_tailwinds")
        if not has_investment_verdict and "investment_verdict" in required_sections:
            reasons.append("missing_investment_verdict")
        if compliance_hard_fail_reasons:
            reasons.append(
                "hard_fail_reasons=" + "|".join(compliance_hard_fail_reasons[:4])
            )
        elif compliance_soft_fail_reasons:
            reasons.append(
                "soft_fail_reasons=" + "|".join(compliance_soft_fail_reasons[:4])
            )
        if compliance_warning_reasons:
            reasons.append(
                "warning_reasons=" + "|".join(compliance_warning_reasons[:4])
            )
        if second_pass_error:
            reasons.append(f"second_pass_error={second_pass_error}")
        if second_pass_warning:
            reasons.append(f"second_pass_warning={second_pass_warning}")

        if (
            second_pass_success is False
            or response_type in {"empty", "fallback_research_log", "json_malformed"}
            or compliance_rating == "red"
            or bool(compliance_hard_fail_reasons)
        ):
            recommendation = "remove"
        elif (
            compliance_rating == "amber"
            or citation_gate_passed is False
            or bool(compliance_soft_fail_reasons)
            or bool(missing_required_sections)
        ):
            recommendation = "watch"
        else:
            recommendation = "keep"

        audits.append(
            {
                "model": model,
                "analysis_provider": provider_meta.get("stage1_analysis_provider"),
                "response_type": response_type,
                "response_chars": len(response),
                "quality_score_detected": quality_score,
                "value_score_detected": value_score,
                "has_price_targets": has_price_targets,
                "has_timeline": has_timeline,
                "has_certainty": has_certainty,
                "has_thesis_map": has_thesis_map,
                "has_headwinds_tailwinds": has_headwinds_tailwinds,
                "has_investment_verdict": has_investment_verdict,
                "required_sections": list(required_sections),
                "missing_required_sections": list(missing_required_sections),
                "second_pass_success": second_pass_success,
                "second_pass_attempts": provider_meta.get("stage1_second_pass_attempts"),
                "compliance_rating": compliance_rating or None,
                "compliance_score": compliance_score,
                "compliance_fail_reasons": compliance_fail_reasons,
                "compliance_warning_reasons": compliance_warning_reasons,
                "compliance_hard_fail_reasons": compliance_hard_fail_reasons,
                "compliance_soft_fail_reasons": compliance_soft_fail_reasons,
                "rubric_coverage_pct": provider_meta.get("stage1_second_pass_rubric_coverage_pct"),
                "numeric_citation_pct": provider_meta.get("stage1_second_pass_citation_numeric_citation_pct"),
                "citation_count": provider_meta.get("stage1_second_pass_citation_count"),
                "citation_gate_passed": citation_gate_passed,
                "second_pass_error": second_pass_error,
                "second_pass_warning": second_pass_warning,
                "recommendation": recommendation,
                "reasons": reasons,
            }
        )
    return audits


def _print_stage1_model_audit(stage1_model_audit: List[Dict[str, Any]]) -> None:
    print("\nSTAGE 1 MODEL AUDIT")
    print("-" * 90)
    if not stage1_model_audit:
        print("No model audit rows available.")
        return
    keep = sum(1 for row in stage1_model_audit if row.get("recommendation") == "keep")
    watch = sum(1 for row in stage1_model_audit if row.get("recommendation") == "watch")
    remove = sum(1 for row in stage1_model_audit if row.get("recommendation") == "remove")
    print(f"Recommendations summary: keep={keep} watch={watch} remove={remove}")
    for row in stage1_model_audit:
        print(
            f"[{(row.get('recommendation') or 'watch').upper()}] {row.get('model')} "
            f"(provider={row.get('analysis_provider')}, type={row.get('response_type')})"
        )
        print(
            "  conformance: "
            f"second_pass_success={row.get('second_pass_success')} "
            f"rating={row.get('compliance_rating')} "
            f"score={row.get('compliance_score')} "
            f"citation_gate={row.get('citation_gate_passed')}"
        )
        print(
            "  output_signals: "
            f"quality={row.get('quality_score_detected')} "
            f"value={row.get('value_score_detected')} "
            f"price_targets={row.get('has_price_targets')} "
            f"timeline={row.get('has_timeline')} "
            f"certainty={row.get('has_certainty')} "
            f"thesis_map={row.get('has_thesis_map')} "
            f"headwinds_tailwinds={row.get('has_headwinds_tailwinds')} "
            f"investment_verdict={row.get('has_investment_verdict')}"
        )
        missing_required = row.get("missing_required_sections") or []
        if missing_required:
            print(f"  missing_required_sections: {', '.join(missing_required)}")
        hard_fails = row.get("compliance_hard_fail_reasons") or []
        soft_fails = row.get("compliance_soft_fail_reasons") or []
        warnings = row.get("compliance_warning_reasons") or []
        if hard_fails:
            print(f"  compliance_hard_fail_reasons: {' | '.join(hard_fails[:6])}")
        if soft_fails:
            print(f"  compliance_soft_fail_reasons: {' | '.join(soft_fails[:6])}")
        if warnings:
            print(f"  compliance_warning_reasons: {' | '.join(warnings[:6])}")
        reasons = row.get("reasons") or []
        if reasons:
            print(f"  reasons: {' | '.join(reasons[:6])}")


def _print_stage2(stage2_results: list[Dict[str, Any]], aggregate_rankings: list[Dict[str, Any]]) -> None:
    print("\nSTAGE 2")
    print("-" * 90)
    print(f"Rankings received: {len(stage2_results)}")
    if aggregate_rankings:
        print("Aggregate ranking:")
        for i, item in enumerate(aggregate_rankings, 1):
            print(f"  {i}. {item['model']} (avg rank: {item['average_rank']:.2f})")


def _print_stage2_revision(
    stage2_revision_summary: Dict[str, Any],
    stage2_revision_results: List[Dict[str, Any]],
) -> None:
    print("\nSTAGE 2.5 (REVISION PASS)")
    print("-" * 90)
    if not stage2_revision_summary or not stage2_revision_summary.get("enabled"):
        print("Revision pass disabled.")
        return
    print(
        "Revision summary: "
        f"attempted={len(stage2_revision_summary.get('models_attempted', []) or [])} "
        f"accepted={stage2_revision_summary.get('accepted_count', 0)} "
        f"changed={stage2_revision_summary.get('changed_count', 0)} "
        f"no_amendment={stage2_revision_summary.get('no_amendment_count', 0)} "
        f"empty_response={stage2_revision_summary.get('empty_response_count', 0)} "
        f"parse_failed={stage2_revision_summary.get('parse_failed_count', 0)}"
    )
    apply_summary = stage2_revision_summary.get("apply") or {}
    if apply_summary:
        print(
            "Applied deltas: "
            f"{apply_summary.get('revisions_applied', 0)}/"
            f"{apply_summary.get('models_total', 0)}"
        )
    for row in stage2_revision_results or []:
        parse_error = row.get("parse_error") or "none"
        if parse_error == "empty_response":
            parse_error = "no_amendment(empty_response)"
        print(
            f"  - {row.get('model')}: "
            f"accepted={row.get('accepted')} "
            f"changed={row.get('changed')} "
            f"parse_error={parse_error} "
            f"response_chars={row.get('response_chars', 0)}"
        )


def _print_stage3(stage3_result: Dict[str, Any], *, title: str = "STAGE 3") -> None:
    print(f"\n{title}")
    print("-" * 90)
    print(f"Chairman model: {stage3_result.get('model', 'unknown')}")
    if stage3_result.get("parse_error"):
        print(f"Parse error: {stage3_result['parse_error']}")

    structured = stage3_result.get("structured_data") or {}
    if not structured:
        print("No structured_data returned. Raw response preview:")
        text = (stage3_result.get("response") or "").strip()
        if len(text) > 600:
            text = text[:597] + "..."
        print(text or "(empty)")
        return

    company_name = (
        structured.get("company_name")
        or structured.get("company")
        or "Unknown"
    )
    print(f"Company: {company_name}")
    print(f"Ticker: {structured.get('ticker', 'Unknown')}")

    required_fields = [
        "analysis_type",
        "ticker",
        "company_name",
        "quality_score",
        "value_score",
        "price_targets",
        "development_timeline",
        "investment_recommendation",
    ]
    present_required = [
        field
        for field in required_fields
        if structured.get(field) not in (None, "", [], {})
    ]
    missing_required = [field for field in required_fields if field not in present_required]
    print(
        "Template Coverage: "
        f"{len(present_required)}/{len(required_fields)} required fields populated"
    )
    if missing_required:
        print(f"Missing required fields: {', '.join(missing_required)}")

    recommendation = structured.get("investment_recommendation", {}) or {}
    if recommendation:
        print(
            "Investment Recommendation: "
            f"rating={recommendation.get('rating', 'n/a')} "
            f"conviction={recommendation.get('conviction', 'n/a')} "
            f"summary={recommendation.get('summary', '')[:180]}"
        )

    quality = structured.get("quality_score", {}) or {}
    value = structured.get("value_score", {}) or {}
    if quality.get("total") is not None or value.get("total") is not None:
        print(
            "Scoring: "
            f"quality_total={quality.get('total', 'n/a')} "
            f"value_total={value.get('total', 'n/a')}"
        )


    # Keep compatibility with financial_quality_mvp fields.
    if structured.get("confidence_pct") is not None:
        print(f"Confidence (%): {structured.get('confidence_pct')}")
    if structured.get("recommendation") is not None:
        print(f"Recommendation: {structured.get('recommendation')}")

    price_targets = structured.get("price_targets", {}) or {}
    if price_targets:
        print(
            "Price Targets: "
            f"12m={price_targets.get('target_12m', 'n/a')} "
            f"24m={price_targets.get('target_24m', 'n/a')} "
            f"base={((price_targets.get('scenarios') or {}).get('base', 'n/a'))} "
            f"bull={((price_targets.get('scenarios') or {}).get('bull', 'n/a'))} "
            f"bear={((price_targets.get('scenarios') or {}).get('bear', 'n/a'))}"
        )

    timeline = structured.get("development_timeline", []) or []
    if timeline:
        print("Development Timeline (top 3):")
        for item in timeline[:3]:
            if isinstance(item, dict):
                print(
                    "  - "
                    f"{item.get('milestone', 'milestone')} | "
                    f"{item.get('target_period', 'period')} | "
                    f"status={item.get('status', 'n/a')} | "
                    f"confidence={item.get('confidence_pct', 'n/a')}"
                )
            else:
                print(f"  - {item}")

    ht = structured.get("headwinds_tailwinds", {}) or {}
    q_hw = (ht.get("quantitative") or [])[:3]
    ql_hw = (ht.get("qualitative") or [])[:3]
    if q_hw:
        print("Quantitative Headwinds/Tailwinds:")
        for item in q_hw:
            print(f"  - {item}")
    if ql_hw:
        print("Qualitative Headwinds/Tailwinds:")
        for item in ql_hw:
            print(f"  - {item}")

    verdict = structured.get("investment_verdict", {}) or {}
    if verdict:
        print("Investment Verdict:")
        reasons = verdict.get("top_reasons") or []
        failures = verdict.get("failure_conditions") or []
        if reasons:
            print("  Top reasons:")
            for item in reasons[:3]:
                print(f"    - {item}")
        if failures:
            print("  Failure conditions:")
            for item in failures[:3]:
                print(f"    - {item}")

    analyst_document = stage3_result.get("analyst_document") or {}
    analyst_model = str(analyst_document.get("model") or "").strip()
    analyst_content = str(analyst_document.get("content_markdown") or "").strip()
    if analyst_content:
        print(
            "Analyst Memo: "
            f"model={analyst_model or 'n/a'} "
            f"chars={len(analyst_content)} "
            f"stage1_rows={len(analyst_document.get('stage1_reference_rows') or [])}"
        )

    extended = structured.get("extended_analysis", {}) or {}
    if extended:
        catalysts = (extended.get("next_major_catalysts") or [])[:3]
        sensitivities = (extended.get("sensitivity_analysis") or [])[:3]
        market_context = (extended.get("market_context") or [])[:3]
        if catalysts:
            print("Next Catalysts:")
            for item in catalysts:
                print(f"  - {item}")
        if sensitivities:
            print("Sensitivity Analysis (top 3):")
            for item in sensitivities:
                print(f"  - {item}")
        if market_context:
            print("Market Context:")
            for item in market_context:
                print(f"  - {item}")

    rationale = structured.get("rationale", "")
    if rationale:
        print("Rationale:")
        print(f"  {rationale}")

    evidence = structured.get("evidence_used", []) or []
    if evidence:
        print("Top evidence:")
        for item in evidence[:5]:
            print(f"  - {item.get('source', '')}")

    missing = structured.get("missing_information", []) or []
    if missing:
        print("Missing information:")
        for item in missing[:5]:
            print(f"  - {item}")


def _write_stage3_memo_files(
    *,
    dump_json_path: str,
    stage3_result_primary: Dict[str, Any],
    stage3_result_secondary: Optional[Dict[str, Any]],
) -> List[str]:
    base_path = Path(dump_json_path).expanduser().resolve()
    out_paths: List[str] = []
    rows: List[Tuple[str, Optional[Dict[str, Any]]]] = [
        ("primary", stage3_result_primary),
        ("secondary", stage3_result_secondary),
    ]
    for label, row in rows:
        if not row:
            continue
        model = str(row.get("model") or "chairman")
        structured = row.get("structured_data") if isinstance(row.get("structured_data"), dict) else {}
        company_title = str(
            structured.get("company_name")
            or structured.get("company")
            or "Company"
        ).strip()
        chairman_document = row.get("chairman_document") or {}
        content = str(
            chairman_document.get("content")
            or row.get("response")
            or ""
        ).strip()
        if not content:
            continue
        model_slug = re.sub(r"[^a-zA-Z0-9]+", "_", model).strip("_").lower()
        memo_path = base_path.parent / f"{base_path.stem}.stage3_{label}_{model_slug}.md"
        lines = [f"# Chairman Synthesis: {company_title}", "", content, ""]
        memo_path.write_text("\n".join(lines), encoding="utf-8")
        out_paths.append(str(memo_path))

        analyst_document = row.get("analyst_document") or {}
        analyst_content = str(analyst_document.get("content_markdown") or "").strip()
        if analyst_content:
            analyst_model = str(analyst_document.get("model") or "analyst")
            analyst_slug = re.sub(r"[^a-zA-Z0-9]+", "_", analyst_model).strip("_").lower()
            analyst_path = (
                base_path.parent
                / f"{base_path.stem}.stage3_{label}_analyst_{analyst_slug}.md"
            )
            analyst_lines = [
                "# Stage 3 Analyst Memo",
                "",
                f"- run_artifact: `{base_path.name}`",
                f"- chairman: `{model}`",
                f"- analyst_model: `{analyst_model}`",
                f"- generated_utc: `{analyst_document.get('generated_utc') or datetime.utcnow().isoformat() + 'Z'}`",
                "",
                "## Memo",
                "",
                analyst_content,
                "",
            ]
            if analyst_content.lstrip().startswith("#"):
                analyst_lines = [analyst_content, ""]
            else:
                analyst_lines = [f"# Investment Analysis: {company_title}", "", analyst_content, ""]
            analyst_path.write_text("\n".join(analyst_lines), encoding="utf-8")
            out_paths.append(str(analyst_path))
    return out_paths


def _dump_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """Atomically write JSON payload to disk."""
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, default=str),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _write_stage_checkpoint(
    *,
    dump_json_path: Optional[str],
    stage_name: str,
    payload: Dict[str, Any],
) -> Optional[Path]:
    """Persist a durable checkpoint for each completed stage."""
    if not dump_json_path:
        return None
    base_path = Path(dump_json_path).expanduser().resolve()
    # Keep checkpoints out of top-level outputs so one canonical run artifact remains
    # for UI listing and downstream tooling.
    checkpoint_dir = base_path.parent / "checkpoints" / base_path.stem
    checkpoint_path = checkpoint_dir / f"{stage_name}.checkpoint.json"
    _dump_json_atomic(checkpoint_path, payload)
    return checkpoint_path


def _build_stage3_comparison(
    primary: Dict[str, Any],
    secondary: Dict[str, Any] | None,
) -> Dict[str, Any]:
    if not secondary:
        return {}

    p = primary.get("structured_data") or {}
    s = secondary.get("structured_data") or {}
    if not isinstance(p, dict) or not isinstance(s, dict):
        return {
            "available": False,
            "reason": "missing_structured_data",
            "primary_model": primary.get("model"),
            "secondary_model": secondary.get("model"),
        }

    def _score_total(block: Dict[str, Any], key: str) -> Optional[float]:
        obj = block.get(key) or {}
        if isinstance(obj, dict):
            val = obj.get("total")
            try:
                return float(val) if val is not None else None
            except (TypeError, ValueError):
                return None
        return None

    p_quality = _score_total(p, "quality_score")
    s_quality = _score_total(s, "quality_score")
    p_value = _score_total(p, "value_score")
    s_value = _score_total(s, "value_score")

    p_targets = p.get("price_targets") or {}
    s_targets = s.get("price_targets") or {}

    p_rec = (p.get("investment_recommendation") or {}).get("rating")
    s_rec = (s.get("investment_recommendation") or {}).get("rating")

    return {
        "available": True,
        "primary_model": primary.get("model"),
        "secondary_model": secondary.get("model"),
        "quality_total": {"primary": p_quality, "secondary": s_quality},
        "value_total": {"primary": p_value, "secondary": s_value},
        "quality_delta_secondary_minus_primary": (
            (s_quality - p_quality)
            if (s_quality is not None and p_quality is not None)
            else None
        ),
        "value_delta_secondary_minus_primary": (
            (s_value - p_value)
            if (s_value is not None and p_value is not None)
            else None
        ),
        "recommendation": {"primary": p_rec, "secondary": s_rec},
        "target_12m": {
            "primary": p_targets.get("target_12m"),
            "secondary": s_targets.get("target_12m"),
        },
        "target_24m": {
            "primary": p_targets.get("target_24m"),
            "secondary": s_targets.get("target_24m"),
        },
    }


def _print_stage3_comparison(stage3_comparison: Dict[str, Any]) -> None:
    print("\nSTAGE 3 COMPARISON")
    print("-" * 90)
    if not stage3_comparison:
        print("No secondary chairman run.")
        return
    if not stage3_comparison.get("available"):
        print(
            "Comparison unavailable: "
            f"{stage3_comparison.get('reason', 'unknown')}"
        )
        print(
            "Models: "
            f"{stage3_comparison.get('primary_model')} vs "
            f"{stage3_comparison.get('secondary_model')}"
        )
        return

    print(
        "Models: "
        f"{stage3_comparison.get('primary_model')} vs "
        f"{stage3_comparison.get('secondary_model')}"
    )
    print(
        "Quality totals: "
        f"primary={stage3_comparison.get('quality_total', {}).get('primary')} "
        f"secondary={stage3_comparison.get('quality_total', {}).get('secondary')} "
        f"delta={stage3_comparison.get('quality_delta_secondary_minus_primary')}"
    )
    print(
        "Value totals: "
        f"primary={stage3_comparison.get('value_total', {}).get('primary')} "
        f"secondary={stage3_comparison.get('value_total', {}).get('secondary')} "
        f"delta={stage3_comparison.get('value_delta_secondary_minus_primary')}"
    )
    print(
        "Recommendations: "
        f"primary={stage3_comparison.get('recommendation', {}).get('primary')} "
        f"secondary={stage3_comparison.get('recommendation', {}).get('secondary')}"
    )
    print(
        "12m targets: "
        f"primary={stage3_comparison.get('target_12m', {}).get('primary')} "
        f"secondary={stage3_comparison.get('target_12m', {}).get('secondary')}"
    )
    print(
        "24m targets: "
        f"primary={stage3_comparison.get('target_24m', {}).get('primary')} "
        f"secondary={stage3_comparison.get('target_24m', {}).get('secondary')}"
    )


def _market_facts_audit_summary(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    facts = payload or {}
    normalized = facts.get("normalized_facts") or {}
    core_present = [
        key
        for key in ("current_price", "market_cap", "shares_outstanding", "enterprise_value", "currency")
        if normalized.get(key) is not None
    ]
    commodity_suffixes = (
        "_price_usd_lb",
        "_price_aud_lb",
        "_price_usd_oz",
        "_price_aud_oz",
        "_price_usd_kg",
        "_price_aud_kg",
        "_price_usd_bbl",
        "_price_aud_bbl",
        "_price_usd_mmbtu",
        "_price_aud_mmbtu",
    )
    commodity_present = [
        key
        for key, value in normalized.items()
        if value is not None and any(key.endswith(suffix) for suffix in commodity_suffixes)
    ]
    return {
        "status": facts.get("status"),
        "reason": facts.get("reason"),
        "commodity_profile": normalized.get("commodity_profile"),
        "core_fields_present": core_present,
        "commodity_fields_present": commodity_present,
        "source_count": len(facts.get("source_urls") or []),
    }


def _prepass_audit_summary(meta: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = meta or {}
    return {
        "strategy": payload.get("strategy"),
        "bundle_path": payload.get("bundle_path"),
        "generated_at_utc": payload.get("generated_at_utc"),
        "docs_in_bundle": payload.get("docs_in_bundle"),
        "rows_built": payload.get("rows_built"),
        "kept_for_injection": payload.get("kept_for_injection"),
        "prepass_retrieved_sources": payload.get("prepass_retrieved_sources"),
        "prepass_selected_primary_candidates": payload.get("prepass_selected_primary_candidates"),
        "selection_counts": payload.get("selection_counts") or {},
    }


def _selection_audit_summary(selection: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "template_id": selection.get("template_id"),
        "company_type": selection.get("company_type"),
        "company_name": selection.get("company_name"),
        "exchange": selection.get("exchange"),
        "selection_source": selection.get("selection_source"),
        "exchange_selection_source": selection.get("exchange_selection_source"),
    }


def _build_input_audit_payload(
    *,
    selection: Dict[str, Any],
    stage1_research_brief: str,
    market_facts_query_prefix: str,
    market_facts: Dict[str, Any] | None,
    injection_bundle_meta: Dict[str, Any] | None,
    injection_source_rows_count: int,
) -> Dict[str, Any]:
    return {
        "selection": selection,
        "selection_summary": _selection_audit_summary(selection),
        "stage1_research_brief": stage1_research_brief,
        "market_facts_query_prefix": market_facts_query_prefix,
        "market_facts": market_facts or {},
        "market_facts_summary": _market_facts_audit_summary(market_facts),
        "primary_injection_bundle_meta": injection_bundle_meta,
        "primary_injection_source_rows_count": int(injection_source_rows_count),
        "prepass_summary": _prepass_audit_summary(injection_bundle_meta),
    }


def _print_input_audit(
    selection: Dict[str, Any],
    effective_query: str,
    stage1_query_sent: str,
    stage1_research_brief: str,
    market_facts: Dict[str, Any] | None,
    market_facts_query_prefix: str,
    injection_bundle_meta: Dict[str, Any] | None,
    injection_source_rows_count: int,
) -> None:

    print("\nINPUT AUDIT")
    print("-" * 90)
    print("Selection:")
    print(json.dumps(selection, indent=2))
    print("\nSelection Summary:")
    print(json.dumps(_selection_audit_summary(selection), indent=2))
    print("\nEffective Query (template/user query before market-facts prefix):")
    print(effective_query)
    print("\nStage 1 Query Sent (with minimal market-facts prefix):")
    print(stage1_query_sent)
    print("\nStage 1 Research Brief (sent as research_brief to Stage 1):")
    print(stage1_research_brief)
    print("\nMarket Facts Query Prefix (minimal block):")
    print(market_facts_query_prefix or "(none)")
    print("\nMarket Facts Object:")
    print(json.dumps(market_facts or {}, indent=2))
    print("\nMarket Facts Summary:")
    print(json.dumps(_market_facts_audit_summary(market_facts), indent=2))
    print("\nPrimary Injection Bundle Meta:")
    print(json.dumps(injection_bundle_meta or {}, indent=2))
    print("\nPrimary Injection Summary:")
    print(json.dumps(_prepass_audit_summary(injection_bundle_meta), indent=2))
    print(f"\nPrimary Injection Source Rows Prepared: {int(injection_source_rows_count)}")


def _print_provider_request_audit(
    *,
    endpoint: str,
    models: list[str],
    prompt: str,
    payloads: Dict[str, Dict[str, Any]],
) -> None:
    print("\nPROVIDER REQUEST AUDIT")
    print("-" * 90)
    print(f"Endpoint: {endpoint}")
    print(f"Models: {models}")
    print(f"Prompt chars: {len(prompt)}")
    print("\nFull Prompt Sent To Provider:")
    print(prompt)
    for model in models:
        print(f"\nPayload for model={model}:")
        print(json.dumps(payloads.get(model, {}), indent=2))


async def _run(args: argparse.Namespace) -> None:
    # Import runtime config from .env (authoritative).
    from backend.council import (
        stage1_collect_perplexity_research_responses,
        stage2_collect_rankings,
        stage2_collect_revision_deltas,
        apply_stage2_revision_deltas,
        stage3_synthesize_final,
        calculate_aggregate_rankings,
    )
    from backend.config import CHAIRMAN_MODEL
    from backend.config import ENABLE_MARKET_FACTS_PREPASS
    from backend.config import STAGE2_REVISION_PASS_ENABLED
    from backend.config import PERPLEXITY_API_URL, PERPLEXITY_COUNCIL_MODELS, MAX_SOURCES
    from backend.main import build_enhanced_context
    from backend.search import extract_ticker_from_query
    from backend.market_facts import (
        gather_market_facts_prepass,
        format_market_facts_query_prefix,
        prepend_market_facts_to_query,
    )
    from backend.research.providers.perplexity import PerplexityResearchProvider
    from backend.template_loader import get_template_loader, resolve_template_selection

    # Resolve ticker before any downstream stage. Silent no-ticker runs are not
    # allowed when deterministic market-facts prepass is enabled.
    effective_ticker = str(args.ticker or "").strip()
    if not effective_ticker:
        inferred_ticker = extract_ticker_from_query(args.query or "")
        if inferred_ticker:
            effective_ticker = inferred_ticker
            _progress(f"Inferred ticker from query: {effective_ticker}")
    if effective_ticker:
        args.ticker = effective_ticker

    selection = resolve_template_selection(
        user_query=args.query or "",
        ticker=effective_ticker,
        explicit_template_id=args.template_id,
        company_type=args.company_type,
        exchange=args.exchange,
    )
    selected_template_id = selection["template_id"]
    selected_company_name = selection.get("company_name")
    selected_company_type = selection.get("company_type")
    selected_exchange = selection.get("exchange")
    loader = get_template_loader()
    use_structured_analysis = loader.is_structured_template(selected_template_id)

    effective_query = loader.render_template_rubric(
        selected_template_id,
        company_name=selected_company_name,
        exchange=selected_exchange,
    )
    if not effective_query:
        effective_query = loader.render_stage1_query_prompt(
            selected_template_id,
            company_name=selected_company_name,
            exchange=selected_exchange,
        )
    if not effective_query:
        raise ValueError(
            f"Template '{selected_template_id}' has no Stage 1 prompt to use as query."
        )

    stage1_research_brief = loader.get_stage1_research_brief(
        selected_template_id,
        selected_company_type,
        selected_exchange,
        selected_company_name,
        include_rubric=(
            True
            if args.brief_include_rubric == "always"
            else (
                False
                if args.brief_include_rubric == "never"
                else False
            )
        ),
    )

    total_start = perf_counter()
    _print_header(args, selection)

    market_facts = None
    if ENABLE_MARKET_FACTS_PREPASS:
        if not effective_ticker:
            raise ValueError(
                "Market facts prepass is enabled but ticker is unresolved. "
                "Provide --ticker EXCHANGE:SYMBOL (e.g., ASX:BRK) or include a parseable ticker in the query."
            )
        _progress("Market facts prepass start")
        market_facts = await gather_market_facts_prepass(
            ticker=effective_ticker,
            company_name=selected_company_name,
            exchange=selected_exchange,
            template_id=selected_template_id,
            company_type=selected_company_type,
        )
        market_status = str((market_facts or {}).get("status") or "").strip().lower()
        market_facts_text = format_market_facts_query_prefix(market_facts)
        print(f"Market facts status: {market_status or 'unknown'}")
        if market_status in {"skipped", "error", "empty"} or not market_facts_text:
            reason = str((market_facts or {}).get("reason") or "").strip()
            raise RuntimeError(
                "Market facts prepass failed or returned no injectable normalized_facts. "
                f"status={market_status or 'unknown'} reason={reason or 'n/a'}"
            )
        print(
            "Market facts prepass prepared; minimal normalized_facts block will be prepended to Stage 1 query."
        )
        _progress("Market facts prepass done")

    stage1_effective_research_brief = stage1_research_brief
    supplementary_context_file = str(args.supplementary_context_file or "").strip()
    if supplementary_context_file:
        supplementary_text = await _load_supplementary_context(supplementary_context_file)
        if supplementary_text:
            stage1_effective_research_brief = (
                f"{stage1_effective_research_brief}\n\n{supplementary_text}".strip()
            )
    market_facts_query_prefix = format_market_facts_query_prefix(market_facts)
    stage1_effective_query = prepend_market_facts_to_query(effective_query, market_facts)
    injection_bundle_rows: List[Dict[str, Any]] = []
    injection_bundle_meta: Dict[str, Any] = {}
    if args.dry_run_input:
        injection_bundle_meta = {"strategy": "skipped_dry_run"}
    else:
        repo_root = Path(__file__).resolve().parent
        injection_bundle_rows, injection_bundle_meta = _prepare_primary_injection_bundle(
            repo_root=repo_root,
            ticker=effective_ticker,
            company_name=str(selected_company_name or ""),
            query_hint=(args.query or selected_company_name or effective_ticker),
            exchange=str(selected_exchange or args.exchange or ""),
            exchange_retrieval_params=loader.get_exchange_retrieval_params(selected_exchange),
            reuse_recent_bundle=bool(args.reuse_recent_bundle),
        )
        if not injection_bundle_rows:
            raise RuntimeError(
                "Primary injection bundle produced zero source rows; refusing to run Stage 1 with junk/empty evidence."
            )
        _progress(
            "Primary injection bundle ready: "
            f"rows={len(injection_bundle_rows)} "
            f"strategy={injection_bundle_meta.get('strategy', 'unknown')} "
            f"path={injection_bundle_meta.get('bundle_path', '')}"
        )

    _print_input_audit(
        selection=selection,
        effective_query=effective_query,
        stage1_query_sent=stage1_effective_query,
        stage1_research_brief=stage1_effective_research_brief,
        market_facts=market_facts,
        market_facts_query_prefix=market_facts_query_prefix,
        injection_bundle_meta=injection_bundle_meta,
        injection_source_rows_count=len(injection_bundle_rows),
    )

    if args.dry_run_input:
        provider = PerplexityResearchProvider()
        depth = "deep"
        max_sources = MAX_SOURCES
        models = [item.strip() for item in PERPLEXITY_COUNCIL_MODELS if item.strip()]
        prompt = provider._build_prompt(
            user_query=stage1_effective_query,
            ticker=effective_ticker,
            depth=depth,
            max_sources=max_sources,
            research_brief=stage1_effective_research_brief,
        )
        payloads: Dict[str, Dict[str, Any]] = {}
        for model in models:
            payloads[model] = provider._build_payload(
                prompt=prompt,
                depth=depth,
                max_sources=max_sources,
                model_override=model,
            )
        _print_provider_request_audit(
            endpoint=PERPLEXITY_API_URL,
            models=models,
            prompt=prompt,
            payloads=payloads,
        )
        return

    _progress("Stage 1 start")

    stage1_start = perf_counter()
    stage1_results, metadata = await stage1_collect_perplexity_research_responses(
        user_query=stage1_effective_query,
        ticker=effective_ticker,
        prepass_source_rows=injection_bundle_rows,
        depth="deep",
        research_brief=stage1_effective_research_brief,
        template_id=selected_template_id,
        diagnostic_mode=bool(args.diagnostic_mode),
    )
    _progress(f"Stage 1 done in {perf_counter() - stage1_start:.1f}s")
    _print_stage1(metadata)
    stage1_model_audit = _build_stage1_model_audit(stage1_results, metadata)
    _print_stage1_model_audit(stage1_model_audit)
    checkpoint_input_audit = _build_input_audit_payload(
        selection=selection,
        stage1_research_brief=stage1_effective_research_brief,
        market_facts_query_prefix=market_facts_query_prefix,
        market_facts=market_facts,
        injection_bundle_meta=injection_bundle_meta,
        injection_source_rows_count=len(injection_bundle_rows),
    )
    stage1_checkpoint = _write_stage_checkpoint(
        dump_json_path=args.dump_json,
        stage_name="stage1",
        payload={
            "effective_query": effective_query,
            "stage1_query_sent": stage1_effective_query,
            "input_audit": checkpoint_input_audit,
            "stage1_results": stage1_results,
            "stage1_model_audit": stage1_model_audit,
            "metadata": metadata,
            "selection": selection,
        },
    )
    if stage1_checkpoint:
        print(f"Saved checkpoint to: {stage1_checkpoint}")

    if not stage1_results:
        print("\nNo Stage 1 responses generated. Stopping.")
        return

    if args.stage1_only:
        if args.dump_json:
            payload = {
                "effective_query": effective_query,
                "stage1_query_sent": stage1_effective_query,
                "input_audit": checkpoint_input_audit,
                "stage1_results": stage1_results,
                "stage1_model_audit": stage1_model_audit,
                "stage2_results": [],
                "stage3_result": {},
                "metadata": metadata,
                "selection": selection,
            }
            _dump_json_atomic(Path(args.dump_json), payload)
            print(f"\nSaved JSON output to: {args.dump_json}")
        _progress(f"Run complete in {perf_counter() - total_start:.1f}s")
        print("\nStage 1-only test complete.")
        return

    search_results = metadata.get("aggregated_search_results", {})
    enhanced_context = build_enhanced_context(
        effective_query,
        search_results,
        [],
        market_facts=market_facts,
    )
    ranking_models = [item.get("model") for item in stage1_results if item.get("model")]
    primary_chairman_model = (
        CHAIRMAN_MODEL
        if CHAIRMAN_MODEL in ranking_models
        else (ranking_models[0] if ranking_models else CHAIRMAN_MODEL)
    )
    secondary_chairman_model = str(args.secondary_chairman_model or "").strip() or None

    _progress("Stage 2 start")
    stage2_start = perf_counter()
    stage2_results, label_to_model = await stage2_collect_rankings(
        enhanced_context,
        stage1_results,
        ranking_models=ranking_models,
    )
    aggregate_rankings = calculate_aggregate_rankings(stage2_results, label_to_model)
    _progress(f"Stage 2 done in {perf_counter() - stage2_start:.1f}s")
    _print_stage2(stage2_results, aggregate_rankings)
    stage2_checkpoint = _write_stage_checkpoint(
        dump_json_path=args.dump_json,
        stage_name="stage2",
        payload={
            "effective_query": effective_query,
            "selection": selection,
            "input_audit": checkpoint_input_audit,
            "stage1_results": stage1_results,
            "stage1_model_audit": stage1_model_audit,
            "metadata": metadata,
            "stage2_results": stage2_results,
            "stage2_aggregate_rankings": aggregate_rankings,
            "label_to_model": label_to_model,
        },
    )
    if stage2_checkpoint:
        print(f"Saved checkpoint to: {stage2_checkpoint}")

    revision_mode = (args.stage2_revision_pass or "auto").strip().lower()
    revision_enabled = (
        bool(STAGE2_REVISION_PASS_ENABLED)
        if revision_mode == "auto"
        else (revision_mode == "on")
    )
    stage2_revision_results: List[Dict[str, Any]] = []
    stage2_revision_summary: Dict[str, Any] = {"enabled": False}
    stage1_results_for_stage3 = stage1_results
    if revision_enabled:
        _progress("Stage 2.5 revision pass start")
        stage2_revision_start = perf_counter()
        stage2_revision_results, stage2_revision_summary = await stage2_collect_revision_deltas(
            enhanced_context,
            stage1_results,
            stage2_results,
            label_to_model,
            revision_models=ranking_models,
        )
        stage1_results_for_stage3, apply_summary = apply_stage2_revision_deltas(
            stage1_results,
            stage2_revision_results,
        )
        stage2_revision_summary["apply"] = apply_summary
        _progress(
            "Stage 2.5 revision pass done in "
            f"{perf_counter() - stage2_revision_start:.1f}s"
        )
    _print_stage2_revision(stage2_revision_summary, stage2_revision_results)
    stage25_checkpoint = _write_stage_checkpoint(
        dump_json_path=args.dump_json,
        stage_name="stage2_5",
        payload={
            "effective_query": effective_query,
            "selection": selection,
            "input_audit": checkpoint_input_audit,
            "stage1_results": stage1_results,
            "stage1_results_for_stage3": stage1_results_for_stage3,
            "stage1_model_audit": stage1_model_audit,
            "metadata": metadata,
            "stage2_results": stage2_results,
            "stage2_revision_results": stage2_revision_results,
            "stage2_revision_summary": stage2_revision_summary,
            "stage2_aggregate_rankings": aggregate_rankings,
            "label_to_model": label_to_model,
        },
    )
    if stage25_checkpoint:
        print(f"Saved checkpoint to: {stage25_checkpoint}")

    _progress("Stage 3 start")
    stage3_primary_start = perf_counter()
    stage3_result_primary = await stage3_synthesize_final(
        enhanced_context,
        stage1_results_for_stage3,
        stage2_results,
        label_to_model=label_to_model,
        use_structured_analysis=use_structured_analysis,
        template_id=selected_template_id,
        ticker=effective_ticker,
        company_name=selected_company_name,
        exchange=selected_exchange,
        chairman_model=primary_chairman_model,
        market_facts=market_facts,
        evidence_pack=search_results.get("evidence_pack", {}),
    )
    stage3_primary_elapsed = perf_counter() - stage3_primary_start
    _progress(f"Stage 3 primary done in {stage3_primary_elapsed:.1f}s")

    stage3_primary_checkpoint = _write_stage_checkpoint(
        dump_json_path=args.dump_json,
        stage_name="stage3_primary",
        payload={
            "effective_query": effective_query,
            "selection": selection,
            "input_audit": checkpoint_input_audit,
            "stage1_results_for_stage3": stage1_results_for_stage3,
            "stage2_results": stage2_results,
            "stage2_aggregate_rankings": aggregate_rankings,
            "stage2_revision_results": stage2_revision_results,
            "stage2_revision_summary": stage2_revision_summary,
            "label_to_model": label_to_model,
            "stage3_primary_model": primary_chairman_model,
            "stage3_result_primary": stage3_result_primary,
            "metadata": metadata,
        },
    )
    if stage3_primary_checkpoint:
        print(f"Saved checkpoint to: {stage3_primary_checkpoint}")

    try:
        _print_stage3(stage3_result_primary, title="STAGE 3 (PRIMARY CHAIRMAN)")
    except Exception as exc:
        print(f"[warn] Stage 3 primary print failed: {exc}")

    stage3_result_secondary: Dict[str, Any] | None = None
    stage3_secondary_elapsed: Optional[float] = None
    if secondary_chairman_model and secondary_chairman_model != primary_chairman_model:
        _progress(f"Stage 3 secondary start ({secondary_chairman_model})")
        stage3_secondary_start = perf_counter()
        stage3_result_secondary = await stage3_synthesize_final(
            enhanced_context,
            stage1_results_for_stage3,
            stage2_results,
            label_to_model=label_to_model,
            use_structured_analysis=use_structured_analysis,
            template_id=selected_template_id,
            ticker=effective_ticker,
            company_name=selected_company_name,
            exchange=selected_exchange,
            chairman_model=secondary_chairman_model,
            market_facts=market_facts,
            evidence_pack=search_results.get("evidence_pack", {}),
        )
        stage3_secondary_elapsed = perf_counter() - stage3_secondary_start
        _progress(f"Stage 3 secondary done in {stage3_secondary_elapsed:.1f}s")
        try:
            _print_stage3(stage3_result_secondary, title="STAGE 3 (SECONDARY CHAIRMAN)")
        except Exception as exc:
            print(f"[warn] Stage 3 secondary print failed: {exc}")

    stage3_comparison = _build_stage3_comparison(
        stage3_result_primary,
        stage3_result_secondary,
    )
    try:
        _print_stage3_comparison(stage3_comparison)
    except Exception as exc:
        print(f"[warn] Stage 3 comparison print failed: {exc}")

    stage3_result = stage3_result_primary

    if args.dump_json:
        stage3_memo_files = _write_stage3_memo_files(
            dump_json_path=args.dump_json,
            stage3_result_primary=stage3_result_primary,
            stage3_result_secondary=stage3_result_secondary,
        )
        payload = {
            "effective_query": effective_query,
            "stage1_query_sent": stage1_effective_query,
            "input_audit": {
                "selection": selection,
                "stage1_research_brief": stage1_effective_research_brief,
                "market_facts_query_prefix": market_facts_query_prefix,
                "market_facts": market_facts or {},
                "primary_injection_bundle_meta": injection_bundle_meta,
                "primary_injection_source_rows_count": len(injection_bundle_rows),
            },
            "stage1_results": stage1_results,
            "stage1_results_for_stage3": stage1_results_for_stage3,
            "stage1_model_audit": stage1_model_audit,
            "stage2_results": stage2_results,
            "stage2_aggregate_rankings": aggregate_rankings,
            "stage2_revision_results": stage2_revision_results,
            "stage2_revision_summary": stage2_revision_summary,
            "stage3_primary_model": primary_chairman_model,
            "stage3_secondary_model": secondary_chairman_model,
            "stage3_timing_seconds": {
                "primary": round(stage3_primary_elapsed, 3),
                "secondary": round(stage3_secondary_elapsed, 3)
                if stage3_secondary_elapsed is not None
                else None,
            },
            "stage3_result_primary": stage3_result_primary,
            "stage3_result_secondary": stage3_result_secondary,
            "stage3_comparison": stage3_comparison,
            "stage3_memo_files": stage3_memo_files,
            "stage3_result": stage3_result,
            "metadata": metadata,
            "selection": selection,
        }
        _dump_json_atomic(Path(args.dump_json), payload)
        print(f"\nSaved JSON output to: {args.dump_json}")
        for memo_path in stage3_memo_files:
            print(f"Saved Stage 3 memo to: {memo_path}")
        final_checkpoint = _write_stage_checkpoint(
            dump_json_path=args.dump_json,
            stage_name="stage3_full",
            payload=payload,
        )
        if final_checkpoint:
            print(f"Saved checkpoint to: {final_checkpoint}")

    _progress(f"Run complete in {perf_counter() - total_start:.1f}s")
    print("\nMVP quality test complete.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run end-to-end MVP financial quality score test.",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help=(
            "Optional company/query hint for template/company detection. "
            "The rendered template Stage 1 prompt is used as the Stage 1 task prompt."
        ),
    )
    parser.add_argument(
        "--brief-include-rubric",
        type=str,
        choices=["auto", "always", "never"],
        default="auto",
        help=(
            "Control whether template rubric is included in Stage 1 research_brief. "
            "auto preserves existing behavior."
        ),
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Optional ticker (e.g. WWI, BHP).",
    )
    parser.add_argument(
        "--template-id",
        type=str,
        default=None,
        help="Optional explicit template ID override for structured synthesis.",
    )
    parser.add_argument(
        "--company-type",
        type=str,
        default=None,
        help="Optional company type (e.g., gold_miner, pharma_biotech, software_saas).",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default=None,
        help="Optional exchange (e.g., asx, nyse, nasdaq, tsx, tsxv, lse, aim).",
    )
    parser.add_argument(
        "--secondary-chairman-model",
        type=str,
        default=None,
        help=(
            "Optional secondary chairman model for side-by-side Stage 3 output "
            "(e.g., openai/gpt-5.3)."
        ),
    )
    parser.add_argument(
        "--dump-json",
        type=str,
        default=None,
        help="Optional path to write full JSON output.",
    )
    parser.add_argument(
        "--dry-run-input",
        action="store_true",
        help="Print exact Stage 1 prompt/payload and exit without API calls.",
    )
    parser.add_argument(
        "--stage1-only",
        action="store_true",
        help="Run only Stage 1 and exit (skips Stage 2/3).",
    )
    parser.add_argument(
        "--stage2-revision-pass",
        type=str,
        choices=["auto", "on", "off"],
        default="on",
        help=(
            "Run Stage 2.5 self-revision pass. "
            "on=enabled by default, off=disable, auto=use .env STAGE2_REVISION_PASS_ENABLED."
        ),
    )
    parser.add_argument(
        "--reuse-recent-bundle",
        action="store_true",
        help=(
            "Reuse a recent injection bundle (<=24h) for the same ticker. "
            "Default behavior is fresh prepass for each run."
        ),
    )
    parser.add_argument(
        "--diagnostic-mode",
        action="store_true",
        help="Allow Stage 1 execution while SYSTEM_ENABLED=false (audit-only).",
    )
    parser.add_argument(
        "--supplementary-context-file",
        type=str,
        default=None,
        help="Optional supplementary document (.pdf, .md, .txt, .json) appended to the Stage 1 research brief.",
    )
    return parser


if __name__ == "__main__":
    _ensure_pymupdf_runtime()
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))
