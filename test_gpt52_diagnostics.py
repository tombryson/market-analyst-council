"""Grid-search diagnostics for Stage 1 GPT-5.2 behavior.

This runner is profile-driven:
- Base settings come from `.env`
- Optional overlays come from `.env.<profile>` via ENV_PROFILE

Each profile run executes Stage 1 only in diagnostic mode so it can run while
SYSTEM_ENABLED=false. No runtime parameter overrides are applied.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
from statistics import mean, median
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run GPT-5.2 Stage-1 diagnostics across env profiles.")
    parser.add_argument("--ticker", default="WWI")
    parser.add_argument("--template-id", default="gold_miner")
    parser.add_argument("--exchange", default="asx")
    parser.add_argument("--query-mode", default="template_only", choices=["template_only", "user"])
    parser.add_argument("--query", default=None)
    parser.add_argument(
        "--profiles",
        default="current_env",
        help="Comma-separated ENV_PROFILE values. Use 'current_env' for base .env only.",
    )
    parser.add_argument("--out-dir", default="/tmp/gpt52_diag")
    parser.add_argument("--run-timeout-seconds", type=int, default=2400)
    parser.add_argument("--repeats", type=int, default=3, help="Number of runs per profile.")
    parser.add_argument("--cooldown-seconds", type=float, default=3.0, help="Sleep between runs.")
    parser.add_argument(
        "--stop-on-consecutive-failures",
        type=int,
        default=2,
        help="Stop remaining repeats for a profile after this many consecutive hard failures.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _scan_log_signatures(log_text: str) -> Dict[str, int]:
    patterns = {
        "stream_payload_empty": r"Perplexity stream ended without response payload or text",
        "perplexity_timeout": r"Perplexity request timed out",
        "openrouter_error": r"Error querying model openai/gpt-5\.2",
        "http_402": r"\b402\b",
        "http_429": r"\b429\b",
        "http_500": r"\b500\b",
        "http_504": r"\b504\b",
        "conformance_gate_failed": r"conformance gate failed",
        "second_pass_empty_response": r"Stage1 second-pass empty response model=openai/gpt-5\.2",
    }
    counts: Dict[str, int] = {}
    for key, pattern in patterns.items():
        counts[key] = len(re.findall(pattern, log_text, flags=re.IGNORECASE))
    return counts


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _extract_symbol_from_ticker(raw_ticker: str) -> str:
    text = str(raw_ticker or "").strip().upper()
    if not text:
        return ""
    parts = [part for part in re.split(r"[:/\s]+", text) if part]
    return parts[-1] if parts else ""


def _build_entity_terms(payload: Dict[str, Any], fallback_ticker: str) -> Tuple[List[str], str]:
    selection = payload.get("selection", {}) or {}
    company_name = str(selection.get("company_name", "")).strip()
    ticker = str((payload.get("input_audit", {}) or {}).get("ticker", "")).strip()
    if not ticker:
        ticker = str(fallback_ticker or "").strip()
    symbol = _extract_symbol_from_ticker(ticker)

    terms: set[str] = set()
    if symbol:
        terms.add(symbol.lower())
    if company_name:
        lowered = re.sub(r"[^a-z0-9 ]+", " ", company_name.lower())
        lowered = re.sub(r"\s+", " ", lowered).strip()
        if lowered:
            terms.add(lowered)
            no_suffix = re.sub(r"\b(limited|ltd|inc|corp|corporation|plc)\b", "", lowered)
            no_suffix = re.sub(r"\s+", " ", no_suffix).strip()
            if no_suffix:
                terms.add(no_suffix)
            words = [word for word in no_suffix.split(" ") if len(word) >= 2]
            if len(words) >= 2:
                terms.add(" ".join(words[:2]))
            if len(words) >= 3:
                terms.add(" ".join(words[:3]))

    cleaned = sorted(term for term in terms if len(term) >= 3)
    return cleaned[:10], symbol


def _extract_source_relevance(
    run_result: Dict[str, Any],
    *,
    entity_terms: List[str],
    expected_symbol: str,
) -> Dict[str, Any]:
    results = run_result.get("results", []) or []
    total = 0
    relevant = 0
    wrong_company_hints = 0
    mismatch_examples: List[str] = []
    expected = str(expected_symbol or "").upper()

    for row in results:
        if not isinstance(row, dict):
            continue
        total += 1
        title = str(row.get("title", "")).strip()
        excerpt = str(row.get("decoded_excerpt", "")).strip() or str(row.get("content", "")).strip()
        blob = f"{title}\n{excerpt}".lower()
        hit = any(term in blob for term in entity_terms)
        if hit:
            relevant += 1

        # Detect obvious ticker contamination (e.g., ASX:EVN when targeting ASX:WWI).
        asx_mentions = re.findall(r"\bASX[:\s]+([A-Z]{2,5})\b", f"{title}\n{excerpt}".upper())
        mention_mismatch = any(m != expected for m in asx_mentions if expected)
        if mention_mismatch and not hit:
            wrong_company_hints += 1
            if len(mismatch_examples) < 3:
                mismatch_examples.append(title or "(untitled)")

    ratio = (relevant / total) if total > 0 else 0.0
    wrong_source_count = max(0, total - relevant)
    return {
        "source_count": total,
        "source_relevant_count": relevant,
        "source_relevance_ratio": ratio,
        "wrong_company_source_count": wrong_source_count,
        "wrong_company_hint_count": wrong_company_hints,
        "wrong_company_examples": mismatch_examples,
    }


def _compute_run_scores(diag: Dict[str, Any], log_signatures: Dict[str, int]) -> Dict[str, Any]:
    provider_error = str(diag.get("provider_error", "") or "")
    second_pass_enabled = bool(diag.get("second_pass_enabled", False))
    second_pass_success = bool(diag.get("second_pass_success", False))
    second_pass_error = str(diag.get("second_pass_error", "") or "")

    hard_fail = bool(
        provider_error
        or (second_pass_enabled and not second_pass_success)
        or second_pass_error in {"empty_response", "timeline_guard_failed"}
    )

    reliability = 1.0
    if provider_error:
        reliability -= 0.40
    if second_pass_enabled and not second_pass_success:
        reliability -= 0.35
    if second_pass_error:
        reliability -= 0.20
    if _safe_int(log_signatures.get("stream_payload_empty")) > 0:
        reliability -= 0.15
    if _safe_int(log_signatures.get("perplexity_timeout")) > 0:
        reliability -= 0.15
    if _safe_int(log_signatures.get("second_pass_empty_response")) > 0:
        reliability -= 0.20
    reliability = max(0.0, min(1.0, reliability))

    compliance = _safe_float(diag.get("second_pass_compliance_score"), 0.0)
    template_ok = 1.0 if bool(diag.get("stage1_final_template_compliant", False)) else (
        1.0 if bool(diag.get("template_compliant", False)) else 0.0
    )
    source_relevance = _safe_float(diag.get("source_relevance_ratio"), 0.0)
    numeric_citation = _safe_float(diag.get("second_pass_numeric_citation_pct"), 0.0)
    response_chars = _safe_int(diag.get("second_pass_response_chars"))
    response_density = min(1.0, response_chars / 8000.0) if response_chars > 0 else 0.0

    quality = (
        (0.40 * compliance)
        + (0.20 * template_ok)
        + (0.20 * source_relevance)
        + (0.10 * numeric_citation)
        + (0.10 * response_density)
    )
    quality = max(0.0, min(1.0, quality))

    return {
        "hard_fail": hard_fail,
        "reliability_score": round(reliability, 4),
        "quality_score": round(quality, 4),
    }


def _extract_diag(payload: Dict[str, Any], runtime_s: float, ticker: str) -> Dict[str, Any]:
    stage1_results = payload.get("stage1_results", []) or []
    metadata = payload.get("metadata", {}) or {}
    per_runs = metadata.get("per_model_research_runs", []) or []

    gpt_run = None
    for item in per_runs:
        if str(item.get("model", "")).strip().lower() == "openai/gpt-5.2":
            gpt_run = item
            break
    if gpt_run is None and per_runs:
        gpt_run = per_runs[0]

    run_result = (gpt_run or {}).get("result", {}) or {}
    pm = run_result.get("provider_metadata", {}) or {}
    decode = pm.get("source_decoding", {}) or {}
    attempt_history = pm.get("stage1_attempt_history", []) or []
    entity_terms, expected_symbol = _build_entity_terms(payload, ticker)
    source_relevance = _extract_source_relevance(
        run_result,
        entity_terms=entity_terms,
        expected_symbol=expected_symbol,
    )

    # Find corresponding stage1 response text entry
    stage1_entry = None
    for item in stage1_results:
        if str(item.get("model", "")).strip().lower() == "openai/gpt-5.2":
            stage1_entry = item
            break
    if stage1_entry is None and stage1_results:
        stage1_entry = stage1_results[0]

    second_pass_compliance = pm.get("stage1_second_pass_compliance_score")
    if second_pass_compliance is not None:
        try:
            second_pass_compliance = round(float(second_pass_compliance), 4)
        except Exception:
            pass

    return {
        "runtime_s": round(runtime_s, 2),
        "models_attempted": metadata.get("models_attempted", []),
        "models_succeeded": metadata.get("models_succeeded", []),
        "gpt_only_models": sorted(metadata.get("models_attempted", [])) == ["openai/gpt-5.2"],
        "stage1_response_chars": len(str((stage1_entry or {}).get("response", "") or "")),
        "result_count": run_result.get("result_count"),
        "provider_error": run_result.get("error"),
        "preset": pm.get("preset"),
        "reasoning_effort_applied": pm.get("reasoning_effort_applied"),
        "request_attempts": pm.get("request_attempts"),
        "stream_requested": pm.get("stream_requested"),
        "stream_used": pm.get("stream_used"),
        "stream_event_count": pm.get("stream_event_count"),
        "stream_completed_event_seen": pm.get("stream_completed_event_seen"),
        "decode_attempted": decode.get("attempted"),
        "decode_decoded": decode.get("decoded"),
        "decode_failed": decode.get("failed"),
        "stage1_attempts": pm.get("stage1_attempts"),
        "stage1_attempt_history": attempt_history,
        "template_compliant": pm.get("template_compliant"),
        "template_compliance_reason": pm.get("template_compliance_reason"),
        "second_pass_enabled": pm.get("stage1_second_pass_enabled"),
        "second_pass_success": pm.get("stage1_second_pass_success"),
        "second_pass_attempts": pm.get("stage1_second_pass_attempts"),
        "second_pass_warning": pm.get("stage1_second_pass_warning"),
        "second_pass_error": pm.get("stage1_second_pass_error"),
        "second_pass_response_chars": pm.get("stage1_second_pass_response_chars"),
        "second_pass_prompt_chars": pm.get("stage1_second_pass_prompt_chars"),
        "second_pass_compliance_score": second_pass_compliance,
        "second_pass_compliance_rating": pm.get("stage1_second_pass_compliance_rating"),
        "second_pass_numeric_citation_pct": pm.get("stage1_second_pass_citation_numeric_citation_pct"),
        "second_pass_citation_gate_passed": pm.get("stage1_second_pass_citation_gate_passed"),
        "second_pass_timeline_guard_passed": pm.get("stage1_second_pass_timeline_guard_passed"),
        "stage1_final_template_compliant": pm.get("stage1_final_template_compliant"),
        "stage1_final_template_reason": pm.get("stage1_final_template_reason"),
        "second_pass_last_finish_reason": pm.get("stage1_second_pass_last_finish_reason"),
        "second_pass_last_reasoning_effort": pm.get("stage1_second_pass_last_reasoning_effort"),
        "second_pass_last_usage_cost": _safe_float(
            (pm.get("stage1_second_pass_last_usage", {}) or {}).get("cost"),
            0.0,
        ),
        "entity_terms": entity_terms,
        "expected_symbol": expected_symbol,
        **source_relevance,
        "config_snapshot": {
            "stage1_max_attempts": metadata.get("stage1_max_attempts"),
            "stage1_preset_strategy": metadata.get("stage1_preset_strategy"),
            "stage1_preset_deep": metadata.get("stage1_preset_deep"),
            "stage1_preset_advanced": metadata.get("stage1_preset_advanced"),
            "stage1_openai_base_guardrails_enabled": metadata.get("stage1_openai_guardrails_enabled"),
            "stage1_openai_base_max_sources": metadata.get("stage1_openai_base_max_sources"),
            "stage1_openai_base_max_steps": metadata.get("stage1_openai_base_max_steps"),
            "stage1_openai_base_reasoning_effort": metadata.get("stage1_openai_base_reasoning_effort"),
            "stage1_second_pass_enabled": metadata.get("stage1_second_pass_enabled"),
            "stage1_second_pass_timeout_seconds": metadata.get("stage1_second_pass_timeout_seconds"),
            "stage1_second_pass_max_attempts": metadata.get("stage1_second_pass_max_attempts"),
            "stage1_second_pass_reasoning_effort": metadata.get("stage1_second_pass_reasoning_effort"),
            "max_sources": metadata.get("stage1_openai_base_max_sources") or None,
            "shared_retrieval_used": metadata.get("stage1_shared_retrieval_used"),
        },
    }


def _validate_profile_file(profile: str, cwd: Path) -> None:
    if profile == "current_env":
        return
    profile_path = cwd / f".env.{profile}"
    if not profile_path.exists():
        raise FileNotFoundError(
            f"Missing profile file: {profile_path}. Create it from .env.{profile}.example or .env.example."
        )


def _run_profile_once(
    args: argparse.Namespace,
    profile: str,
    out_dir: Path,
    cwd: Path,
    run_index: int,
) -> Dict[str, Any]:
    _validate_profile_file(profile, cwd)

    suffix = f"{profile}__run{run_index}"
    log_path = out_dir / f"{suffix}.log"
    json_path = out_dir / f"{suffix}.json"
    env = dict(os.environ)
    env["ENV_PROFILE"] = "" if profile == "current_env" else profile

    cmd: List[str] = [
        sys.executable,
        "test_quality_mvp.py",
        "--query-mode",
        args.query_mode,
        "--ticker",
        args.ticker,
        "--template-id",
        args.template_id,
        "--exchange",
        args.exchange,
        "--stage1-only",
        "--diagnostic-mode",
        "--dump-json",
        str(json_path),
    ]
    if args.query_mode == "user" and args.query:
        cmd.extend(["--query", args.query])

    if args.dry_run:
        return {
            "profile": profile,
            "run_index": run_index,
            "ok": True,
            "dry_run": True,
            "cmd": cmd,
            "env_profile": env["ENV_PROFILE"],
            "log_path": str(log_path),
            "json_path": str(json_path),
        }

    start = time.perf_counter()
    proc = subprocess.run(
        cmd,
        env=env,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=max(60, int(args.run_timeout_seconds)),
    )
    runtime_s = time.perf_counter() - start
    log_text = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    log_path.write_text(log_text, encoding="utf-8")

    payload: Dict[str, Any] = {}
    if json_path.exists():
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}

    result: Dict[str, Any] = {
        "profile": profile,
        "run_index": run_index,
        "env_profile": env["ENV_PROFILE"],
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "runtime_s": round(runtime_s, 2),
        "cmd": cmd,
        "log_path": str(log_path),
        "json_path": str(json_path),
        "log_signatures": _scan_log_signatures(log_text),
        "log_tail": "\n".join(log_text.splitlines()[-60:]),
    }
    diag = _extract_diag(payload, runtime_s=runtime_s, ticker=args.ticker) if payload else {}
    result["diagnostics"] = diag
    result["scores"] = _compute_run_scores(diag, result["log_signatures"])
    result["hard_fail"] = bool((result.get("scores") or {}).get("hard_fail", False))
    return result


def _percent(value: float) -> float:
    return round(float(value) * 100.0, 2)


def _profile_stats(profile: str, runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not runs:
        return {
            "profile": profile,
            "runs": 0,
        }

    hard_fails = sum(1 for run in runs if bool(run.get("hard_fail", False)))
    success = sum(1 for run in runs if bool(run.get("ok", False)))
    second_pass_success = sum(
        1
        for run in runs
        if bool((run.get("diagnostics", {}) or {}).get("second_pass_success", False))
    )
    runtimes = [_safe_float(run.get("runtime_s")) for run in runs]
    reliabilities = [_safe_float((run.get("scores", {}) or {}).get("reliability_score")) for run in runs]
    qualities = [_safe_float((run.get("scores", {}) or {}).get("quality_score")) for run in runs]
    source_rel = [_safe_float((run.get("diagnostics", {}) or {}).get("source_relevance_ratio")) for run in runs]
    wrong_counts = [_safe_int((run.get("diagnostics", {}) or {}).get("wrong_company_source_count")) for run in runs]
    source_counts = [_safe_int((run.get("diagnostics", {}) or {}).get("source_count")) for run in runs]
    costs = [_safe_float((run.get("diagnostics", {}) or {}).get("second_pass_last_usage_cost")) for run in runs]

    avg_wrong_ratio = 0.0
    total_sources = sum(source_counts)
    if total_sources > 0:
        avg_wrong_ratio = sum(wrong_counts) / float(total_sources)

    quality_std = 0.0
    if len(qualities) >= 2:
        q_mean = mean(qualities)
        quality_std = math.sqrt(sum((q - q_mean) ** 2 for q in qualities) / len(qualities))

    quality_consistency_adjusted = max(0.0, mean(qualities) - (0.5 * quality_std))
    second_pass_rate = second_pass_success / len(runs)
    hard_fail_rate = hard_fails / len(runs)
    success_rate = success / len(runs)
    reliability_mean = mean(reliabilities)
    quality_mean = mean(qualities)
    source_relevance_mean = mean(source_rel)

    stable_gate = bool(
        success_rate >= 0.67
        and second_pass_rate >= 0.67
        and reliability_mean >= 0.70
        and avg_wrong_ratio <= 0.35
    )
    optimized_gate = bool(
        stable_gate
        and quality_mean >= 0.55
        and source_relevance_mean >= 0.70
        and quality_consistency_adjusted >= 0.45
    )

    return {
        "profile": profile,
        "runs": len(runs),
        "success_rate": round(success_rate, 4),
        "hard_fail_rate": round(hard_fail_rate, 4),
        "second_pass_success_rate": round(second_pass_rate, 4),
        "reliability_mean": round(reliability_mean, 4),
        "quality_mean": round(quality_mean, 4),
        "quality_std": round(quality_std, 4),
        "quality_consistency_adjusted": round(quality_consistency_adjusted, 4),
        "source_relevance_mean": round(source_relevance_mean, 4),
        "wrong_company_source_ratio": round(avg_wrong_ratio, 4),
        "runtime_p50_s": round(median(runtimes), 2),
        "runtime_p90_s": round(sorted(runtimes)[max(0, math.ceil(0.9 * len(runtimes)) - 1)], 2),
        "second_pass_cost_mean": round(mean(costs), 6) if costs else 0.0,
        "stable_gate_passed": stable_gate,
        "optimized_gate_passed": optimized_gate,
    }


def _pick_recommendations(profile_stats: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not profile_stats:
        return {"stable_profile": "", "optimized_profile": ""}

    stable_sorted = sorted(
        profile_stats,
        key=lambda item: (
            _safe_float(item.get("stable_gate_passed"), 0.0),
            _safe_float(item.get("reliability_mean"), 0.0),
            _safe_float(item.get("second_pass_success_rate"), 0.0),
            -_safe_float(item.get("runtime_p50_s"), 9_999.0),
        ),
        reverse=True,
    )
    stable_profile = stable_sorted[0].get("profile", "")

    optimized_candidates = [
        item for item in profile_stats if bool(item.get("stable_gate_passed", False))
    ] or profile_stats
    optimized_sorted = sorted(
        optimized_candidates,
        key=lambda item: (
            _safe_float(item.get("optimized_gate_passed"), 0.0),
            _safe_float(item.get("quality_consistency_adjusted"), 0.0),
            _safe_float(item.get("quality_mean"), 0.0),
            _safe_float(item.get("reliability_mean"), 0.0),
            -_safe_float(item.get("runtime_p50_s"), 9_999.0),
        ),
        reverse=True,
    )
    optimized_profile = optimized_sorted[0].get("profile", "")
    return {
        "stable_profile": stable_profile,
        "optimized_profile": optimized_profile,
    }


def main() -> None:
    args = _parse_args()
    cwd = Path(__file__).resolve().parent
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    profiles = [item.strip() for item in args.profiles.split(",") if item.strip()]
    results: List[Dict[str, Any]] = []
    for profile in profiles:
        print(f"[diag] profile={profile} repeats={args.repeats}")
        consecutive_hard_fails = 0
        for run_index in range(1, max(1, int(args.repeats)) + 1):
            print(f"[diag] running {profile} run={run_index}/{args.repeats}")
            try:
                result = _run_profile_once(args, profile, out_dir, cwd, run_index)
            except subprocess.TimeoutExpired:
                result = {
                    "profile": profile,
                    "run_index": run_index,
                    "ok": False,
                    "hard_fail": True,
                    "error": f"Subprocess timed out after {args.run_timeout_seconds}s",
                }
            except Exception as exc:  # noqa: BLE001
                result = {
                    "profile": profile,
                    "run_index": run_index,
                    "ok": False,
                    "hard_fail": True,
                    "error": f"{type(exc).__name__}: {exc}",
                }

            results.append(result)
            if bool(result.get("hard_fail", False)):
                consecutive_hard_fails += 1
            else:
                consecutive_hard_fails = 0

            if (
                args.stop_on_consecutive_failures > 0
                and consecutive_hard_fails >= int(args.stop_on_consecutive_failures)
            ):
                print(
                    f"[diag] stopping profile={profile} early after "
                    f"{consecutive_hard_fails} consecutive hard failures"
                )
                break
            if run_index < int(args.repeats) and args.cooldown_seconds > 0:
                time.sleep(float(args.cooldown_seconds))

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in results:
        grouped.setdefault(str(item.get("profile", "unknown")), []).append(item)

    profile_stats = [_profile_stats(profile, runs) for profile, runs in grouped.items()]
    recommendations = _pick_recommendations(profile_stats)

    summary_path = out_dir / "summary.json"
    summary_payload = {
        "model": "openai/gpt-5.2",
        "profiles": profiles,
        "results": results,
        "profile_stats": profile_stats,
        "recommendations": recommendations,
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    print("\n=== GPT-5.2 RUN SUMMARY ===")
    for item in results:
        profile = item.get("profile", "")
        run_index = item.get("run_index", "?")
        ok = bool(item.get("ok", False))
        diagnostics = item.get("diagnostics", {}) or {}
        scores = item.get("scores", {}) or {}
        print(
            f"- {profile} run={run_index}: ok={ok} hard_fail={item.get('hard_fail', False)} "
            f"runtime_s={item.get('runtime_s')} "
            f"preset={diagnostics.get('preset')} stage1_attempts={diagnostics.get('stage1_attempts')} "
            f"2nd_pass_success={diagnostics.get('second_pass_success')} "
            f"2nd_pass_score={diagnostics.get('second_pass_compliance_score')} "
            f"source_rel={_percent(_safe_float(diagnostics.get('source_relevance_ratio')))}% "
            f"rel_score={scores.get('reliability_score')} qual_score={scores.get('quality_score')}"
        )

    print("\n=== GPT-5.2 PROFILE STATS ===")
    for item in sorted(profile_stats, key=lambda x: x.get("profile", "")):
        print(
            f"- {item.get('profile')}: runs={item.get('runs')} "
            f"success={_percent(item.get('success_rate', 0.0))}% "
            f"hard_fail={_percent(item.get('hard_fail_rate', 0.0))}% "
            f"2nd_pass_success={_percent(item.get('second_pass_success_rate', 0.0))}% "
            f"reliability={item.get('reliability_mean')} "
            f"quality={item.get('quality_mean')}±{item.get('quality_std')} "
            f"source_rel={_percent(item.get('source_relevance_mean', 0.0))}% "
            f"wrong_src={_percent(item.get('wrong_company_source_ratio', 0.0))}% "
            f"p50={item.get('runtime_p50_s')}s p90={item.get('runtime_p90_s')}s "
            f"stable_gate={item.get('stable_gate_passed')} "
            f"optimized_gate={item.get('optimized_gate_passed')}"
        )

    print("\n=== RECOMMENDATIONS ===")
    print(f"- Most stable profile: {recommendations.get('stable_profile') or '(none)'}")
    print(f"- Most optimized profile: {recommendations.get('optimized_profile') or '(none)'}")
    print(f"\nWrote summary: {summary_path}")


if __name__ == "__main__":
    main()
