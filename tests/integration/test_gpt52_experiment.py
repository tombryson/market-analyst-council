"""Rigorous GPT-5.2 Stage-1 experiment runner.

This script runs randomized, blocked, repeatable experiments over parameter arms
and produces statistical summaries intended for operational decisions.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Arm:
    arm_id: str
    profile: str
    env_overrides: Dict[str, str]
    description: str
    cost_rank: int


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run randomized GPT-5.2 Stage-1 experiments with statistical summaries."
    )
    parser.add_argument("--arms-file", required=True, help="JSON file with experiment arms.")
    parser.add_argument("--ticker", default="WWI")
    parser.add_argument("--template-id", default="gold_miner")
    parser.add_argument("--exchange", default="asx")
    parser.add_argument("--query-mode", default="template_only", choices=["template_only", "user"])
    parser.add_argument("--query", default=None)
    parser.add_argument(
        "--brief-include-rubric",
        default="auto",
        choices=["auto", "always", "never"],
        help="Pass-through to test_quality_mvp.py --brief-include-rubric.",
    )
    parser.add_argument("--model", default="openai/gpt-5.2")
    parser.add_argument("--replicates", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--schedule-mode",
        default="randomized_blocked",
        choices=["randomized_blocked", "cost_ascending_blocked"],
        help=(
            "randomized_blocked: shuffle arms each replicate. "
            "cost_ascending_blocked: run arms from cheapest to most expensive each replicate."
        ),
    )
    parser.add_argument("--run-timeout-seconds", type=int, default=2400)
    parser.add_argument("--cooldown-seconds", type=float, default=5.0)
    parser.add_argument("--max-consecutive-hard-fails-per-arm", type=int, default=3)
    parser.add_argument("--out-dir", default="/tmp/gpt52_experiment")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _load_arms(path: Path) -> List[Arm]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("arms file must be a non-empty JSON array")
    arms: List[Arm] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("each arm entry must be an object")
        arm_id = str(item.get("id", "")).strip()
        if not arm_id:
            raise ValueError("arm entry missing 'id'")
        if arm_id in seen:
            raise ValueError(f"duplicate arm id: {arm_id}")
        seen.add(arm_id)
        profile = str(item.get("profile", "current_env")).strip() or "current_env"
        desc = str(item.get("description", "")).strip()
        cost_rank = _safe_int(item.get("cost_rank"), 100)
        if cost_rank < 0:
            cost_rank = 100
        overrides = item.get("env_overrides", {}) or {}
        if not isinstance(overrides, dict):
            raise ValueError(f"arm {arm_id}: env_overrides must be an object")
        normalized: Dict[str, str] = {}
        for key, value in overrides.items():
            normalized[str(key)] = str(value)
        arms.append(
            Arm(
                arm_id=arm_id,
                profile=profile,
                env_overrides=normalized,
                description=desc,
                cost_rank=cost_rank,
            )
        )
    return arms


def _schedule(
    arms: List[Arm],
    replicates: int,
    seed: int,
    mode: str,
) -> List[Tuple[int, int, Arm]]:
    rng = random.Random(seed)
    out: List[Tuple[int, int, Arm]] = []
    for rep in range(1, replicates + 1):
        if mode == "cost_ascending_blocked":
            # Deterministic low-to-high cost progression with seeded tie shuffle.
            grouped: Dict[int, List[Arm]] = {}
            for arm in arms:
                grouped.setdefault(int(arm.cost_rank), []).append(arm)
            ordered: List[Arm] = []
            for rank in sorted(grouped.keys()):
                bucket = grouped[rank]
                rng.shuffle(bucket)
                ordered.extend(bucket)
        else:
            ordered = list(arms)
            rng.shuffle(ordered)
        for pos, arm in enumerate(ordered, start=1):
            out.append((rep, pos, arm))
    return out


def _scan_log_signatures(text: str) -> Dict[str, int]:
    patterns = {
        "api_timeout": r"\bapi timeout\b",
        "api_http_error": r"\bapi http_error\b",
        "stream_empty": r"stream ended without response payload",
        "invalid_request": r"invalid request|invalid_request",
        "openrouter_error": r"Error querying model openai/gpt-5\.2",
        "http_402": r"(?:status=|api error:\s*|http[_\s-]?code[=:]?\s*)402\b",
        "http_429": r"(?:status=|api error:\s*|http[_\s-]?code[=:]?\s*)429\b",
        "http_500": r"(?:status=|api error:\s*|http[_\s-]?code[=:]?\s*)500\b",
        "http_504": r"(?:status=|api error:\s*|http[_\s-]?code[=:]?\s*)504\b",
    }
    out: Dict[str, int] = {}
    for key, pat in patterns.items():
        out[key] = len(re.findall(pat, text, flags=re.IGNORECASE))
    return out


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


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "on"}:
        return True
    if s in {"0", "false", "no", "off"}:
        return False
    return default


def _effective_subprocess_timeout(args: argparse.Namespace, arm: Arm) -> int:
    """Prevent harness truncation when provider-level timeouts are higher."""
    configured = max(60, int(args.run_timeout_seconds))
    env = arm.env_overrides or {}

    provider_timeout = max(0, _safe_int(env.get("PERPLEXITY_TIMEOUT_SECONDS"), 0))
    stage1_attempts = max(1, _safe_int(env.get("PERPLEXITY_STAGE1_MAX_ATTEMPTS"), 1))
    second_pass_enabled = _to_bool(env.get("PERPLEXITY_STAGE1_SECOND_PASS_ENABLED"), False)
    second_pass_timeout = max(
        0,
        _safe_int(env.get("PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS"), 0),
    )
    second_pass_attempts = max(
        1,
        _safe_int(env.get("PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS"), 1),
    )

    estimated = 0
    if provider_timeout > 0:
        estimated += provider_timeout * stage1_attempts
    if second_pass_enabled and second_pass_timeout > 0:
        estimated += second_pass_timeout * second_pass_attempts
    if estimated > 0:
        estimated += 90  # startup + teardown guard band

    return max(configured, estimated)


def _extract_model_run(payload: Dict[str, Any], model: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    metadata = payload.get("metadata", {}) or {}
    per_model = metadata.get("per_model_research_runs", []) or []
    stage1_results = payload.get("stage1_results", []) or []

    run_obj: Dict[str, Any] = {}
    for item in per_model:
        if str(item.get("model", "")).strip().lower() == model.strip().lower():
            run_obj = item
            break
    if not run_obj and per_model:
        run_obj = per_model[0]

    stage1_obj: Dict[str, Any] = {}
    for item in stage1_results:
        if str(item.get("model", "")).strip().lower() == model.strip().lower():
            stage1_obj = item
            break
    if not stage1_obj and stage1_results:
        stage1_obj = stage1_results[0]

    result = (run_obj or {}).get("result", {}) or {}
    provider_meta = result.get("provider_metadata", {}) or {}
    return metadata, result, provider_meta


def _extract_run_metrics(
    payload: Dict[str, Any],
    *,
    model: str,
    returncode: int,
    runtime_s: float,
    timed_out: bool,
    log_signatures: Dict[str, int],
) -> Dict[str, Any]:
    metadata, result, provider_meta = _extract_model_run(payload, model)
    response_text = ""
    for item in payload.get("stage1_results", []) or []:
        if str(item.get("model", "")).strip().lower() == model.strip().lower():
            response_text = str(item.get("response", "") or "")
            break
    if not response_text:
        stage1_results = payload.get("stage1_results", []) or []
        if stage1_results:
            response_text = str(stage1_results[0].get("response", "") or "")

    provider_error = str(result.get("error", "") or "")
    result_count = _safe_int(result.get("result_count"), 0)
    stage1_chars = len(response_text or "")

    second_pass_enabled = bool(provider_meta.get("stage1_second_pass_enabled", False))
    second_pass_success = bool(provider_meta.get("stage1_second_pass_success", False))
    second_pass_error = str(provider_meta.get("stage1_second_pass_error", "") or "")

    template_final = provider_meta.get("stage1_final_template_compliant")
    if template_final is None:
        template_final = provider_meta.get("template_compliant")
    template_final = None if template_final is None else bool(template_final)

    citation_gate = provider_meta.get("stage1_second_pass_citation_gate_passed")
    citation_gate = None if citation_gate is None else bool(citation_gate)
    timeline_guard = provider_meta.get("stage1_second_pass_timeline_guard_passed")
    timeline_guard = None if timeline_guard is None else bool(timeline_guard)

    compliance_score = provider_meta.get("stage1_second_pass_compliance_score")
    if compliance_score is not None:
        compliance_score = _safe_float(compliance_score, 0.0)

    rubric_cov = provider_meta.get("stage1_second_pass_rubric_coverage_pct")
    if rubric_cov is not None:
        rubric_cov = _safe_float(rubric_cov, 0.0)

    numeric_citation = provider_meta.get("stage1_second_pass_citation_numeric_citation_pct")
    if numeric_citation is not None:
        numeric_citation = _safe_float(numeric_citation, 0.0)

    hard_reasons: List[str] = []
    if timed_out:
        hard_reasons.append("subprocess_timeout")
    if returncode != 0:
        hard_reasons.append("subprocess_nonzero")
    if provider_error:
        hard_reasons.append("provider_error")
    if result_count <= 0:
        hard_reasons.append("result_count_zero")
    if stage1_chars <= 0:
        hard_reasons.append("empty_stage1_response")

    soft_warnings: List[str] = []
    if not hard_reasons:
        if second_pass_enabled and not second_pass_success:
            soft_warnings.append("second_pass_failed")
        if second_pass_error:
            soft_warnings.append(f"second_pass_error:{second_pass_error}")
        if template_final is False:
            soft_warnings.append("template_nonconformant")
        if citation_gate is False:
            soft_warnings.append("citation_gate_failed")
        if timeline_guard is False:
            soft_warnings.append("timeline_guard_failed")

    # Composite scores are transparent and only used for ranking among surviving arms.
    quality_components = {
        "compliance": float(compliance_score or 0.0),
        "template": 1.0 if template_final else 0.0,
        "citation": 1.0 if citation_gate else 0.0,
        "timeline": 1.0 if (timeline_guard is True) else 0.0,
        "response_density": min(1.0, stage1_chars / 12000.0) if stage1_chars > 0 else 0.0,
    }
    quality_score = (
        0.40 * quality_components["compliance"]
        + 0.20 * quality_components["template"]
        + 0.15 * quality_components["citation"]
        + 0.15 * quality_components["timeline"]
        + 0.10 * quality_components["response_density"]
    )

    reliability_score = 1.0
    if hard_reasons:
        reliability_score = 0.0
    else:
        if soft_warnings:
            reliability_score -= min(0.6, 0.2 * len(soft_warnings))
        if log_signatures.get("api_timeout", 0) > 0:
            reliability_score -= 0.15
    reliability_score = max(0.0, min(1.0, reliability_score))

    return {
        "runtime_s": round(runtime_s, 2),
        "returncode": returncode,
        "timed_out": timed_out,
        "provider_error": provider_error,
        "result_count": result_count,
        "stage1_response_chars": stage1_chars,
        "models_attempted": metadata.get("models_attempted", []),
        "models_succeeded": metadata.get("models_succeeded", []),
        "stage1_attempts": provider_meta.get("stage1_attempts"),
        "preset": provider_meta.get("preset"),
        "stream_requested": provider_meta.get("stream_requested"),
        "stream_used": provider_meta.get("stream_used"),
        "stream_event_count": provider_meta.get("stream_event_count"),
        "request_attempts": provider_meta.get("request_attempts"),
        "timeout_retry_applied": provider_meta.get("timeout_retry_applied"),
        "reasoning_effort_applied": provider_meta.get("reasoning_effort_applied"),
        "second_pass_enabled": second_pass_enabled,
        "second_pass_success": second_pass_success,
        "second_pass_error": second_pass_error,
        "second_pass_attempts": provider_meta.get("stage1_second_pass_attempts"),
        "template_final_compliant": template_final,
        "citation_gate_passed": citation_gate,
        "timeline_guard_passed": timeline_guard,
        "compliance_score": compliance_score,
        "rubric_coverage_pct": rubric_cov,
        "numeric_citation_pct": numeric_citation,
        "hard_failure": bool(hard_reasons),
        "hard_failure_reasons": hard_reasons,
        "soft_failure": bool(soft_warnings),
        "soft_failure_warnings": soft_warnings,
        "quality_score": round(quality_score, 4),
        "reliability_score": round(reliability_score, 4),
    }


def _wilson_ci(k: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return (0.0, 1.0)
    phat = k / n
    denom = 1.0 + z * z / n
    center = (phat + z * z / (2.0 * n)) / denom
    spread = (z / denom) * math.sqrt((phat * (1.0 - phat) + z * z / (4.0 * n)) / n)
    return (max(0.0, center - spread), min(1.0, center + spread))


def _percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = p * (len(sorted_vals) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_vals[lo]
    frac = idx - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def _bootstrap_mean_ci(values: List[float], seed: int, iters: int = 2000) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if not values:
        return (None, None, None)
    mean_val = sum(values) / len(values)
    if len(values) == 1:
        return (mean_val, mean_val, mean_val)
    rng = random.Random(seed)
    boots: List[float] = []
    n = len(values)
    for _ in range(iters):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        boots.append(sum(sample) / n)
    boots.sort()
    lo = _percentile(boots, 0.025)
    hi = _percentile(boots, 0.975)
    return (mean_val, lo, hi)


def _p90(values: List[float]) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    return _percentile(vals, 0.9)


def _summarize_arm(arm_id: str, rows: List[Dict[str, Any]], seed: int) -> Dict[str, Any]:
    n = len(rows)
    hard_success = [row for row in rows if not row["metrics"]["hard_failure"]]
    hard_fail_count = n - len(hard_success)
    soft_fail_count = sum(1 for row in hard_success if row["metrics"]["soft_failure"])

    hard_success_rate = (len(hard_success) / n) if n else 0.0
    hard_ci_lo, hard_ci_hi = _wilson_ci(len(hard_success), n)
    soft_fail_rate = (soft_fail_count / len(hard_success)) if hard_success else None

    runtimes = [float(row["metrics"]["runtime_s"]) for row in rows]
    runtime_success = [float(row["metrics"]["runtime_s"]) for row in hard_success]
    quality_vals = [float(row["metrics"]["quality_score"]) for row in hard_success]
    rel_vals = [float(row["metrics"]["reliability_score"]) for row in rows]
    comp_vals = [
        float(row["metrics"]["compliance_score"])
        for row in hard_success
        if row["metrics"]["compliance_score"] is not None
    ]

    q_mean, q_lo, q_hi = _bootstrap_mean_ci(quality_vals, seed=seed + hash(arm_id) % 10000)
    c_mean, c_lo, c_hi = _bootstrap_mean_ci(comp_vals, seed=seed + 17 + hash(arm_id) % 10000)

    timeout_events = sum(int((row.get("log_signatures", {}) or {}).get("api_timeout", 0) > 0) for row in rows)
    http500_events = sum(int((row.get("log_signatures", {}) or {}).get("http_500", 0) > 0) for row in rows)
    http429_events = sum(int((row.get("log_signatures", {}) or {}).get("http_429", 0) > 0) for row in rows)
    http402_events = sum(int((row.get("log_signatures", {}) or {}).get("http_402", 0) > 0) for row in rows)

    stable_gate = bool(
        hard_ci_lo >= 0.70
        and (soft_fail_rate is not None and soft_fail_rate <= 0.40)
        and (median(runtime_success) if runtime_success else 10_000.0) <= 1200.0
    )
    quality_gate = bool(
        stable_gate
        and (q_mean or 0.0) >= 0.55
        and (c_mean or 0.0) >= 0.60
    )

    return {
        "arm_id": arm_id,
        "n": n,
        "hard_success_count": len(hard_success),
        "hard_fail_count": hard_fail_count,
        "hard_success_rate": round(hard_success_rate, 4),
        "hard_success_ci95": [round(hard_ci_lo, 4), round(hard_ci_hi, 4)],
        "soft_fail_count": soft_fail_count,
        "soft_fail_rate_on_hard_success": (
            round(float(soft_fail_rate), 4) if soft_fail_rate is not None else None
        ),
        "runtime_median_s_all": round(median(runtimes), 2) if runtimes else None,
        "runtime_p90_s_all": round(_p90(runtimes), 2) if runtimes else None,
        "runtime_median_s_hard_success": round(median(runtime_success), 2) if runtime_success else None,
        "quality_score_mean": round(q_mean, 4) if q_mean is not None else None,
        "quality_score_ci95": [round(q_lo, 4), round(q_hi, 4)] if q_lo is not None else None,
        "compliance_score_mean": round(c_mean, 4) if c_mean is not None else None,
        "compliance_score_ci95": [round(c_lo, 4), round(c_hi, 4)] if c_lo is not None else None,
        "reliability_score_mean": round(sum(rel_vals) / len(rel_vals), 4) if rel_vals else None,
        "timeout_event_runs": timeout_events,
        "http500_event_runs": http500_events,
        "http429_event_runs": http429_events,
        "http402_event_runs": http402_events,
        "stable_gate_passed": stable_gate,
        "quality_gate_passed": quality_gate,
    }


def _rank_arms(stats: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not stats:
        return {"most_stable": "", "most_optimized": ""}
    stable_sorted = sorted(
        stats,
        key=lambda x: (
            float(x["hard_success_ci95"][0]),
            float(x.get("hard_success_rate", 0.0)),
            -float(x.get("runtime_median_s_hard_success") or 99999.0),
            -float(
                x.get("soft_fail_rate_on_hard_success")
                if x.get("soft_fail_rate_on_hard_success") is not None
                else 1.0
            ),
        ),
        reverse=True,
    )
    most_stable = stable_sorted[0]["arm_id"]
    candidates = [x for x in stats if bool(x.get("stable_gate_passed"))] or stats
    optimized_sorted = sorted(
        candidates,
        key=lambda x: (
            1.0 if x.get("quality_gate_passed") else 0.0,
            float(x.get("quality_score_mean") or 0.0),
            float(x.get("compliance_score_mean") or 0.0),
            float(x["hard_success_ci95"][0]),
            -float(x.get("runtime_median_s_hard_success") or 99999.0),
        ),
        reverse=True,
    )
    return {
        "most_stable": most_stable,
        "most_optimized": optimized_sorted[0]["arm_id"],
    }


def _run_one(
    *,
    args: argparse.Namespace,
    arm: Arm,
    rep: int,
    order: int,
    run_dir: Path,
) -> Dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    json_path = run_dir / "result.json"
    log_path = run_dir / "run.log"
    repo_root = Path(__file__).resolve().parent

    env = dict(os.environ)
    resolved_overrides: Dict[str, str] = dict(arm.env_overrides or {})
    if (
        "PERPLEXITY_REASONING_EFFORT" in resolved_overrides
        and "PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT" not in resolved_overrides
    ):
        # Keep OpenAI Stage-1 guardrail effort aligned with experiment arm intent.
        resolved_overrides["PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT"] = str(
            resolved_overrides.get("PERPLEXITY_REASONING_EFFORT", "")
        )

    temp_profile_name = ""
    temp_profile_path: Optional[Path] = None

    # Important: backend/config.py loads .env with override=True, so subprocess env
    # overrides are not reliable. We therefore emit a temporary .env.<profile> file
    # and point ENV_PROFILE to it so arm-level settings are authoritative.
    if resolved_overrides:
        safe_arm = re.sub(r"[^a-zA-Z0-9_]+", "_", arm.arm_id).strip("_") or "arm"
        temp_profile_name = f"exp_{safe_arm}_r{rep}_o{order}_{int(time.time() * 1000) % 1_000_000}"
        temp_profile_path = repo_root / f".env.{temp_profile_name}"

        lines: List[str] = []
        if arm.profile != "current_env":
            base_profile = repo_root / f".env.{arm.profile}"
            if base_profile.exists():
                lines.append(base_profile.read_text(encoding="utf-8"))
                lines.append("")
        lines.append("# Auto-generated by test_gpt52_experiment.py")
        for key in sorted(resolved_overrides.keys()):
            lines.append(f"{key}={resolved_overrides[key]}")
        temp_profile_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
        env["ENV_PROFILE"] = temp_profile_name
    else:
        env["ENV_PROFILE"] = "" if arm.profile == "current_env" else arm.profile

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
        "--brief-include-rubric",
        args.brief_include_rubric,
    ]
    if args.query_mode == "user" and args.query:
        cmd.extend(["--query", args.query])

    if args.dry_run:
        return {
            "arm_id": arm.arm_id,
            "profile": arm.profile,
            "replicate": rep,
            "order": order,
            "dry_run": True,
            "cmd": cmd,
            "env_profile": env.get("ENV_PROFILE", ""),
            "temp_profile_path": str(temp_profile_path) if temp_profile_path else None,
            "env_overrides": resolved_overrides,
            "run_dir": str(run_dir),
        }
    try:
        started = time.perf_counter()
        timed_out = False
        effective_timeout_s = _effective_subprocess_timeout(args, arm)
        try:
            proc = subprocess.run(
                cmd,
                env=env,
                cwd=str(repo_root),
                capture_output=True,
                text=True,
                timeout=effective_timeout_s,
            )
            returncode = proc.returncode
            stdout = proc.stdout or ""
            stderr = proc.stderr or ""
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            returncode = 124
            stdout = str(exc.stdout or "")
            stderr = str(exc.stderr or "")

        runtime_s = time.perf_counter() - started
        log_text = stdout + ("\n" + stderr if stderr else "")
        log_path.write_text(log_text, encoding="utf-8")
        log_signatures = _scan_log_signatures(log_text)

        payload: Dict[str, Any] = {}
        if json_path.exists():
            try:
                payload = json.loads(json_path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}

        metrics = _extract_run_metrics(
            payload,
            model=args.model,
            returncode=returncode,
            runtime_s=runtime_s,
            timed_out=timed_out,
            log_signatures=log_signatures,
        ) if payload else {
            "runtime_s": round(runtime_s, 2),
            "returncode": returncode,
            "timed_out": timed_out,
            "provider_error": "missing_or_invalid_json_output",
            "result_count": 0,
            "stage1_response_chars": 0,
            "hard_failure": True,
            "hard_failure_reasons": (
                (["subprocess_timeout"] if timed_out else [])
                + (["subprocess_nonzero"] if returncode != 0 else [])
                + ["missing_or_invalid_json_output"]
            ),
            "soft_failure": False,
            "soft_failure_warnings": [],
            "quality_score": 0.0,
            "reliability_score": 0.0,
        }

        return {
            "arm_id": arm.arm_id,
            "profile": arm.profile,
            "description": arm.description,
            "replicate": rep,
            "order": order,
            "env_profile": env.get("ENV_PROFILE", ""),
            "temp_profile_path": str(temp_profile_path) if temp_profile_path else None,
            "env_overrides": resolved_overrides,
            "cmd": cmd,
            "run_dir": str(run_dir),
            "log_path": str(log_path),
            "json_path": str(json_path),
            "effective_subprocess_timeout_s": effective_timeout_s,
            "log_signatures": log_signatures,
            "metrics": metrics,
        }
    finally:
        if temp_profile_path and temp_profile_path.exists():
            try:
                temp_profile_path.unlink()
            except Exception:
                pass


def _write_report(
    *,
    report_path: Path,
    manifest: Dict[str, Any],
    arm_stats: List[Dict[str, Any]],
    recommendations: Dict[str, Any],
) -> None:
    lines: List[str] = []
    lines.append("# GPT-5.2 Experiment Report")
    lines.append("")
    lines.append("## Protocol")
    lines.append("- Randomized blocked design by replicate (`seed` fixed).")
    lines.append("- Hard failures separated from soft conformance warnings.")
    lines.append("- Stability measured by hard-success Wilson 95% CI lower bound.")
    lines.append("- Quality measured on hard-success runs with bootstrap 95% CI.")
    lines.append("")
    lines.append("## Run Context")
    lines.append(f"- Started (UTC): `{manifest.get('started_at_utc')}`")
    lines.append(f"- Replicates: `{manifest.get('replicates')}`")
    lines.append(f"- Arms: `{len(manifest.get('arms', []))}`")
    lines.append(f"- Seed: `{manifest.get('seed')}`")
    lines.append(f"- Model: `{manifest.get('model')}`")
    lines.append("")
    lines.append("## Arm Summary")
    lines.append("| Arm | n | Hard Success | 95% CI | Soft Fail (on hard-success) | Runtime p50 (s) | Runtime p90 (s) | Quality mean | Compliance mean | Stable Gate | Quality Gate |")
    lines.append("|---|---:|---:|---|---:|---:|---:|---:|---:|---|---|")
    for item in arm_stats:
        ci = item.get("hard_success_ci95") or [0.0, 1.0]
        q = item.get("quality_score_mean")
        c = item.get("compliance_score_mean")
        sf = item.get("soft_fail_rate_on_hard_success")
        sf_text = f"{float(sf):.2%}" if sf is not None else "n/a"
        lines.append(
            "| {arm} | {n} | {hs:.2%} | [{lo:.2%}, {hi:.2%}] | {sf} | {p50} | {p90} | {q} | {c} | {sg} | {qg} |".format(
                arm=item.get("arm_id"),
                n=item.get("n", 0),
                hs=float(item.get("hard_success_rate", 0.0)),
                lo=float(ci[0]),
                hi=float(ci[1]),
                sf=sf_text,
                p50=item.get("runtime_median_s_hard_success"),
                p90=item.get("runtime_p90_s_all"),
                q=f"{q:.3f}" if q is not None else "n/a",
                c=f"{c:.3f}" if c is not None else "n/a",
                sg="yes" if item.get("stable_gate_passed") else "no",
                qg="yes" if item.get("quality_gate_passed") else "no",
            )
        )
    lines.append("")
    lines.append("## Recommendation")
    lines.append(f"- Most stable arm: `{recommendations.get('most_stable') or 'n/a'}`")
    lines.append(f"- Most optimized arm: `{recommendations.get('most_optimized') or 'n/a'}`")
    lines.append("")
    lines.append("## Notes")
    lines.append("- Treat conclusions as preliminary when sample size per arm is small (<5).")
    lines.append("- Re-run with larger replicates for final promotion decisions.")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = _parse_args()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    arms = _load_arms(Path(args.arms_file).resolve())
    schedule = _schedule(arms, args.replicates, args.seed, args.schedule_mode)

    manifest = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "model": args.model,
        "ticker": args.ticker,
        "template_id": args.template_id,
        "exchange": args.exchange,
        "query_mode": args.query_mode,
        "query": args.query,
        "replicates": int(args.replicates),
        "seed": int(args.seed),
        "schedule_mode": str(args.schedule_mode),
        "run_timeout_seconds": int(args.run_timeout_seconds),
        "cooldown_seconds": float(args.cooldown_seconds),
        "max_consecutive_hard_fails_per_arm": int(args.max_consecutive_hard_fails_per_arm),
        "arms": [
            {
                "id": arm.arm_id,
                "profile": arm.profile,
                "cost_rank": arm.cost_rank,
                "description": arm.description,
                "env_overrides": arm.env_overrides,
            }
            for arm in arms
        ],
        "schedule": [
            {"replicate": rep, "order": order, "arm_id": arm.arm_id}
            for rep, order, arm in schedule
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    runs: List[Dict[str, Any]] = []
    hard_fail_streak: Dict[str, int] = {arm.arm_id: 0 for arm in arms}

    for idx, (rep, order, arm) in enumerate(schedule, start=1):
        if hard_fail_streak.get(arm.arm_id, 0) >= args.max_consecutive_hard_fails_per_arm:
            skipped = {
                "arm_id": arm.arm_id,
                "profile": arm.profile,
                "replicate": rep,
                "order": order,
                "skipped": True,
                "skip_reason": "max_consecutive_hard_failures_reached",
            }
            runs.append(skipped)
            continue

        print(
            f"[exp] {idx}/{len(schedule)} arm={arm.arm_id} rep={rep} order={order} "
            f"profile={arm.profile}"
        )
        run_dir = out_dir / "runs" / arm.arm_id / f"rep{rep:02d}_ord{order:02d}"
        run = _run_one(args=args, arm=arm, rep=rep, order=order, run_dir=run_dir)
        runs.append(run)

        if not run.get("dry_run"):
            hard_failure = bool((run.get("metrics", {}) or {}).get("hard_failure", False))
            if hard_failure:
                hard_fail_streak[arm.arm_id] = hard_fail_streak.get(arm.arm_id, 0) + 1
            else:
                hard_fail_streak[arm.arm_id] = 0

        if idx < len(schedule) and args.cooldown_seconds > 0:
            time.sleep(float(args.cooldown_seconds))

    runs_jsonl = out_dir / "runs.jsonl"
    with runs_jsonl.open("w", encoding="utf-8") as fh:
        for row in runs:
            fh.write(json.dumps(row) + "\n")

    if args.dry_run:
        summary = {
            "manifest": manifest,
            "dry_run": True,
            "runs_count": len(runs),
            "runs_path": str(runs_jsonl),
        }
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"Wrote dry-run summary: {out_dir / 'summary.json'}")
        return

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in runs:
        if row.get("skipped"):
            continue
        arm_id = str(row.get("arm_id", "unknown"))
        grouped.setdefault(arm_id, []).append(row)

    arm_stats = [_summarize_arm(arm_id, rows, seed=args.seed) for arm_id, rows in grouped.items()]
    arm_stats.sort(key=lambda x: x["arm_id"])
    recommendations = _rank_arms(arm_stats)

    summary = {
        "manifest": manifest,
        "arm_stats": arm_stats,
        "recommendations": recommendations,
        "runs_path": str(runs_jsonl),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _write_report(
        report_path=out_dir / "report.md",
        manifest=manifest,
        arm_stats=arm_stats,
        recommendations=recommendations,
    )

    print("\n=== EXPERIMENT SUMMARY ===")
    for item in arm_stats:
        ci = item.get("hard_success_ci95") or [0.0, 1.0]
        soft_fail = item.get("soft_fail_rate_on_hard_success")
        soft_fail_text = f"{soft_fail:.2%}" if soft_fail is not None else "n/a"
        print(
            f"- {item['arm_id']}: n={item['n']} "
            f"hard_success={item['hard_success_rate']:.2%} "
            f"ci95=[{ci[0]:.2%},{ci[1]:.2%}] "
            f"soft_fail={soft_fail_text} "
            f"p50={item.get('runtime_median_s_hard_success')}s "
            f"quality={item.get('quality_score_mean')} "
            f"stable_gate={item.get('stable_gate_passed')} "
            f"quality_gate={item.get('quality_gate_passed')}"
        )
    print(f"Most stable: {recommendations.get('most_stable')}")
    print(f"Most optimized: {recommendations.get('most_optimized')}")
    print(f"Wrote: {out_dir / 'summary.json'}")
    print(f"Wrote: {out_dir / 'report.md'}")


if __name__ == "__main__":
    main()
