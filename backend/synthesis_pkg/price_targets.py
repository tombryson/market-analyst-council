"""Price-target extraction and scenario-driver enrichment."""

import re
from typing import Any, Dict, List, Optional
import logging

from .verdict import _to_float

logger = logging.getLogger(__name__)

def _extract_stage1_price_targets_from_response(response_text: str) -> Dict[str, float]:
    """Best-effort extraction of Stage 1 target numbers for chairman consensus nudging."""
    import re

    text = " ".join(str(response_text or "").split())
    lower = text.lower()
    extracted: Dict[str, float] = {}

    def _find_section(starts: List[str], ends: List[str]) -> str:
        start_positions = [lower.find(token.lower()) for token in starts]
        start_positions = [pos for pos in start_positions if pos >= 0]
        if not start_positions:
            return ""
        start = min(start_positions)
        end_positions = [lower.find(token.lower(), start + 1) for token in ends]
        end_positions = [pos for pos in end_positions if pos >= 0]
        end = min(end_positions) if end_positions else len(text)
        return text[start:end]

    section_12m = _find_section(
        ["12-month targets", "12 month targets", "12m targets", "12-month", "12m"],
        ["24-month targets", "24 month targets", "24m targets", "24-month", "24m"],
    )
    section_24m = _find_section(
        ["24-month targets", "24 month targets", "24m targets", "24-month", "24m"],
        [],
    )

    def _extract_scenario_value(section: str, label: str) -> Optional[float]:
        if not section:
            return None
        match = re.search(
            rf"{label}[^A$]{{0,140}}(?:A\$|\$)\s*([0-9]+(?:\.[0-9]+)?)",
            section,
            re.IGNORECASE,
        )
        if not match:
            return None
        try:
            return float(match.group(1))
        except (TypeError, ValueError):
            return None

    for horizon, section in (("12m", section_12m), ("24m", section_24m)):
        for label in ("bull", "base", "bear"):
            value = _extract_scenario_value(section, label)
            if value is not None:
                extracted[f"{horizon}_{label}"] = value

    probability_patterns = {
        "12m_prob": [
            r"12m probability-weighted target[^0-9]{0,12}(?:A\$|\$)\s*([0-9]+(?:\.[0-9]+)?)",
            r"12-month probability-weighted target[^0-9]{0,12}(?:A\$|\$)\s*([0-9]+(?:\.[0-9]+)?)",
            r"probability-weighted 12m target[^0-9]{0,12}(?:A\$|\$)\s*([0-9]+(?:\.[0-9]+)?)",
            r"probability-weighted target[^0-9]{0,12}(?:A\$|\$)\s*([0-9]+(?:\.[0-9]+)?)",
        ],
        "24m_prob": [
            r"24m probability-weighted target[^0-9]{0,12}(?:A\$|\$)\s*([0-9]+(?:\.[0-9]+)?)",
            r"24-month probability-weighted target[^0-9]{0,12}(?:A\$|\$)\s*([0-9]+(?:\.[0-9]+)?)",
            r"probability-weighted 24m target[^0-9]{0,12}(?:A\$|\$)\s*([0-9]+(?:\.[0-9]+)?)",
        ],
    }
    for field, patterns in probability_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    extracted[field] = float(match.group(1))
                    break
                except (TypeError, ValueError):
                    continue

    return extracted


def _build_top_rank_consensus_nudge(
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str],
    *,
    top_n: int = 3,
) -> str:
    """Build a compact consensus anchor from the top-ranked Stage 1 models."""
    from statistics import median
    from ..council.stage2 import calculate_aggregate_rankings

    aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)
    if not aggregate:
        return ""

    top_models = [item.get("model") for item in aggregate[:top_n] if item.get("model")]
    if not top_models:
        return ""

    response_by_model = {
        str(result.get("model")): str(result.get("response") or "")
        for result in stage1_results
        if result.get("model")
    }

    extracted_rows: List[Tuple[str, Dict[str, float]]] = []
    for model in top_models:
        extracted = _extract_stage1_price_targets_from_response(response_by_model.get(model, ""))
        if extracted:
            extracted_rows.append((model, extracted))

    if not extracted_rows:
        return ""

    lines = [f"- top_models: {', '.join(model for model, _ in extracted_rows)}"]

    def _add_summary(field: str, label: str) -> None:
        values = [row[field] for _, row in extracted_rows if field in row]
        if len(values) < 2:
            return
        lines.append(
            f"- {label}: range A${min(values):.2f}-A${max(values):.2f}; median A${median(values):.2f}"
        )

    _add_summary("12m_base", "top3 12m base targets")
    _add_summary("12m_prob", "top3 12m probability-weighted targets")
    _add_summary("24m_base", "top3 24m base targets")
    _add_summary("24m_prob", "top3 24m probability-weighted targets")

    lines.append(
        "- Use these top-ranked medians/ranges as the default numeric starting point. If your final 12m or 24m base target differs by roughly >15%, explain why briefly."
    )
    return "\n".join(lines)


def _extract_price_target_scenario_drivers(
    chairman_text: str,
) -> Dict[str, Dict[str, List[str]]]:
    """Extract 12m/24m base-bull-bear driver bullets from chairman XML text."""
    import re

    def _clean_text(value: str) -> str:
        text = str(value or "").strip()
        text = re.sub(r"[*_`]+", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _split_drivers(raw: str) -> List[str]:
        cleaned = _clean_text(raw)
        if not cleaned:
            return []
        parts = [p.strip(" .") for p in cleaned.split(";")]
        parts = [p for p in parts if p]
        if not parts:
            return [cleaned]
        return parts[:5]

    out: Dict[str, Dict[str, List[str]]] = {
        "12m": {"base": [], "bull": [], "bear": []},
        "24m": {"base": [], "bull": [], "bear": []},
    }

    text = str(chairman_text or "")
    section_match = re.search(
        r"<price_targets_and_scenarios>\s*(.*?)\s*</price_targets_and_scenarios>",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    section = section_match.group(1) if section_match else text

    current_horizon: Optional[str] = None
    current_scenario: Optional[str] = None

    for raw_line in section.splitlines():
        line = _clean_text(raw_line)
        if not line:
            continue
        lower = line.lower()

        if re.search(r"\b12[\s-]*month\b", lower):
            current_horizon = "12m"
            current_scenario = None
            continue
        if re.search(r"\b24[\s-]*month\b", lower):
            current_horizon = "24m"
            current_scenario = None
            continue

        scenario_match = re.search(r"\b(base|bull|bear)\b", lower)
        if scenario_match and ("case" in lower or "scenario" in lower or line.startswith("-") or line.startswith("*")):
            current_scenario = scenario_match.group(1)

        driver_match = re.search(r"drivers?\s*:\s*(.+)", line, re.IGNORECASE)
        if driver_match and current_horizon and current_scenario:
            for driver in _split_drivers(driver_match.group(1)):
                if driver and driver not in out[current_horizon][current_scenario]:
                    out[current_horizon][current_scenario].append(driver)
            continue

        if (
            current_horizon
            and current_scenario
            and (line.startswith("-") or line.startswith("*"))
            and "drivers" not in lower
        ):
            bullet = _clean_text(re.sub(r"^[-*]\s*", "", line))
            if bullet and bullet not in out[current_horizon][current_scenario]:
                out[current_horizon][current_scenario].append(bullet)

    return out


def _extract_price_target_values(
    chairman_text: str,
) -> Dict[str, Dict[str, Optional[float]]]:
    """Extract 12m/24m base-bull-bear numeric targets from chairman XML text."""
    import re

    out: Dict[str, Dict[str, Optional[float]]] = {
        "12m": {"base": None, "bull": None, "bear": None},
        "24m": {"base": None, "bull": None, "bear": None},
    }

    text = str(chairman_text or "")
    section_match = re.search(
        r"<price_targets_and_scenarios>\s*(.*?)\s*</price_targets_and_scenarios>",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    section = section_match.group(1) if section_match else text

    def _clean(value: str) -> str:
        line = str(value or "").strip()
        line = re.sub(r"[*_`]+", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        return line

    current_horizon: Optional[str] = None
    for raw_line in section.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()

        if re.search(r"\b12[\s-]*month\b", lower):
            current_horizon = "12m"
            continue
        if re.search(r"\b24[\s-]*month\b", lower):
            current_horizon = "24m"
            continue
        if not current_horizon:
            continue

        m = re.search(
            r"\b(base|bull|bear)\b[^:]{0,60}:\s*(?:A\$|\$|USD\s*)?\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)",
            line,
            re.IGNORECASE,
        )
        if not m:
            continue
        scenario = m.group(1).lower()
        try:
            value = float(m.group(2).replace(",", ""))
        except (TypeError, ValueError):
            value = None
        if value is not None:
            out[current_horizon][scenario] = value

    return out


def _extract_price_target_probabilities(
    chairman_text: str,
) -> Dict[str, Dict[str, Optional[float]]]:
    """Extract 12m/24m base-bull-bear scenario probabilities from chairman XML text."""
    import re

    out: Dict[str, Dict[str, Optional[float]]] = {
        "12m": {"base": None, "bull": None, "bear": None},
        "24m": {"base": None, "bull": None, "bear": None},
    }

    text = str(chairman_text or "")
    section_match = re.search(
        r"<price_targets_and_scenarios>\s*(.*?)\s*</price_targets_and_scenarios>",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    section = section_match.group(1) if section_match else text

    def _clean(value: str) -> str:
        line = str(value or "").strip()
        line = re.sub(r"[*_`]+", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        return line

    def _normalize_probability(value: float) -> Optional[float]:
        if value is None:
            return None
        if value > 1.0 and value <= 100.0:
            return round(value / 100.0, 6)
        if 0.0 <= value <= 1.0:
            return round(value, 6)
        return None

    current_horizon: Optional[str] = None
    for raw_line in section.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()

        if re.search(r"\b12[\s-]*month\b|\b12m\b", lower):
            current_horizon = "12m"
            continue
        if re.search(r"\b24[\s-]*month\b|\b24m\b", lower):
            current_horizon = "24m"
            continue
        if not current_horizon:
            continue

        scenario_match = re.search(r"\b(base|bull|bear)\b", lower)
        if not scenario_match:
            continue
        scenario = scenario_match.group(1).lower()

        # Pattern A: scenario line with percentage before "Prob/Probability",
        # e.g. "Base (55% Prob): A$0.55"
        prob_match = re.search(
            r"\(([0-9]+(?:\.[0-9]+)?)\s*%\s*[^)]*prob(?:ability)?[^)]*\)",
            line,
            re.IGNORECASE,
        )
        if not prob_match:
            # Pattern B: explicit probability label,
            # e.g. "Probability: 55%"
            prob_match = re.search(
                r"\bprob(?:ability)?\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)\s*%?",
                line,
                re.IGNORECASE,
            )
        if not prob_match:
            continue

        try:
            raw_prob = float(prob_match.group(1))
        except (TypeError, ValueError):
            continue
        normalized = _normalize_probability(raw_prob)
        if normalized is not None:
            out[current_horizon][scenario] = normalized

    return out


def _apply_scenario_driver_enrichment(
    structured_data: Dict[str, Any],
    chairman_text: str,
) -> None:
    """Ensure scenario drivers are present in structured JSON for Gantt/thesis tracking."""
    if not isinstance(structured_data, dict):
        return

    extracted = _extract_price_target_scenario_drivers(chairman_text)
    extracted_targets = _extract_price_target_values(chairman_text)
    extracted_probabilities = _extract_price_target_probabilities(chairman_text)

    price_targets = structured_data.get("price_targets")
    if not isinstance(price_targets, dict):
        price_targets = {}
        structured_data["price_targets"] = price_targets

    scenario_drivers = price_targets.get("scenario_drivers")
    if not isinstance(scenario_drivers, dict):
        scenario_drivers = {}

    scenario_targets = price_targets.get("scenario_targets")
    if not isinstance(scenario_targets, dict):
        scenario_targets = {}
    scenario_probabilities = price_targets.get("scenario_probabilities")
    if not isinstance(scenario_probabilities, dict):
        scenario_probabilities = {}

    def _normalize_probability(value: Any) -> Optional[float]:
        parsed = _to_float(value)
        if parsed is None:
            return None
        if 0.0 <= parsed <= 1.0:
            return round(parsed, 6)
        if 1.0 < parsed <= 100.0:
            return round(parsed / 100.0, 6)
        return None

    for horizon in ("12m", "24m"):
        horizon_map = scenario_drivers.get(horizon)
        if not isinstance(horizon_map, dict):
            horizon_map = {}
        target_map = scenario_targets.get(horizon)
        if not isinstance(target_map, dict):
            target_map = {}
        prob_map = scenario_probabilities.get(horizon)
        if not isinstance(prob_map, dict):
            prob_map = {}
        for scenario in ("base", "bull", "bear"):
            existing = horizon_map.get(scenario)
            if isinstance(existing, list) and existing:
                pass
            else:
                horizon_map[scenario] = extracted.get(horizon, {}).get(scenario, [])[:5]

            if _to_float(target_map.get(scenario)) is None:
                parsed_target = extracted_targets.get(horizon, {}).get(scenario)
                target_map[scenario] = parsed_target

            parsed_probability = extracted_probabilities.get(horizon, {}).get(scenario)
            if parsed_probability is not None:
                prob_map[scenario] = parsed_probability
            else:
                normalized_existing_prob = _normalize_probability(prob_map.get(scenario))
                prob_map[scenario] = normalized_existing_prob

        scenario_drivers[horizon] = horizon_map
        scenario_targets[horizon] = target_map
        # Normalize probabilities if all present but not summing to ~1.
        current_probs = [_normalize_probability(prob_map.get(s)) for s in ("base", "bull", "bear")]
        if all(p is not None for p in current_probs):
            prob_sum = float(sum(current_probs))
            if prob_sum > 0 and abs(prob_sum - 1.0) > 0.001:
                prob_map = {
                    "base": round(current_probs[0] / prob_sum, 6),
                    "bull": round(current_probs[1] / prob_sum, 6),
                    "bear": round(current_probs[2] / prob_sum, 6),
                }
        scenario_probabilities[horizon] = prob_map

    price_targets["scenario_drivers"] = scenario_drivers
    price_targets["scenario_targets"] = scenario_targets
    price_targets["scenario_probabilities"] = scenario_probabilities

    if _to_float(price_targets.get("target_12m")) is None:
        price_targets["target_12m"] = scenario_targets.get("12m", {}).get("base")
    if _to_float(price_targets.get("target_24m")) is None:
        price_targets["target_24m"] = scenario_targets.get("24m", {}).get("base")

    scenarios = price_targets.get("scenarios")
    if not isinstance(scenarios, dict):
        scenarios = {}
    for scenario in ("base", "bull", "bear"):
        if _to_float(scenarios.get(scenario)) is None:
            scenarios[scenario] = scenario_targets.get("12m", {}).get(scenario)
    price_targets["scenarios"] = scenarios

    def _weighted_target(horizon: str) -> Optional[float]:
        t_map = scenario_targets.get(horizon, {}) or {}
        p_map = scenario_probabilities.get(horizon, {}) or {}
        terms: List[Tuple[str, float, float]] = []
        for scenario in ("base", "bull", "bear"):
            target = _to_float(t_map.get(scenario))
            prob = _normalize_probability(p_map.get(scenario))
            if target is None or prob is None:
                return None
            terms.append((scenario, prob, target))
        return round(sum(prob * target for _, prob, target in terms), 6)

    weighted_12m = _weighted_target("12m")
    weighted_24m = _weighted_target("24m")
    if weighted_12m is not None:
        price_targets["prob_weighted_target_12m"] = weighted_12m
        price_targets["prob_weighted_formula_12m"] = "sum(p_i * target_i), i in {base,bull,bear}"
    if weighted_24m is not None:
        price_targets["prob_weighted_target_24m"] = weighted_24m
        price_targets["prob_weighted_formula_24m"] = "sum(p_i * target_i), i in {base,bull,bear}"

    current_price = _to_float(price_targets.get("current_price"))
    if current_price is None:
        market_data = structured_data.get("market_data")
        if isinstance(market_data, dict):
            market_current = _to_float(market_data.get("current_price"))
            if market_current is not None:
                price_targets["current_price"] = market_current
                current_price = market_current

    target_12m = _to_float(price_targets.get("target_12m"))
    target_24m = _to_float(price_targets.get("target_24m"))
    if current_price and current_price > 0:
        if _to_float(price_targets.get("upside_12m_pct")) is None and target_12m is not None:
            price_targets["upside_12m_pct"] = round(((target_12m / current_price) - 1.0) * 100.0, 2)
        if _to_float(price_targets.get("upside_24m_pct")) is None and target_24m is not None:
            price_targets["upside_24m_pct"] = round(((target_24m / current_price) - 1.0) * 100.0, 2)

