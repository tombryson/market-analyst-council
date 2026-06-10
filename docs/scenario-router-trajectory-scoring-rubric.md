# Scenario Router Trajectory Scoring Rubric

The announcement router scores each filing as a thesis trajectory event. The score is not a replacement for the saved bull/base/bear thesis map. It is a small overlay that shows whether new announcements are nudging the saved run toward bull, base, or bear over time.

## Output Fields

Each routed filing may carry a `trajectory_score` object:

- `direction`: `positive`, `negative`, `neutral`, or `mixed`.
- `intensity`: `none`, `low`, `medium`, `high`, or `critical`.
- `event_delta`: the one-filing score movement.
- `cumulative_delta`: the running score movement for the ticker and saved run.
- `baseline_score`: the starting score implied by the saved thesis path.
- `score_after_event`: baseline plus this filing only.
- `score_after_cumulative`: baseline plus all routed filings for the saved run.
- `position_band`: the event-level band after this filing.
- `cumulative_position_band`: the rolling band after all filings.
- `position_label`: human label for the current path.
- `validation_type`: why this score received its weight.
- `validation_weight`: the score floor created by the validation type.
- `mapped_condition`: whether the filing engaged a saved thesis, watchlist, or verification condition.
- `confidence`: max of parser confidence and thesis-match confidence where a condition was engaged.

## Baseline Path

The saved council run starts on a simple score axis:

| Saved path | Score |
| --- | ---: |
| Bear | -4.0 |
| Base | 0.0 |
| Bull | 4.0 |
| Mixed / unknown | 0.0 |

## Position Bands

| Score | Band |
| ---: | --- |
| `<= -4` | Bear |
| `> -4` and `<= -2` | Bear-leaning |
| `> -2` and `< 2` | Base |
| `>= 2` and `< 4` | Bull-leaning |
| `>= 4` | Bull |

This is deliberately not a hard flip from base to bull or bear on one ordinary filing. The router can say a filing is bull-leaning without claiming the saved bull case is proven.

## Base Intensity Scale

If a filing has no stronger validation source, event magnitude comes from intensity:

| Intensity | Magnitude |
| --- | ---: |
| None | 0.0 |
| Low | 0.5 |
| Medium | 2.0 |
| High | 3.0 |
| Critical | 3.0 |

The sign is set by `direction`. Positive filings add the magnitude. Negative filings subtract it. Neutral and mixed filings score `0.0`.

## Validation Weighting

Some evidence is more important because it validates something the saved council run explicitly told us to monitor. The router therefore applies a validation weight floor. The final event magnitude is:

```text
max(base intensity magnitude, validation weight)
```

Positive validation:

| Validation type | Meaning | Weight |
| --- | --- | ---: |
| `saved_thesis_condition` | Announcement fully satisfies a saved bull/base/bear required condition. | +3.0 |
| `watchlist_confirmatory_full` | Announcement fully satisfies a confirmatory watchlist item. | +2.5 |
| `watchlist_confirmatory_partial` | Announcement is a real precursor or partial match to a confirmatory watchlist item. | +2.0 |
| `verification_queue` | Announcement resolves or supplies a verification-queue item. | +1.5 |

Negative validation:

| Validation type | Meaning | Weight |
| --- | --- | ---: |
| `saved_thesis_failure` | Announcement fully satisfies a saved failure condition. | -4.0 |
| `watchlist_red_flag_full` | Announcement fully satisfies a red-flag watchlist item. | -3.0 |
| `watchlist_red_flag_partial` | Announcement is a real precursor or partial match to a red-flag item. | -2.0 |

Unmapped material filings:

| Validation type | Meaning | Weight |
| --- | --- | ---: |
| `material_unmapped` | Filing appears material, but no saved thesis, watchlist, or verification condition covers it. | 0.0 validation bonus |

An unmapped material filing can still score from its base intensity, usually `+2.0` or `-2.0` for a medium filing. It should be labelled as trajectory evidence outside the saved thesis map, not as a fully validated bull or bear case.

## Guardrails

- Saved thesis conditions outrank generic materiality.
- Confirmatory watchlist hits outrank unmapped positive filings.
- Partial watchlist hits are scored, but below full watchlist hits.
- Saved failure conditions are the strongest negative signal.
- Red flags are stronger than generic negative filings.
- Verification-queue hits support evidence refresh, but should not be treated as strongly as a thesis condition or watchlist catalyst.
- Do not label a filing as "Bull case" solely because it is positive and material. Use "Bull-leaning" unless the cumulative score reaches the bull band or the saved bull condition is actually satisfied.
- Review state must not erase case type. A reviewed unmapped material filing remains an unmapped material filing in history.

## Examples

| Filing | Saved condition context | Expected score |
| --- | --- | --- |
| Power infrastructure contract; no saved condition covers it | Material but unmapped | `+2.0`, bull-leaning evidence outside map |
| Binding offtake agreement where watchlist asks for binding offtake | Full confirmatory watchlist hit | `+2.5` |
| Non-binding offtake LoI where watchlist asks for binding offtake | Partial confirmatory watchlist hit | `+2.0` |
| Announcement satisfies a saved bull required condition | Saved thesis condition | `+3.0` |
| Permit revocation satisfies a saved bull failure condition | Saved thesis failure | `-4.0` |

## Implementation

The rubric is implemented in `backend/scenario_router/trajectory_scoring.py`.

`backend/scenario_router/thesis_comparator.py` supplies the scorer with:

- required thesis hits
- failure thesis hits
- full red-flag hits
- partial red-flag hits
- full confirmatory watchlist hits
- partial confirmatory watchlist hits
- verification-queue hits

The focused regression coverage is in `tests/test_scenario_router_trajectory.py`.
