from backend.synthesis.price_targets import _build_top_rank_consensus_nudge
from backend.synthesis.prompts import create_weighted_context
from backend.synthesis.synthesis import create_rankings_summary


def _sample_stage2_results():
    return [
        {"model": "judge-1", "parsed_ranking": ["A", "B"]},
        {"model": "judge-2", "parsed_ranking": ["A", "B"]},
    ]


def _sample_label_map():
    return {"A": "model-a", "B": "model-b"}


def test_create_weighted_context_uses_stage2_rankings():
    text = create_weighted_context(
        [
            {"model": "model-a", "response": "response a"},
            {"model": "model-b", "response": "response b"},
        ],
        _sample_stage2_results(),
        _sample_label_map(),
    )

    assert "model-a" in text
    assert "Average Peer Rank: 1.00" in text
    assert "model-b" in text


def test_build_top_rank_consensus_nudge_uses_stage2_rankings():
    text = _build_top_rank_consensus_nudge(
        [
            {
                "model": "model-a",
                "response": (
                    "12m probability-weighted target A$1.20\n"
                    "24m probability-weighted target A$1.80\n"
                    "bull case A$2.40\n"
                    "base case A$1.60\n"
                    "bear case A$0.80"
                ),
            },
            {
                "model": "model-b",
                "response": (
                    "12m probability-weighted target A$1.00\n"
                    "24m probability-weighted target A$1.50\n"
                    "bull case A$2.00\n"
                    "base case A$1.40\n"
                    "bear case A$0.70"
                ),
            },
        ],
        _sample_stage2_results(),
        _sample_label_map(),
    )

    assert "top_models: model-a, model-b" in text
    assert "12m probability-weighted targets" in text
    assert "median A$1.10" in text


def test_create_rankings_summary_uses_stage2_rankings():
    text = create_rankings_summary(
        _sample_stage2_results(),
        _sample_label_map(),
    )

    assert "Aggregate Peer Rankings" in text
    assert "1. model-a: Avg Rank 1.00" in text
