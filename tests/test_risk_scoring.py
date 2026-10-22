"""Sanity tests: the generator is deterministic and the score rediscovers the injected problems."""
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import generate_data  # noqa: E402
import risk_scoring  # noqa: E402

RED_PROFILE = {name for name, _, p in generate_data.PROJECTS if p == "red"}
GREEN_PROFILE = {name for name, _, p in generate_data.PROJECTS if p == "green"}
CHRONIC_SUPPLIERS = {sid for sid, *_, chronic in generate_data.SUPPLIERS if chronic}


@pytest.fixture(scope="module")
def results(tmp_path_factory):
    raw = tmp_path_factory.mktemp("raw")
    for name, df in generate_data.generate().items():
        df.to_csv(raw / f"{name}.csv", index=False)
    return risk_scoring.run(raw)


def test_generator_is_deterministic():
    a, b = generate_data.generate(), generate_data.generate()
    for name in a:
        pd.testing.assert_frame_equal(a[name], b[name])


def test_row_counts_in_spec():
    t = generate_data.generate()
    assert len(t["projects"]) == 20
    assert len(t["suppliers"]) == 15
    per_project = t["tasks"].groupby("project_id").size()
    assert per_project.between(15, 30).all()


def test_red_profiles_are_scored_red(results):
    scores, _ = results
    red = set(scores.loc[scores["risk_level"] == "red", "name"])
    assert red == RED_PROFILE


def test_no_green_profile_scored_red(results):
    scores, _ = results
    assert not (scores.loc[scores["name"].isin(GREEN_PROFILE), "risk_level"] == "red").any()


def test_score_is_sum_of_factor_points(results):
    scores, _ = results
    pts = scores[[f"pts_{k}" for k in risk_scoring.WEIGHTS]].sum(axis=1).round(0)
    assert (pts.astype(int) == scores["risk_score"]).all()
    assert scores["risk_score"].between(0, 100).all()


def test_every_red_project_has_reasons(results):
    scores, _ = results
    assert scores.loc[scores["risk_level"] == "red", "risk_reasons"].apply(len).min() >= 2


def test_chronic_suppliers_flagged(results):
    _, suppliers = results
    flagged = set(suppliers.loc[suppliers["reliability_flag"] == "unreliable", "supplier_id"])
    assert CHRONIC_SUPPLIERS <= flagged
