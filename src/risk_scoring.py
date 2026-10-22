"""
Explainable, rule-based project risk score (0-100) - pandas reference version.

Every point in the score traces back to one of five factors, each with a
fixed weight and a linear ramp between a "no concern" and a "max concern"
value. No ML, no hidden state: a PM can reproduce any score by hand.

    Factor                     Weight  Metric                                   Full points at
    -------------------------  ------  ---------------------------------------  ---------------------------
    schedule (SPI)               25    EV% / PV%                                SPI <= 0.70
    cost (CPI)                   25    earned value / actual cost               CPI <= 0.75
    supply (critical path)       20    late critical-path deliveries            5 late orders + 14d avg late
    change orders                15    count and cost as % of budget            10 COs + 8% of budget
    weather                      15    % of shift hours stopped, last 90 days   30% stopped

risk_level: green < 35 <= amber < 60 <= red

The PySpark port (spark/risk_logic.py) uses the same constants; keep them
in sync (tests/test_parity.py checks this when PySpark is available).

Usage:
    python src/risk_scoring.py --raw data/raw --out data/output
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_AS_OF = date(2026, 3, 15)

WEIGHTS = {"schedule": 25, "cost": 25, "supply": 20, "change_orders": 15, "weather": 15}

# metric value at which a factor (or sub-factor) earns its full points
SPI_FULL = 0.70            # SPI 1.0 -> 0 pts, 0.70 -> 25 pts
CPI_FULL = 0.75            # CPI 1.0 -> 0 pts, 0.75 -> 25 pts
LATE_CP_COUNT_FULL = 5     # half of supply points
LATE_CP_AVG_DAYS_FULL = 14 # other half
CO_COUNT_FULL = 10         # half of change-order points
CO_COST_PCT_FULL = 0.08    # other half (8% of budget)
WEATHER_STOP_PCT_FULL = 0.30
WEATHER_WINDOW_DAYS = 90
SHIFT_HOURS = 8.0
LATE_GRACE_DAYS = 2        # deliveries up to 2 days late are not "late"

AMBER_AT, RED_AT = 35, 60
REASON_MIN_SHARE = 0.4     # factor appears in reasons if it earns >= 40% of its weight


def _ramp(value: pd.Series, zero_at: float, full_at: float) -> pd.Series:
    """Linear 0..1 between zero_at and full_at (works for rising or falling)."""
    return ((value - zero_at) / (full_at - zero_at)).clip(0, 1).fillna(0)


def load_raw(raw_dir: Path) -> dict[str, pd.DataFrame]:
    dates = {
        "projects": ["start_date", "planned_end_date"],
        "tasks": ["planned_start", "planned_end", "actual_start", "actual_end"],
        "material_orders": ["promised_date", "delivered_date"],
        "change_orders": ["submitted_date"],
        "site_logs": ["date"],
        "suppliers": [],
    }
    return {name: pd.read_csv(raw_dir / f"{name}.csv", parse_dates=cols) for name, cols in dates.items()}


def compute_project_metrics(t: dict[str, pd.DataFrame], as_of: date) -> pd.DataFrame:
    as_of_ts = pd.Timestamp(as_of)
    projects = t["projects"].copy()
    tasks = t["tasks"].copy()

    # --- schedule: planned % complete (PV) from the task plan, duration-weighted
    tasks["planned_days"] = (tasks["planned_end"] - tasks["planned_start"]).dt.days.clip(lower=1)
    tasks["planned_frac"] = ((as_of_ts - tasks["planned_start"]).dt.days / tasks["planned_days"]).clip(0, 1)
    pv = (tasks.assign(w=tasks["planned_days"] * tasks["planned_frac"])
          .groupby("project_id").agg(w=("w", "sum"), d=("planned_days", "sum")))
    pv["planned_pct_complete"] = 100 * pv["w"] / pv["d"]
    m = projects.merge(pv[["planned_pct_complete"]], left_on="project_id", right_index=True, how="left")

    m["spi"] = m["percent_complete"] / m["planned_pct_complete"].replace(0, np.nan)
    m["earned_value"] = m["budget"] * m["percent_complete"] / 100
    m["cpi"] = m["earned_value"] / m["actual_cost_to_date"].replace(0, np.nan)
    m["delayed_task_count"] = m["project_id"].map(tasks[tasks["status"] == "delayed"].groupby("project_id").size()).fillna(0).astype(int)

    # --- supply: late deliveries on critical-path tasks, due on/before as_of
    orders = t["material_orders"].merge(
        tasks[["task_id", "project_id", "is_critical_path"]], on="task_id", how="left")
    orders = orders[orders["promised_date"] <= as_of_ts].copy()
    effective = orders["delivered_date"].fillna(as_of_ts)
    orders["days_late"] = (effective - orders["promised_date"]).dt.days.clip(lower=0)
    orders["is_late"] = orders["days_late"] > LATE_GRACE_DAYS
    cp_late = orders[orders["is_critical_path"].astype(bool) & orders["is_late"]]
    sup = cp_late.groupby("project_id").agg(
        late_cp_deliveries=("order_id", "count"),
        avg_cp_days_late=("days_late", "mean"),
        late_cp_supplier_ids=("supplier_id", lambda s: sorted(set(s))))
    m = m.merge(sup, left_on="project_id", right_index=True, how="left")
    m["late_cp_deliveries"] = m["late_cp_deliveries"].fillna(0).astype(int)
    m["avg_cp_days_late"] = m["avg_cp_days_late"].fillna(0.0)
    m["late_cp_supplier_ids"] = m["late_cp_supplier_ids"].apply(lambda v: v if isinstance(v, list) else [])

    # --- change orders: approved + pending (rejected ones never hit cost)
    co = t["change_orders"]
    co = co[co["status"] != "rejected"]
    coa = co.groupby("project_id").agg(co_count=("co_id", "count"), co_cost=("cost_impact", "sum"),
                                       co_days=("days_impact", "sum"))
    m = m.merge(coa, left_on="project_id", right_index=True, how="left")
    m[["co_count", "co_cost", "co_days"]] = m[["co_count", "co_cost", "co_days"]].fillna(0)
    m["co_count"] = m["co_count"].astype(int)
    m["co_cost_pct_budget"] = m["co_cost"] / m["budget"]

    # --- weather: share of shift hours stopped in the trailing window
    logs = t["site_logs"]
    recent = logs[(logs["date"] > as_of_ts - timedelta(days=WEATHER_WINDOW_DAYS)) & (logs["date"] <= as_of_ts)]
    wx = recent.groupby("project_id").agg(stopped_hours_90d=("work_stopped_hours", "sum"),
                                          log_days_90d=("log_id", "count"),
                                          incidents_90d=("incident_flag", "sum"))
    m = m.merge(wx, left_on="project_id", right_index=True, how="left")
    m[["stopped_hours_90d", "log_days_90d", "incidents_90d"]] = m[["stopped_hours_90d", "log_days_90d", "incidents_90d"]].fillna(0)
    m["weather_stop_pct_90d"] = m["stopped_hours_90d"] / (m["log_days_90d"] * SHIFT_HOURS).replace(0, np.nan)
    m["weather_stop_pct_90d"] = m["weather_stop_pct_90d"].fillna(0)
    return m


def score(m: pd.DataFrame) -> pd.DataFrame:
    m = m.copy()
    m["pts_schedule"] = WEIGHTS["schedule"] * _ramp(m["spi"], 1.0, SPI_FULL)
    m["pts_cost"] = WEIGHTS["cost"] * _ramp(m["cpi"], 1.0, CPI_FULL)
    m["pts_supply"] = WEIGHTS["supply"] / 2 * (
        _ramp(m["late_cp_deliveries"], 0, LATE_CP_COUNT_FULL) + _ramp(m["avg_cp_days_late"], 0, LATE_CP_AVG_DAYS_FULL))
    m["pts_change_orders"] = WEIGHTS["change_orders"] / 2 * (
        _ramp(m["co_count"], 0, CO_COUNT_FULL) + _ramp(m["co_cost_pct_budget"], 0, CO_COST_PCT_FULL))
    m["pts_weather"] = WEIGHTS["weather"] * _ramp(m["weather_stop_pct_90d"], 0, WEATHER_STOP_PCT_FULL)

    pts_cols = [f"pts_{k}" for k in WEIGHTS]
    m[pts_cols] = m[pts_cols].round(1)
    m["risk_score"] = m[pts_cols].sum(axis=1).round(0).astype(int)
    m["risk_level"] = np.select([m["risk_score"] >= RED_AT, m["risk_score"] >= AMBER_AT], ["red", "amber"], "green")
    m["top_risk_driver"] = m[pts_cols].idxmax(axis=1).str.replace("pts_", "", regex=False)
    m["risk_reasons"] = m.apply(build_reasons, axis=1)
    return m


def build_reasons(r: pd.Series) -> list[str]:
    """Plain-English reasons, strongest first. These feed the Ontology and the AIP agent."""
    candidates = [
        ("schedule", r["pts_schedule"],
         f"Schedule slipping: SPI {r['spi']:.2f} - {r['percent_complete']:.0f}% complete vs "
         f"{r['planned_pct_complete']:.0f}% planned; {r['delayed_task_count']} tasks delayed"),
        ("cost", r["pts_cost"],
         f"Cost overrun: CPI {r['cpi']:.2f} - ${r['actual_cost_to_date'] / 1e6:.1f}M spent for "
         f"${r['earned_value'] / 1e6:.1f}M of earned value"),
        ("supply", r["pts_supply"],
         f"Supply risk: {r['late_cp_deliveries']} late deliveries on critical-path tasks, "
         f"avg {r['avg_cp_days_late']:.0f} days late (suppliers: {', '.join(r['late_cp_supplier_ids']) or 'n/a'})"),
        ("change_orders", r["pts_change_orders"],
         f"Scope churn: {r['co_count']} open/approved change orders worth "
         f"{100 * r['co_cost_pct_budget']:.1f}% of budget (+{int(r['co_days'])} days claimed)"),
        ("weather", r["pts_weather"],
         f"Weather: {100 * r['weather_stop_pct_90d']:.0f}% of shift hours stopped in last "
         f"{WEATHER_WINDOW_DAYS} days ({r['stopped_hours_90d']:.0f} h)"),
    ]
    hits = [(pts, text) for key, pts, text in candidates if pts >= REASON_MIN_SHARE * WEIGHTS[key]]
    return [text for _, text in sorted(hits, key=lambda x: -x[0])]


def supplier_scorecard(t: dict[str, pd.DataFrame], as_of: date) -> pd.DataFrame:
    """Observed delivery performance per supplier (vs. their stated on_time_rate)."""
    as_of_ts = pd.Timestamp(as_of)
    o = t["material_orders"]
    o = o[o["promised_date"] <= as_of_ts].copy()
    o["days_late"] = (o["delivered_date"].fillna(as_of_ts) - o["promised_date"]).dt.days.clip(lower=0)
    o["is_late"] = o["days_late"] > LATE_GRACE_DAYS
    o["is_open_overdue"] = o["delivered_date"].isna()
    o = o.merge(t["tasks"][["task_id", "project_id"]], on="task_id", how="left")
    sc = o.groupby("supplier_id").agg(
        orders_due=("order_id", "count"), late_orders=("is_late", "sum"),
        avg_days_late=("days_late", lambda s: s[s > LATE_GRACE_DAYS].mean() if (s > LATE_GRACE_DAYS).any() else 0.0),
        open_overdue_orders=("is_open_overdue", "sum"), projects_supplied=("project_id", "nunique"),
        order_value=("cost", "sum")).reset_index()
    sc["observed_on_time_rate"] = (1 - sc["late_orders"] / sc["orders_due"]).round(3)
    sc = t["suppliers"].merge(sc, on="supplier_id", how="left")
    count_cols = ["orders_due", "late_orders", "open_overdue_orders", "projects_supplied"]
    sc[count_cols] = sc[count_cols].fillna(0).astype(int)
    sc[["avg_days_late", "order_value"]] = sc[["avg_days_late", "order_value"]].fillna(0.0)
    sc["reliability_flag"] = np.select(
        [sc["orders_due"] == 0,
         (sc["observed_on_time_rate"] < 0.75) & (sc["orders_due"] >= 5),
         sc["observed_on_time_rate"] < 0.88],
        ["no_history", "unreliable", "watch"], "ok")
    # writeback columns edited by the "Flag Supplier" Action in Foundry
    sc["is_flagged"], sc["flag_reason"], sc["flagged_at"] = False, None, None
    return sc.round({"avg_days_late": 1, "order_value": 2})


OUTPUT_COLS = [
    "project_id", "name", "type", "city", "project_manager", "start_date", "planned_end_date",
    "as_of_date", "risk_score", "risk_level",
    "top_risk_driver", "risk_reasons", "pts_schedule", "pts_cost", "pts_supply", "pts_change_orders",
    "pts_weather", "spi", "cpi", "percent_complete", "planned_pct_complete", "budget",
    "actual_cost_to_date", "earned_value", "delayed_task_count", "late_cp_deliveries",
    "avg_cp_days_late", "co_count", "co_cost_pct_budget", "co_days", "weather_stop_pct_90d",
    "stopped_hours_90d", "incidents_90d",
]


def run(raw_dir: Path, as_of: date = DEFAULT_AS_OF) -> tuple[pd.DataFrame, pd.DataFrame]:
    t = load_raw(raw_dir)
    scored = score(compute_project_metrics(t, as_of))
    scored["as_of_date"] = as_of.isoformat()
    out = scored[OUTPUT_COLS].sort_values("risk_score", ascending=False).reset_index(drop=True)
    for c in ["spi", "cpi", "planned_pct_complete", "avg_cp_days_late", "co_cost_pct_budget", "weather_stop_pct_90d"]:
        out[c] = out[c].round(3)
    out["earned_value"] = out["earned_value"].round(2)
    return out, supplier_scorecard(t, as_of)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", type=Path, default=root / "data" / "raw")
    ap.add_argument("--out", type=Path, default=root / "data" / "output")
    ap.add_argument("--as-of", type=date.fromisoformat, default=DEFAULT_AS_OF)
    args = ap.parse_args()

    scores, suppliers = run(args.raw, args.as_of)
    args.out.mkdir(parents=True, exist_ok=True)
    # risk_reasons is written as a JSON array string; Foundry parses it to array<string>
    scores.assign(risk_reasons=scores["risk_reasons"].apply(json.dumps)).to_csv(
        args.out / "project_risk_scores.csv", index=False)
    suppliers.to_csv(args.out / "supplier_scorecard.csv", index=False)

    pd.set_option("display.width", 200)
    print(scores[["project_id", "name", "risk_score", "risk_level", "top_risk_driver", "spi", "cpi",
                  "late_cp_deliveries", "co_count", "weather_stop_pct_90d"]].to_string(index=False))
    print()
    print(suppliers[["supplier_id", "name", "on_time_rate", "observed_on_time_rate", "late_orders",
                     "open_overdue_orders", "reliability_flag"]].to_string(index=False))
    for _, r in scores.head(1).iterrows():
        print(f"\nWhy is {r['project_id']} ({r['name']}) {r['risk_level']}?")
        for reason in r["risk_reasons"]:
            print("  -", reason)


if __name__ == "__main__":
    main()
