"""
Seeded synthetic data generator for the Construction Project Risk demo.

Produces six raw CSVs (projects, tasks, suppliers, material_orders,
change_orders, site_logs) that look like exports from separate systems
(ERP, scheduling tool, procurement, field app) joined only by IDs.

Problems are *injected through causes*, not by labelling projects:
  - 3 chronically late suppliers (steel, glazing, precast)
  - 4 "red" project profiles: low productivity, cost overruns, many change
    orders, and a bias toward the unreliable suppliers
  - Regional winter weather clusters (Jan-2026 cold snap, Feb-2026 storm)
    that stop work and stretch task durations

The risk score in risk_scoring.py never sees the profile - it has to
rediscover the troubled projects from the data alone.

Usage:
    python src/generate_data.py                      # defaults below
    python src/generate_data.py --as-of 2026-03-15 --seed 42 --out data/raw
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

DEFAULT_AS_OF = date(2026, 3, 15)
DEFAULT_SEED = 42

# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

CITIES = [
    "Toronto", "Mississauga", "Brampton", "Waterloo", "Kitchener", "Cambridge",
    "Guelph", "Hamilton", "London", "Ottawa", "Barrie", "Oakville",
]

# (name, type, health profile). Profiles drive the *causes* of trouble only.
PROJECTS = [
    ("Riverside Commons Office Tower", "commercial", "green"),
    ("Grand River Medical Pavilion", "commercial", "amber"),
    ("Kingsway Mixed-Use Podium", "commercial", "red"),
    ("Tech Park Building C", "commercial", "green"),
    ("Lakeshore Retail Centre", "commercial", "green"),
    ("Uptown Data Centre Expansion", "commercial", "amber"),
    ("Harbourfront Hotel Retrofit", "commercial", "green"),
    ("Maple Ridge Condominiums Phase 2", "residential", "red"),
    ("Willow Creek Townhomes", "residential", "green"),
    ("Bayview Seniors Residence", "residential", "green"),
    ("Northgate Rental Apartments", "residential", "amber"),
    ("Cedar Heights Stacked Towns", "residential", "green"),
    ("Parkside Student Housing", "residential", "green"),
    ("Highway 7 Interchange Widening", "infrastructure", "red"),
    ("Hespeler Road Watermain Replacement", "infrastructure", "green"),
    ("Speed River Pedestrian Bridge", "infrastructure", "green"),
    ("Eastside Transit Hub Platform", "infrastructure", "red"),
    ("Conestoga Stormwater Facility", "infrastructure", "green"),
    ("Dundas Street LRT Utility Relocation", "infrastructure", "amber"),
    ("Airport Road Resurfacing", "infrastructure", "amber"),
]

PROFILES = {
    #             productivity  duration overrun  cost factor    CO rate  bad-supplier bias  incident p
    "green": dict(start_slip=(0, 3), overrun=(0.00, 0.08), cost=(0.95, 1.04), co_lambda=2.0, bad_bias=0.15, incident_p=0.010),
    "amber": dict(start_slip=(0, 4), overrun=(0.03, 0.11), cost=(1.05, 1.12), co_lambda=5.0, bad_bias=0.40, incident_p=0.020),
    "red":   dict(start_slip=(3, 10), overrun=(0.18, 0.38), cost=(1.18, 1.36), co_lambda=10.0, bad_bias=0.80, incident_p=0.035),
}

# (supplier_id, name, material_category, historical on_time_rate, chronic_late)
SUPPLIERS = [
    ("SUP-01", "Ontario Ready-Mix Concrete", "Concrete", 0.94, False),
    ("SUP-02", "Grand River Concrete Co.", "Concrete", 0.90, False),
    ("SUP-03", "Steeltown Rebar Ltd.", "Rebar", 0.91, False),
    ("SUP-04", "Great Lakes Structural Steel", "Structural Steel", 0.52, True),
    ("SUP-05", "Maple Leaf Steel Fabricators", "Structural Steel", 0.89, False),
    ("SUP-06", "Northshore Glazing Systems", "Glazing & Cladding", 0.48, True),
    ("SUP-07", "Laurentian Curtain Wall", "Glazing & Cladding", 0.87, False),
    ("SUP-08", "Trillium Electrical Supply", "Electrical", 0.93, False),
    ("SUP-09", "Canuck Mechanical & HVAC", "Mechanical", 0.88, False),
    ("SUP-10", "Bayview Plumbing Wholesale", "Plumbing", 0.92, False),
    ("SUP-11", "Northern Gypsum & Finishes", "Drywall & Finishes", 0.90, False),
    ("SUP-12", "Escarpment Aggregates", "Aggregates", 0.95, False),
    ("SUP-13", "Huron Precast & Pipe", "Pipe & Precast", 0.55, True),
    ("SUP-14", "Lakehead Pipe & Precast", "Pipe & Precast", 0.90, False),
    ("SUP-15", "401 Asphalt Paving Supply", "Asphalt", 0.91, False),
]

# material_category -> list of (material name incl. unit, qty range, unit price range CAD)
MATERIALS = {
    "Concrete": [("Ready-mix concrete 32 MPa (m3)", (80, 600), (210, 260))],
    "Rebar": [("15M/20M rebar (tonnes)", (10, 120), (1500, 1900))],
    "Structural Steel": [("Wide-flange beams & columns (tonnes)", (20, 250), (3800, 5200))],
    "Glazing & Cladding": [("Unitized curtain wall panels (each)", (40, 400), (2200, 3600)),
                           ("Metal cladding panels (m2)", (300, 2500), (140, 220))],
    "Electrical": [("Switchgear & panelboards (each)", (2, 20), (18000, 42000)),
                   ("Copper building wire (m)", (2000, 15000), (6, 11))],
    "Mechanical": [("Rooftop HVAC units (each)", (2, 12), (35000, 90000)),
                   ("Ductwork & fittings (kg)", (2000, 12000), (9, 15))],
    "Plumbing": [("Copper & PEX piping (m)", (800, 6000), (12, 28))],
    "Drywall & Finishes": [("Gypsum board 5/8in (sheets)", (1500, 9000), (18, 26))],
    "Aggregates": [("Granular A base (tonnes)", (500, 6000), (22, 34))],
    "Asphalt": [("HL3 hot-mix asphalt (tonnes)", (300, 4000), (95, 130))],
    "Pipe & Precast": [("Precast box culvert sections (each)", (6, 40), (9000, 16000)),
                       ("HDPE storm pipe 900mm (m)", (100, 900), (310, 450))],
}

# phase name -> (share of project duration, outdoor exposure 0-1,
#                critical-path probability, material categories, task name pool)
BUILDING_PHASES = [
    ("Site Preparation", 0.08, 1.0, 0.6, ["Aggregates"],
     ["Site clearing & grubbing", "Temporary services & hoarding", "Bulk excavation", "Shoring installation"]),
    ("Foundation", 0.14, 1.0, 0.7, ["Concrete", "Rebar"],
     ["Footings formwork & pour", "Foundation walls", "Underslab services", "Waterproofing & drainage", "Slab on grade"]),
    ("Structure", 0.22, 0.85, 0.75, ["Structural Steel", "Concrete", "Rebar"],
     ["Steel erection levels 1-3", "Steel erection levels 4-6", "Elevated slab pours", "Stair & elevator cores", "Roof structure"]),
    ("Building Envelope", 0.16, 0.8, 0.5, ["Glazing & Cladding"],
     ["Curtain wall installation", "Exterior cladding", "Roofing membrane", "Window installation"]),
    ("MEP Rough-in", 0.18, 0.2, 0.4, ["Electrical", "Mechanical", "Plumbing"],
     ["Electrical rough-in", "HVAC ductwork", "Plumbing risers", "Fire protection piping", "Rooftop equipment set"]),
    ("Interior Finishes", 0.15, 0.05, 0.3, ["Drywall & Finishes"],
     ["Framing & drywall", "Ceilings", "Flooring", "Millwork & doors", "Painting"]),
    ("Commissioning & Closeout", 0.07, 0.0, 0.6, [],
     ["MEP commissioning", "Deficiency walkthrough", "Occupancy permit"]),
]

INFRA_PHASES = [
    ("Survey & Mobilization", 0.06, 1.0, 0.6, [],
     ["Topographic survey", "Traffic management setup", "Mobilization"]),
    ("Earthworks", 0.16, 1.0, 0.7, ["Aggregates"],
     ["Excavation & grading", "Subgrade preparation", "Granular base placement", "Embankment construction"]),
    ("Drainage & Utilities", 0.20, 1.0, 0.6, ["Pipe & Precast"],
     ["Storm sewer installation", "Culvert placement", "Watermain installation", "Utility relocation"]),
    ("Structures", 0.24, 0.9, 0.75, ["Concrete", "Rebar", "Structural Steel"],
     ["Pier & abutment pours", "Girder erection", "Deck pour", "Retaining walls", "Barrier walls"]),
    ("Paving", 0.16, 1.0, 0.6, ["Asphalt", "Aggregates"],
     ["Base course paving", "Surface course paving", "Curb & gutter"]),
    ("Signage & Lighting", 0.10, 0.7, 0.3, ["Electrical"],
     ["Street lighting", "Traffic signals", "Signage & pavement markings"]),
    ("Closeout", 0.08, 0.3, 0.6, [],
     ["Landscaping & restoration", "Final inspection"]),
]

CO_REASONS = [
    "Owner-requested scope change", "Unforeseen site condition", "Design error/omission",
    "Code/permit compliance", "Material substitution", "Utility conflict", "Value engineering",
]

WEATHER_STATES = ["clear", "rain", "snow", "extreme_cold"]
# monthly base distribution over WEATHER_STATES for southern Ontario
MONTH_WEATHER = {
    1: [0.42, 0.04, 0.36, 0.18], 2: [0.45, 0.05, 0.34, 0.16], 3: [0.56, 0.14, 0.22, 0.08],
    4: [0.70, 0.27, 0.03, 0.00], 5: [0.80, 0.20, 0.00, 0.00], 6: [0.80, 0.20, 0.00, 0.00],
    7: [0.82, 0.18, 0.00, 0.00], 8: [0.80, 0.20, 0.00, 0.00], 9: [0.78, 0.22, 0.00, 0.00],
    10: [0.70, 0.28, 0.02, 0.00], 11: [0.55, 0.26, 0.16, 0.03], 12: [0.45, 0.07, 0.35, 0.13],
}
# Regional events forced across all cities (clusters the agent can point to)
WEATHER_EVENTS = [
    (date(2025, 1, 20), date(2025, 1, 26), "extreme_cold", 0.7),
    (date(2026, 1, 19), date(2026, 1, 30), "extreme_cold", 0.75),
    (date(2026, 2, 9), date(2026, 2, 18), "snow", 0.8),
]


def _d(x: date) -> str | None:
    return x.isoformat() if x is not None else None


# --------------------------------------------------------------------------
# Generators
# --------------------------------------------------------------------------

def generate_weather(rng: np.random.Generator, start: date, end: date) -> dict:
    """Per-city daily weather with persistence (Markov-style) so storms cluster.

    Returns {city: {date: (weather, base_stopped_hours)}}. Base hours assume a
    fully exposed outdoor site; projects scale them by phase exposure.
    """
    out = {}
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    for city in CITIES:
        severity = 1.15 if city in ("Ottawa", "Barrie") else 1.0
        prev = "clear"
        series = {}
        for d in days:
            probs = np.array(MONTH_WEATHER[d.month], dtype=float)
            forced = next((ev for ev in WEATHER_EVENTS if ev[0] <= d <= ev[1]), None)
            if forced and rng.random() < forced[3]:
                w = forced[2]
            elif probs[WEATHER_STATES.index(prev)] > 0 and rng.random() < 0.55:
                w = prev  # persistence -> clusters
            else:
                w = WEATHER_STATES[rng.choice(4, p=probs / probs.sum())]
            if w == "clear":
                hrs = rng.integers(1, 3) if rng.random() < 0.04 else 0
            elif w == "rain":
                hrs = rng.integers(0, 3)
            elif w == "snow":
                hrs = rng.integers(0, 5)
            else:
                hrs = rng.integers(1, 7)
            series[d] = (w, float(min(10, hrs * severity)))
            prev = w
        out[city] = series
    return out


def generate_suppliers(rng: np.random.Generator) -> pd.DataFrame:
    rows = []
    for sid, name, cat, rate, _ in SUPPLIERS:
        rows.append(dict(supplier_id=sid, name=name, material_category=cat,
                         on_time_rate=rate, city=CITIES[rng.integers(len(CITIES))]))
    return pd.DataFrame(rows)


def _plan_tasks(rng, project_num, phases, start, end):
    """Lay out planned tasks phase by phase with overlap and a critical chain."""
    n_tasks = int(rng.integers(15, 31))
    total_days = (end - start).days
    shares = np.array([p[1] for p in phases])
    # at least 1 task per phase, rest proportional to phase share
    counts = np.ones(len(phases), dtype=int)
    extra = rng.multinomial(n_tasks - len(phases), shares / shares.sum())
    counts += extra

    tasks = []
    phase_start = start
    last_critical = None
    last_task = None
    seq = 0
    for (phase, share, exposure, crit_p, mats, pool), k in zip(phases, counts):
        phase_days = max(10, int(round(total_days * share)))
        # phases overlap ~10% with the previous one
        ps = phase_start
        slot = phase_days / k
        crit_flags = rng.random(k) < crit_p
        if not crit_flags.any():
            crit_flags[0] = True
        for j in range(k):
            seq += 1
            base = pool[j % len(pool)]
            name = base if j < len(pool) else f"{base} - Zone {chr(65 + j // len(pool))}"
            p_start = ps + timedelta(days=int(round(j * slot * 0.85)))
            dur = max(4, int(round(slot * rng.uniform(1.0, 1.4))))
            p_end = min(p_start + timedelta(days=dur), ps + timedelta(days=phase_days))
            if p_end <= p_start:
                p_end = p_start + timedelta(days=4)
            is_crit = bool(crit_flags[j])
            depends = last_critical if is_crit else (last_task or last_critical)
            tid = f"T-{project_num}-{seq:02d}"
            tasks.append(dict(task_id=tid, name=name, phase=phase,
                              planned_start=p_start, planned_end=p_end,
                              is_critical_path=is_crit, depends_on_task_id=depends,
                              _exposure=exposure, _materials=mats))
            last_task = tid
            if is_crit:
                last_critical = tid
        phase_start = ps + timedelta(days=int(round(phase_days * 0.9)))
    # stretch/compress so the last task ends on planned_end
    last_end = max(t["planned_end"] for t in tasks)
    scale = (end - start).days / max(1, (last_end - start).days)
    for t in tasks:
        t["planned_start"] = start + timedelta(days=int(round((t["planned_start"] - start).days * scale)))
        t["planned_end"] = start + timedelta(days=int(round((t["planned_end"] - start).days * scale)))
        if t["planned_end"] <= t["planned_start"]:
            t["planned_end"] = t["planned_start"] + timedelta(days=3)
    return tasks


def generate(as_of: date = DEFAULT_AS_OF, seed: int = DEFAULT_SEED) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    fake = Faker("en_CA")
    Faker.seed(seed)

    suppliers = generate_suppliers(rng)
    sup_by_cat: dict[str, list] = {}
    for sid, _, cat, rate, chronic in SUPPLIERS:
        sup_by_cat.setdefault(cat, []).append((sid, rate, chronic))

    weather = generate_weather(rng, date(2024, 1, 1), as_of)

    projects, tasks_out, orders_out, cos_out, logs_out = [], [], [], [], []
    order_seq = co_seq = log_seq = 0

    for idx, (pname, ptype, profile_name) in enumerate(PROJECTS):
        prof = PROFILES[profile_name]
        project_num = 1001 + idx
        pid = f"P-{project_num}"
        city = CITIES[rng.integers(len(CITIES))]
        start = as_of - timedelta(days=int(rng.integers(200, 560)))
        duration = int(rng.integers(480, 900))
        end = start + timedelta(days=duration)
        if end < as_of + timedelta(days=60):
            end = as_of + timedelta(days=int(rng.integers(60, 200)))
        budget_range = {"commercial": (15e6, 80e6), "residential": (8e6, 45e6),
                        "infrastructure": (20e6, 120e6)}[ptype]
        budget = round(float(rng.uniform(*budget_range)), -3)
        phases = INFRA_PHASES if ptype == "infrastructure" else BUILDING_PHASES

        tasks = _plan_tasks(rng, project_num, phases, start, end)

        # ---- material orders (planned against planned dates) -------------
        task_orders: dict[str, list] = {}
        for t in tasks:
            if not t["_materials"]:
                continue
            for _ in range(int(rng.integers(1, 4))):
                cat = t["_materials"][rng.integers(len(t["_materials"]))]
                cands = sup_by_cat[cat]
                bad = [c for c in cands if c[2]]
                good = [c for c in cands if not c[2]]
                if bad and good:
                    pick = bad[0] if rng.random() < prof["bad_bias"] else good[rng.integers(len(good))]
                else:
                    pick = cands[rng.integers(len(cands))]
                sid, rate, chronic = pick
                mname, qty_r, price_r = MATERIALS[cat][rng.integers(len(MATERIALS[cat]))]
                qty = int(rng.integers(*qty_r))
                cost = round(qty * float(rng.uniform(*price_r)), 2)
                promised = t["planned_start"] - timedelta(days=int(rng.integers(3, 15)))
                if rng.random() > rate:
                    late = 1 + int(rng.gamma(2.0, 6.5 if chronic else 2.5))
                    delivered = promised + timedelta(days=late)
                else:
                    delivered = promised - timedelta(days=int(rng.integers(0, 4)))
                task_orders.setdefault(t["task_id"], []).append(dict(
                    task_id=t["task_id"], supplier_id=sid, material=mname, quantity=qty,
                    cost=cost, promised_date=promised, delivered_date=delivered))

        # ---- change orders: pick tasks + impacts up front -----------------
        n_co = int(rng.poisson(prof["co_lambda"]))
        started = [t for t in tasks if t["planned_start"] <= as_of - timedelta(days=45)]
        co_plans = []
        for _ in range(n_co):
            if not started:
                break
            t = started[rng.integers(len(started))]
            pct = float(rng.lognormal(np.log(0.006 if profile_name != "red" else 0.010), 0.6))
            co_plans.append(dict(task=t, cost_impact=round(budget * pct, -2),
                                 days_impact=int(rng.integers(0, 6) if profile_name == "green" else rng.integers(2, 21)),
                                 reason=CO_REASONS[rng.integers(len(CO_REASONS))]))
        co_days_by_task: dict[str, int] = {}
        for c in co_plans:
            co_days_by_task[c["task"]["task_id"]] = co_days_by_task.get(c["task"]["task_id"], 0) + c["days_impact"]

        # ---- simulate actual execution ------------------------------------
        end_delay: dict[str, int] = {}
        city_wx = weather[city]
        for t in tasks:
            pdur = (t["planned_end"] - t["planned_start"]).days
            shift = max(0, end_delay.get(t["depends_on_task_id"], 0)) if t["depends_on_task_id"] else 0
            if not t["is_critical_path"]:
                shift = int(shift * 0.5)
            shift += int(rng.integers(*prof["start_slip"])) - 1
            shifted_start = t["planned_start"] + timedelta(days=shift)
            mat_delay = 0
            for o in task_orders.get(t["task_id"], []):
                mat_delay = max(mat_delay, (o["delivered_date"] - shifted_start).days)
            a_start = shifted_start + timedelta(days=max(0, mat_delay))
            wx_hours = sum(city_wx.get(a_start + timedelta(days=i), ("clear", 0.0))[1] for i in range(pdur))
            wx_days = wx_hours * t["_exposure"] / 8.0 * 0.6
            overrun = rng.uniform(*prof["overrun"]) if rng.random() < 0.7 else rng.uniform(0, 0.05)
            a_dur = pdur * (1 + overrun) + wx_days + 0.5 * co_days_by_task.get(t["task_id"], 0)
            a_end = a_start + timedelta(days=int(round(a_dur)))
            end_delay[t["task_id"]] = (a_end - t["planned_end"]).days
            t["_a_start"], t["_a_end"], t["_a_dur"] = a_start, a_end, max(1, (a_end - a_start).days)

        # ---- mask to what is knowable on as_of ----------------------------
        ev_weighted = total_weight = 0.0
        for t in tasks:
            pdur = max(1, (t["planned_end"] - t["planned_start"]).days)
            total_weight += pdur
            a_start, a_end = t["_a_start"], t["_a_end"]
            if a_start > as_of:
                actual_start = actual_end = None
                frac = 0.0
                status = "delayed" if t["planned_start"] < as_of else "not_started"
            elif a_end > as_of:
                actual_start, actual_end = a_start, None
                frac = min(0.95, (as_of - a_start).days / t["_a_dur"])
                status = "delayed" if as_of > t["planned_end"] else "in_progress"
            else:
                actual_start, actual_end, frac, status = a_start, a_end, 1.0, "completed"
            ev_weighted += pdur * frac
            tasks_out.append(dict(
                task_id=t["task_id"], project_id=pid, name=t["name"], phase=t["phase"],
                planned_start=_d(t["planned_start"]), planned_end=_d(t["planned_end"]),
                actual_start=_d(actual_start), actual_end=_d(actual_end),
                is_critical_path=t["is_critical_path"], depends_on_task_id=t["depends_on_task_id"],
                status=status))

        for orders in task_orders.values():
            for o in orders:
                # Orders far in the future are not placed yet
                if o["promised_date"] > as_of + timedelta(days=45):
                    continue
                order_seq += 1
                delivered = o["delivered_date"] if o["delivered_date"] <= as_of else None
                orders_out.append(dict(
                    order_id=f"MO-{order_seq:05d}", task_id=o["task_id"], supplier_id=o["supplier_id"],
                    material=o["material"], quantity=o["quantity"], cost=o["cost"],
                    promised_date=_d(o["promised_date"]), delivered_date=_d(delivered)))

        task_by_id = {t["task_id"]: t for t in tasks}
        for c in co_plans:
            t = task_by_id[c["task"]["task_id"]]
            if t["_a_start"] > as_of:
                continue
            window = max(1, min((as_of - t["_a_start"]).days, int(t["_a_dur"] * 0.7)))
            submitted = t["_a_start"] + timedelta(days=int(rng.integers(0, window + 1)))
            if (as_of - submitted).days <= 21:
                status = "pending" if rng.random() < 0.6 else "approved"
            else:
                status = rng.choice(["approved", "rejected", "pending"], p=[0.75, 0.15, 0.10])
            co_seq += 1
            cos_out.append(dict(
                co_id=f"CO-{co_seq:04d}", project_id=pid, task_id=t["task_id"], reason=c["reason"],
                cost_impact=c["cost_impact"], days_impact=c["days_impact"], status=str(status),
                submitted_date=_d(submitted)))

        # ---- site logs: Mon-Sat from start to as_of -----------------------
        base_crew = int(min(60, 10 + budget / 2.5e6))
        d = start
        while d <= as_of:
            if d.weekday() != 6:
                active = [t for t in tasks if t["_a_start"] <= d <= t["_a_end"]]
                exposure = max((t["_exposure"] for t in active), default=0.5)
                w, base_hrs = city_wx[d]
                stopped = round(min(10.0, base_hrs * exposure), 1)
                crew = int(base_crew * (0.6 + 0.15 * min(len(active), 4)) * rng.uniform(0.8, 1.2))
                if stopped >= 6:
                    crew = int(crew * rng.uniform(0.3, 0.6))
                p_inc = prof["incident_p"] * (2.0 if w in ("snow", "extreme_cold") else 1.0)
                log_seq += 1
                logs_out.append(dict(
                    log_id=f"L-{log_seq:06d}", project_id=pid, date=_d(d), crew_count=max(2, crew),
                    weather=w, work_stopped_hours=stopped, incident_flag=bool(rng.random() < p_inc)))
            d += timedelta(days=1)

        # ---- project header (ERP view) -------------------------------------
        pct_complete = ev_weighted / total_weight
        cost_factor = rng.uniform(*prof["cost"])
        approved_co_cost = sum(c["cost_impact"] for c in cos_out
                               if c["project_id"] == pid and c["status"] == "approved")
        actual_cost = budget * pct_complete * cost_factor + 0.6 * approved_co_cost
        projects.append(dict(
            project_id=pid, name=pname, type=ptype, city=city, budget=budget,
            start_date=_d(start), planned_end_date=_d(end),
            percent_complete=round(100 * pct_complete, 1),
            actual_cost_to_date=round(actual_cost, 2), project_manager=fake.name()))

    return {
        "projects": pd.DataFrame(projects),
        "tasks": pd.DataFrame(tasks_out),
        "suppliers": suppliers,
        "material_orders": pd.DataFrame(orders_out),
        "change_orders": pd.DataFrame(cos_out),
        "site_logs": pd.DataFrame(logs_out),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--as-of", type=date.fromisoformat, default=DEFAULT_AS_OF)
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parents[1] / "data" / "raw")
    args = ap.parse_args()

    tables = generate(args.as_of, args.seed)
    args.out.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(args.out / f"{name}.csv", index=False)
        print(f"{name:16s} {len(df):6d} rows -> {args.out / (name + '.csv')}")


if __name__ == "__main__":
    main()
