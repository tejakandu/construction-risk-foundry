"""
PySpark port of src/risk_scoring.py - pure functions, no Foundry imports.

The same module runs in two places:
  * Foundry Code Repositories: datasets/*.py wrap these functions in @transform_df
  * Locally: spark/run_local.py reads the CSVs and calls them directly

Keep the constants below in sync with src/risk_scoring.py.
"""
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

WEIGHTS = {"schedule": 25, "cost": 25, "supply": 20, "change_orders": 15, "weather": 15}
SPI_FULL = 0.70
CPI_FULL = 0.75
LATE_CP_COUNT_FULL = 5
LATE_CP_AVG_DAYS_FULL = 14
CO_COUNT_FULL = 10
CO_COST_PCT_FULL = 0.08
WEATHER_STOP_PCT_FULL = 0.30
WEATHER_WINDOW_DAYS = 90
SHIFT_HOURS = 8.0
LATE_GRACE_DAYS = 2
AMBER_AT, RED_AT = 35, 60
REASON_MIN_SHARE = 0.4


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _blank_to_null(c: str) -> Column:
    col = F.col(c).cast("string")
    return F.when(F.trim(col) == "", None).otherwise(col)


def _date(c: str) -> Column:
    return F.to_date(_blank_to_null(c)).alias(c)


def _ramp(value: Column, zero_at: float, full_at: float) -> Column:
    """Linear 0..1 between zero_at and full_at; nulls score 0."""
    r = (value - F.lit(zero_at)) / F.lit(full_at - zero_at)
    return F.coalesce(F.greatest(F.lit(0.0), F.least(F.lit(1.0), r)), F.lit(0.0))


# --------------------------------------------------------------------------
# 1. Clean: enforce types on raw uploads (CSV uploads often arrive as strings)
# --------------------------------------------------------------------------

def clean_projects(df: DataFrame) -> DataFrame:
    return df.select(
        "project_id", "name", "type", "city",
        F.col("budget").cast("double").alias("budget"),
        _date("start_date"), _date("planned_end_date"),
        F.col("percent_complete").cast("double").alias("percent_complete"),
        F.col("actual_cost_to_date").cast("double").alias("actual_cost_to_date"),
        "project_manager",
    ).dropDuplicates(["project_id"])


def clean_tasks(df: DataFrame) -> DataFrame:
    return df.select(
        "task_id", "project_id", "name", "phase",
        _date("planned_start"), _date("planned_end"), _date("actual_start"), _date("actual_end"),
        F.col("is_critical_path").cast("boolean").alias("is_critical_path"),
        _blank_to_null("depends_on_task_id").alias("depends_on_task_id"),
        F.lower("status").alias("status"),
    ).dropDuplicates(["task_id"])


def clean_suppliers(df: DataFrame) -> DataFrame:
    return df.select(
        "supplier_id", "name", "material_category",
        F.col("on_time_rate").cast("double").alias("on_time_rate"), "city",
    ).dropDuplicates(["supplier_id"])


def clean_material_orders(df: DataFrame) -> DataFrame:
    return df.select(
        "order_id", "task_id", "supplier_id", "material",
        F.col("quantity").cast("double").alias("quantity"),
        F.col("cost").cast("double").alias("cost"),
        _date("promised_date"), _date("delivered_date"),
    ).dropDuplicates(["order_id"])


def clean_change_orders(df: DataFrame) -> DataFrame:
    return df.select(
        "co_id", "project_id", "task_id", "reason",
        F.col("cost_impact").cast("double").alias("cost_impact"),
        F.col("days_impact").cast("int").alias("days_impact"),
        F.lower("status").alias("status"), _date("submitted_date"),
    ).dropDuplicates(["co_id"])


def clean_site_logs(df: DataFrame) -> DataFrame:
    return df.select(
        "log_id", "project_id", _date("date"),
        F.col("crew_count").cast("int").alias("crew_count"),
        F.lower("weather").alias("weather"),
        F.col("work_stopped_hours").cast("double").alias("work_stopped_hours"),
        F.col("incident_flag").cast("boolean").alias("incident_flag"),
    ).dropDuplicates(["log_id"])


# --------------------------------------------------------------------------
# 2. Enrich: ontology-ready versions of Task and MaterialOrder
# --------------------------------------------------------------------------

def tasks_enriched(tasks: DataFrame, as_of: str) -> DataFrame:
    as_of_d = F.to_date(F.lit(as_of))
    planned_days = F.greatest(F.datediff("planned_end", "planned_start"), F.lit(1))
    return (
        tasks
        .withColumn("planned_days", planned_days)
        .withColumn("planned_frac", F.greatest(F.lit(0.0), F.least(
            F.lit(1.0), F.datediff(as_of_d, "planned_start") / F.col("planned_days"))))
        .withColumn("start_delay_days", F.when(
            F.col("actual_start").isNotNull(), F.datediff("actual_start", "planned_start"))
            .when(F.col("planned_start") < as_of_d, F.datediff(as_of_d, "planned_start"))
            .otherwise(F.lit(0)))
        .withColumn("is_delayed", F.col("status") == "delayed")
    )


def material_orders_enriched(orders: DataFrame, tasks: DataFrame, as_of: str) -> DataFrame:
    as_of_d = F.to_date(F.lit(as_of))
    is_due = F.col("promised_date") <= as_of_d
    days_late = F.greatest(F.lit(0), F.datediff(F.coalesce("delivered_date", as_of_d), "promised_date"))
    return (
        orders
        .join(tasks.select("task_id", "project_id", "is_critical_path"), "task_id", "left")
        .withColumn("is_due", is_due)
        .withColumn("days_late", F.when(is_due, days_late))
        .withColumn("is_late", is_due & (days_late > LATE_GRACE_DAYS))
        .withColumn("is_open_overdue", is_due & F.col("delivered_date").isNull())
    )


# --------------------------------------------------------------------------
# 3. Metrics + score
# --------------------------------------------------------------------------

def project_metrics(projects: DataFrame, tasks_enr: DataFrame, orders_enr: DataFrame,
                    change_orders: DataFrame, site_logs: DataFrame, as_of: str) -> DataFrame:
    as_of_d = F.to_date(F.lit(as_of))

    pv = tasks_enr.groupBy("project_id").agg(
        (100 * F.sum(F.col("planned_days") * F.col("planned_frac")) / F.sum("planned_days"))
        .alias("planned_pct_complete"),
        F.sum(F.col("is_delayed").cast("int")).alias("delayed_task_count"))

    supply = (orders_enr
              .where(F.col("is_due") & F.col("is_critical_path") & F.col("is_late"))
              .groupBy("project_id").agg(
                  F.count("order_id").alias("late_cp_deliveries"),
                  F.avg("days_late").alias("avg_cp_days_late"),
                  F.array_sort(F.collect_set("supplier_id")).alias("late_cp_supplier_ids")))

    co = (change_orders.where(F.col("status") != "rejected")
          .groupBy("project_id").agg(
              F.count("co_id").alias("co_count"),
              F.sum("cost_impact").alias("co_cost"),
              F.sum("days_impact").alias("co_days")))

    wx = (site_logs
          .where((F.col("date") > F.date_sub(as_of_d, WEATHER_WINDOW_DAYS)) & (F.col("date") <= as_of_d))
          .groupBy("project_id").agg(
              F.sum("work_stopped_hours").alias("stopped_hours_90d"),
              F.count("log_id").alias("log_days_90d"),
              F.sum(F.col("incident_flag").cast("int")).alias("incidents_90d")))

    m = (projects
         .join(pv, "project_id", "left")
         .join(supply, "project_id", "left")
         .join(co, "project_id", "left")
         .join(wx, "project_id", "left")
         .fillna(0, subset=["delayed_task_count", "late_cp_deliveries", "avg_cp_days_late",
                            "co_count", "co_cost", "co_days",
                            "stopped_hours_90d", "log_days_90d", "incidents_90d"])
         .withColumn("late_cp_supplier_ids",
                     F.coalesce("late_cp_supplier_ids", F.array().cast("array<string>"))))

    return (m
            .withColumn("spi", F.when(F.col("planned_pct_complete") != 0,
                                      F.col("percent_complete") / F.col("planned_pct_complete")))
            .withColumn("earned_value", F.col("budget") * F.col("percent_complete") / 100)
            .withColumn("cpi", F.when(F.col("actual_cost_to_date") != 0,
                                      F.col("earned_value") / F.col("actual_cost_to_date")))
            .withColumn("co_cost_pct_budget", F.col("co_cost") / F.col("budget"))
            .withColumn("weather_stop_pct_90d", F.coalesce(
                F.when(F.col("log_days_90d") > 0,
                       F.col("stopped_hours_90d") / (F.col("log_days_90d") * SHIFT_HOURS)),
                F.lit(0.0))))


def _reasons(m: DataFrame) -> Column:
    """Array of plain-English reasons, strongest factor first (mirrors build_reasons)."""
    def r(c, n):  # half-even, so the text matches Python's f-string rounding
        return F.bround(F.col(c) if isinstance(c, str) else c, n)

    texts = {
        "schedule": F.format_string(
            "Schedule slipping: SPI %.2f - %.0f%% complete vs %.0f%% planned; %d tasks delayed",
            r("spi", 2), r("percent_complete", 0), r("planned_pct_complete", 0),
            F.col("delayed_task_count").cast("int")),
        "cost": F.format_string(
            "Cost overrun: CPI %.2f - $%.1fM spent for $%.1fM of earned value",
            r("cpi", 2), r(F.col("actual_cost_to_date") / 1e6, 1), r(F.col("earned_value") / 1e6, 1)),
        "supply": F.format_string(
            "Supply risk: %d late deliveries on critical-path tasks, avg %.0f days late (suppliers: %s)",
            F.col("late_cp_deliveries").cast("int"), r("avg_cp_days_late", 0),
            F.when(F.size("late_cp_supplier_ids") > 0, F.array_join("late_cp_supplier_ids", ", "))
            .otherwise(F.lit("n/a"))),
        "change_orders": F.format_string(
            "Scope churn: %d open/approved change orders worth %.1f%% of budget (+%d days claimed)",
            F.col("co_count").cast("int"), r(F.col("co_cost_pct_budget") * 100, 1), F.col("co_days").cast("int")),
        "weather": F.format_string(
            "Weather: %.0f%% of shift hours stopped in last %d days (%.0f h)",
            r(F.col("weather_stop_pct_90d") * 100, 0), F.lit(WEATHER_WINDOW_DAYS), r("stopped_hours_90d", 0)),
    }
    items = F.array(*[
        F.struct((-F.col(f"pts_{k}")).alias("neg_pts"), F.lit(i).alias("factor_order"),
                 texts[k].alias("text"), (F.col(f"pts_{k}") >= REASON_MIN_SHARE * WEIGHTS[k]).alias("hit"))
        for i, k in enumerate(WEIGHTS)
    ])
    # sort_array orders structs field by field: strongest first, ties in WEIGHTS order (like pandas)
    return F.transform(F.sort_array(F.filter(items, lambda r: r["hit"])), lambda r: r["text"])


def score(m: DataFrame) -> DataFrame:
    pts = {
        "schedule": WEIGHTS["schedule"] * _ramp(F.col("spi"), 1.0, SPI_FULL),
        "cost": WEIGHTS["cost"] * _ramp(F.col("cpi"), 1.0, CPI_FULL),
        "supply": WEIGHTS["supply"] / 2 * (_ramp(F.col("late_cp_deliveries"), 0, LATE_CP_COUNT_FULL)
                                           + _ramp(F.col("avg_cp_days_late"), 0, LATE_CP_AVG_DAYS_FULL)),
        "change_orders": WEIGHTS["change_orders"] / 2 * (_ramp(F.col("co_count"), 0, CO_COUNT_FULL)
                                                         + _ramp(F.col("co_cost_pct_budget"), 0, CO_COST_PCT_FULL)),
        "weather": WEIGHTS["weather"] * _ramp(F.col("weather_stop_pct_90d"), 0, WEATHER_STOP_PCT_FULL),
    }
    for k, expr in pts.items():
        # bround = half-even, matching pandas/numpy rounding
        m = m.withColumn(f"pts_{k}", F.bround(expr, 1))

    pts_cols = [F.col(f"pts_{k}") for k in WEIGHTS]
    total = pts_cols[0]
    for c in pts_cols[1:]:
        total = total + c
    biggest = F.greatest(*pts_cols)
    driver = None
    for k in WEIGHTS:  # first max wins, same as pandas idxmax
        cond = F.col(f"pts_{k}") == biggest
        driver = F.when(cond, F.lit(k)) if driver is None else driver.when(cond, F.lit(k))

    return (m
            .withColumn("risk_score", F.bround(total, 0).cast("int"))
            .withColumn("risk_level", F.when(F.col("risk_score") >= RED_AT, "red")
                        .when(F.col("risk_score") >= AMBER_AT, "amber").otherwise("green"))
            .withColumn("top_risk_driver", driver)
            .withColumn("risk_reasons", _reasons(m)))


OUTPUT_COLS = [
    "project_id", "name", "type", "city", "project_manager", "start_date", "planned_end_date",
    "as_of_date", "risk_score", "risk_level",
    "top_risk_driver", "risk_reasons", "pts_schedule", "pts_cost", "pts_supply", "pts_change_orders",
    "pts_weather", "spi", "cpi", "percent_complete", "planned_pct_complete", "budget",
    "actual_cost_to_date", "earned_value", "delayed_task_count", "late_cp_deliveries",
    "avg_cp_days_late", "co_count", "co_cost_pct_budget", "co_days", "weather_stop_pct_90d",
    "stopped_hours_90d", "incidents_90d",
]


def project_risk_scores(projects: DataFrame, tasks_enr: DataFrame, orders_enr: DataFrame,
                        change_orders: DataFrame, site_logs: DataFrame, as_of: str) -> DataFrame:
    m = project_metrics(projects, tasks_enr, orders_enr, change_orders, site_logs, as_of)
    scored = score(m).withColumn("as_of_date", F.to_date(F.lit(as_of)))
    return scored.select(*OUTPUT_COLS)


def supplier_scorecard(suppliers: DataFrame, orders_enr: DataFrame) -> DataFrame:
    due = orders_enr.where(F.col("is_due"))
    sc = due.groupBy("supplier_id").agg(
        F.count("order_id").alias("orders_due"),
        F.sum(F.col("is_late").cast("int")).alias("late_orders"),
        F.avg(F.when(F.col("is_late"), F.col("days_late"))).alias("avg_days_late"),
        F.sum(F.col("is_open_overdue").cast("int")).alias("open_overdue_orders"),
        F.countDistinct("project_id").alias("projects_supplied"),
        F.sum("cost").alias("order_value"))
    sc = (suppliers.join(sc, "supplier_id", "left")
          .fillna(0, subset=["orders_due", "late_orders", "open_overdue_orders", "projects_supplied",
                             "avg_days_late", "order_value"])
          .withColumn("observed_on_time_rate", F.when(
              F.col("orders_due") > 0, F.bround(1 - F.col("late_orders") / F.col("orders_due"), 3))))
    # is_flagged / flag_reason / flagged_at are edited by the "Flag Supplier" Action
    sc = (sc.withColumn("is_flagged", F.lit(False))
          .withColumn("flag_reason", F.lit(None).cast("string"))
          .withColumn("flagged_at", F.lit(None).cast("timestamp")))
    return sc.withColumn("reliability_flag", F
                         .when(F.col("orders_due") == 0, "no_history")
                         .when((F.col("observed_on_time_rate") < 0.75) & (F.col("orders_due") >= 5), "unreliable")
                         .when(F.col("observed_on_time_rate") < 0.88, "watch")
                         .otherwise("ok"))
