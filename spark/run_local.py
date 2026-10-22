"""
Run the PySpark pipeline locally against data/raw (needs Java 11/17 + `pip install pyspark`).

    python spark/run_local.py            # writes data/output_spark/*.csv

Uses exactly the same risk_logic module that runs in Foundry.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "foundry" / "transforms-python" / "src"))

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from construction_risk import risk_logic as rl  # noqa: E402
from construction_risk.config import AS_OF  # noqa: E402


def build(spark, raw_dir: Path) -> dict:
    read = lambda n: spark.read.option("header", True).csv(str(raw_dir / f"{n}.csv"))  # noqa: E731
    projects = rl.clean_projects(read("projects"))
    tasks = rl.clean_tasks(read("tasks"))
    suppliers = rl.clean_suppliers(read("suppliers"))
    orders = rl.clean_material_orders(read("material_orders"))
    cos = rl.clean_change_orders(read("change_orders"))
    logs = rl.clean_site_logs(read("site_logs"))

    tasks_enr = rl.tasks_enriched(tasks, AS_OF)
    orders_enr = rl.material_orders_enriched(orders, tasks, AS_OF)
    return {
        "tasks_enriched": tasks_enr,
        "material_orders_enriched": orders_enr,
        "project_risk_scores": rl.project_risk_scores(projects, tasks_enr, orders_enr, cos, logs, AS_OF),
        "supplier_scorecard": rl.supplier_scorecard(suppliers, orders_enr),
    }


def main() -> None:
    spark = SparkSession.builder.master("local[2]").appName("construction-risk").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    out = ROOT / "data" / "output_spark"
    for name, df in build(spark, ROOT / "data" / "raw").items():
        if "risk_reasons" in df.columns:  # CSV can't hold arrays
            df = df.withColumn("risk_reasons", F.to_json("risk_reasons"))
        if "late_cp_supplier_ids" in df.columns:
            df = df.drop("late_cp_supplier_ids")
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out / name))
        print(f"wrote {name}")
    build(spark, ROOT / "data" / "raw")["project_risk_scores"].select(
        "project_id", "name", "risk_score", "risk_level", "top_risk_driver").orderBy(F.desc("risk_score")).show(20, False)


if __name__ == "__main__":
    main()
