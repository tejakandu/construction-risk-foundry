"""Ontology backing datasets: one per object type that needs derived properties.

Project       <- project_risk_scores
Task          <- tasks_enriched
MaterialOrder <- material_orders_enriched
Supplier      <- supplier_scorecard
FollowUpTask  <- follow_up_tasks (empty; rows are created by an Action)
ChangeOrder / SiteLog are backed directly by their clean datasets.
"""
from pyspark.sql import types as T
from transforms.api import transform_df, Input, Output

from construction_risk import risk_logic as rl
from construction_risk.config import CLEAN, ONTOLOGY, AS_OF


@transform_df(Output(f"{ONTOLOGY}/tasks_enriched"), tasks=Input(f"{CLEAN}/tasks"))
def tasks_enriched(tasks):
    return rl.tasks_enriched(tasks, AS_OF)


@transform_df(
    Output(f"{ONTOLOGY}/material_orders_enriched"),
    orders=Input(f"{CLEAN}/material_orders"),
    tasks=Input(f"{CLEAN}/tasks"),
)
def material_orders_enriched(orders, tasks):
    return rl.material_orders_enriched(orders, tasks, AS_OF)


@transform_df(
    Output(f"{ONTOLOGY}/project_risk_scores"),
    projects=Input(f"{CLEAN}/projects"),
    tasks=Input(f"{ONTOLOGY}/tasks_enriched"),
    orders=Input(f"{ONTOLOGY}/material_orders_enriched"),
    change_orders=Input(f"{CLEAN}/change_orders"),
    site_logs=Input(f"{CLEAN}/site_logs"),
)
def project_risk_scores(projects, tasks, orders, change_orders, site_logs):
    return rl.project_risk_scores(projects, tasks, orders, change_orders, site_logs, AS_OF)


@transform_df(
    Output(f"{ONTOLOGY}/supplier_scorecard"),
    suppliers=Input(f"{CLEAN}/suppliers"),
    orders=Input(f"{ONTOLOGY}/material_orders_enriched"),
)
def supplier_scorecard(suppliers, orders):
    return rl.supplier_scorecard(suppliers, orders)


FOLLOW_UP_SCHEMA = T.StructType([
    T.StructField("follow_up_id", T.StringType()),
    T.StructField("project_id", T.StringType()),
    T.StructField("supplier_id", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("description", T.StringType()),
    T.StructField("owner", T.StringType()),
    T.StructField("due_date", T.DateType()),
    T.StructField("status", T.StringType()),
    T.StructField("created_by", T.StringType()),
    T.StructField("created_at", T.TimestampType()),
])


@transform_df(Output(f"{ONTOLOGY}/follow_up_tasks"))
def follow_up_tasks(ctx):
    return ctx.spark_session.createDataFrame([], FOLLOW_UP_SCHEMA)
