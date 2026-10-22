"""Raw uploads -> typed, de-duplicated clean datasets (one transform per table)."""
from transforms.api import transform_df, Input, Output

from construction_risk import risk_logic as rl
from construction_risk.config import RAW, CLEAN


@transform_df(Output(f"{CLEAN}/projects"), raw=Input(f"{RAW}/projects"))
def projects(raw):
    return rl.clean_projects(raw)


@transform_df(Output(f"{CLEAN}/tasks"), raw=Input(f"{RAW}/tasks"))
def tasks(raw):
    return rl.clean_tasks(raw)


@transform_df(Output(f"{CLEAN}/suppliers"), raw=Input(f"{RAW}/suppliers"))
def suppliers(raw):
    return rl.clean_suppliers(raw)


@transform_df(Output(f"{CLEAN}/material_orders"), raw=Input(f"{RAW}/material_orders"))
def material_orders(raw):
    return rl.clean_material_orders(raw)


@transform_df(Output(f"{CLEAN}/change_orders"), raw=Input(f"{RAW}/change_orders"))
def change_orders(raw):
    return rl.clean_change_orders(raw)


@transform_df(Output(f"{CLEAN}/site_logs"), raw=Input(f"{RAW}/site_logs"))
def site_logs(raw):
    return rl.clean_site_logs(raw)
