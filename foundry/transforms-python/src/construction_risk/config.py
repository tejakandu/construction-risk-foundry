"""Dataset paths and the scoring as-of date.

Change ROOT to the Foundry project folder you created (Step 1 of docs/foundry_setup.md).
"""
ROOT = "/jarvis-3cb318/Construction Risk"

RAW = f"{ROOT}/data/raw"
CLEAN = f"{ROOT}/data/clean"
ONTOLOGY = f"{ROOT}/data/ontology"

# The synthetic data is frozen at this date. On real data, swap for
# F.current_date() inside risk_logic (or a parameter) so scores refresh daily.
AS_OF = "2026-03-15"
