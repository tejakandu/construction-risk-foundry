# Foundry setup guide (beginner-friendly)

This walks you from six CSVs to a working Ontology, Workshop dashboard, and AIP agent.
It assumes you know Spark/BigQuery well but have never used Foundry.

> **Heads-up on UI labels.** Foundry's UI changes often, and stacks differ in which apps
> are enabled. If a button isn't where this says, search for the app name with the
> top-left **Search** (or press `Cmd/Ctrl + J`) and look for the closest equivalent.
> The *concepts* (datasets, transforms, object types, links, actions) don't change.

**Mental model for a GCP engineer**

| GCP / your world          | Foundry                                                  |
|---------------------------|----------------------------------------------------------|
| GCS bucket + BQ table     | **Dataset** (versioned files + schema, every build is a transaction) |
| Dataform / dbt / Composer | **Code Repositories** (PySpark transforms) or **Pipeline Builder** (no-code), plus **Schedules** |
| Data Catalog lineage      | **Data Lineage** app (automatic)                         |
| A semantic/entity layer   | **Ontology**: object types, properties, link types       |
| Writeback API / CRUD      | **Actions** (governed edits to objects)                  |
| Looker / Data Studio app  | **Workshop**                                             |
| Vertex AI agent + tools   | **AIP Agent Studio** / **AIP Logic** with Ontology-aware tools |

Target layout you'll build (Day 1 = steps 1–5, Day 2 = steps 6–8):

```
/<your-space>/Construction Risk/
├── data/raw/          6 uploaded CSV datasets
├── data/clean/        6 typed datasets              (Code Repo: datasets/clean.py)
├── data/ontology/     tasks_enriched, material_orders_enriched,
│                      project_risk_scores, supplier_scorecard,
│                      follow_up_tasks               (Code Repo: datasets/ontology.py)
├── construction-risk-transforms   (Code Repository, Python)
├── construction-risk-functions    (Code Repository, TypeScript Functions)
├── Construction Risk Cockpit      (Workshop app)
└── Project Risk Agent             (AIP Agent)
```

---

## Step 0 - Generate the data locally (5 min)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python src/generate_data.py      # -> data/raw/*.csv
python src/risk_scoring.py       # -> data/output/*.csv (reference answers to check Foundry against)
```

Keep `data/output/project_risk_scores.csv` open. Once Foundry builds, its scores should match it
(P-1014 = 94, P-1017 = 86, P-1008 = 84, P-1003 = 66).

---

## Step 1 - Create a Project and upload the raw datasets (15 min)

1. **Files** (or **Projects & Files**) → **+ New project**. Name it `Construction Risk`.
   Put it in a space/portfolio you own.
   Note the full path (e.g. `/<your-space>/Construction Risk`). If it differs,
   edit `ROOT` in `foundry/transforms-python/src/construction_risk/config.py`.
2. Inside the project create folders: `data/raw`, `data/clean`, `data/ontology`.
3. Open `data/raw` → **+ New → Upload files** → select all six CSVs from `data/raw/`.
   - Choose **"Upload as individual datasets"** (one dataset per file).
   - Keep the names exactly: `projects`, `tasks`, `suppliers`, `material_orders`,
     `change_orders`, `site_logs` (drop the `.csv` suffix if Foundry keeps it).
4. Open each dataset. If you see a banner like *"This dataset has no schema"*, click
   **Apply schema** (or **Edit schema → Infer**) and save. Types don't matter much: the clean
   transforms cast every column explicitly, so all-strings is fine.

✅ Check: the `site_logs` preview shows ~6,000 rows; `tasks` ~450.

---

## Step 2 - PySpark transforms in Code Repositories (main path, 45 min)

### 2.1 Create the repository
1. In the project root: **+ New → Code repository**.
   Name `construction-risk-transforms`, language/template **Python → Transforms** (sometimes
   labelled "Python Library / Pipelines" - pick the one that mentions *transforms*).
2. Wait for it to initialize. You'll see `transforms-python/src/myproject/` with
   `pipeline.py` and `datasets/examples.py`, plus `transforms-python/setup.py`
   (newer templates may use `pyproject.toml` / `build.gradle` instead).

### 2.2 Copy the code in
The repo layout in this project mirrors the Foundry template on purpose.

1. Delete `src/myproject/datasets/examples.py`.
2. Create a folder `src/construction_risk/` and copy in these files from
   `foundry/transforms-python/src/construction_risk/`:
   `__init__.py`, `config.py`, `pipeline.py`, `risk_logic.py`,
   `datasets/__init__.py`, `datasets/clean.py`, `datasets/ontology.py`.
   (In the web editor: right-click the folder → New file → paste. Or clone the repo
   locally with the **Work locally** button and copy the folder in.)
3. Delete the old `src/myproject/` folder.
4. Point the build at the new package. In `transforms-python/setup.py`, find
   ```python
   entry_points={'transforms.pipelines': ['root = myproject.pipeline:my_pipeline']}
   ```
   and change `myproject` → `construction_risk`. (If your template has no `setup.py`, search the
   repo for `myproject.pipeline` and update it wherever it appears.)
5. Check `config.py` → `ROOT` matches your project path from Step 1.

### 2.3 Preview, commit, build
1. Open `datasets/clean.py`, click into `def tasks(...)`, and press **Preview** (top right).
   Foundry runs the transform on a sample. Confirm the date columns show as `date`
   and `is_critical_path` as `boolean`.
2. **Commit** (top right, write a message) → this triggers **Checks** (CI). Wait for green.
3. Build the outputs. Either:
   - open each output dataset (the path in `Output(...)` is a link after CI) and click **Build**, or
   - open **Data Lineage** from `data/clean/projects`, select all downstream nodes →
     **Build** (builds in dependency order).
   Build order: 6 clean datasets → `tasks_enriched`, `material_orders_enriched` →
   `project_risk_scores`, `supplier_scorecard`, `follow_up_tasks`.
4. Open `data/ontology/project_risk_scores` and sort by `risk_score` descending.

✅ Check: 4 red projects (P-1014, P-1017, P-1008, P-1003), scores within ±1 of the local
reference CSV, `risk_reasons` is an **array** column.

### 2.4 Schedule it (2 min)
On `project_risk_scores` → **Schedules → New schedule** → trigger "when `data/raw/*` updates"
(or daily) → target the whole lineage. Mention this in the demo: *"new site logs land, the
score refreshes, no one rebuilds a spreadsheet."*

> **Talking point:** `risk_logic.py` is pure PySpark with no Foundry imports. The same file runs
> locally (`spark/run_local.py`) and is unit-tested against the pandas reference. The Foundry
> wrappers in `datasets/*.py` are ~5 lines each.

---

## Step 3 - (Appendix) The same cleaning in Pipeline Builder (optional, 20 min)

Do this for **one** table so you can say you've used both tools. Code Repos remains the system of record.

1. **+ New → Pipeline** → *Batch pipeline* → name `construction-risk-pipeline-builder`.
2. **Add data** → pick `data/raw/tasks` and `data/raw/material_orders`.
3. On `tasks`: **Transform** → add boards:
   - *Cast* `planned_start`, `planned_end`, `actual_start`, `actual_end` → Date
   - *Cast* `is_critical_path` → Boolean
   - *Replace empty string with null* on `depends_on_task_id`
4. On `material_orders`: *Cast* dates; then **Join** (left) to the cleaned `tasks` on
   `task_id`, keeping `project_id`, `is_critical_path`.
5. Add *New column* `days_late` = `date_diff(coalesce(delivered_date, '2026-03-15'), promised_date)`
   and *New column* `is_late` = `days_late > 2 AND promised_date <= '2026-03-15'`.
6. **Add output** → new dataset `data/clean/pb_material_orders_enriched` → **Deploy** → **Build**.
7. Compare row counts / `is_late` totals with the Code Repo output.

Why not the whole score in Pipeline Builder? You could (aggregate + join + expression
boards), but the 5-factor score with array-of-reasons is clearer, testable, and diffable as code.

---

## Step 4 - Build the Ontology (60 min)

Open **Ontology Manager** → **+ New → Object type** for each row below.
For every object type: pick the backing dataset, set the **primary key**, set the **title
property**, and keep the auto-generated property **API names** (camelCase of the column name,
e.g. `risk_score` → `riskScore`). The TypeScript functions assume those defaults.

### 4.1 Object types

| Object type    | API name        | Backing dataset                         | Primary key    | Title          | Icon / color hint |
|----------------|-----------------|------------------------------------------|----------------|----------------|-------------------|
| Project        | `Project`       | `data/ontology/project_risk_scores`      | `project_id`   | `name`         | building, blue    |
| Task           | `Task`          | `data/ontology/tasks_enriched`           | `task_id`      | `name`         | checklist         |
| Material Order | `MaterialOrder` | `data/ontology/material_orders_enriched` | `order_id`     | `material`     | truck             |
| Supplier       | `Supplier`      | `data/ontology/supplier_scorecard`       | `supplier_id`  | `name`         | factory           |
| Change Order   | `ChangeOrder`   | `data/clean/change_orders`               | `co_id`        | `reason`       | document          |
| Site Log       | `SiteLog`       | `data/clean/site_logs`                   | `log_id`       | `date`         | calendar          |
| Follow-up Task | `FollowUpTask`  | `data/ontology/follow_up_tasks`          | `follow_up_id` | `title`        | flag, orange      |

Property tips:
- **Project**: give `risk_reasons` the type *Array<String>*. Set **Display name** on the
  `pts_*` columns ("Schedule points", ...). Add a **value formatting** rule on `risk_level`
  (red/amber/green colors). Mark `budget`, `actual_cost_to_date`, `earned_value` as currency (CAD).
- **Supplier** and **FollowUpTask**: open the **Capabilities** (or *Datasources → Edits*) tab
  and make sure **edits are allowed**. This is what lets Actions write `is_flagged` and create follow-ups.
  `follow_up_tasks` is intentionally empty; every row will come from an Action.
- Write a 1-line **description** on every object type and the key properties. *AIP reads these.*
  For example, on `Project.spi`: "Schedule Performance Index = % complete ÷ % planned. Below 1 is behind schedule."

### 4.2 Link types

**+ New → Link type**. All are one-to-many using a foreign key on the "many" side.

| Link (one side → many side)          | Foreign key on many side | API name on one side | API name on many side |
|---------------------------------------|--------------------------|----------------------|-----------------------|
| Project → Task                        | `Task.project_id`        | `tasks`              | `project`             |
| Task → MaterialOrder                  | `MaterialOrder.task_id`  | `materialOrders`     | `task`                |
| Supplier → MaterialOrder              | `MaterialOrder.supplier_id` | `materialOrders`  | `supplier`            |
| Project → MaterialOrder *(shortcut)*  | `MaterialOrder.project_id` | `materialOrders`   | `project`             |
| Project → ChangeOrder                 | `ChangeOrder.project_id` | `changeOrders`       | `project`             |
| Task → ChangeOrder                    | `ChangeOrder.task_id`    | `changeOrders`       | `task`                |
| Project → SiteLog                     | `SiteLog.project_id`     | `siteLogs`           | `project`             |
| Task → Task (dependency)              | `Task.depends_on_task_id`| `dependents`         | `dependsOn`           |
| Project → FollowUpTask                | `FollowUpTask.project_id`| `followUpTasks`      | `project`             |
| Supplier → FollowUpTask               | `FollowUpTask.supplier_id` | `followUpTasks`    | `supplier`            |

Click **Save** (top right) → review the change list → **Save to Ontology**. Indexing takes a few minutes.

✅ Check in **Object Explorer**: search "Highway 7" → open the Project → the **Links** panel
shows 20-ish Tasks, Change Orders, Site Logs. Click a Task → Material Orders → Supplier = *Huron
Precast & Pipe*. That path, Project → Task → MaterialOrder → Supplier, is the traversal the agent uses.

---

## Step 5 - Actions (30 min)

Actions are how users and the agent make *governed* changes. Each has parameters, rules, and
submission criteria, and Foundry logs who did what.

### 5.1 TypeScript Functions repo (needed for Action 2 and the agent tools)
1. **+ New → Code repository** → **TypeScript → Functions**. Name `construction-risk-functions`.
2. Settings → **Ontology imports** (or the *Resource imports* sidebar) → add all 7 object types
   so `@foundry/ontology-api` generates their types.
3. Replace `functions-typescript/src/index.ts` with `foundry/functions-typescript/src/index.ts`
   from this repo. If anything is red, it's almost always an API name that differs from the
   tables above. Hover to see the generated name and fix it in code.
4. Use the **Functions** helper panel to **Run** `explainProjectRisk` with Project `P-1014`.
   You should see JSON containing the five Huron Precast late orders.
5. **Commit** → wait for checks → **Tag version** (e.g. `0.1.0`) → **Publish**. Functions
   are only usable in Actions/Workshop/AIP after a published tag.

### 5.2 Action: Flag Supplier (no-code rule)
Ontology Manager → **+ New → Action type**.
- Name `Flag Supplier`, API name `flag-supplier`.
- Parameters: `supplier` (Object reference → Supplier, required), `reason` (String, required, multiline).
- Rule: **Modify object** → object = `supplier` parameter → set
  `is_flagged` = `true`, `flag_reason` = `reason`, `flagged_at` = *current time*.
- Submission criteria (optional but impressive): `supplier.is_flagged` **is not** `true`
  → failure message "Supplier is already flagged".
- (Alternative: a **Function rule** pointing at `flagSupplier` from 5.1.)

### 5.3 Action: Create Follow-up Task (function-backed)
- Name `Create Follow-up Task`, API name `create-follow-up-task`.
- Rule: **Function** → `createFollowUpTask` (from your published TS repo). Foundry auto-creates
  the parameters `project`, `title`, `description`, `owner`, `dueDate`, `supplier (optional)`.
  It's function-backed because the function generates the primary key (`FU-<project>-<timestamp>`).
- Default `owner` to the Project's `project_manager` via parameter default (*Object property*).

Test both from Object Explorer (Project or Supplier → **Actions** menu). Then check the
`follow_up_tasks` object type has a row.

---

## Step 6 - Workshop dashboard "Construction Risk Cockpit" (Day 2, 60 min)

**+ New → Workshop module**. Layout: header, then **two columns** (left 60% / right 40%).

**Variables** (left sidebar → *Variables*): create `selectedProject` (Object set, Project),
and `riskFilter` (string array).

**Left column**
1. **Metric cards** row (3 widgets, *Metric card* / *Object set aggregation*):
   - Red projects = count of Project where `risk_level = red`
   - Budget at risk = sum of `budget` where `risk_level = red` (≈ $214M)
   - Unreliable suppliers = count of Supplier where `reliability_flag = unreliable`
2. **Filter list** on Project.`risk_level` and `type` → output to the table's input.
3. **Object table**: input = all Projects (filtered), columns: name, risk_level, risk_score,
   top_risk_driver, spi, cpi, project_manager. Sort by `risk_score` desc. Conditional formatting on
   `risk_level`. **Active object** → `selectedProject`.
4. **Chart: XY / bar**: Projects, x = name, y = risk_score, color by risk_level (top 10).

**Right column (driven by `selectedProject`)**
5. **Object view** / *Property list*: risk_score, risk_level, `risk_reasons` (renders as a list),
   the five `pts_*` values, SPI/CPI.
6. **Linked objects table**: `selectedProject` → *Search Around* → MaterialOrder,
   filter `is_late = true AND is_critical_path = true`, columns: material, supplier, days_late.
7. **Button group**: `Create Follow-up Task` (Action, default `project` = selectedProject)
   and `Flag Supplier`.
8. **AIP Agent widget** (Step 7) docked at the bottom/right, with `selectedProject` passed in
   as context.

Second page/tab **Suppliers**: object table of Supplier (observed vs stated on-time rate,
late_orders, projects_supplied, is_flagged) with the Flag Supplier button.

Hit **Save & publish**. Grab screenshots for the README now.

---

## Step 7 - AIP agent "Project Risk Agent" (Day 2, 60-90 min)

Two options. Build **A**, and add **B** if time allows.

### A. AIP Agent Studio (conversational agent with tools)

**+ New → AIP Agent** (Agent Studio). Model: the strongest available on your stack (e.g. a
Claude or GPT-4-class model).

**System prompt** (paste and adjust):

```text
You are the Project Risk Agent for a construction general contractor in Ontario.
You help project managers understand WHY a project is at risk and WHAT to do next.

Rules:
1. Ground every claim in tool output. Never invent numbers, suppliers or dates.
2. For "why is project X at risk?", call explainProjectRisk first. Explain the score
   using its factors (each factor has points out of a max). Lead with the largest factor.
3. Trace root causes through the Ontology: Project -> Task -> MaterialOrder -> Supplier,
   and Project -> ChangeOrder / SiteLog. Name specific tasks, orders, suppliers and days late.
4. For a supplier, call getSupplierImpact to show whether the problem spans multiple projects.
5. Mitigations: call suggestMitigations and present them as a short, prioritized list.
   You may rephrase but not add mitigations that contradict the tool output.
6. Actions (Flag Supplier, Create Follow-up Task) change data. Propose them with the exact
   parameters and ask the user to confirm before applying.
7. The score is rule-based: risk_score = schedule(25) + cost(25) + supply(20) +
   change orders(15) + weather(15). Green < 35 <= amber < 60 <= red.
8. Keep answers under ~200 words unless asked for detail. Use the project's name and ID.
```

**Tools** (Agent Studio → *Tools* → *Add tool*):

| Tool type            | Configure                                   | Description to give the agent |
|----------------------|---------------------------------------------|-------------------------------|
| Object query / Ontology search | Object types: Project, Supplier, Task, MaterialOrder, ChangeOrder | "Find projects or suppliers by name, ID, city, risk level. Use to resolve 'Highway 7' to P-1014 or to list red projects." |
| Function             | `explainProjectRisk(project)`               | "Returns the risk score, factor points, reasons, and evidence (late critical-path orders, delayed critical tasks, largest change orders) for one project." |
| Function             | `getLateCriticalPathOrders(project)`        | "Lists late material orders on critical-path tasks with supplier and days late." |
| Function             | `getSupplierImpact(supplier)`               | "Shows one supplier's observed on-time rate and which projects its late orders affect." |
| Function             | `suggestMitigations(project)`               | "Rule-based mitigation playbook for a project, including alternate suppliers." |
| Action               | `Flag Supplier`                             | "Flag a supplier as unreliable with a reason. Requires user confirmation." |
| Action               | `Create Follow-up Task`                     | "Create a follow-up task for a project (optionally tied to a supplier) with an owner and due date. Requires user confirmation." |

Set both Actions to **require confirmation** (the default in most stacks).
Under **Application state / variables**, add `selectedProject` (Project) so the Workshop widget
can pass the row the user clicked. Then **Publish** and embed it in Workshop (Step 6.8).

**Example prompts to test (and use in the demo):**

1. *"Which projects are red right now and what's driving each?"*
2. *"Why is the Highway 7 Interchange at risk?"*
   Expected answer shape: score 94/100 red. Schedule 22% vs 44% planned (SPI 0.49) and CPI
   0.67. Root cause: Huron Precast & Pipe (SUP-13) delivered culvert sections 45-47 days late
   for *Culvert placement*, a critical-path task, which pushed the drainage phase back. Also
   7 change orders (+88 days) and the Jan-Feb weather stoppages.
3. *"Is Huron Precast hurting other projects?"* → P-1017 Eastside Transit Hub and P-1015.
4. *"What should we do about it?"* → expedite/split the order to Lakehead Pipe & Precast
   (SUP-14, ~97% on-time), re-baseline the critical path, escalate pending change orders.
5. *"Flag Huron Precast and create a follow-up for the PM to get a recovery schedule by Friday."*
   → the agent proposes both Actions with parameters → you click **Confirm** → show the new
   FollowUpTask and the flagged Supplier in Workshop. **This is the money shot of the demo.**
6. *"Why is Tech Park Building C green even though it had late deliveries?"* (shows explainability:
   the agent cites points per factor)

### B. AIP Logic function "Generate Risk Briefing" (optional)
**+ New → AIP Logic**. Input: `project` (Project). Blocks:
1. *Use LLM* block. Prompt: "Write a 5-bullet executive risk briefing for {project.name}…",
   with tools = the `explainProjectRisk` and `suggestMitigations` functions.
2. Output: String.
Publish, then add a **Button → Run function** in Workshop that writes the briefing into a
Markdown widget. Useful for a "weekly owner report" story.

### Evaluate (15 min)
Use **AIP Evals** (if enabled) with 5 test cases: the 4 red projects + 1 green, asserting the
answer mentions the correct top driver and supplier. Talking point: *"I test the agent like code."*

---

## Step 8 - Demo checklist

- [ ] All builds green, schedule set
- [ ] Workshop published; screenshots saved to `docs/img/` (dashboard, project drill-down, agent chat, action confirmation)
- [ ] Agent answers prompts 2-5 cleanly (run twice, check consistency)
- [ ] Reset demo state: un-flag SUP-13 and delete test follow-ups before recording
- [ ] Record the 2-minute demo (`docs/demo_script.md`)

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Input ... not found` in CI | `ROOT` in `config.py` doesn't match the project path, or a raw dataset name has `.csv` on the end |
| Dates all null after clean | Raw column has a non-ISO format; adjust `F.to_date(col, "M/d/yyyy")` in `_date()` |
| `risk_reasons` shows as string in Ontology | Change the property base type to Array<String>; the dataset column is already an array |
| TS: `Property 'ptsSupply' does not exist` | API name differs; check the object type's property API names and update `index.ts` |
| Action "Edits not allowed" | Enable edits on Supplier / FollowUpTask (Ontology Manager → Capabilities / Datasources) |
| Agent ignores tools / hallucinates | Tighten tool descriptions, add object/property descriptions, lower temperature, keep rule 1 in the prompt |
