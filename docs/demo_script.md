# 2-minute demo script

Record in Loom at 1080p, with the Workshop app open and the agent widget visible. Reset demo state first
(un-flag SUP-13, delete test follow-ups).

| Time | Screen | Say |
|---|---|---|
| 0:00-0:15 | Workshop cockpit | "Construction firms find out a job is in trouble at the monthly cost report, when it's expensive to fix. The signals were there earlier, just spread across ERP, scheduling, procurement and site logs. I connected them in Foundry." |
| 0:15-0:35 | Data Lineage → Ontology graph | "Six sources, cleaned in PySpark, modelled as an Ontology: projects, tasks, material orders, suppliers, change orders, site logs. Everything downstream works on these objects." |
| 0:35-0:55 | Project table, click Highway 7 | "Every project gets a rule-based risk score. No black box. Highway 7 is 94 out of 100, and here's exactly why: SPI 0.49, CPI 0.67, late critical-path deliveries, change-order churn." |
| 0:55-1:30 | Agent: "Why is the Highway 7 Interchange at risk?" then "Is Huron Precast hurting other projects?" | "The agent doesn't guess. It walks the Ontology from project to critical-path task to material order to supplier. Huron Precast delivered culverts 45+ days late, and it's hitting two other projects too." |
| 1:30-1:50 | "Flag Huron Precast and create a follow-up for the PM" → Confirm | "It proposes governed Actions. I confirm, and it writes back to the Ontology with an audit trail. Here's the flagged supplier and the new follow-up task." |
| 1:50-2:00 | Back to cockpit | "Two days, from raw CSVs to an operational AIP app. Happy to walk through how I'd adapt it to your client's real data." |

Tips: pre-type the prompts in a notepad, run each once before recording so caches are warm, and keep
the browser zoom at 110% so text is readable.
