/**
 * Foundry Functions for the Construction Risk agent.
 *
 * Read-only tools (the AIP agent calls these to ground its answers):
 *   explainProjectRisk        - score, factor points, reasons, and the evidence behind them
 *   getLateCriticalPathOrders - Project -> Task -> MaterialOrder -> Supplier traversal
 *   getSupplierImpact         - which projects a supplier is hurting right now
 *   suggestMitigations        - deterministic playbook; the LLM phrases, it doesn't invent
 *
 * Edit functions (back the Actions the agent can propose):
 *   flagSupplier, createFollowUpTask
 *
 * API names below assume the Ontology was set up exactly as in docs/foundry_setup.md
 * (Step 4). If you renamed a property or link, update it here - the compiler will tell you.
 */
import { Edits, Function, LocalDate, OntologyEditFunction, Timestamp } from "@foundry/functions-api";
import { FollowUpTask, MaterialOrder, Objects, Project, Supplier } from "@foundry/ontology-api";

const FACTOR_WEIGHTS: Record<string, number> = {
    schedule: 25, cost: 25, supply: 20, change_orders: 15, weather: 15,
};

interface LateOrder {
    orderId: string;
    taskName: string;
    taskStatus: string;
    material: string;
    supplierId: string;
    supplierName: string;
    daysLate: number;
    stillOpen: boolean;
}

function lateCriticalPathOrders(project: Project): LateOrder[] {
    const result: LateOrder[] = [];
    for (const task of project.tasks.all()) {
        if (!task.isCriticalPath) {
            continue;
        }
        for (const order of task.materialOrders.all()) {
            if (!order.isLate) {
                continue;
            }
            const supplier = order.supplier.get();
            result.push({
                orderId: order.orderId,
                taskName: task.name ?? task.taskId,
                taskStatus: task.status ?? "unknown",
                material: order.material ?? "",
                supplierId: order.supplierId ?? "",
                supplierName: supplier?.name ?? "unknown supplier",
                daysLate: order.daysLate ?? 0,
                stillOpen: order.isOpenOverdue ?? false,
            });
        }
    }
    return result.sort((a, b) => b.daysLate - a.daysLate);
}

function bestAlternateSupplier(category: string, excludeId: string): Supplier | undefined {
    const candidates = Objects.search()
        .supplier()
        .filter(s => s.materialCategory.exactMatch(category))
        .all()
        .filter(s => s.supplierId !== excludeId && !s.isFlagged);
    return candidates.sort((a, b) => (b.observedOnTimeRate ?? 0) - (a.observedOnTimeRate ?? 0))[0];
}

export class ConstructionRiskFunctions {
    @Function()
    public explainProjectRisk(project: Project): string {
        const factors = Object.keys(FACTOR_WEIGHTS).map(key => {
            const pts: Record<string, number | undefined> = {
                schedule: project.ptsSchedule,
                cost: project.ptsCost,
                supply: project.ptsSupply,
                change_orders: project.ptsChangeOrders,
                weather: project.ptsWeather,
            };
            return { factor: key, points: pts[key] ?? 0, maxPoints: FACTOR_WEIGHTS[key] };
        });

        const openChangeOrders = project.changeOrders.all()
            .filter(co => co.status !== "rejected")
            .sort((a, b) => (b.costImpact ?? 0) - (a.costImpact ?? 0))
            .slice(0, 5)
            .map(co => ({
                coId: co.coId, reason: co.reason, status: co.status,
                costImpact: co.costImpact, daysImpact: co.daysImpact, taskId: co.taskId,
            }));

        const delayedCriticalTasks = project.tasks.all()
            .filter(t => t.isCriticalPath && t.status === "delayed")
            .map(t => ({ taskId: t.taskId, name: t.name, phase: t.phase, startDelayDays: t.startDelayDays }));

        return JSON.stringify({
            projectId: project.projectId,
            name: project.name,
            projectManager: project.projectManager,
            asOf: project.asOfDate?.toISOString(),
            riskScore: project.riskScore,
            riskLevel: project.riskLevel,
            topRiskDriver: project.topRiskDriver,
            reasons: project.riskReasons ?? [],
            factors,
            metrics: {
                spi: project.spi, cpi: project.cpi,
                percentComplete: project.percentComplete, plannedPercentComplete: project.plannedPctComplete,
                budget: project.budget, actualCostToDate: project.actualCostToDate,
                weatherStopPct90d: project.weatherStopPct90d,
            },
            evidence: {
                lateCriticalPathOrders: lateCriticalPathOrders(project).slice(0, 5),
                delayedCriticalTasks,
                largestOpenChangeOrders: openChangeOrders,
            },
        });
    }

    @Function()
    public getLateCriticalPathOrders(project: Project): string {
        return JSON.stringify(lateCriticalPathOrders(project));
    }

    @Function()
    public getSupplierImpact(supplier: Supplier): string {
        const lateOrders: MaterialOrder[] = supplier.materialOrders.all().filter(o => o.isLate);
        const byProject: Record<string, { orders: number; maxDaysLate: number; criticalPath: number }> = {};
        for (const o of lateOrders) {
            const pid = o.projectId ?? "unknown";
            const entry = byProject[pid] ?? { orders: 0, maxDaysLate: 0, criticalPath: 0 };
            entry.orders += 1;
            entry.maxDaysLate = Math.max(entry.maxDaysLate, o.daysLate ?? 0);
            entry.criticalPath += o.isCriticalPath ? 1 : 0;
            byProject[pid] = entry;
        }
        return JSON.stringify({
            supplierId: supplier.supplierId,
            name: supplier.name,
            category: supplier.materialCategory,
            statedOnTimeRate: supplier.onTimeRate,
            observedOnTimeRate: supplier.observedOnTimeRate,
            reliabilityFlag: supplier.reliabilityFlag,
            alreadyFlagged: supplier.isFlagged ?? false,
            lateOrders: lateOrders.length,
            affectedProjects: byProject,
        });
    }

    @Function()
    public suggestMitigations(project: Project): string[] {
        const out: string[] = [];
        const pts = (v?: number) => v ?? 0;

        if (pts(project.ptsSupply) >= 8) {
            const seen = new Set<string>();
            for (const o of lateCriticalPathOrders(project)) {
                if (seen.has(o.supplierId)) {
                    continue;
                }
                seen.add(o.supplierId);
                const supplier = Objects.search().supplier().filter(s => s.supplierId.exactMatch(o.supplierId)).all()[0];
                const alt = supplier ? bestAlternateSupplier(supplier.materialCategory ?? "", o.supplierId) : undefined;
                out.push(
                    `SUPPLY: ${o.supplierName} is ${o.daysLate} days late on "${o.material}" for critical task "${o.taskName}". ` +
                    (alt ? `Expedite or split the remaining order to ${alt.name} (${Math.round((alt.observedOnTimeRate ?? 0) * 100)}% on-time). `
                         : "Request a recovery schedule with penalty terms. ") +
                    `Action: Flag Supplier ${o.supplierId}.`);
            }
        }
        if (pts(project.ptsSchedule) >= 10) {
            out.push(`SCHEDULE: SPI ${project.spi?.toFixed(2)}. Re-baseline the critical path, add a second shift or ` +
                     `crew to the next delayed critical task, and fast-track non-dependent finishes. Action: Create Follow-up Task for the PM.`);
        }
        if (pts(project.ptsCost) >= 10 && project.cpi) {
            const eac = (project.budget ?? 0) / project.cpi;
            out.push(`COST: CPI ${project.cpi.toFixed(2)} projects an estimate-at-completion of $${(eac / 1e6).toFixed(1)}M ` +
                     `vs $${((project.budget ?? 0) / 1e6).toFixed(1)}M budget. Run a cost-to-complete review and freeze discretionary spend.`);
        }
        if (pts(project.ptsChangeOrders) >= 6) {
            out.push("CHANGE ORDERS: Escalate pending change orders to the owner for a decision this week and bundle related " +
                     "scope changes to cut re-sequencing churn.");
        }
        if (pts(project.ptsWeather) >= 6) {
            out.push("WEATHER: Add winter protection (hoarding + temporary heat) for exterior critical work, and move interior " +
                     "tasks forward during forecast cold/snow windows.");
        }
        return out.length > 0 ? out : ["No factor above its alert threshold - continue standard monitoring."];
    }

    @Edits(Supplier)
    @OntologyEditFunction()
    public flagSupplier(supplier: Supplier, reason: string): void {
        supplier.isFlagged = true;
        supplier.flagReason = reason;
        supplier.flaggedAt = Timestamp.now();
    }

    @Edits(FollowUpTask)
    @OntologyEditFunction()
    public createFollowUpTask(
        project: Project,
        title: string,
        description: string,
        owner: string,
        dueDate: LocalDate,
        supplier?: Supplier,
    ): void {
        const id = `FU-${project.projectId}-${Date.now()}`;
        const task = Objects.create().followUpTask(id);
        task.projectId = project.projectId;
        task.supplierId = supplier?.supplierId;
        task.title = title;
        task.description = description;
        task.owner = owner;
        task.dueDate = dueDate;
        task.status = "open";
        task.createdBy = "AIP Risk Agent";
        task.createdAt = Timestamp.now();
    }
}
