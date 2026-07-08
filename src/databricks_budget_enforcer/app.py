"""Top-level orchestrator.

Notebook / CLI entry points:

    enforcer = BudgetEnforcer(config)
    enforcer.plan()    # weekly: rebuild the allowance from the CUR
    enforcer.check()   # hourly: track pace and (dry-run by default) throttle

All external dependencies (CUR reader, system-tables source, workspace ops,
state store) are injectable for testing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from databricks_budget_enforcer.config import EnforcerConfig
from databricks_budget_enforcer.enforce import scheduler
from databricks_budget_enforcer.enforce.engine import Decision, decide, execute
from databricks_budget_enforcer.enforce.levers import LeverContext
from databricks_budget_enforcer.fiscal import FiscalCalendar
from databricks_budget_enforcer.ingest.calibrate import Calibration, calibrate
from databricks_budget_enforcer.ingest.cur import CurReader
from databricks_budget_enforcer.monitor.tracker import (
    BudgetStatus,
    Severity,
    compute_status,
)
from databricks_budget_enforcer.planner.forecast import forecast_remaining_day
from databricks_budget_enforcer.planner.profile import day_of_week_profile
from databricks_budget_enforcer.planner.weekly import WeeklyPlan, build_weekly_plan
from databricks_budget_enforcer.report import reporter
from databricks_budget_enforcer.state.store import JsonStateStore
from databricks_budget_enforcer.workspace import Priority

log = logging.getLogger(__name__)


@dataclass
class CheckResult:
    status: BudgetStatus
    decision: Decision
    report: str
    #: FIFO-released jobs this check (names), in release order.
    released: list[str] = field(default_factory=list)


class BudgetEnforcer:
    def __init__(
        self,
        config: EnforcerConfig,
        ops=None,
        usage_source=None,
        cur_reader: CurReader | None = None,
        state: JsonStateStore | None = None,
    ):
        self.config = config
        self.calendar = FiscalCalendar(
            fy_start_month=config.fy_start_month,
            fy_start_day=config.fy_start_day,
            week_start_weekday=config.week_start_weekday,
        )
        self.cur_reader = cur_reader or CurReader(config.cur)
        self.state = state or JsonStateStore(config.state.path)
        self._ops = ops
        self._usage_source = usage_source

    # -- lazy SDK wiring ---------------------------------------------------

    @property
    def ops(self):
        if self._ops is None:
            from databricks_budget_enforcer.workspace import SdkWorkspaceOps

            self._ops = SdkWorkspaceOps(self.config.databricks)
        return self._ops

    @property
    def usage_source(self):
        if self._usage_source is None:
            from databricks_budget_enforcer.ingest.dbx_usage import (
                SystemTablesUsageSource,
            )

            self._usage_source = SystemTablesUsageSource(
                self.ops.client, self.config.databricks.warehouse_id
            )
        return self._usage_source

    # -- weekly planning ---------------------------------------------------

    def plan(self, as_of: date | None = None) -> WeeklyPlan:
        """Rebuild the weekly allowance and per-day targets from the spend
        ledger (CUR, plus Databricks-invoiced DBU dollars when
        include_dbu_invoice is on), recalibrate the infra multiplier,
        persist both."""
        as_of = as_of or datetime.now(timezone.utc).date()
        daily = self._ledger_daily_costs(as_of)
        profile = day_of_week_profile(
            daily,
            as_of,
            week_start_weekday=self.config.week_start_weekday,
            trailing_weeks=self.config.forecast.trailing_weeks,
        )
        plan = build_weekly_plan(
            self.calendar, self.config.annual_budget, daily, profile, as_of
        )
        calibration = self._calibrate(daily, as_of)

        self.state.set_plan(plan.to_dict())
        self.state.set_calibration(calibration.__dict__)
        self.state.append_audit(
            "plan",
            {
                "week_start": plan.week_start.isoformat(),
                "weekly_allowance": plan.weekly_allowance,
                "multiplier": calibration.global_multiplier,
            },
        )
        log.info(
            "weekly plan: allowance $%.2f (remaining $%.2f / %.1f weeks), "
            "multiplier %.2f",
            plan.weekly_allowance, plan.remaining_budget, plan.remaining_weeks,
            calibration.global_multiplier,
        )
        return plan

    def _ledger_daily_costs(self, as_of: date):
        """Daily spend ledger. Default: Databricks-attributed CUR costs.
        With include_dbu_invoice: non-Marketplace CUR costs (the infra half)
        plus system-table DBU dollars at list price (the invoice half) -
        Marketplace DBU charges in the CUR are excluded so the two sources
        never both count DBUs."""
        loaded = self.cur_reader.load()
        if not self.config.include_dbu_invoice:
            return self.cur_reader.daily_costs(loaded)

        import pandas as pd

        marketplace_total = float(loaded.loc[loaded["is_marketplace"], "cost"].sum())
        if marketplace_total > 0:
            log.warning(
                "include_dbu_invoice: excluding $%.2f of Databricks "
                "Marketplace charges from the CUR ledger; DBU dollars are "
                "sourced from system.billing.usage instead. If this "
                "workspace is actually Marketplace-billed, turn the flag off.",
                marketplace_total,
            )
        cur_daily = self.cur_reader.daily_costs(loaded[~loaded["is_marketplace"]])

        fy_start = self.calendar.fy_start(as_of)
        dbu_daily = self.usage_source.daily_dbu_dollars(
            fy_start, as_of + timedelta(days=1)
        )
        log.info(
            "ledger: $%.2f CUR infrastructure + $%.2f Databricks-invoice DBU",
            cur_daily["cost"].sum(), dbu_daily["dbu_dollars"].sum(),
        )
        merged = pd.merge(cur_daily, dbu_daily, on="usage_date", how="outer").fillna(0.0)
        merged["cost"] = merged["cost"] + merged["dbu_dollars"]
        return (
            merged[["usage_date", "cost"]]
            .sort_values("usage_date")
            .reset_index(drop=True)
        )

    def _calibrate(self, daily, as_of: date) -> Calibration:
        fc = self.config.forecast
        try:
            now = datetime.now(timezone.utc)
            usage = self.usage_source.hourly_usage(now - timedelta(days=32), now)
            return calibrate(
                daily, usage, as_of,
                default_multiplier=fc.default_infra_multiplier,
                overrides=fc.multiplier_overrides,
            )
        except Exception as exc:
            log.warning(
                "calibration unavailable (%s); using default multiplier %.2f",
                exc, fc.default_infra_multiplier,
            )
            return Calibration(
                global_multiplier=fc.default_infra_multiplier,
                overrides=dict(fc.multiplier_overrides),
            )

    # -- hourly check ------------------------------------------------------

    def check(self, now: datetime | None = None) -> CheckResult:
        now = now or datetime.now(timezone.utc)
        plan = self._current_plan(now)
        calibration = self._stored_calibration()
        fc = self.config.forecast

        window_start = now - timedelta(days=fc.trailing_days + 1)
        usage = self.usage_source.hourly_usage(window_start, now)

        meta, context = self._inventory()
        forecasts = forecast_remaining_day(
            usage, now, calibration,
            trailing_days=fc.trailing_days, workload_meta=meta,
        )
        # Jobs sitting in the deferral queue are paused - they will not run,
        # so their history-based forecasts must not count against today.
        queue = self.state.get_deferral_queue()
        queued_ids = {entry.job_id for entry in queue}
        forecasts = [
            f
            for f in forecasts
            if not (f.workload_type == "JOB" and f.workload_id in queued_ids)
        ]
        context.forecasts = forecasts

        status = compute_status(
            plan, usage, forecasts, now, calibration,
            self.config.thresholds,
            lag_hours=fc.usage_lag_hours,
            trailing_days=fc.trailing_days,
        )

        active = self.state.get_active_actions()
        decision = decide(status, context, active)
        ops = None if self.config.dry_run else self.ops
        updated = execute(decision, ops, active, self.config.dry_run)

        # Newly applied schedule pauses join the FIFO queue.
        for action in decision.to_apply:
            if action.kind == "job_schedule_pause" and action.applied_at:
                queue.append(scheduler.entry_for_pause(action))

        released, queue, updated = self._release_deferred(
            status, queue, updated
        )
        self.state.set_active_actions(updated)
        self.state.set_deferral_queue(queue)

        self.state.append_audit(
            "check",
            {
                "severity": status.severity.value,
                "pace_ratio": round(status.pace_ratio, 4),
                "required_reduction": round(status.required_reduction_today, 2),
                "dry_run": self.config.dry_run,
                "actions": [a.to_dict() for a in decision.to_apply],
                "reverts": [a.to_dict() for a in decision.to_revert],
                "released": released,
                "deferral_queue": [e.to_dict() for e in queue],
            },
        )
        report = reporter.render_status(
            status, decision, self.config.dry_run, queue=queue, released=released
        )
        return CheckResult(
            status=status, decision=decision, report=report, released=released
        )

    def _release_deferred(self, status, queue, active):
        """Release queued jobs strictly in pause order when pace has
        recovered and today has dollar headroom. Returns
        (released job names, remaining queue, updated active actions)."""
        levers = self.config.levers
        if (
            not levers.fifo_release
            or not queue
            or status.severity not in (Severity.OK, Severity.WARN)
        ):
            return [], queue, active

        headroom = max(
            0.0,
            status.day_target - status.spend_today - status.forecast_remaining_today,
        )
        to_release, remaining = scheduler.plan_releases(queue, headroom)
        if not to_release:
            log.info(
                "deferral queue: head '%s' needs $%.2f but headroom is $%.2f - "
                "holding %d job(s)",
                queue[0].name, queue[0].est_run_cost, headroom, len(queue),
            )
            return [], queue, active

        released: list[str] = []
        for entry in to_release:
            if self.config.dry_run:
                log.info(
                    "WOULD release '%s' from deferral queue (est $%.2f): "
                    "unpause schedule%s",
                    entry.name, entry.est_run_cost,
                    " + trigger catch-up run" if levers.release_missed_runs else "",
                )
                released.append(entry.name)
                continue
            try:
                self.ops.set_job_schedule_paused(entry.job_id, False)
                if levers.release_missed_runs:
                    self.ops.run_job_now(entry.job_id)
                active = [
                    a
                    for a in active
                    if not (
                        a.kind == "job_schedule_pause"
                        and a.target_id == entry.job_id
                    )
                ]
                released.append(entry.name)
                log.info("released '%s' from deferral queue", entry.name)
            except Exception:
                log.exception("failed to release job %s", entry.job_id)
                remaining.insert(0, entry)

        # dry-run never mutates the queue (nothing was truly paused/released)
        if self.config.dry_run:
            return released, queue, active
        return released, remaining, active

    def _current_plan(self, now: datetime) -> WeeklyPlan:
        stored = self.state.get_plan()
        if stored:
            plan = WeeklyPlan.from_dict(stored)
            if plan.week_start <= now.date() < plan.week_end:
                return plan
        log.info("no current weekly plan for %s; building one", now.date())
        return self.plan(as_of=now.date())

    def _stored_calibration(self) -> Calibration:
        stored = self.state.get_calibration()
        if stored:
            return Calibration(
                global_multiplier=stored["global_multiplier"],
                overrides=stored.get("overrides", {}),
                window_days=stored.get("window_days", 0),
            )
        return Calibration(
            global_multiplier=self.config.forecast.default_infra_multiplier,
            overrides=dict(self.config.forecast.multiplier_overrides),
        )

    def _inventory(self) -> tuple[dict, LeverContext]:
        """Workspace inventory -> (workload metadata for forecasting, lever
        context). Priorities come from the budget-priority tag."""
        key = self.config.priority_tag_key
        jobs = self.ops.list_jobs()
        warehouses = self.ops.list_warehouses()
        clusters = self.ops.list_all_purpose_clusters()

        meta: dict[tuple[str, str], tuple[str, Priority]] = {}
        for j in jobs:
            meta[("JOB", j.job_id)] = (j.name, Priority.from_tags(j.tags, key))
        for w in warehouses:
            meta[("WAREHOUSE", w.warehouse_id)] = (
                w.name, Priority.from_tags(w.tags, key),
            )
        for c in clusters:
            meta[("CLUSTER", c.cluster_id)] = (
                c.name, Priority.from_tags(c.tags, key),
            )

        context = LeverContext(
            levers=self.config.levers,
            forecasts=[],
            jobs=jobs,
            warehouses=warehouses,
            clusters=clusters,
        )
        return meta, context

    # -- manual operations -------------------------------------------------

    def revert_all(self) -> list[str]:
        """Revert every active throttle and drain the deferral queue
        immediately (live mode only) - the manual escape hatch."""
        active = self.state.get_active_actions()
        queue = self.state.get_deferral_queue()
        if self.config.dry_run:
            return [f"WOULD revert: {a.describe()}" for a in active] + [
                f"WOULD release from deferral queue: {e.name}" for e in queue
            ]
        reverted = []
        remaining = list(active)
        for action in active:
            if not action.reversible:
                continue
            action.revert(self.ops)
            remaining.remove(action)
            reverted.append(action.describe())
        self.state.set_active_actions(remaining)
        self.state.set_deferral_queue([])
        self.state.append_audit(
            "revert_all", {"count": len(reverted), "queue_drained": len(queue)}
        )
        return reverted

    def status_summary(self) -> dict:
        return {
            "plan": self.state.get_plan(),
            "calibration": self.state.get_calibration(),
            "active_actions": [a.to_dict() for a in self.state.get_active_actions()],
            "recent_audit": self.state.audit_entries(limit=20),
        }
