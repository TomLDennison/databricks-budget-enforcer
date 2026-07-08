# databricks-budget-enforcer

**Fixed fiscal-year budget enforcement for Databricks on AWS.**

Federal IT organizations must commit to a fixed yearly budget while cloud
spend is inherently variable. This tool recreates the old on-prem behavior —
fixed capacity, workloads queue and slow down when demand exceeds it — but
optimized for a **fixed cost** instead of fixed hardware: when spend pace
threatens the budget, Databricks compute is throttled until pace recovers,
and relaxed again once it does.

- **Input:** your AWS Cost and Usage Report (CUR) + an annual budget for the
  federal fiscal year (Oct 1 – Sep 30).
- **Weekly:** remaining budget ÷ remaining weeks → this week's allowance,
  split into per-day dollar targets by your historical day-of-week shape.
- **Hourly:** live spend (Databricks system tables) is compared to the day's
  target; if the day is projected to overrun, a solver computes throttle
  settings whose modeled savings bring projected spend back to **100% of the
  day's target** — no more, no less.
- **Dry-run by default.** It logs exactly what it *would* do. Nothing is
  touched until you set `"dry_run": false`.

## How it works

### Two cost signals, calibrated together

| Signal | Freshness | Role |
|---|---|---|
| AWS CUR (parquet on S3 / Databricks volume) | ~24h lag | Authoritative all-in ledger: FY-to-date actuals, weekly reconciliation, day-of-week profiles |
| `system.billing.usage` × `system.billing.list_prices` | few hours' lag | Intraday DBU dollars per job / warehouse / cluster |

The CUR sees everything (Marketplace DBU charges **plus** the EC2/EBS/network
underneath); system tables see only DBUs but see them nearly live. Weekly
calibration computes the **infra multiplier** (all-in ÷ DBU over a trailing
window), so intraday DBU dollars convert to estimated all-in dollars.

### Direct-invoice billing (`include_dbu_invoice`)

The CUR only contains what AWS bills your account. If your organization pays
Databricks **through AWS Marketplace**, DBU charges appear in the CUR and the
default configuration captures everything. If Databricks bills you **by
direct invoice** (common for negotiated contracts — and for serverless
products), the CUR only sees the infrastructure half. Set

```json
"include_dbu_invoice": true
```

and the ledger becomes: non-Marketplace CUR costs (infra) **plus**
`system.billing.usage` × list prices (the invoice half). Any Databricks
Marketplace charges found in the CUR are excluded while the flag is on, so
DBUs are never counted twice. Calibration runs against the blended ledger,
keeping intraday estimates consistent. Note the system tables price at
*list*; if your invoice reflects negotiated discounts, expect the ledger to
be conservatively high.

### The throttle solver

Throttle magnitudes are calculated, not fixed steps. Each hourly check:

1. Forecasts each workload's remaining-day spend from its own history
   (same-weekday preferred — scheduled workloads run at consistent times, so
   the schedule is implicit).
2. Computes the gap: `spend so far today + forecast remaining − day target`.
3. Each lever models its savings for concrete settings (a specific
   `max_workers`, a specific warehouse size).
4. A greedy allocator picks per-workload settings — lowest priority and
   gentlest disruption first — until modeled savings just cover the gap.

When pace recovers (< 90% of expected), applied throttles are reverted from
persisted original settings.

### Enforcement ladder

| Pace vs expected | Level | Behavior |
|---|---|---|
| < 90% | OK | Revert active throttles |
| 90–100% | WARN | Report only |
| 100–130% | THROTTLE | Solver-sized: spot enforcement, worker caps, warehouse downsizing / auto-stop, spot-bid reduction, idle-cluster termination |
| > 130% or weekly allowance exhausted | CRITICAL | Adds schedule pausing: low priority first, never `critical` |

### Priorities

Tag jobs, clusters, and warehouses with `budget-priority: critical | normal | low`.

- `critical` is **never** touched.
- `low` is throttled first.
- Untagged workloads are `normal`.

### A note on spot bids

Since AWS's 2017 spot pricing change, `spot_bid_price_percent` is a price
*ceiling*, not an old-style market bid — when the market price sits above
your bid, instance launches simply **fail** and the workload waits (the
on-prem-queue analog), rather than costs smoothly decreasing. The savings
this lever reports are therefore an expectation, not a guarantee. It engages
last among the THROTTLE levers.

## Installation

```bash
pip install git+https://github.com/tomldennison/databricks-budget-enforcer.git
```

## Prerequisites

1. **CUR delivery** to an S3 bucket you can read — directly (`s3://…`) or
   through a Databricks volume (`/Volumes/…`). Both legacy CUR and CUR 2.0
   ("standard" data export) are supported, in parquet or CSV/.csv.gz; point
   `cur.path` at a single file or at the prefix and it is searched
   recursively. **Create the export with daily or hourly time granularity**
   — monthly granularity collapses each month to one date and breaks weekly
   pacing (the reader warns if it detects this). Use *overwrite existing
   report* delivery so re-deliveries don't double-count.
2. **Cost-allocation tags activated** in AWS Billing for the tags Databricks
   stamps on the instances it launches (`Vendor`, `ClusterId`, `ClusterName`,
   `JobId`), so Databricks-attributed EC2/EBS spend is identifiable in the CUR.
3. **Unity Catalog system tables** (`system.billing.*`) readable, and a SQL
   warehouse id to query them with.
4. A service principal / PAT with permission to read jobs, clusters, and
   warehouses — plus edit permission if you enable live enforcement.

## Quick start

Copy [`examples/config.example.json`](examples/config.example.json) and edit:

```json
{
  "annual_budget": 1200000,
  "dry_run": true,
  "cur": { "path": "/Volumes/main/finops/cur/myorg-cur/data" },
  "databricks": { "warehouse_id": "abcdef0123456789" },
  "state": { "path": "/Volumes/main/finops/budget_enforcer/state.json" }
}
```

In a notebook:

```python
from databricks_budget_enforcer import BudgetEnforcer, EnforcerConfig

enforcer = BudgetEnforcer(EnforcerConfig.from_file(CONFIG_PATH))
plan = enforcer.plan()      # weekly
result = enforcer.check()   # hourly; dry-run logs WOULD-actions
print(result.report)
```

Or from a terminal:

```bash
dbe plan   --config config.json
dbe check  --config config.json          # dry-run
dbe check  --config config.json --live   # enforce
dbe status --config config.json
dbe revert --config config.json --live   # undo all active throttles
```

### Scheduled deployment

`deploy/` contains reference assets:

- `weekly_plan_notebook.py` + `weekly_plan_job.json` — Sundays 00:30 UTC
- `hourly_check_notebook.py` + `hourly_check_job.json` — hourly at :05
  (fails its run on CRITICAL so job alerting fires)

Import the notebooks, adjust the config path, then
`databricks jobs create --json @deploy/weekly_plan_job.json` (and the same
for the hourly job). Tag these jobs `budget-priority: critical` (the
reference JSONs already do) so the enforcer never throttles itself.

## Going live safely

1. Run in dry-run for at least a week or two.
2. Verify CUR attribution: compare `dbe plan` FY-to-date totals against your
   AWS bill's Databricks-related charges.
3. Read the audit trail (`dbe status`) and sanity-check the WOULD-actions.
4. Flip `"dry_run": false` — ideally with only the gentlest levers enabled
   at first (`idle_termination`, `enforce_spot`), adding the rest as trust
   builds.

## Rollback / escape hatches

- `dbe revert --config … --live` reverts every active throttle immediately.
- Original settings are captured in the state file before each change.
- Tag anything sacred `budget-priority: critical`.

## GovCloud

Nothing here is region-specific; set `databricks.host` to your GovCloud
workspace URL and use GovCloud S3 paths. Untested by the authors so far —
reports welcome.

## Current scope and roadmap

v1 covers AWS + legacy CUR + the levers above. Roadmap: CUR 2.0 / FOCUS
exports, instance pools as hard capacity caps, serverless budget policies,
cron-schedule-aware job forecasts, holiday-aware profiles, a Databricks App
UI, Azure/GCP.

## Development

```bash
pip install -e ".[dev]"
pytest
```

The test suite runs entirely offline against synthetic CUR and usage
fixtures, including an end-to-end dry run over an engineered overspend week.

## License

Apache-2.0
