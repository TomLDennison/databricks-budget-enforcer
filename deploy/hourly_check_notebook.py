# Databricks notebook source
# MAGIC %md
# MAGIC # Hourly budget check
# MAGIC Compares estimated spend pace against the weekly plan and, when running
# MAGIC hot, computes solver-sized throttle actions. **Dry-run by default** -
# MAGIC set `"dry_run": false` in the config to let actions execute.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/tomldennison/databricks-budget-enforcer.git
# MAGIC %restart_python

# COMMAND ----------

CONFIG_PATH = "/Volumes/main/finops/budget_enforcer/config.json"

# COMMAND ----------

import logging

from databricks_budget_enforcer import BudgetEnforcer, EnforcerConfig

logging.basicConfig(level=logging.INFO)

enforcer = BudgetEnforcer(EnforcerConfig.from_file(CONFIG_PATH))
result = enforcer.check()

# COMMAND ----------

displayHTML(f"<pre>{result.report}</pre>")  # noqa: F821

# COMMAND ----------

# Fail the job run loudly when the budget is critical so alerting fires.
if result.status.severity.value == "CRITICAL":
    raise Exception(
        f"BUDGET CRITICAL: pace {result.status.pace_ratio:.2f}x, "
        f"required reduction today ${result.status.required_reduction_today:,.2f}"
    )
