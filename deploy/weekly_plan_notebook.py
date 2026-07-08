# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly budget plan
# MAGIC Runs every Sunday at 00:30 UTC: recomputes the week's allowance from the
# MAGIC AWS CUR (remaining FY budget / remaining weeks), splits it into per-day
# MAGIC targets by day-of-week profile, and recalibrates the infra multiplier.

# COMMAND ----------

# MAGIC %pip install --force-reinstall --no-deps git+https://github.com/tomldennison/databricks-budget-enforcer.git
# MAGIC %restart_python

# COMMAND ----------

CONFIG_PATH = "/Volumes/main/finops/budget_enforcer/config.json"

# COMMAND ----------

import logging

from databricks_budget_enforcer import BudgetEnforcer, EnforcerConfig
from databricks_budget_enforcer.report import reporter

logging.basicConfig(level=logging.INFO)

enforcer = BudgetEnforcer(EnforcerConfig.from_file(CONFIG_PATH))
plan = enforcer.plan()

# COMMAND ----------

displayHTML(f"<pre>{reporter.render_plan(plan)}</pre>")  # noqa: F821
