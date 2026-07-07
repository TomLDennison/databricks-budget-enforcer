"""Thin CLI over the library - the same four verbs the reference notebooks
call. Dry-run is the default everywhere; pass --live to let actions execute.
"""

from __future__ import annotations

import json
import logging

import click

from databricks_budget_enforcer.app import BudgetEnforcer
from databricks_budget_enforcer.config import EnforcerConfig
from databricks_budget_enforcer.report import reporter


def _enforcer(config_path: str, live: bool) -> BudgetEnforcer:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config = EnforcerConfig.from_file(config_path)
    if live:
        config.dry_run = False
    return BudgetEnforcer(config)


@click.group()
def main():
    """databricks-budget-enforcer: fiscal-year budget enforcement."""


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
def plan(config_path: str):
    """Build this week's allowance and per-day targets from the CUR."""
    enforcer = _enforcer(config_path, live=False)
    weekly = enforcer.plan()
    click.echo(reporter.render_plan(weekly))


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--live", is_flag=True, help="Execute throttle actions (default: dry-run).")
def check(config_path: str, live: bool):
    """Check budget pace and throttle (dry-run unless --live)."""
    result = _enforcer(config_path, live).check()
    click.echo(result.report)


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
def status(config_path: str):
    """Show the stored plan, calibration, active throttles, recent audit."""
    click.echo(json.dumps(_enforcer(config_path, live=False).status_summary(), indent=2))


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--live", is_flag=True, help="Actually revert (default: dry-run).")
def revert(config_path: str, live: bool):
    """Revert all active throttles."""
    for line in _enforcer(config_path, live).revert_all():
        click.echo(line)


if __name__ == "__main__":
    main()
