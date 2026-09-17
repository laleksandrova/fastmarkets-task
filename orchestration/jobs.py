import os

import requests
from dagster import (
    AssetSelection,
    DagsterRunStatus,
    RunStatusSensorContext,
    ScheduleDefinition,
    define_asset_job,
    run_status_sensor,
)

# One job covering the whole pipeline: raw_orders, then every dbt model
# in dependency order, since they're all connected in a single asset
# graph.
load_and_transform_job = define_asset_job(
    name="load_and_transform_job",
    selection=AssetSelection.all(),
)

# Weekly - every Monday at 06:00. Manual triggering needs no separate
# code: any job defined here already has a "Launchpad" in Dagster's UI
# to run it on demand, and any asset can be materialized individually
# from the Asset Graph view.
weekly_schedule = ScheduleDefinition(
    job=load_and_transform_job,
    cron_schedule="0 6 * * 1",
)


@run_status_sensor(run_status=DagsterRunStatus.FAILURE)
def slack_failure_sensor(context: RunStatusSensorContext):
    """Posts to a Slack incoming webhook when any run fails.

    Uses a plain webhook URL (no dagster-slack package, no Slack bot
    setup) - the webhook URL itself is the only secret, read from the
    environment, never hardcoded.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        context.log.warning("SLACK_WEBHOOK_URL not set - skipping Slack alert")
        return

    run = context.dagster_run
    message = (
        f":rotating_light: Dagster run failed\n"
        f"*Job*: {run.job_name}\n"
        f"*Run ID*: {run.run_id}"
    )
    requests.post(webhook_url, json={"text": message}, timeout=10)
