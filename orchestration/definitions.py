from dagster import Definitions

from .assets import raw_orders, sales_analytics_dbt_assets
from .jobs import load_and_transform_job, slack_failure_sensor, weekly_schedule
from .resources import dbt_resource

defs = Definitions(
    assets=[raw_orders, sales_analytics_dbt_assets],
    resources={"dbt": dbt_resource},
    jobs=[load_and_transform_job],
    schedules=[weekly_schedule],
    sensors=[slack_failure_sensor],
)
