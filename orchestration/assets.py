import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from dagster_dbt import DbtCliResource, dbt_assets, get_asset_key_for_source

from .resources import dbt_project, dbt_resource

# load_to_snowflake.py lives at the repo root, one level up from this
# folder - reused directly rather than duplicated, so there's exactly one
# copy of the load logic.
sys.path.insert(0, str(Path(__file__).parent.parent))
from load_to_snowflake import (  # noqa: E402
    CSV_URL,
    EXPECTED_COLUMNS,
    download_csv,
    get_connection,
    load_data,
    setup_raw_table,
    validate_columns,
)


@dbt_assets(manifest=dbt_project.manifest_path)
def sales_analytics_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # `build` (not `run`) so dbt tests execute as part of the same
    # command - dagster-dbt surfaces each test as an asset check on its
    # model, visible pass/fail in the UI, at no extra cost.
    yield from dbt.cli(["build"], context=context).stream()


@asset(
    key=get_asset_key_for_source([sales_analytics_dbt_assets], "raw"),
    group_name="loading",
    # Covers a transient failure (e.g. a flaky download or a brief
    # Snowflake connection drop) without treating every failure as fatal
    # on the first try.
    retry_policy=RetryPolicy(max_retries=2, delay=30),
)
def raw_orders(context: AssetExecutionContext) -> MaterializeResult:
    """Downloads the source CSV and lands it in SALES_DB.RAW.RAW_ORDERS.

    Reuses load_to_snowflake.py's own functions directly - this asset is
    just Dagster's entry point into the existing, already-tested load
    logic, not a reimplementation of it.
    """
    df = download_csv(CSV_URL)
    validate_columns(df, EXPECTED_COLUMNS)

    conn = get_connection()
    try:
        setup_raw_table(conn)
        row_count = load_data(conn, df)
    finally:
        conn.close()

    context.log.info(f"Loaded {row_count} rows into RAW.RAW_ORDERS")
    return MaterializeResult(metadata={"row_count": MetadataValue.int(row_count)})
