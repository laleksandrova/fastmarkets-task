"""
Loads the homework CSV (customer + order + order_items OBT) into a raw
Snowflake table.

This is the "raw" layer of a raw -> staging -> marts (Medallion) pattern.
No cleaning happens here on purpose - every column lands as-is, with
order_items parsed into a VARIANT so dbt staging models can flatten it
later. Keeping raw untouched means we can always re-derive staging/marts
from here without re-hitting the source URL.

Run:
    python load_to_snowflake.py

Requires a .env file (see .env.example) with Snowflake credentials.
"""

import io
import os
import sys

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv

CSV_URL = (
    "https://gist.githubusercontent.com/fm-dp/e5beb68cf717dc6a8db91250e9320b9e"
    "/raw/c442fe6feb1f73f9f9db04171aee6fe02505ceb8"
)

EXPECTED_COLUMNS = [
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "order_id",
    "order_date",
    "order_total",
    "order_items",
]

DATABASE = "SALES_DB"
SCHEMA = "RAW"
TABLE = "RAW_ORDERS"


def download_csv(url: str) -> pd.DataFrame:
    """Download the CSV from the given URL and load it into a DataFrame."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to download CSV from {url}: {e}") from e

    try:
        df = pd.read_csv(io.StringIO(response.text))
    except Exception as e:
        raise RuntimeError(f"Downloaded file could not be parsed as CSV: {e}") from e

    return df


def validate_columns(df: pd.DataFrame, expected: list[str]) -> None:
    """Fail fast with a clear message if the source schema doesn't match
    what we expect, rather than letting a cryptic Snowflake error surface
    later."""
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(
            f"CSV is missing expected column(s): {missing}. "
            f"Found columns: {list(df.columns)}"
        )

    # Flag (but don't fail on) missing values in key columns - useful to see
    # before load rather than discovering it as a downstream data quality
    # surprise. Deeper null/quality handling belongs in dbt staging tests,
    # not here.
    null_counts = df[expected].isnull().sum()
    if null_counts.any():
        print("Warning: null values found in source data before load:")
        print(null_counts[null_counts > 0].to_string())


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Open a Snowflake connection using credentials from environment
    variables (loaded from .env). Raises a clear error on missing config
    or a failed connection rather than a raw stack trace."""
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_ROLE",
    ]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variable(s): {missing}. "
            f"Check your .env file against .env.example."
        )

    try:
        return snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
    except snowflake.connector.errors.Error as e:
        raise RuntimeError(f"Failed to connect to Snowflake: {e}") from e


def setup_raw_table(conn: snowflake.connector.SnowflakeConnection) -> None:
    """Create the schema/table if they don't exist, then truncate the
    table for a clean full-refresh load. Full-refresh is fine here since
    the source is a static file and there's no incremental requirement
    in scope - see README for the trade-off."""
    with conn.cursor() as cur:
        # Database is created once by the ACCOUNTADMIN bootstrap script
        # (setup/01_snowflake_bootstrap.sql), not here - TRANSFORM_ROLE is
        # deliberately not granted account-level CREATE DATABASE, in line
        # with the least-privilege approach used for reviewer access too.
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(f"USE SCHEMA {SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                customer_id     VARCHAR,
                customer_name   VARCHAR,
                customer_phone  VARCHAR,
                customer_email  VARCHAR,
                order_id        VARCHAR,
                order_date      VARCHAR,
                order_total     VARCHAR,
                order_items     VARIANT
            )
            """
        )
        cur.execute(f"TRUNCATE TABLE {TABLE}")


def load_data(conn: snowflake.connector.SnowflakeConnection, df: pd.DataFrame) -> int:
    """Insert the DataFrame into the raw table, one row at a time.

    order_items is inserted as a JSON string and parsed into VARIANT with
    PARSE_JSON. This can't use cursor.executemany()'s bulk multi-row
    INSERT optimization, because that fast path only accepts literal
    constant values - not function calls like PARSE_JSON(). At this data
    volume (1,000 rows), row-by-row execute() is effectively instant, so
    trading a small amount of throughput for a working, simple insert is
    the right call rather than fighting the bulk-load path."""
    insert_sql = f"""
        INSERT INTO {TABLE}
            (customer_id, customer_name, customer_phone, customer_email,
             order_id, order_date, order_total, order_items)
        SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
    """

    def _null_safe(value):
        """pandas represents missing values as NaN (a float), not None.
        Convert explicitly so nulls bind correctly instead of as text."""
        return None if pd.isna(value) else value

    row_count = 0
    try:
        with conn.cursor() as cur:
            for row in df.itertuples(index=False):
                cur.execute(
                    insert_sql,
                    (
                        _null_safe(row.customer_id),
                        _null_safe(row.customer_name),
                        _null_safe(row.customer_phone),
                        _null_safe(row.customer_email),
                        _null_safe(row.order_id),
                        _null_safe(row.order_date),
                        _null_safe(row.order_total),
                        _null_safe(row.order_items),
                    ),
                )
                row_count += 1
    except snowflake.connector.errors.Error as e:
        raise RuntimeError(f"Failed to load data into {TABLE}: {e}") from e

    return row_count


def main() -> None:
    load_dotenv()

    print(f"Downloading CSV from {CSV_URL} ...")
    df = download_csv(CSV_URL)
    print(f"Downloaded {len(df)} rows.")

    validate_columns(df, EXPECTED_COLUMNS)

    conn = get_connection()
    try:
        print(f"Setting up {DATABASE}.{SCHEMA}.{TABLE} ...")
        setup_raw_table(conn)

        print("Loading data ...")
        row_count = load_data(conn, df)
        print(f"Loaded {row_count} rows into {DATABASE}.{SCHEMA}.{TABLE}.")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
