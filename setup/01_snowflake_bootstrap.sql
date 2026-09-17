-- Run once as ACCOUNTADMIN (the role your Snowflake trial user starts with).
-- Sets up the warehouse, database, and two functional roles:
--   TRANSFORM_ROLE - used by the Python load script and dbt (not admin)
--   REVIEWER_ROLE  - read-only access for the interview panel

-- Warehouse: XS and auto-suspending, this is a demo-scale workload
CREATE WAREHOUSE IF NOT EXISTS SALES_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Database used by the load script and dbt
CREATE DATABASE IF NOT EXISTS SALES_DB;

-- --- Functional role for loading + transforming data ---
CREATE ROLE IF NOT EXISTS TRANSFORM_ROLE;

GRANT USAGE, OPERATE ON WAREHOUSE SALES_WH TO ROLE TRANSFORM_ROLE;
GRANT USAGE, CREATE SCHEMA ON DATABASE SALES_DB TO ROLE TRANSFORM_ROLE;

-- Replace <YOUR_USERNAME> with your actual Snowflake trial username
GRANT ROLE TRANSFORM_ROLE TO USER <YOUR_USERNAME>;

-- --- Read-only reviewer role + dedicated user ---
CREATE ROLE IF NOT EXISTS REVIEWER_ROLE;

GRANT USAGE ON WAREHOUSE SALES_WH TO ROLE REVIEWER_ROLE;
GRANT USAGE ON DATABASE SALES_DB TO ROLE REVIEWER_ROLE;
-- Schema/table-level SELECT grants happen in grant_reviewer_access.sql,
-- after the load script and dbt have actually created those objects.

-- Replace <CHOOSE_A_PASSWORD> with a real password before running
CREATE USER IF NOT EXISTS REVIEWER_USER
    PASSWORD = '<CHOOSE_A_PASSWORD>'
    DEFAULT_ROLE = REVIEWER_ROLE
    DEFAULT_WAREHOUSE = SALES_WH
    MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE REVIEWER_ROLE TO USER REVIEWER_USER;
