-- Run as ACCOUNTADMIN, once, after load_to_snowflake.py has created
-- RAW.RAW_ORDERS.
--
-- Only RAW needs a manual grant here: STAGING and MARTS access for
-- REVIEWER_ROLE is declared in dbt_project.yml's +grants config, so dbt
-- re-applies it automatically on every `dbt run`. A manual SQL grant on
-- those schemas would be a bad idea - dbt rebuilds tables with CREATE OR
-- REPLACE, which drops and recreates the object and would silently wipe
-- a manually-granted privilege on the next run.

GRANT USAGE ON SCHEMA SALES_DB.RAW TO ROLE REVIEWER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SALES_DB.RAW TO ROLE REVIEWER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SALES_DB.RAW TO ROLE REVIEWER_ROLE;
