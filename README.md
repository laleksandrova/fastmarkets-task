# Fastmarkets Technical Task

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Reproducing This Setup](#reproducing-this-setup)
- [Running Tests and Docs as a Reviewer](#running-tests-and-docs-as-a-reviewer)
- [The Core Task](#the-core-task)
  - [1. Load](#1-load)
  - [2. Model](#2-model)
  - [3. Aggregate](#3-aggregate)
  - [Transform with dbt](#transform-with-dbt)
- [Orchestration with Dagster (Section 3)](#orchestration-with-dagster-section-3)
- [Production-Readiness and Data Quality (Section 3)](#production-readiness-and-data-quality-section-3)
- [Semantic & AI Layer (Section 3) — Note Only](#semantic--ai-layer-section-3--note-only)
- [AI Usage](#ai-usage)
- [Actual Prompts](#actual-prompts)

## Introduction

This project loads a CSV into Snowflake, models it into a small star schema with dbt, and builds a weekly top-seller aggregation view. It's built to be reviewed both as code and as a walkthrough of how I used AI (Claude) throughout.

## Prerequisites

- **Snowflake**: a free trial account (Enterprise edition), which grants ACCOUNTADMIN by default. Used only to bootstrap a scoped `TRANSFORM_ROLE` (for loading/dbt) and a read-only `REVIEWER_ROLE` — see `setup/`.
- **Data source**: [homework.csv](https://gist.githubusercontent.com/fm-dp/e5beb68cf717dc6a8db91250e9320b9e/raw/c442fe6feb1f73f9f9db04171aee6fe02505ceb8) — a one-big-table CSV with customer, order, and order-items (JSON array) columns.
- **Python, run locally** (not Snowpark). For a static, 1,000-row CSV, local Python is simpler to set up, easier to debug, and has no meaningful performance disadvantage. Snowpark's advantage is pushing transformation logic to run inside the warehouse itself, which pays off with larger datasets or heavier in-warehouse transforms — not the case here.
- **Git repo**, structured as:
  ```
  ├── load_to_snowflake.py
  ├── requirements.txt / .env.example / .gitignore
  ├── setup/            (Snowflake bootstrap + reviewer grants)
  ├── orchestration/    (Dagster pipeline - Section 3)
  └── dbt/              (staging + marts models, tests, docs, profiles.yml.example)
  ```
  `profiles.yml.example` documents the dbt connection profile; the real `profiles.yml` (credentials resolved via environment variables, never hardcoded) lives outside the repo at `~/.dbt/profiles.yml`.
- **Reviewer access**: a dedicated `REVIEWER_USER` / `REVIEWER_ROLE`, scoped to `SELECT`-only on the raw, staging, and marts schemas — including the `weekly_top_seller` aggregation view — with no admin rights. Credentials (account URL, username, password) are sent by email, not committed here.

## Reproducing This Setup

1. **Clone the repo** and install Python dependencies:
   ```
   pip install -r requirements.txt
   pip install dbt-core dbt-snowflake
   ```
2. **Set up Snowflake**: log in to your trial account as `ACCOUNTADMIN` and run `setup/01_snowflake_bootstrap.sql`, filling in your own username and a reviewer password first.
3. **Configure credentials**:
   - Copy `.env.example` to `.env` in the repo root and fill in your Snowflake account, `TRANSFORM_ROLE` user/password, warehouse, and role.
   - Copy `dbt/profiles.yml.example` to `~/.dbt/profiles.yml` (unchanged — it reads the same environment variables).
4. **Load the data**:
   ```
   python load_to_snowflake.py
   ```
5. **Run dbt** (from the `dbt/` folder):
   ```
   dbt deps
   dbt run
   dbt test
   dbt docs generate && dbt docs serve
   ```
6. **Grant reviewer access**: run `setup/02_grant_reviewer_access.sql` as `ACCOUNTADMIN` (one-time, for the `RAW` schema and schema-level `USAGE`) — staging/marts object-level grants are applied automatically by dbt's own `grants` config on every `dbt run`.

## Running Tests and Docs as a Reviewer

`REVIEWER_ROLE` is intentionally read-only, but `dbt test` and `dbt docs generate`/`dbt docs serve` only need to *read* the already-built tables and views — they don't create or replace anything — so no elevated privileges are required to run them. `dbt run` is deliberately not available to this role, since it would need write access to rebuild tables.

To run these yourself:
1. Clone this repo and install dbt: `pip install dbt-core dbt-snowflake`.
2. Create `~/.dbt/profiles.yml` from `dbt/profiles.yml.example`, using the `REVIEWER_USER` / `REVIEWER_ROLE` credentials (sent by email) instead of `TRANSFORM_ROLE`.
3. From the `dbt/` folder:
   ```
   dbt deps
   dbt test
   dbt docs generate && dbt docs serve
   ```

## The Core Task

### 1. Load

`load_to_snowflake.py` downloads the CSV, validates it has the expected columns, and lands it as-is into `RAW.RAW_ORDERS` — every column as `VARCHAR`, except `order_items`, stored as `VARIANT` (Snowflake's semi-structured type) so it can later be queried and flattened directly with `LATERAL FLATTEN`, rather than re-parsed from text on every read. No cleaning happens at this stage on purpose — raw stays a faithful copy of the source, so staging/marts can always be rebuilt from it.

Data quality issues found during profiling, deliberately **not** fixed at load time (left to dbt staging instead):
- Phone numbers have embedded quote characters, plus placeholder junk (`"123456"`, `"N/A"`, blank) in ~10% of rows.
- Emails have case inconsistency and placeholder junk (`n/a`, `invalid-email`, blank) in ~6% of rows.

### 2. Model

Layering follows **Medallion** (raw → cleaned → business-ready), which maps directly onto dbt's own raw/staging/marts convention. For the marts shape, I used a **Kimball star schema** (fact tables of measurable events, surrounded by descriptive dimension tables) — `Customer` and `Product` as dimensions, `Orders` and `OrderItem` as facts, with `OrderItem` as the real grain (one row per product per order, built by flattening the `order_items` JSON array).

`Product` isn't in the task's requirements, but I added it anyway: `product_name` otherwise only exists nested inside the JSON with no dedicated home, and every downstream query would need to re-derive or repeat it.

I considered and rejected:
- **3NF** — normalizes for transactional consistency, the wrong trade-off for a reporting layer.
- **Data Vault** — built for enterprise-scale auditability/history-tracking; heavy overhead for a single-source, one-off task.
- **Snowflake schema** (normalized sub-dimensions) — the dimensions here (3 products, no hierarchy) are too small and flat to benefit.
- **Surrogate keys / SCD** — natural keys are already unique and stable here, and there's no change-over-time scenario to justify either.

One naming note: the `Order` mart is named `orders` (plural) — `ORDER` is a reserved SQL keyword in Snowflake and breaks table creation if used as a bare identifier.

### 3. Aggregate

`weekly_top_seller` aggregates `OrderItem` by `DATE_TRUNC('week', order_date)` (Monday-start, ISO-style) and `product_id`, producing `total_quantity`, `total_revenue`, and `is_top_seller`.

"Top seller" isn't defined in the task, so I checked both readings against the actual data first: quantity-based and revenue-based rankings **disagree in 80 of 144 weeks (55%)**, and quantity-based ties in 7 weeks while **revenue-based never ties**. I went with revenue, since it stays deterministic without inventing a tiebreak rule. The flag uses `RANK() OVER (PARTITION BY week ORDER BY total_revenue DESC) = 1` rather than `ROW_NUMBER()`, so a genuine tie would be honestly flagged on both products instead of one being arbitrarily picked.

Had I gone with quantity instead, the fix would be a one-line metric swap plus a secondary sort (`total_revenue DESC`) as a tiebreaker for those 7 weeks — a legitimate rule, just one more decision than revenue alone requires.

### Transform with dbt

```
dbt/models/
├── staging/   (views — cleans phone/email, flattens JSON, casts types)
└── marts/     (tables — customer, product, orders, order_item; view — weekly_top_seller)
```

Materializations follow the task's own wording: "tables" for Customer/Order/OrderItem/Product, "view" for the aggregation.

**Tests**: generic `unique`/`not_null`/`relationships` on all natural keys, `dbt_utils.unique_combination_of_columns` for the `(order_id, product_id)` composite key on `OrderItem`, and a custom singular test asserting `orders.order_total` reconciles with `SUM(order_item.line_total)` per order — 27/27 tests passing.

**Documentation**: every model/column described in `_staging.yml`/`_marts.yml`; `dbt docs generate` + `dbt docs serve` gives a full lineage graph and docs site, no extra tooling needed.

**Reviewer access**: handled via dbt's own `grants` config, not a manual script — `dbt run` rebuilds tables with `CREATE OR REPLACE`, which would silently wipe a manually-granted privilege on the next run. Schema-level `USAGE` (which dbt's grants config doesn't cover) is granted once, manually, in `setup/02_grant_reviewer_access.sql`.

## Orchestration with Dagster (Section 3)

The core task's two steps (load script, dbt project) previously had to be run manually, one after the other. `orchestration/` wires them into a single Dagster pipeline: one trigger runs the load, then every dbt model and test, in order.

**What it does**:
- `raw_orders`: a Dagster asset calling the existing load script's functions directly — no duplicated logic.
- `sales_analytics_dbt_assets`: every dbt model, loaded via `@dbt_assets`, running `dbt build` so tests execute as Dagster asset checks alongside the models.
- Both connected as **one graph** (verified in Dagster's lineage view), via `get_asset_key_for_source` matching `raw_orders` to the dbt source's derived asset key.
- **Schedule**: weekly cron (Monday 06:00). **Manual trigger**: built into Dagster's UI by default, no extra code.
- **Retries**: `raw_orders` has a `RetryPolicy` for transient failures (flaky download, brief connection drop).
- **Monitoring**: Dagster's UI gives full run history/logs out of the box; persisted to a fixed `DAGSTER_HOME` so it survives restarts.
- **Alerting**: a `run_status_sensor` posts to a Slack webhook on failure — tested for real by deliberately breaking the load script and confirming both the failure and the Slack message.

**Why asset-based, not a simpler linear job**: it's Dagster's more idiomatic approach — gives lineage, per-model observability, and dbt tests as first-class UI checks, for a modest added learning cost.

**What I'd improve**: the first full run took ~9-10 minutes, almost entirely subprocess startup overhead (the dbt step itself took ~40 seconds). Switching to Dagster's `in_process_executor` helped but didn't fully resolve it — worth further profiling, possibly related to antivirus scanning of freshly spawned Python processes (a pattern seen elsewhere in this project too).

**How to run it**:
1. `pip install -r orchestration/requirements.txt` (from repo root).
2. Set `DAGSTER_HOME` to a permanent folder (`setx DAGSTER_HOME "<path>"`, once) so run history persists.
3. Load `.env` into the session; ensure `dbt`/`dagster` are on PATH.
4. From the repo root: `dagster dev -m orchestration.definitions`.
5. Open `http://localhost:3000` → **Jobs → load_and_transform_job** → launch from there (not "Materialize all", which may bypass the job's configured executor).

## Production-Readiness and Data Quality (Section 3)

Several of these are already covered by decisions made in the core task, rather than being separate additions:

- **Idempotency**: the load script does a full-refresh (`TRUNCATE` + reload) rather than incremental — safe to re-run, no duplicate-row risk, appropriate for a static source file (see *Load*).
- **Secrets handling**: credentials never hardcoded — `.env` (gitignored) for the load script, `~/.dbt/profiles.yml` with `env_var()` for dbt, both outside git history.
- **Error handling**: the load script fails fast on missing columns/connection errors with clear messages (see *Load*); Dagster adds a `RetryPolicy` for transient failures (see *Orchestration*).
- **Observability**: Dagster's UI provides run history and per-asset status out of the box; a Slack sensor alerts on failure (see *Orchestration*).
- **Data quality**: profiled and documented upfront — phone/email placeholder junk, embedded quote characters (see *Load*) — and enforced going forward via dbt tests, including a custom test reconciling `orders.order_total` against summed line items (see *Transform with dbt*).

## Semantic & AI Layer (Section 3) — Note Only

Not built yet — but here's what I'd do: put **Snowflake Cortex Analyst** on top of the `marts` layer, so a business user could ask "which product sold best in the last 4 weeks?" in plain English.

- Point it at `weekly_top_seller` first — it's already aggregated with the flag column, so it needs minimal semantic mapping to get useful answers.
- The real work is a semantic model YAML describing what each column means (e.g., telling it `is_top_seller` means "best seller"), so it translates questions into correct SQL rather than guessing from column names.
- I'd scope it to just this one view initially rather than exposing the whole marts layer at once, and expand from there.
- Given more time, this is the Section 3 item I'd build first — it extends the "AI usage" theme of this whole exercise into the data layer itself, not just the build process.

## AI Usage

I used **Claude** throughout — for planning, code, and debugging. The strongest, most concrete moments:

**Snowpark vs. local Python** Snowpark's DataFrame API pushes execution into the warehouse server-side; plain Python does everything client-side. For 1,000 rows that difference is irrelevant, while Snowpark adds session setup overhead for no practical gain here. The technical case for it depends on data volume and transform complexity this task doesn't have.

**Checking an assumption against real data before committing to it** The task doesn't define "top seller." Rather than picking a metric and asserting it, I asked Claude to test quantity vs. revenue against the actual dataset first — it found the 55%-of-weeks disagreement and the tie-count difference, which is what actually decided the metric.

**A genuine multi-step debugging chain** Loading `order_items` (JSON) into a `VARIANT` column failed three different ways in a row, each with a distinct, verifiable cause read from the actual Snowflake error: an `INSERT...SELECT` batching incompatibility, then a `NaN`-rendered-as-bare-identifier error after switching to `VALUES`, then a `PARSE_JSON` function call being incompatible with the bulk `VALUES` optimization. The working fix drops batching entirely and inserts row-by-row — a small, deliberate throughput trade-off for reliability.

**Output validation** After setting up reviewer access via dbt's `grants` config, a clean `dbt run` looked like proof it worked. I pushed back and asked to verify the actual grant list in Snowflake directly — which surfaced a real gap: schema-level `USAGE` was missing entirely, meaning the reviewer role couldn't have reached the tables despite every individual grant looking correct. Confirmed fixed by logging in as the reviewer role and running a real query.

## Actual Prompts

**Python script**
Write a Python script that loads a CSV into Snowflake.

Source data can be found here - https://gist.githubusercontent.com/fm-dp/e5beb68cf717dc6a8db91250e9320b9e/raw/c442fe6feb1f73f9f9db04171aee6fe02505ceb8
The file is a single One Big Table (OBT) containing customer details, order details, and an order-items array held as a JSON string.

I want to use plain Python locally. The script should get the CSV from the URL, connect to Snowflake (perhaps a connector library should be used), then create a target table that will be our raw table (three-layer pattern - raw/staging/marts as we will include dbt at a later step) and keep the order_items as a VARIANT column, then load the CSV data into the table. Include basic error handling - connection failures, missing columns/values.

If you do not understand something or have questions that need to be clarified, start with asking them. If you have any concerns/suggestions, let's discuss them first. Keep the script readable and simple, do not over-engineer.

**Dagster Setup**
Write a Dagster orchestration setup for an existing data pipeline. 

The current pipeline consists of a Python script that downloads a CSV from a URL and loads it into a raw table in Snowflake. Then, we have a dbt project that transforms that raw table through staging models into mart models. 

The purpose of our task now is to set up Dagster to orchestrate these two steps as a single pipeline. Include a suggestion on how to run this on a schedule once a week as well as manually triggering it from Dagster's UI. Include basic error handling, monitoring and messaging. 

Before producing anything, ask me clarifying questions and let's discuss which Dagster approach will be best to use. Have in mind that Dagster is not yet installed. Keep everything simple and clear.