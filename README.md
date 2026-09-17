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
- [AI Usage](#ai-usage)
- [Actual prompts](#actual-prompts)

## Introduction

This project loads a CSV into Snowflake, models it into a small star schema with dbt, and builds a weekly top-seller aggregation view. It's built to be reviewed both as code and as a walkthrough of how I used AI (Claude) throughout.

## Prerequisites

- **Snowflake**: a free trial account (Enterprise edition), which grants ACCOUNTADMIN by default. Used to bootstrap a scoped `TRANSFORM_ROLE` (for loading/dbt) and a read-only `REVIEWER_ROLE` — see `setup/`.
- **Data source**: [homework.csv](https://gist.githubusercontent.com/fm-dp/e5beb68cf717dc6a8db91250e9320b9e/raw/c442fe6feb1f73f9f9db04171aee6fe02505ceb8) — a one-big-table CSV with customer, order, and order-items (JSON array) columns.
- **Python, run locally**: For a static, 1,000-row CSV, local Python is simpler to set up, easier to debug, and has no meaningful performance disadvantage — the whole load takes seconds either way. Snowpark's advantage is pushing transformation logic to run inside the warehouse itself (via its DataFrame API translating to server-side SQL), which pays off with larger datasets, heavier in-warehouse transforms, or when you want to avoid pulling data client-side at all. None of that applies here.
- **Git repo**, structured as:
  ```
  ├── load_to_snowflake.py
  ├── requirements.txt / .env.example / .gitignore
  ├── setup/            (Snowflake bootstrap + reviewer grants)
  └── dbt/              (staging + marts models, tests, docs, profiles.yml.example)
  ```
  `profiles.yml.example` documents the dbt connection profile; the real `profiles.yml` (with credentials resolved via environment variables, never hardcoded) lives outside the repo at `~/.dbt/profiles.yml`, dbt's default lookup location.
- **Reviewer access**: a dedicated `REVIEWER_USER` / `REVIEWER_ROLE`, scoped to `SELECT`-only on the raw, staging, and marts schemas — including the `weekly_top_seller` aggregation view — with no admin rights. Credentials (account URL, username, password) are sent by email, not committed here.

## Reproducing this Setup

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

`load_to_snowflake.py` downloads the CSV, validates it has the expected columns, and lands it as-is into `RAW.RAW_ORDERS` — every column as `VARCHAR`, except `order_items`, which is parsed into `VARIANT` via `PARSE_JSON`. `order_items` holds a JSON array (multiple line items per order), and `VARIANT` is Snowflake's native semi-structured type — it lets later steps query and flatten the array directly with `LATERAL FLATTEN` and path syntax (`value:product_id`), rather than storing raw JSON text that would need re-parsing on every read. No cleaning happens at this stage on purpose: raw stays a faithful copy of the source, so staging/marts can always be rebuilt from it without re-hitting the source URL.

Data quality issues found during profiling, deliberately **not** fixed at load time (left to dbt staging instead):
- Phone numbers have literal embedded quote characters, plus placeholder junk (`"123456"`, `"N/A"`, blank) in ~10% of rows.
- Emails have case inconsistency and placeholder junk (`n/a`, `invalid-email`, blank) in ~6% of rows.

### 2. Model

Layering follows the **Medallion architecture** — a pattern of progressively refining data through Bronze (raw, untouched), Silver (cleaned/typed), and Gold (business-ready) layers. This maps directly onto dbt's own convention: **raw** (the untouched source-aligned landing table), **staging** (1:1 with source, cleaned and cast to proper types, no business logic), and **marts** (the final, business-ready models people actually query).

For the marts layer's shape, I used a **Kimball star schema** — a dimensional modeling method built around one or more central **fact tables** (rows of measurable events, e.g. an order line) surrounded by denormalized **dimension tables** (descriptive context, e.g. a customer or product), optimized for fast aggregation and BI-style querying rather than transactional writes. Concretely: `Customer` and `Product` are dimensions, `Orders` and `OrderItem` are facts (`OrderItem` being the real grain — one row per product per order, built by flattening the `order_items` JSON array).

`Product` isn't in the task's requirements, but I added it anyway: `product_name` otherwise only exists nested inside the JSON array with no dedicated home, meaning every downstream query would need to re-derive it or repeat the raw text redundantly. Pulling it into its own dimension gives it a proper place, and a natural spot for any future product attributes (category, price tier, etc.).

I considered and rejected:
- **3NF (Third Normal Form)** — a normalization technique from OLTP system design that decomposes data into many small, single-responsibility tables to eliminate redundancy, at the cost of requiring more joins. It optimizes for safe, consistent transactional writes, not analytical reads — the wrong trade-off for a reporting layer like this one.
- **Data Vault** — a modeling methodology built around Hubs (business keys), Links (relationships between them), and Satellites (descriptive, historized attributes), designed for large enterprises needing full auditability and change history across many source systems. Significant overhead with no payoff for a single-source, 1,000-row, one-off task.
- **Snowflake schema** — a variant of the star schema where dimension tables are further normalized into sub-dimensions (e.g. `Product → Category → Region`), reducing redundancy in large, hierarchical dimensions at the cost of extra joins. The dimensions here (3 products, no hierarchy) are too small and flat to benefit.
- **Surrogate keys** (artificial, system-generated IDs with no business meaning) and **SCD** (Slowly Changing Dimension patterns for tracking how a dimension's attributes change over time, e.g. Type 2 keeping a new row per version) — natural keys (`customer_id`, `order_id`, `product_id`) are already unique and stable in this data, and there's no change-over-time scenario in scope to justify the added complexity of either.

One naming note: the `Order` mart is named `orders` (plural) — `ORDER` is a reserved SQL keyword in Snowflake and breaks table creation if used as a bare identifier.

### 3. Aggregate

`weekly_top_seller` aggregates `OrderItem` by `DATE_TRUNC('week', order_date)` (Monday-start, ISO-style) and `product_id`, producing `total_quantity`, `total_revenue`, and `is_top_seller`.

"Top seller" isn't defined in the task, so I checked both readings against the actual data before picking one:
- **Quantity-based** and **revenue-based** rankings disagree in 80 of 144 weeks (55%).
- Quantity-based has ties in 7 weeks; **revenue-based has zero ties** across the whole dataset.

I went with **revenue**, since it's both a common business reading of "top seller" and the only definition that stays deterministic without inventing a tiebreak rule. The flag uses `RANK() OVER (PARTITION BY week ORDER BY total_revenue DESC) = 1` rather than `ROW_NUMBER()`, so that if a tie ever did occur, both products would be honestly flagged instead of one being arbitrarily picked.

Had I gone with quantity instead, the code change itself would be small — swap `total_revenue` for `total_quantity` in the `RANK()` clause — but it would also mean adding a secondary sort as a tiebreaker for the 7 weeks that do tie on quantity, e.g. `ORDER BY total_quantity DESC, total_revenue DESC`. That's a legitimate, explainable rule, just one more decision to design and defend than revenue alone requires.

### Transform with dbt

```
dbt/models/
├── staging/   (views — cleans phone/email, flattens JSON, casts types)
└── marts/     (tables — customer, product, orders, order_item; view — weekly_top_seller)
```

Materializations follow the task's own wording literally: "tables" for Customer/Order/OrderItem/Product, "view" for the aggregation, set via `+materialized` config per folder in `dbt_project.yml`.

**Staging models** clean and type-cast, nothing more: `stg_customers` strips embedded quote characters from phone numbers and nulls out known placeholder values (`123456`, `N/A`, `n/a`, `invalid-email`) rather than keeping them as fake strings; `stg_order_items` uses `LATERAL FLATTEN(input => order_items)` to turn the JSON array into one row per product per order, extracting fields via `value:field::type` path syntax; `stg_products` extracts distinct `product_id`/`product_name` pairs the same way.

**Marts models** are thin pass-throughs from staging (`select * from {{ ref(...) }}`) for `customer`, `product`, and `orders`; `order_item`'s grain is `(order_id, product_id)`, confirmed safe as a composite natural key during data profiling (no order repeats a product).

**Tests**: generic `unique`/`not_null`/`relationships` on all natural keys (defined in `_staging.yml`/`_marts.yml`), `dbt_utils.unique_combination_of_columns` enforcing the `(order_id, product_id)` composite key on `OrderItem`, and a custom singular test (`tests/assert_order_total_matches_items.sql`) — a query that returns any order where `orders.order_total` doesn't reconcile with `SUM(order_item.line_total)`; dbt treats a non-empty result as a failure. Currently 27/27 tests pass, including this one, across all 1,000 orders.

**Documentation**: every model and key column is described via `description:` fields in `_staging.yml`/`_marts.yml`. `dbt docs generate` builds a `manifest.json`/`catalog.json` from the project plus live warehouse metadata; `dbt docs serve` hosts a local static site with a full interactive lineage (DAG) graph and per-model docs — no extra tooling or account needed.

**Reviewer access**: handled via dbt's own `grants` config (`+grants: select: ['REVIEWER_ROLE']` set per model folder in `dbt_project.yml`), not a manual SQL script — `dbt run` rebuilds tables with `CREATE OR REPLACE`, which drops and recreates the object and would silently wipe a manually-granted privilege on the next run; dbt's declarative config re-applies the grant every time instead. Schema-level `USAGE` (which dbt's grants config doesn't cover — it only grants on the objects inside a schema, not the schema itself) is granted once, manually, in `setup/02_grant_reviewer_access.sql`.

## AI Usage

I used **Claude** throughout — for planning, code, and debugging. A few real moments:

**Getting a spec instead of jumping to code.** My first prompt for the load script gave the full requirement document (download CSV, land raw with `order_items` as VARIANT, error handling) and explicitly asked Claude to raise questions/concerns before writing anything. It came back with six concrete open decisions (auth method, naming, re-run policy, load method, raw table shape, error scope) rather than guessing — which meant we agreed on the design before any code existed, instead of me discovering disagreements in review.

**Snowpark vs. local Python** Snowpark's DataFrame API compiles operations down to SQL that executes inside the warehouse (server-side), meaning no data is pulled client-side and heavier transforms scale with warehouse compute. Plain Python + pandas does the opposite: everything happens client-side, in memory, using whatever the local machine can offer. For 1,000 rows, that difference is irrelevant — the entire load takes seconds regardless of approach — while Snowpark adds session/connection setup overhead and a less familiar API for no practical gain. 

**A genuine multi-step debugging chain**, not a single fix. Loading `order_items` (JSON) into a `VARIANT` column failed three different ways in a row, each with a distinct, verifiable cause read directly from the Snowflake error:
1. `INSERT ... SELECT %s, ...` batched via `cursor.executemany()` failed because the connector's automatic multi-row rewrite optimization only recognizes `INSERT ... VALUES (...)` syntax, not `SELECT`-form inserts.
2. Switching to `VALUES (...)` fixed that, but then failed on `NaN` values: pandas represents missing data as a float `NaN`, and the connector rendered it as a bare, unquoted `NAN` in the generated SQL — which Snowflake parsed as an invalid identifier rather than a null.
3. After converting `NaN` to `None`, it failed again: the bulk multi-row `VALUES` optimization only accepts literal constants, not function calls — and `PARSE_JSON(%s)` is a function call.

The working fix drops `executemany()`'s batching entirely and inserts row-by-row with `cursor.execute()`, trading a small amount of throughput (irrelevant at 1,000 rows) for a load that actually works with `PARSE_JSON`.

**Checking an assumption against real data before committing to it.** The task doesn't define "top seller." Rather than picking a metric and asserting it, I asked Claude to test quantity vs. revenue against the actual dataset first — it found the 55%-of-weeks disagreement and the tie-count difference, which is what actually decided the metric.

**Reviewer access missing grants** After setting up reviewer access via dbt's `grants` config, `dbt run` completed with no errors — which looked like confirmation the grants had worked. I pushed back on treating a clean run as proof, and verified the actual grant list in Snowflake directly (`SHOW GRANTS TO ROLE REVIEWER_ROLE`). That surfaced a real gap Claude's setup had missed: schema-level `USAGE` on `STAGING`/`MARTS` was never granted, only the object-level `SELECT` privileges — meaning the reviewer role couldn't have reached the tables at all despite every individual grant looking correct. Confirmed fixed by logging in as the reviewer role and running a real query, not just re-checking the grant list.

**Validation**: every suggestion got checked against something concrete — terminal output for the load script and dbt runs, `dbt test` results (27/27 passing, including the custom reconciliation test), direct SQL checks in Snowflake worksheets, and a real login-as-reviewer test rather than trusting the grant list alone.

**What made this work well**: feeding Claude the actual CSV early (not describing it) meant modeling and metric decisions were grounded in real data quirks (embedded quote characters, junk placeholders, the ISO week-52 boundary) rather than generic assumptions. Being explicit up front about scope ("1-2 evenings," "don't over-engineer") kept suggestions right-sized — e.g., it's why Data Vault, SCD, and Snowpark all got a one-line "considered, rejected" rather than being built. And asking for open questions before code, consistently, meant most of these decisions were made once, deliberately, instead of being discovered and re-litigated during review.

## Actual prompts

---
*This is a first draft covering the core task. Section 3 (optional extras) and any remaining polish will be added separately.*