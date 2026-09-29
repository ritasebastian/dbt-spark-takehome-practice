# DBT + Spark / Databricks Take-Home Practice

This is a realistic 60-minute practice assessment.

## Scenario

You are given raw payment transactions. Build a small analytics pipeline that:

1. Cleans and standardizes raw transactions.
2. Removes bad rows.
3. Creates daily user metrics.
4. Identifies suspicious transactions.
5. Adds dbt data-quality tests.
6. Supports incremental processing.

## Business Rules

A transaction is suspicious when **any** of these are true:

- `amount_usd >= 1000`
- transaction is cross-border AND `amount_usd >= 500`
- the user has 3 or more completed transactions on the same day
- transaction amount is at least 3x the user's average completed transaction amount for that day

Only `COMPLETED` transactions should be used in the analytical models.

## Expected project

```text
dbt_spark_takehome_practice/
├── dbt_project.yml
├── profiles.yml.example
├── data/
│   └── transactions.csv
├── notebooks/
│   └── 01_create_raw_table.py
├── models/
│   ├── sources.yml
│   ├── staging/
│   │   ├── stg_transactions.sql
│   │   └── staging.yml
│   └── marts/
│       ├── int_user_daily_metrics.sql
│       ├── fct_suspicious_transactions.sql
│       └── marts.yml
├── macros/
│   └── cents_to_dollars.sql
└── tests/
    └── assert_completed_only.sql
```

## Databricks setup

Run `notebooks/01_create_raw_table.py` in a Databricks notebook first. It creates:

- schema: `raw`
- table: `raw.transactions`

Then configure dbt with a Databricks SQL Warehouse or an all-purpose cluster.

## Typical commands

```bash
dbt debug
dbt run
dbt test
dbt build
```

Run one model:

```bash
dbt run --select stg_transactions
```

Run a model and everything downstream:

```bash
dbt build --select stg_transactions+
```

Run only suspicious-transaction model:

```bash
dbt build --select fct_suspicious_transactions
```

## What to explain in an interview

- `source()` represents raw/source-system data.
- `ref()` creates dependencies between dbt models.
- Staging models clean/standardize data.
- Intermediate models calculate reusable business metrics.
- Mart/fact models expose business-ready data.
- dbt tests validate assumptions such as uniqueness, nullability, accepted values, and business rules.
- An incremental model processes only new/changed records instead of rebuilding the entire table.
