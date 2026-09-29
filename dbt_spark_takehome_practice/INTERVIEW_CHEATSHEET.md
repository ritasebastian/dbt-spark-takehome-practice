# Interview Cheat Sheet

## `source()` vs `ref()`

`source()` points to raw/external source data.

`ref()` points to another dbt model. It also creates dependency information so dbt can build models in the correct order.

## View vs table vs incremental

- **view**: SQL is executed when queried. Good for light staging transformations.
- **table**: rebuilt and physically stored. Good when repeated reads should be fast.
- **incremental**: after the initial build, processes only new/changed data. Useful for large datasets.

## Why a lookback window?

Late-arriving data may have an older event timestamp. Reprocessing the last day (or another safe window) allows those records to be picked up.

## dbt test

A dbt data test is a SQL query that should return zero invalid rows.

Examples:
- unique
- not_null
- accepted_values
- relationships
- custom business-rule tests

## Spark performance

Common shuffle-heavy operations:
- `groupBy`
- large joins
- `distinct`
- `orderBy`

For large production workloads:
- partition data appropriately
- filter early
- select only needed columns
- avoid unnecessary shuffles
- broadcast genuinely small dimension tables
- use incremental processing
- monitor skew
- use Delta/Iceberg optimizations appropriate to the platform
