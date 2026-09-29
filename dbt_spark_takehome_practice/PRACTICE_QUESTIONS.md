# Practice Questions

Try these without looking at the solution first.

## Part A — SQL / dbt

1. Create a staging model that:
   - trims string columns
   - uppercases status/currency/country
   - converts timestamp correctly
   - removes null IDs and non-positive amounts
   - derives `is_cross_border`

2. Create one row per `user_id, txn_date` with:
   - completed transaction count
   - total amount
   - average amount
   - maximum amount

3. Return suspicious transactions using the rules in README.

4. Add tests for:
   - unique transaction ID
   - non-null transaction ID and user ID
   - allowed transaction status values
   - fact table must contain only suspicious rows

5. Convert the suspicious model to incremental materialization.

## Part B — PySpark

Using `raw.transactions`:

1. Filter completed records.
2. Derive `txn_date` and `is_cross_border`.
3. Group by user/day.
4. Join metrics back to transactions.
5. Derive the four risk flags.
6. Return only suspicious transactions.

## Part C — Interview follow-up questions

Be ready to explain:

- Why use `ref()` instead of hardcoding model names?
- Difference between dbt `view`, `table`, and `incremental`.
- Why might an incremental model use a lookback window?
- Difference between a dbt schema test and a singular test.
- What happens if a late-arriving record comes in?
- How would you handle millions of transactions per day?
- What Spark operations may cause a shuffle?
- Why can joins and `groupBy()` become expensive?
- When would you use broadcast joins?
- How would you avoid duplicate rows after reruns?
