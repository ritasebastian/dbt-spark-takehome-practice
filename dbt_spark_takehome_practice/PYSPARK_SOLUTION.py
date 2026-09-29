from pyspark.sql import functions as F

transactions = spark.table("raw.transactions")

clean = (
    transactions
    .withColumn("txn_date", F.to_date("txn_timestamp"))
    .withColumn(
        "is_cross_border",
        F.upper(F.trim("from_country")) != F.upper(F.trim("to_country"))
    )
    .filter(F.col("txn_id").isNotNull())
    .filter(F.col("user_id").isNotNull())
    .filter(F.col("amount_usd") > 0)
)

completed = clean.filter(F.upper(F.trim("txn_status")) == "COMPLETED")

daily_metrics = (
    completed
    .groupBy("user_id", "txn_date")
    .agg(
        F.count("*").alias("completed_txn_count"),
        F.sum("amount_usd").alias("total_amount_usd"),
        F.avg("amount_usd").alias("avg_amount_usd"),
        F.max("amount_usd").alias("max_amount_usd"),
    )
)

scored = (
    completed.alias("t")
    .join(
        daily_metrics.alias("m"),
        on=["user_id", "txn_date"],
        how="inner",
    )
    .withColumn(
        "high_amount_flag",
        F.when(F.col("amount_usd") >= 1000, 1).otherwise(0),
    )
    .withColumn(
        "cross_border_flag",
        F.when(
            (F.col("is_cross_border") == True) &
            (F.col("amount_usd") >= 500),
            1,
        ).otherwise(0),
    )
    .withColumn(
        "velocity_flag",
        F.when(F.col("completed_txn_count") >= 3, 1).otherwise(0),
    )
    .withColumn(
        "amount_spike_flag",
        F.when(
            (F.col("avg_amount_usd") > 0) &
            (F.col("amount_usd") >= 3 * F.col("avg_amount_usd")),
            1,
        ).otherwise(0),
    )
    .withColumn(
        "suspicious_flag",
        F.when(
            (F.col("high_amount_flag") == 1) |
            (F.col("cross_border_flag") == 1) |
            (F.col("velocity_flag") == 1) |
            (F.col("amount_spike_flag") == 1),
            1,
        ).otherwise(0),
    )
)

result = scored.filter(F.col("suspicious_flag") == 1)

display(
    result
    .select(
        "txn_id",
        "user_id",
        "txn_timestamp",
        "amount_usd",
        "is_cross_border",
        "completed_txn_count",
        "avg_amount_usd",
        "high_amount_flag",
        "cross_border_flag",
        "velocity_flag",
        "amount_spike_flag",
        "suspicious_flag",
    )
    .orderBy("txn_timestamp")
)
