# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DecimalType
)
from decimal import Decimal
from datetime import datetime

spark.sql("CREATE SCHEMA IF NOT EXISTS raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS dbt_practice")

schema = StructType([
    StructField("txn_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("txn_timestamp", TimestampType(), False),
    StructField("amount_usd", DecimalType(18, 2), False),
    StructField("currency", StringType(), False),
    StructField("from_country", StringType(), False),
    StructField("to_country", StringType(), False),
    StructField("txn_status", StringType(), False),
    StructField("payment_method", StringType(), True),
])

raw_data = [
    ("T001","U001","M001","Tokyo Camera","2026-09-01 09:00:00","120.00","USD","US","US","COMPLETED","CARD"),
    ("T002","U001","M002","Best Electronics","2026-09-01 09:10:00","130.00","USD","US","US","COMPLETED","CARD"),
    ("T003","U001","M003","Gaming World","2026-09-01 09:20:00","140.00","USD","US","US","COMPLETED","CARD"),
    ("T004","U002","M004","Luxury Store","2026-09-01 10:00:00","1500.00","USD","US","US","COMPLETED","WALLET"),
    ("T005","U003","M005","Osaka Shop","2026-09-01 11:00:00","700.00","USD","US","JP","COMPLETED","QR"),
    ("T006","U004","M006","Local Grocery","2026-09-01 12:00:00","80.00","USD","US","US","COMPLETED","CARD"),
    ("T007","U004","M007","Local Fuel","2026-09-01 12:30:00","75.00","USD","US","US","COMPLETED","CARD"),
    ("T008","U005","M008","Online Store","2026-09-01 13:00:00","50.00","USD","US","US","FAILED","CARD"),
    ("T009","U006","M009","Seoul Market","2026-09-02 08:00:00","200.00","USD","US","KR","COMPLETED","QR"),
    ("T010","U006","M010","Seoul Electronics","2026-09-02 08:05:00","250.00","USD","US","KR","COMPLETED","QR"),
    ("T011","U006","M011","Seoul Luxury","2026-09-02 08:10:00","1600.00","USD","US","KR","COMPLETED","QR"),
    ("T012","U007","M012","Coffee Shop","2026-09-02 09:00:00","15.00","USD","US","US","PENDING","CARD"),
    ("T013","U008","M013","London Shop","2026-09-02 10:00:00","550.00","USD","US","GB","COMPLETED","WALLET"),
    ("T014","U009","M014","Book Store","2026-09-02 11:00:00","45.00","USD","US","US","COMPLETED","CARD"),
    ("T015","U010","M015","Home Store","2026-09-02 12:00:00","300.00","USD","US","US","COMPLETED","CARD"),
]

rows = []
for r in raw_data:
    rows.append((
        r[0], r[1], r[2], r[3],
        datetime.strptime(r[4], "%Y-%m-%d %H:%M:%S"),
        Decimal(r[5]),
        r[6], r[7], r[8], r[9], r[10]
    ))

df = spark.createDataFrame(rows, schema)

(
    df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("raw.transactions")
)

display(spark.table("raw.transactions").orderBy("txn_timestamp"))
