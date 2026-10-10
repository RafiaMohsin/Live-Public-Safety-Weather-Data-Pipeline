# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Demo: idempotency, quarantine and audit log
# MAGIC Run notebooks 00-02 first. This notebook re-executes the pipeline on the **same raw data** and proves that
# MAGIC no table changes. Use screenshots of its output in the report.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from nws_pipeline.config import Config, BRONZE_TABLES, SILVER_TABLES
from nws_pipeline.pipeline import run_raw_to_bronze, run_bronze_to_silver

dbutils.widgets.text("db", "workspace.nws", "Catalog.Schema")
dbutils.widgets.text("raw_base", "/Volumes/workspace/nws/raw", "Raw folder (Volume path)")
dbutils.widgets.text("start_date", "2026-10-09", "Backfill start")
dbutils.widgets.text("end_date", "2026-10-09", "Backfill end")
g = dbutils.widgets.get
cfg = Config(db=g("db"), raw_base=g("raw_base"))

def counts():
    names = list(BRONZE_TABLES.values()) + sorted(set(SILVER_TABLES.values())) + ["quarantine_records"]
    return {n: spark.table(cfg.t(n)).count() for n in names}

# COMMAND ----------

before = counts()
print("BEFORE re-run:", before)

# COMMAND ----------

# Re-run both layers for the same date window = a backfill of data that is already loaded
r1 = run_raw_to_bronze(spark, cfg, mode="backfill", start_date=g("start_date"), end_date=g("end_date"))
r2 = run_bronze_to_silver(spark, cfg, mode="backfill", start_date=g("start_date"), end_date=g("end_date"))
after = counts()
print("AFTER re-run: ", after)
assert before == after, "pipeline is NOT idempotent!"
print("IDEMPOTENT: row counts identical after re-running on the same raw data")

# COMMAND ----------

# MAGIC %md ### Audit log for the re-run (rows_inserted and rows_updated should be 0)

# COMMAND ----------

display(spark.sql(f"""
  SELECT layer, source, load_type, parameter, status, rows_inserted, rows_updated, rows_quarantined,
         start_time, end_time, message
  FROM {cfg.db}.pipeline_execution_logs ORDER BY start_time DESC LIMIT 30"""))

# COMMAND ----------

# MAGIC %md ### Quarantined records (schema drift / type changes / invalid values)

# COMMAND ----------

display(spark.sql(f"SELECT layer, source, reason, count(*) AS n FROM {cfg.db}.quarantine_records GROUP BY ALL"))

# COMMAND ----------

tables = [
    "bronze_alerts",
    "bronze_forecast_periods",
    "bronze_observations",
    "bronze_iem_asos",
    "silver_alerts",
    "silver_forecast_periods",
    "silver_observations"
]

for table in tables:
    df = spark.table(f"workspace.nws.{table}")
    has_column = "load_timestamp" in df.columns
    print(f"{table}: {'PASS' if has_column else 'MISSING load_timestamp'}")

# COMMAND ----------

from pyspark.sql import functions as F

tables = [
    "bronze_alerts",
    "bronze_forecast_periods",
    "bronze_observations",
    "bronze_iem_asos",
    "silver_alerts",
    "silver_forecast_periods",
    "silver_observations"
]

for table in tables:
    df = spark.table(f"workspace.nws.{table}")
    null_count = df.filter(F.col("load_timestamp").isNull()).count()
    print(f"{table}: {null_count} null timestamps")
    

# COMMAND ----------

