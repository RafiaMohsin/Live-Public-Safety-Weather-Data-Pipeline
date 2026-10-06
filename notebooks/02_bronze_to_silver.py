# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze -> Silver
# MAGIC Casts to strict types, validates business rules, quarantines rejected rows and upserts with `MERGE INTO`.
# MAGIC Unchanged rows are skipped via `row_hash`, so re-running never creates duplicates.
# MAGIC
# MAGIC Select Bronze rows by `ingest_date` window (`mode`, `start_date`, `end_date`, `lookback_days`) and optionally
# MAGIC by one Bronze `bronze_batch_id`.

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from nws_pipeline.config import Config
from nws_pipeline.pipeline import run_bronze_to_silver, setup_tables

dbutils.widgets.text("db", "workspace.nws", "Catalog.Schema")
dbutils.widgets.text("raw_base", "/Volumes/workspace/nws/raw", "Raw folder (Volume path)")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill"], "Mode")
dbutils.widgets.text("sources", "all", "Sources")
dbutils.widgets.text("start_date", "", "Start date (backfill)")
dbutils.widgets.text("end_date", "", "End date (backfill)")
dbutils.widgets.text("lookback_days", "1", "Lookback days (incremental)")
dbutils.widgets.text("bronze_batch_id", "", "Only this Bronze batch (optional)")
dbutils.widgets.text("batch_id", "", "Batch id (optional)")

g = dbutils.widgets.get
cfg = Config(db=g("db"), raw_base=g("raw_base"))
setup_tables(spark, cfg)

# COMMAND ----------
results = run_bronze_to_silver(
    spark, cfg, sources=g("sources"), mode=g("mode"),
    start_date=g("start_date") or None, end_date=g("end_date") or None,
    lookback_days=int(g("lookback_days") or 1),
    batch_id=g("batch_id") or None, bronze_batch_id=g("bronze_batch_id") or None)
display(spark.createDataFrame(results))

# COMMAND ----------
display(spark.sql(f"SELECT * FROM {cfg.db}.pipeline_execution_logs ORDER BY start_time DESC LIMIT 20"))
