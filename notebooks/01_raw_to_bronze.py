# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Raw -> Bronze
# MAGIC Reads raw files with **explicit StructType schemas**, quarantines non-conforming records, and loads the rest with
# MAGIC `MERGE INTO` (re-running the same files inserts nothing). Every file is logged in `pipeline_execution_logs`.
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `mode` | `incremental` = last `lookback_days` days of files; `backfill` = explicit `start_date`..`end_date` |
# MAGIC | `sources` | `all` or a comma list: `alerts,forecasts,observations,iem_asos` |
# MAGIC | `load_type` | `all`, `full`, `incremental` or `history` (which raw sub-folder) |
# MAGIC | `start_date` / `end_date` | `YYYY-MM-DD`, used in backfill mode (matches the date in the file name) |
# MAGIC | `batch_id` | optional label stored on every row and log entry (default: timestamp) |

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from nws_pipeline.config import Config
from nws_pipeline.pipeline import run_raw_to_bronze, setup_tables

dbutils.widgets.text("db", "workspace.nws", "Catalog.Schema")
dbutils.widgets.text("raw_base", "/Volumes/workspace/nws/raw", "Raw folder (Volume path)")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill"], "Mode")
dbutils.widgets.text("sources", "all", "Sources")
dbutils.widgets.dropdown("load_type", "all", ["all", "full", "incremental", "history"], "Load type")
dbutils.widgets.text("start_date", "", "Start date (backfill)")
dbutils.widgets.text("end_date", "", "End date (backfill)")
dbutils.widgets.text("lookback_days", "1", "Lookback days (incremental)")
dbutils.widgets.text("batch_id", "", "Batch id (optional)")

g = dbutils.widgets.get
cfg = Config(db=g("db"), raw_base=g("raw_base"))
setup_tables(spark, cfg)

# COMMAND ----------
results = run_raw_to_bronze(
    spark, cfg, sources=g("sources"), load_type=g("load_type"), mode=g("mode"),
    start_date=g("start_date") or None, end_date=g("end_date") or None,
    lookback_days=int(g("lookback_days") or 1), batch_id=g("batch_id") or None)
display(spark.createDataFrame(results))

# COMMAND ----------
display(spark.sql(f"SELECT * FROM {cfg.db}.pipeline_execution_logs ORDER BY start_time DESC LIMIT 20"))
