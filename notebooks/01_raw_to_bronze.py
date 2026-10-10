# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 - Raw -> Bronze
# MAGIC
# MAGIC Reads raw JSON and CSV files using explicit StructType schemas, quarantines non-conforming records, and loads valid records into Bronze tables using MERGE INTO to prevent duplicate inserts when the same files are reprocessed. Every processed file is recorded in pipeline_execution_logs.
# MAGIC
# MAGIC | Parameter                 | Meaning                                                                                                                             |
# MAGIC | ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
# MAGIC | `mode`                    | `incremental` processes files within the last `lookback_days` days; `backfill` processes files between `start_date` and `end_date`. |
# MAGIC | `sources`                 | `all` or a comma-separated list: `alerts,forecasts,observations,iem_asos`.                                                          |
# MAGIC | `load_type`               | `all`, `full`, `incremental` or `history`; selects the corresponding raw-data subfolder.                                            |
# MAGIC | `start_date` / `end_date` | Dates in `YYYY-MM-DD` format, used in backfill mode.                                                                                |
# MAGIC | `lookback_days`           | Number of days included in the incremental processing window.                                                                       |
# MAGIC | `batch_id`                | Optional label recorded with processed rows and audit logs.                                                                         |
# MAGIC
# MAGIC ## Initial live-data load
# MAGIC
# MAGIC Historical `iem_asos` data has already been loaded separately. For the initial ingestion of live National Weather Service (NWS) data, use:
# MAGIC
# MAGIC * `mode`: `incremental`
# MAGIC * `sources`: `alerts,forecasts,observations`
# MAGIC * `load_type`: `full`
# MAGIC * `lookback_days`: `1`
# MAGIC * `batch_id`: `live-baseline-20261009`
# MAGIC
# MAGIC This processes the newly collected live NWS files without intentionally reprocessing the historical IEM ASOS dataset. Future runs can use the `incremental` raw-data folders once incremental collection is configured.
# MAGIC

# COMMAND ----------

import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from nws_pipeline.config import Config
from nws_pipeline.pipeline import run_raw_to_bronze, setup_tables

# Pipeline configuration

dbutils.widgets.text("db", "workspace.nws", "Catalog.Schema")
dbutils.widgets.text(
"raw_base",
"/Volumes/workspace/nws/raw",
"Raw folder (Volume path)"
)

# Initial live-data load settings

dbutils.widgets.dropdown(
"mode", "incremental", ["incremental", "backfill"], "Mode"
)
dbutils.widgets.text(
"sources",
"alerts,forecasts,observations",
"Sources"
)
dbutils.widgets.dropdown(
"load_type", "full", ["all", "full", "incremental", "history"], "Load type"
)
dbutils.widgets.text("start_date", "", "Start date (backfill)")
dbutils.widgets.text("end_date", "", "End date (backfill)")
dbutils.widgets.text("lookback_days", "1", "Lookback days (incremental)")
dbutils.widgets.text(
"batch_id", "live-baseline-20261009", "Batch id (optional)"
)

# Read widget values and prepare tables

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

# COMMAND ----------

for table in [
    "bronze_alerts",
    "bronze_forecast_periods",
    "bronze_observations"
]:
    print(f"\n--- {table} ---")
    spark.sql(
        f"SELECT COUNT(*) AS row_count FROM workspace.nws.{table}"
    ).show()

# COMMAND ----------

