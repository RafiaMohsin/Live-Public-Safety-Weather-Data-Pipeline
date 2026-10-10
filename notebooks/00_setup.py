# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates the schema, the raw-files Volume and **every table from the explicit schemas** in `nws_pipeline/schemas.py`
# MAGIC (Bronze, Silver, `pipeline_execution_logs`, `quarantine_records`). Safe to re-run.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))   # repo root (notebook lives in notebooks/)
from nws_pipeline.config import Config
from nws_pipeline.pipeline import setup_tables

dbutils.widgets.text("db", "workspace.nws", "Catalog.Schema")
dbutils.widgets.text("raw_base", "/Volumes/workspace/nws/raw", "Raw folder (Volume path)")
cfg = Config(db=dbutils.widgets.get("db"), raw_base=dbutils.widgets.get("raw_base"))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.db}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.db}.raw")      # -> /Volumes/<catalog>/<schema>/raw
setup_tables(spark, cfg)
display(spark.sql(f"SHOW TABLES IN {cfg.db}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now upload the raw files into the Volume, keeping this folder layout:
# MAGIC ```
# MAGIC raw/full/{alerts,forecasts,observations}/<UTC timestamp>.json         (from samples/full_load)
# MAGIC raw/incremental/{alerts,forecasts,observations}/<UTC timestamp>.json  (from samples/incremental)
# MAGIC raw/history/iem_asos/<UTC timestamp>_<STATION>_<start>_<end>.csv      (from scripts/download_iem_asos.py)
# MAGIC ```