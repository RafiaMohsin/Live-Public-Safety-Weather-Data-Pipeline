# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 - Silver -> Gold (star schema)
# MAGIC Builds the dimensional model and refreshes it with `MERGE INTO`, logging every table to `pipeline_execution_logs`
# MAGIC (`layer = silver_to_gold`, rows read / inserted / updated, start and end time).
# MAGIC
# MAGIC | Table | Type | Grain |
# MAGIC |---|---|---|
# MAGIC | `dim_date`, `dim_station`, `dim_location`, `dim_alert_type` | Dimensions | one row per member |
# MAGIC | `fact_alerts` | Fact | one row per alert |
# MAGIC | `fact_observations` | Fact | one row per station, timestamp and source |
# MAGIC | `fact_forecast_periods` | Fact | one row per location, forecast version and period |
# MAGIC | `fact_daily_weather` | Aggregate fact | one row per station, day and source |
# MAGIC
# MAGIC **First run:** `mode = full`. **Scheduled runs (after notebook 02 in the Job):** `mode = incremental`.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from nws_pipeline.config import Config, GOLD_TABLES
from nws_pipeline.gold import create_views, optimize_gold
from nws_pipeline.pipeline import run_silver_to_gold, setup_tables

dbutils.widgets.text("db", "workspace.nws", "Catalog.Schema")
dbutils.widgets.text("raw_base", "/Volumes/workspace/nws/raw", "Raw folder (Volume path)")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill", "full"], "Mode")
dbutils.widgets.text("start_date", "", "Start date (backfill)")
dbutils.widgets.text("end_date", "", "End date (backfill)")
dbutils.widgets.text("lookback_days", "1", "Lookback days (incremental)")
dbutils.widgets.text("tables", "all", "Gold tables (comma list or all)")
dbutils.widgets.dropdown("optimize", "no", ["no", "yes"], "Run OPTIMIZE / Liquid Clustering")
dbutils.widgets.text("batch_id", "", "Batch id (optional)")

g = dbutils.widgets.get
cfg = Config(db=g("db"), raw_base=g("raw_base"))
setup_tables(spark, cfg)

# COMMAND ----------

results = run_silver_to_gold(
    spark, cfg, mode=g("mode"),
    start_date=g("start_date") or None, end_date=g("end_date") or None,
    lookback_days=int(g("lookback_days") or 1),
    batch_id=g("batch_id") or None, tables=g("tables"),
)
display(spark.createDataFrame(results))

failures = [r for r in results if r.get("status") == "Failure"]
if failures:
    raise RuntimeError(f"{len(failures)} Silver-to-Gold operation(s) failed. "
                       "Check pipeline_execution_logs for details.")

# COMMAND ----------

# MAGIC %md ## Row counts and audit trail

# COMMAND ----------

display(spark.sql(" UNION ALL ".join(
    f"SELECT '{t}' AS table_name, count(*) AS row_count FROM {cfg.t(t)}" for t in GOLD_TABLES)))

# COMMAND ----------

display(spark.sql(f"""
  SELECT layer, source, load_type, status, start_time, end_time,
         round(unix_timestamp(end_time) - unix_timestamp(start_time), 1) AS seconds,
         rows_inserted, rows_updated, message
  FROM {cfg.db}.pipeline_execution_logs WHERE layer = 'silver_to_gold'
  ORDER BY start_time DESC LIMIT 20"""))

# COMMAND ----------

# MAGIC %md ## BI views for the dashboard's audit page

# COMMAND ----------

create_views(spark, cfg)
display(spark.sql(f"SELECT * FROM {cfg.db}.v_pipeline_runs ORDER BY start_time DESC LIMIT 20"))

# COMMAND ----------

# MAGIC %md ## Read optimisation (Liquid Clustering + OPTIMIZE)
# MAGIC Set the `optimize` widget to `yes`. Run the query in the next cell before and after to compare timings.

# COMMAND ----------

if g("optimize") == "yes":
    display(spark.createDataFrame(optimize_gold(spark, cfg), "table_name string, optimisation string"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {cfg.t('fact_observations')}"))

# COMMAND ----------

# MAGIC %md ## Heaviest Gold query (take the Spark UI DAG screenshot of this one)
# MAGIC Star join of the largest fact with two dimensions plus a group-by: shows scans with partition/file pruning,
# MAGIC broadcast of small dimensions, and a shuffle for the aggregation.

# COMMAND ----------

import time
t0 = time.time()
q = spark.sql(f"""
  SELECT d.year, d.month_name, s.station_id, f.source,
         count(*) AS observations, round(avg(f.temperature_c), 1) AS avg_temp_c,
         round(max(f.wind_gust_kmh), 1) AS max_gust_kmh
  FROM {cfg.t('fact_observations')} f
  JOIN {cfg.t('dim_station')} s ON f.station_key = s.station_key
  JOIN {cfg.t('dim_date')}    d ON f.date_key = d.date_key
  WHERE d.year >= 2025
  GROUP BY d.year, d.month_name, s.station_id, f.source
  ORDER BY d.year, d.month_name, s.station_id""")
display(q)
print(f"query seconds: {time.time() - t0:.1f}")
