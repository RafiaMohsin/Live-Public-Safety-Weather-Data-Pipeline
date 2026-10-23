"""Orchestration: setup, raw->bronze and bronze->silver runs. Used by the notebooks and the tests."""
import uuid
from datetime import datetime, timezone

from . import bronze, gold, silver
from .audit import Audit
from .config import (BRONZE_TABLES, GOLD_TABLES, LOG_TABLE, QUARANTINE_TABLE, SILVER_TABLES, SOURCES,
                     list_raw_files, resolve_window)
from .delta_utils import create_table
from .schemas import BRONZE_SCHEMAS, GOLD_SCHEMAS, LOG_SCHEMA, QUARANTINE_SCHEMA, SILVER_SCHEMAS


def setup_tables(spark, cfg):
    """Create the schema and every Bronze / Silver / operational table from the explicit contracts."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.db}")
    for key, value in (("spark.sql.session.timeZone", "UTC"),
                       ("spark.databricks.delta.schema.autoMerge.enabled", "true")):
        try:
            spark.conf.set(key, value)
        except Exception as e:                      # some settings are fixed on serverless compute
            print(f"note: could not set {key}: {e}")
    for source, name in BRONZE_TABLES.items():
        create_table(spark, cfg.t(name), BRONZE_SCHEMAS[source])
    for name, schema in SILVER_SCHEMAS.items():
        create_table(spark, cfg.t(name), schema)
    for name in GOLD_TABLES:
        create_table(spark, cfg.t(name), GOLD_SCHEMAS[name])
    create_table(spark, cfg.t(LOG_TABLE), LOG_SCHEMA)
    create_table(spark, cfg.t(QUARANTINE_TABLE), QUARANTINE_SCHEMA)


def _ids(batch_id=None):
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return str(uuid.uuid4()), batch_id or f"batch_{now}"


def _sources(sources):
    chosen = list(SOURCES) if sources in (None, "all") else [s.strip() for s in sources.split(",")]
    unknown = [s for s in chosen if s not in SOURCES]
    if unknown:
        raise ValueError(f"unknown source(s) {unknown}; choose from {list(SOURCES)}")
    return chosen


def run_raw_to_bronze(spark, cfg, sources="all", load_type="all", mode="incremental",
                      start_date=None, end_date=None, lookback_days=1, batch_id=None):
    """Load raw files into Bronze.

    mode='incremental' -> files from the last `lookback_days` days (default: today + yesterday)
    mode='backfill'    -> files whose timestamp date is between start_date and end_date
    load_type          -> all | full | incremental | history
    """
    start, end = resolve_window(mode, start_date, end_date, lookback_days)
    run_id, batch_id = _ids(batch_id)
    results = []
    for source in _sources(sources):
        files = list_raw_files(cfg, source, load_type, start, end)
        if not files:
            with Audit(spark, cfg, run_id, batch_id, "raw_to_bronze", source, load_type,
                       f"{start}..{end}") as a:
                a.message = "no raw files found in window"
            results.append(a.summary())
            continue
        for path, kind, file_date in files:
            with Audit(spark, cfg, run_id, batch_id, "raw_to_bronze", source, kind, path) as a:
                bronze.process_file(spark, cfg, source, path, file_date, batch_id, a)
            results.append(a.summary())
    return results


def run_bronze_to_silver(spark, cfg, sources="all", mode="incremental", start_date=None,
                         end_date=None, lookback_days=1, batch_id=None, bronze_batch_id=None):
    """Upsert Bronze rows (selected by ingest_date window and/or Bronze batch id) into Silver."""
    start, end = resolve_window(mode, start_date, end_date, lookback_days)
    run_id, batch_id = _ids(batch_id)
    results = []
    for source in _sources(sources):
        param = f"{start}..{end}" + (f" bronze_batch={bronze_batch_id}" if bronze_batch_id else "")
        with Audit(spark, cfg, run_id, batch_id, "bronze_to_silver", source, mode, param) as a:
            silver.process_table(spark, cfg, source, start, end, bronze_batch_id, batch_id, a)
        results.append(a.summary())
    return results


def run_silver_to_gold(spark, cfg, mode="incremental", start_date=None, end_date=None,
                       lookback_days=1, batch_id=None, tables=None):
    """Refresh the Gold star schema from Silver.

    mode='incremental' -> Silver rows loaded in the last `lookback_days` days (facts) + affected daily aggregates
    mode='backfill'    -> Silver rows whose load_timestamp date is between start_date and end_date
    mode='full'        -> rebuild everything from the whole Silver layer (use for the first Gold load)
    Dimensions are always rebuilt from full Silver (small) and merged; unchanged rows are skipped.
    """
    since, until = gold.window_bounds(mode, start_date, end_date, lookback_days)
    run_id, batch_id = _ids(batch_id)
    param = "all silver rows" if since is None else f"silver load_timestamp {since}..{until}"
    chosen = GOLD_TABLES if tables in (None, "all") else [t.strip() for t in tables.split(",")]
    unknown = [t for t in chosen if t not in GOLD_TABLES]
    if unknown:
        raise ValueError(f"unknown Gold table(s) {unknown}; choose from {GOLD_TABLES}")
    results = []
    for name in [t for t in GOLD_TABLES if t in chosen]:            # keep dimension-first order
        with Audit(spark, cfg, run_id, batch_id, "silver_to_gold", name, mode, param) as a:
            gold.process_table(spark, cfg, name, since, until, a)
        results.append(a.summary())
    return results
