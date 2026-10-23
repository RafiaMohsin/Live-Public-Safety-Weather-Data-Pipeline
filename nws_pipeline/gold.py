"""Silver -> Gold. Builds the star schema (dimensions, facts, daily aggregate) and upserts with MERGE INTO.

Design choices
- Surrogate keys are deterministic (xxhash64 of the natural key): re-running never renumbers rows, so
  MERGE stays idempotent and facts never need a lookup join against a dimension.
- Facts are loaded incrementally: only Silver rows whose `load_timestamp` falls in the run window.
- Dimensions are small, so they are rebuilt from the full Silver table and merged (unchanged rows skipped).
- `fact_daily_weather` is recomputed only for the (station, day, source) groups touched by the window,
  so an incremental run aggregates a few hours of data, not the whole history.
- Every table is written with MERGE and a `row_hash`: unchanged rows are not rewritten.
"""
from datetime import date, timedelta

from pyspark.sql import functions as F

from .config import GOLD_TABLES, LOG_TABLE, QUARANTINE_TABLE
from .delta_utils import conform, merge_into
from .schemas import GOLD_CLUSTER_BY, GOLD_KEYS, GOLD_SCHEMAS

SEVERITY_RANK = {"Extreme": 4, "Severe": 3, "Moderate": 2, "Minor": 1}


# ---------------------------------------------------------------- helpers
def _skey(*cols):
    """Deterministic 64-bit surrogate key from natural-key columns."""
    return F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols])


def _date_key(col):
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


def _with_hash(df, schema):
    """row_hash over the business columns (everything except row_hash / load_timestamp)."""
    cols = [f.name for f in schema.fields if f.name not in ("row_hash", "load_timestamp")]
    h = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols]), 256)
    return df.withColumn("row_hash", h).withColumn("load_timestamp", F.current_timestamp())


def _hours(later, earlier):
    return F.round((F.unix_timestamp(later) - F.unix_timestamp(earlier)) / 3600.0, 2)


def _count(arr):
    return F.when(F.col(arr).isNull(), F.lit(0)).otherwise(F.size(arr)).cast("int")


# ---------------------------------------------------------------- dimensions
def build_dim_date(spark, start, end):
    df = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
    return df.select(
        _date_key("date").alias("date_key"), "date", F.year("date").alias("year"),
        F.quarter("date").alias("quarter"), F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"), F.weekofyear("date").alias("week_of_year"),
        F.dayofmonth("date").alias("day"), F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"))


def build_dim_station(obs):
    return (obs.groupBy("station_id")
            .agg(F.concat_ws(",", F.sort_array(F.collect_set("source"))).alias("sources"),
                 F.min("observation_ts").alias("first_observation_ts"),
                 F.max("observation_ts").alias("last_observation_ts"))
            .withColumn("station_key", _skey("station_id")))


def build_dim_location(fc):
    return (fc.groupBy("location_name")
            .agg(F.min("forecast_generated_at").alias("first_forecast_ts"),
                 F.max("forecast_generated_at").alias("last_forecast_ts"))
            .withColumn("location_key", _skey("location_name")))


def build_dim_alert_type(alerts):
    rank = F.create_map(*[x for k, v in SEVERITY_RANK.items() for x in (F.lit(k), F.lit(v))])
    return (alerts.select("event", "category", "severity", "certainty", "urgency").distinct()
            .withColumn("alert_type_key", _skey("event", "category", "severity", "certainty", "urgency"))
            .withColumn("severity_rank", F.coalesce(rank[F.col("severity")], F.lit(0)).cast("int")))


# ---------------------------------------------------------------- facts
def build_fact_alerts(alerts):
    return alerts.select(
        "alert_id",
        _skey("event", "category", "severity", "certainty", "urgency").alias("alert_type_key"),
        _date_key("sent_ts").alias("sent_date_key"), _date_key("onset_ts").alias("onset_date_key"),
        "sent_ts", "effective_ts", "onset_ts", "expires_ts", "ends_ts", "status", "message_type",
        ((F.col("message_type") != "Alert") | (_count("referenced_alert_ids") > 0)).alias("is_update"),
        _hours("expires_ts", "effective_ts").alias("duration_hours"),
        _hours("onset_ts", "sent_ts").alias("lead_time_hours"),
        _count("affected_zones").alias("affected_zone_count"), _count("ugc_codes").alias("ugc_code_count"),
        "area_desc", "headline", "sender_name", F.col("row_hash").alias("row_hash"),
        F.current_timestamp().alias("load_timestamp"))


def build_fact_observations(obs):
    return obs.select(
        _skey("station_id").alias("station_key"), _date_key("observation_ts").alias("date_key"),
        "observation_ts", "source", F.hour("observation_ts").alias("observation_hour"),
        "text_description", "temperature_c", "dewpoint_c", "wind_direction_deg", "wind_speed_kmh",
        "wind_gust_kmh", "pressure_pa", "visibility_m", "humidity_pct", "precip_last_hour_mm",
        "row_hash", F.current_timestamp().alias("load_timestamp"))


def build_fact_forecast_periods(fc):
    return fc.select(
        _skey("location_name").alias("location_key"), "forecast_generated_at", "period_number",
        _date_key("start_time").alias("start_date_key"), "period_name", "start_time", "end_time",
        "is_daytime", "temperature_f", F.round((F.col("temperature_f") - 32) * 5 / 9, 1).alias("temperature_c"),
        "precip_probability_pct", "wind_speed_min_mph", "wind_speed_max_mph", "wind_direction",
        "short_forecast", "row_hash", F.current_timestamp().alias("load_timestamp"))


def build_daily_weather(obs_all, obs_window):
    """Daily per-station aggregate. Only groups touched by `obs_window` are recomputed (semi-join),
    then aggregated over ALL their Silver rows, so late-arriving data corrects the day."""
    day = F.to_date("observation_ts")
    touched = obs_window.select("station_id", day.alias("d"), "source").distinct()
    scoped = (obs_all.withColumn("d", day)
              .join(touched, ["station_id", "d", "source"], "left_semi"))
    agg = (scoped.groupBy("station_id", "d", "source").agg(
        F.count("*").cast("int").alias("obs_count"),
        F.round(F.avg("temperature_c"), 2).alias("avg_temp_c"),
        F.round(F.min("temperature_c"), 2).alias("min_temp_c"),
        F.round(F.max("temperature_c"), 2).alias("max_temp_c"),
        F.round(F.avg("humidity_pct"), 2).alias("avg_humidity_pct"),
        F.round(F.avg("wind_speed_kmh"), 2).alias("avg_wind_speed_kmh"),
        F.round(F.max("wind_gust_kmh"), 2).alias("max_wind_gust_kmh"),
        F.round(F.avg("pressure_pa"), 1).alias("avg_pressure_pa"),
        F.round(F.min("visibility_m"), 1).alias("min_visibility_m"),
        F.round(F.sum("precip_last_hour_mm"), 2).alias("precip_sum_mm")))
    return (agg.withColumn("station_key", _skey("station_id"))
               .withColumn("date_key", _date_key("d")))


# ---------------------------------------------------------------- orchestration of one Gold table
def _window(df, since, until):
    return df if since is None else df.filter((F.col("load_timestamp") >= since) & (F.col("load_timestamp") < until))


def window_bounds(mode, start_date=None, end_date=None, lookback_days=1):
    """Return (since, until) timestamps on Silver `load_timestamp`, or (None, None) for a full rebuild."""
    if mode == "full":
        return None, None
    from .config import resolve_window
    s, e = resolve_window(mode, start_date, end_date, lookback_days)
    until = (date.fromisoformat(e) + timedelta(days=1)).isoformat()
    return s, until


def process_table(spark, cfg, name, since, until, audit):
    """Build one Gold table and MERGE it. Records rows read / inserted / updated on `audit`."""
    sv = lambda t: spark.table(cfg.t(t))
    schema = GOLD_SCHEMAS[name]
    read = None

    if name == "dim_date":
        r = spark.sql(f"""
            SELECT to_date(min(d)) AS lo, to_date(max(d)) AS hi FROM (
              SELECT min(sent_ts) AS d FROM {cfg.t('silver_alerts')} UNION ALL
              SELECT max(expires_ts) FROM {cfg.t('silver_alerts')} UNION ALL
              SELECT min(observation_ts) FROM {cfg.t('silver_observations')} UNION ALL
              SELECT max(observation_ts) FROM {cfg.t('silver_observations')} UNION ALL
              SELECT min(start_time) FROM {cfg.t('silver_forecast_periods')} UNION ALL
              SELECT max(start_time) FROM {cfg.t('silver_forecast_periods')})""").collect()[0]
        if r["lo"] is None:
            audit.message = "silver is empty, nothing to build"
            return
        out = build_dim_date(spark, r["lo"].isoformat(), r["hi"].isoformat())
        read = f"date range {r['lo']}..{r['hi']}"
        audit.rows_read = out.count()
    elif name == "dim_station":
        src = sv("silver_observations"); out = build_dim_station(src)
    elif name == "dim_location":
        src = sv("silver_forecast_periods"); out = build_dim_location(src)
    elif name == "dim_alert_type":
        src = sv("silver_alerts"); out = build_dim_alert_type(src)
    elif name == "fact_alerts":
        src = _window(sv("silver_alerts"), since, until); out = build_fact_alerts(src)
    elif name == "fact_observations":
        src = _window(sv("silver_observations"), since, until); out = build_fact_observations(src)
    elif name == "fact_forecast_periods":
        src = _window(sv("silver_forecast_periods"), since, until); out = build_fact_forecast_periods(src)
    elif name == "fact_daily_weather":
        src = _window(sv("silver_observations"), since, until)
        out = build_daily_weather(sv("silver_observations"), src)
    else:
        raise ValueError(f"unknown Gold table {name}")

    if name != "dim_date":
        audit.rows_read = src.count()
    if audit.rows_read == 0:
        audit.message = "no new Silver rows in window"
        return

    out = conform(_with_hash(out, schema) if "row_hash" not in out.columns else out, schema)
    audit.rows_inserted, audit.rows_updated = merge_into(
        spark, cfg.t(name), out, GOLD_KEYS[name], hash_col="row_hash")
    verb = "silver rows aggregated" if name == "fact_daily_weather" else "silver rows read"
    audit.message = f"{verb}={audit.rows_read}" + (f" ({read})" if read else "")


# ---------------------------------------------------------------- performance + BI helpers
def optimize_gold(spark, cfg):
    """Liquid Clustering on the join keys of each fact, then OPTIMIZE every Gold table.
    Falls back to ZORDER, then plain OPTIMIZE, if a feature is unavailable on the compute."""
    report = []
    for name in GOLD_TABLES:
        full, cols = cfg.t(name), GOLD_CLUSTER_BY.get(name)
        mode = "OPTIMIZE"
        try:
            if cols:
                spark.sql(f"ALTER TABLE {full} CLUSTER BY ({', '.join(cols)})")
                mode = f"LIQUID CLUSTER BY ({', '.join(cols)}) + OPTIMIZE"
            spark.sql(f"OPTIMIZE {full}")
        except Exception as e:
            try:
                spark.sql(f"OPTIMIZE {full} ZORDER BY ({', '.join(cols)})" if cols else f"OPTIMIZE {full}")
                mode = f"ZORDER fallback ({str(e)[:80]})"
            except Exception as e2:
                mode = f"skipped: {str(e2)[:120]}"
        report.append((name, mode))
    return report


def create_views(spark, cfg):
    """BI-facing views over the operational tables, used by the dashboard's audit page."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {cfg.t('v_pipeline_runs')} AS
        SELECT run_id, batch_id, layer, source, load_type, status, start_time, end_time,
               to_date(start_time) AS run_date,
               round(unix_timestamp(end_time) - unix_timestamp(start_time), 2) AS duration_seconds,
               rows_inserted, rows_updated, rows_quarantined, message
        FROM {cfg.t(LOG_TABLE)}""")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {cfg.t('v_quarantine_summary')} AS
        SELECT layer, source, reason, count(*) AS records, max(load_timestamp) AS last_seen
        FROM {cfg.t(QUARANTINE_TABLE)} GROUP BY layer, source, reason""")
