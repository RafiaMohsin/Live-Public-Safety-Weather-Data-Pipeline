"""Bronze -> Silver. Cast to strict types, validate, quarantine bad rows, MERGE INTO (upsert)."""
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

from .config import BRONZE_TABLES, SILVER_TABLES
from .delta_utils import conform, merge_into, write_quarantine
from .schemas import SILVER_KEYS, SILVER_SCHEMAS


def _ts(col):
    """String -> timestamp; unparseable values become NULL instead of failing the job (ANSI-safe)."""
    return F.expr(f"try_cast({col} AS timestamp)")


def _latest(df, keys):
    """Keep one Bronze row per business key (the most recently loaded one)."""
    w = Window.partitionBy(*keys).orderBy(F.col("load_timestamp").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _chain(*pairs):
    """First matching (condition, message) wins; NULL means the row is valid."""
    expr = None
    for cond, msg in pairs:
        expr = F.when(cond, F.lit(msg)) if expr is None else expr.when(cond, F.lit(msg))
    return expr


def _out_of_range(col, lo, hi):
    return (F.col(col) < lo) | (F.col(col) > hi)


# ---------------------------------------------------------------- alerts
def build_alerts(b):
    b = _latest(b, ["alert_id"])
    df = b.select(
        "alert_id", _ts("sent").alias("sent_ts"), _ts("effective").alias("effective_ts"),
        _ts("onset").alias("onset_ts"), _ts("expires").alias("expires_ts"), _ts("ends").alias("ends_ts"),
        "status", "message_type", "category", "severity", "certainty", "urgency", "event",
        "sender_name", "headline", "description", "instruction", "response", "area_desc",
        F.col("geocode_ugc").alias("ugc_codes"), "affected_zones", "referenced_alert_ids",
        F.col("record_key").alias("_record_key"), "_raw_json", F.col("sent").alias("_sent_raw"))
    reason = _chain(
        (F.coalesce(F.lower("status"), F.lit("")) != "actual", "test/exercise alert (status is not Actual)"),
        (F.col("_sent_raw").isNotNull() & F.col("sent_ts").isNull(), "unparseable sent timestamp"),
    )
    return df.withColumn("_reason", reason).drop("_sent_raw")


# ---------------------------------------------------------------- forecasts
def build_forecasts(b):
    b = _latest(b, ["location_name", "forecast_generated_at", "period_number"])
    ws = F.col("wind_speed")
    wmin_s = F.regexp_extract(ws, r"(\d+)", 1)
    wmax_s = F.regexp_extract(ws, r"\d+\D+(\d+)", 1)       # "5 to 8 mph" -> 8
    wmin = F.when(wmin_s != "", wmin_s).cast("double")
    wmax = F.coalesce(F.when(wmax_s != "", wmax_s).cast("double"), wmin)
    temp_f = F.when(F.upper("temperature_unit") == "C", F.col("temperature") * 9 / 5 + 32) \
              .otherwise(F.col("temperature"))
    df = b.select(
        "location_name", _ts("forecast_generated_at").alias("forecast_generated_at"), "period_number",
        F.col("period_name"), _ts("start_time").alias("start_time"), _ts("end_time").alias("end_time"),
        "is_daytime", temp_f.alias("temperature_f"), F.col("precip_probability_value").alias("precip_probability_pct"),
        wmin.alias("wind_speed_min_mph"), wmax.alias("wind_speed_max_mph"), "wind_direction",
        "short_forecast", "detailed_forecast", _ts("forecast_update_time").alias("forecast_update_time"),
        F.col("record_key").alias("_record_key"), "_raw_json")
    reason = _chain(
        (F.col("forecast_generated_at").isNull(), "unparseable forecast_generated_at"),
        (_out_of_range("temperature_f", -100, 140), "temperature outside plausible range"),
        (F.col("wind_speed_max_mph") > 250, "wind speed outside plausible range"),
    )
    return df.withColumn("_reason", reason)


# ---------------------------------------------------------------- observations
def _obs_reason():
    return _chain(
        (F.col("observation_ts").isNull(), "missing or unparseable observation timestamp"),
        (F.col("_bad_numeric"), "non-numeric value in a numeric field"),
        (_out_of_range("temperature_c", -90, 60), "temperature outside plausible range"),
        (_out_of_range("wind_speed_kmh", 0, 400), "wind speed outside plausible range"),
    )


def build_observations(b):
    """NWS API observations (already numeric, SI units)."""
    b = _latest(b, ["station_id", "observation_timestamp"])
    df = b.select(
        "station_id", _ts("observation_timestamp").alias("observation_ts"),
        F.lit("nws_api").alias("source"), "text_description",
        F.col("temperature_value").alias("temperature_c"), F.col("dewpoint_value").alias("dewpoint_c"),
        F.col("wind_direction_value").alias("wind_direction_deg"),
        F.col("wind_speed_value").alias("wind_speed_kmh"), F.col("wind_gust_value").alias("wind_gust_kmh"),
        F.col("barometric_pressure_value").alias("pressure_pa"), F.col("visibility_value").alias("visibility_m"),
        F.col("relative_humidity_value").alias("humidity_pct"),
        F.col("precipitation_last_hour_value").alias("precip_last_hour_mm"),
        F.lit(False).alias("_bad_numeric"),
        F.col("record_key").alias("_record_key"), "_raw_json")
    return df.withColumn("_reason", _obs_reason()).drop("_bad_numeric")


def _num(c):
    """IEM text -> double. 'M' / blank = missing -> NULL, 'T' = trace -> 0."""
    raw = F.trim(F.col(c))
    return (F.when(raw.isin("M", ""), F.lit(None).cast("double"))
             .when(raw == "T", F.lit(0.0))
             .otherwise(F.expr(f"try_cast(trim({c}) AS double)")))


def _num_is_bad(c):
    raw = F.trim(F.col(c))
    return raw.isNotNull() & ~raw.isin("M", "", "T") & F.expr(f"try_cast(trim({c}) AS double)").isNull()


def build_iem_asos(b):
    """IEM historical ASOS: convert F->C, knots->km/h, inches->mm, miles->m, mb->Pa."""
    b = _latest(b, ["station", "valid"])
    numeric = ["tmpf", "dwpf", "relh", "drct", "sknt", "p01i", "mslp", "vsby", "gust"]
    bad_numeric = reduce(lambda a, c: a | _num_is_bad(c), numeric[1:], _num_is_bad(numeric[0]))
    station = F.upper(F.trim("station"))
    df = b.select(
        F.when(F.length(station) == 3, F.concat(F.lit("K"), station)).otherwise(station).alias("station_id"),
        F.expr("try_to_timestamp(valid, 'yyyy-MM-dd HH:mm')").alias("observation_ts"),
        F.lit("iem_asos").alias("source"),
        F.when(F.trim("wxcodes").isin("M", ""), F.lit(None).cast("string")).otherwise(F.col("wxcodes")).alias("text_description"),
        ((_num("tmpf") - 32) * 5 / 9).alias("temperature_c"),
        ((_num("dwpf") - 32) * 5 / 9).alias("dewpoint_c"),
        _num("drct").alias("wind_direction_deg"),
        (_num("sknt") * 1.852).alias("wind_speed_kmh"),
        (_num("gust") * 1.852).alias("wind_gust_kmh"),
        (_num("mslp") * 100).alias("pressure_pa"),
        (_num("vsby") * 1609.344).alias("visibility_m"),
        _num("relh").alias("humidity_pct"),
        (_num("p01i") * 25.4).alias("precip_last_hour_mm"),
        bad_numeric.alias("_bad_numeric"),
        F.col("record_key").alias("_record_key"), "_raw_json")
    return df.withColumn("_reason", _obs_reason()).drop("_bad_numeric")


BUILD = {"alerts": build_alerts, "forecasts": build_forecasts,
         "observations": build_observations, "iem_asos": build_iem_asos}


# ---------------------------------------------------------------- public entry point
def process_table(spark, cfg, source, start_date, end_date, batch_id, run_batch, audit):
    """Bronze -> Silver for one source and one ingest_date window (and optional Bronze batch_id)."""
    table = SILVER_TABLES[source]
    schema = SILVER_SCHEMAS[table]
    b = spark.table(cfg.t(BRONZE_TABLES[source])).filter(F.col("ingest_date").between(start_date, end_date))
    if batch_id:
        b = b.filter(F.col("batch_id") == batch_id)
    built = BUILD[source](b)

    data_cols = [f.name for f in schema.fields if f.name not in ("row_hash", "load_timestamp")]
    row_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in data_cols]), 256)
    good = (built.filter(F.col("_reason").isNull())
                 .withColumn("row_hash", row_hash).withColumn("load_timestamp", F.current_timestamp()))
    good = conform(good, schema)
    bad = built.filter(F.col("_reason").isNotNull()).select(
        F.col("_record_key").alias("record_key"), F.col("_reason").alias("reason"),
        F.col("_raw_json").alias("raw_payload"))

    audit.rows_quarantined = write_quarantine(spark, cfg, bad, "bronze_to_silver", source, run_batch)
    audit.rows_inserted, audit.rows_updated = merge_into(
        spark, cfg.t(table), good, SILVER_KEYS[table], hash_col="row_hash")
