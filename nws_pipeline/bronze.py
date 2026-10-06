"""Raw -> Bronze. One raw file at a time: explicit-schema read, quarantine bad records, MERGE the rest."""
import hashlib

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructField, StructType

from .config import BRONZE_TABLES
from .delta_utils import conform, merge_into, write_quarantine
from .schemas import (BRONZE_SCHEMAS, CORRUPT, IEM_COLUMNS, KNOWN_FIELDS, RAW_TOP_SCHEMAS,
                      RECORD_SCHEMAS, STR, fld)

PERMISSIVE = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
_BAD_SCHEMA = StructType([fld("record_key", STR), fld("reason", STR), fld("raw_payload", STR)])


# ---------------------------------------------------------------- extraction (raw file -> one row per record)
def _extract_alerts(top):
    return (top.select(F.explode_outer("features").alias("f"))
               .filter(F.col("f").isNotNull())
               .select(F.get_json_object("f", "$.properties").alias("_record_json")))


def _extract_forecasts(top):
    g = top.select("ingestion_time", F.explode("grids").alias("location_name", "fjson"))
    periods = F.from_json(F.get_json_object("fjson", "$.properties.periods"), ArrayType(STR))
    return g.select(
        "location_name", "ingestion_time",
        F.get_json_object("fjson", "$.properties.generatedAt").alias("forecast_generated_at"),
        F.get_json_object("fjson", "$.properties.updateTime").alias("forecast_update_time"),
        F.explode(periods).alias("_record_json"))


_FEATS = StructType([fld("features", ArrayType(STR))])


def _extract_observations(top):
    s = top.select(F.explode("stations").alias("station_id", "fjson"))
    feats = F.from_json("fjson", _FEATS)["features"]
    return (s.select("station_id", F.explode_outer(feats).alias("feat"))
             .filter(F.col("feat").isNotNull())
             .select("station_id", F.get_json_object("feat", "$.properties").alias("_record_json")))


EXTRACT = {"alerts": _extract_alerts, "forecasts": _extract_forecasts, "observations": _extract_observations}


# ---------------------------------------------------------------- shaping (typed record -> Bronze columns)
def _with_key(df, source, key_cols):
    ok = F.lit(True)
    for c in key_cols:
        ok = ok & F.col(c).isNotNull()
    key = F.sha2(F.concat_ws("|", F.lit(source), *[F.col(c).cast("string") for c in key_cols],
                             F.when(F.col("_corrupt_record").isNotNull(), F.col("_raw_json"))), 256)
    return df.withColumn("_key_ok", ok).withColumn("record_key", key)


def _shape_alerts(df):
    r = F.col("r")
    out = df.select(
        r["id"].alias("alert_id"), r["areaDesc"].alias("area_desc"),
        r["geocode"]["UGC"].alias("geocode_ugc"), r["geocode"]["SAME"].alias("geocode_same"),
        r["affectedZones"].alias("affected_zones"),
        F.transform(r["references"], lambda x: x["identifier"]).alias("referenced_alert_ids"),
        r["sent"].alias("sent"), r["effective"].alias("effective"), r["onset"].alias("onset"),
        r["expires"].alias("expires"), r["ends"].alias("ends"), r["status"].alias("status"),
        r["messageType"].alias("message_type"), r["category"].alias("category"),
        r["severity"].alias("severity"), r["certainty"].alias("certainty"),
        r["urgency"].alias("urgency"), r["event"].alias("event"),
        r["senderName"].alias("sender_name"), r["headline"].alias("headline"),
        r["description"].alias("description"), r["instruction"].alias("instruction"),
        r["response"].alias("response"),
        r["_corrupt_record"].alias("_corrupt_record"), F.col("_record_json").alias("_raw_json"))
    return _with_key(out, "alerts", ["alert_id"])


def _shape_forecasts(df):
    r = F.col("r")
    out = df.select(
        "location_name", "forecast_generated_at", "forecast_update_time",
        F.col("ingestion_time").alias("source_ingestion_time"),
        r["number"].alias("period_number"), r["name"].alias("period_name"),
        r["startTime"].alias("start_time"), r["endTime"].alias("end_time"),
        r["isDaytime"].alias("is_daytime"), r["temperature"].alias("temperature"),
        r["temperatureUnit"].alias("temperature_unit"),
        r["probabilityOfPrecipitation"]["value"].alias("precip_probability_value"),
        r["windSpeed"].alias("wind_speed"), r["windDirection"].alias("wind_direction"),
        r["shortForecast"].alias("short_forecast"), r["detailedForecast"].alias("detailed_forecast"),
        r["_corrupt_record"].alias("_corrupt_record"), F.col("_record_json").alias("_raw_json"))
    return _with_key(out, "forecasts", ["location_name", "forecast_generated_at", "period_number"])


def _shape_observations(df):
    r = F.col("r")
    out = df.select(
        "station_id", r["timestamp"].alias("observation_timestamp"),
        r["textDescription"].alias("text_description"),
        r["temperature"]["value"].alias("temperature_value"),
        r["dewpoint"]["value"].alias("dewpoint_value"),
        r["windDirection"]["value"].alias("wind_direction_value"),
        r["windSpeed"]["value"].alias("wind_speed_value"),
        r["windGust"]["value"].alias("wind_gust_value"),
        r["barometricPressure"]["value"].alias("barometric_pressure_value"),
        r["visibility"]["value"].alias("visibility_value"),
        r["relativeHumidity"]["value"].alias("relative_humidity_value"),
        r["precipitationLastHour"]["value"].alias("precipitation_last_hour_value"),
        r["_corrupt_record"].alias("_corrupt_record"), F.col("_record_json").alias("_raw_json"))
    return _with_key(out, "observations", ["station_id", "observation_timestamp"])


SHAPE = {"alerts": _shape_alerts, "forecasts": _shape_forecasts, "observations": _shape_observations}


# ---------------------------------------------------------------- per-source preparation
def _file_quarantine(spark, path, reason, payload=None):
    key = hashlib.sha256(path.encode()).hexdigest()
    return spark.createDataFrame([(key, reason, payload)], _BAD_SCHEMA)


def _split(shaped):
    """Split shaped records into (good, bad). Bad = type mismatch / malformed, or missing key."""
    ok = F.col("_corrupt_record").isNull() & F.col("_key_ok")
    good = shaped.filter(ok)
    reason = F.when(F.col("_corrupt_record").isNotNull(),
                    F.lit("type mismatch or malformed record")).otherwise(F.lit("missing required key field"))
    bad = shaped.filter(~ok).select(
        "record_key", reason.alias("reason"),
        F.coalesce(F.col("_raw_json"), F.col("_corrupt_record")).alias("raw_payload"))
    return good, bad


def _prepare_nws(spark, source, path):
    top = (spark.read.schema(RAW_TOP_SCHEMAS[source]).option("multiLine", True)
           .options(**PERMISSIVE).json(path))
    top = top.select("_corrupt_record", *[c for c in top.columns if c != "_corrupt_record"])
    corrupt = top.filter(F.col("_corrupt_record").isNotNull()).limit(1).collect()
    if corrupt:
        return None, _file_quarantine(spark, path, "unparseable or non-conforming file",
                                      corrupt[0]["_corrupt_record"]), []
    records = EXTRACT[source](top)
    known = F.array(*[F.lit(k) for k in sorted(KNOWN_FIELDS[source])])
    keys = F.map_keys(F.from_json("_record_json", MapType(STR, STR)))
    new_fields = sorted(r[0] for r in records.select(
        F.explode(F.array_except(keys, known)).alias("k")).distinct().collect())
    typed = records.withColumn("r", F.from_json("_record_json", RECORD_SCHEMAS[source], PERMISSIVE))
    good, bad = _split(SHAPE[source](typed))
    return good, bad, new_fields


def _prepare_iem(spark, path):
    with open(path, encoding="utf-8") as fh:
        header = fh.readline().strip().split(",")
    n = len(IEM_COLUMNS)
    if header[:n] != IEM_COLUMNS:
        return None, _file_quarantine(
            spark, path, f"schema drift: header does not start with expected columns {IEM_COLUMNS}",
            ",".join(header)), []
    extra = header[n:]            # columns appended by the source: read as strings, keep in _raw_json
    schema = StructType([fld(c, STR) for c in IEM_COLUMNS + extra] + [CORRUPT])
    df = (spark.read.schema(schema).option("header", True).options(**PERMISSIVE).csv(path))
    shaped = df.select(*IEM_COLUMNS, "_corrupt_record",
                       F.to_json(F.struct(*[F.col(c) for c in IEM_COLUMNS + extra])).alias("_raw_json"))
    good, bad = _split(_with_key(shaped, "iem_asos", ["station", "valid"]))
    return good, bad, extra


# ---------------------------------------------------------------- public entry point
def process_file(spark, cfg, source, path, file_date, batch_id, audit):
    """Load ONE raw file into its Bronze table. Safe to re-run: MERGE inserts nothing the second time."""
    prepare = _prepare_iem if source == "iem_asos" else (lambda s, p: _prepare_nws(s, source, p))
    good, bad, new_fields = prepare(spark, path)

    audit.rows_quarantined = write_quarantine(spark, cfg, bad, "raw_to_bronze", source, batch_id)
    notes = []
    if new_fields:
        notes.append(f"schema drift: new fields kept in _raw_json: {new_fields}")
    if good is None:
        notes.append("whole file quarantined")
    else:
        good = (good.withColumn("_source_file", F.lit(path))
                    .withColumn("ingest_date", F.lit(file_date).cast("date"))
                    .withColumn("batch_id", F.lit(batch_id))
                    .withColumn("load_timestamp", F.current_timestamp()))
        good = conform(good, BRONZE_SCHEMAS[source]).dropDuplicates(["record_key", "_source_file"])
        audit.rows_inserted, audit.rows_updated = merge_into(
            spark, cfg.t(BRONZE_TABLES[source]), good, ["record_key", "_source_file"])
    audit.message = "; ".join(notes)
