"""
Explicit schemas (StructType / StructField). Nothing in this project uses inferSchema.

RAW_*      : schemas used when reading raw files (schema-on-read)
BRONZE_*   : Bronze table contracts (raw values, mostly strings, plus metadata)
SILVER_*   : Silver table contracts (strictly typed)
"""
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DoubleType, IntegerType, MapType,
    StringType, StructField, StructType, TimestampType,
)

STR, DBL, INT, BOOL, TS, DATE = (StringType(), DoubleType(), IntegerType(),
                                 BooleanType(), TimestampType(), DateType())


def fld(name, dtype):
    return StructField(name, dtype, True)


CORRUPT = fld("_corrupt_record", STR)
QV = StructType([fld("value", DBL)])          # NWS "quantitative value": {unitCode, value, qualityControl}

# ----------------------------------------------------------------------------------------------
# RAW (schema-on-read). The big nested payloads are read as strings first (so one bad record
# cannot fail a whole file) and then parsed record by record with the typed schemas below.
# ----------------------------------------------------------------------------------------------
RAW_TOP_SCHEMAS = {
    "alerts": StructType([fld("features", ArrayType(STR)), CORRUPT]),
    "forecasts": StructType([fld("ingestion_time", STR), fld("grids", MapType(STR, STR)), CORRUPT]),
    "observations": StructType([fld("ingestion_time", STR), fld("start", STR),
                                fld("stations", MapType(STR, STR)), CORRUPT]),
}

ALERT_PROPS = StructType([
    fld("id", STR), fld("areaDesc", STR),
    fld("geocode", StructType([fld("SAME", ArrayType(STR)), fld("UGC", ArrayType(STR))])),
    fld("affectedZones", ArrayType(STR)),
    fld("references", ArrayType(StructType([fld("identifier", STR)]))),
    fld("sent", STR), fld("effective", STR), fld("onset", STR), fld("expires", STR), fld("ends", STR),
    fld("status", STR), fld("messageType", STR), fld("category", STR), fld("severity", STR),
    fld("certainty", STR), fld("urgency", STR), fld("event", STR), fld("senderName", STR),
    fld("headline", STR), fld("description", STR), fld("instruction", STR), fld("response", STR),
    CORRUPT,
])

FORECAST_PERIOD = StructType([
    fld("number", INT), fld("name", STR), fld("startTime", STR), fld("endTime", STR),
    fld("isDaytime", BOOL), fld("temperature", DBL), fld("temperatureUnit", STR),
    fld("probabilityOfPrecipitation", QV), fld("windSpeed", STR), fld("windDirection", STR),
    fld("shortForecast", STR), fld("detailedForecast", STR), CORRUPT,
])

OBS_PROPS = StructType([
    fld("timestamp", STR), fld("textDescription", STR),
    fld("temperature", QV), fld("dewpoint", QV), fld("windDirection", QV), fld("windSpeed", QV),
    fld("windGust", QV), fld("barometricPressure", QV), fld("visibility", QV),
    fld("relativeHumidity", QV), fld("precipitationLastHour", QV), CORRUPT,
])

RECORD_SCHEMAS = {"alerts": ALERT_PROPS, "forecasts": FORECAST_PERIOD, "observations": OBS_PROPS}

# Every field the NWS API is known to return per record. A field outside these sets is "schema drift":
# it is preserved in _raw_json and reported in the audit log instead of crashing the batch.
KNOWN_FIELDS = {
    "alerts": {"@id", "@type", "id", "areaDesc", "geocode", "affectedZones", "references", "sent",
               "effective", "onset", "expires", "ends", "status", "messageType", "category",
               "severity", "certainty", "urgency", "event", "sender", "senderName", "headline",
               "description", "instruction", "response", "parameters", "replacedBy", "replacedAt",
               "code", "eventCode", "language", "note", "scope", "web"},
    "forecasts": {"number", "name", "startTime", "endTime", "isDaytime", "temperature",
                  "temperatureUnit", "temperatureTrend", "probabilityOfPrecipitation", "dewpoint",
                  "relativeHumidity", "windSpeed", "windDirection", "icon", "shortForecast",
                  "detailedForecast"},
    "observations": {"@id", "@type", "elevation", "station", "stationId", "stationName", "timestamp",
                     "rawMessage", "textDescription", "icon", "presentWeather", "temperature",
                     "dewpoint", "windDirection", "windSpeed", "windGust", "barometricPressure",
                     "seaLevelPressure", "visibility", "maxTemperatureLast24Hours",
                     "minTemperatureLast24Hours", "precipitationLastHour", "precipitationLast3Hours",
                     "precipitationLast6Hours", "relativeHumidity", "windChill", "heatIndex",
                     "cloudLayers"},
}

# IEM ASOS historical CSV: the downloader script writes exactly these columns in this order.
IEM_COLUMNS = ["station", "valid", "tmpf", "dwpf", "relh", "drct", "sknt", "p01i", "mslp",
               "vsby", "gust", "wxcodes"]
IEM_SCHEMA = StructType([fld(c, STR) for c in IEM_COLUMNS] + [CORRUPT])

# ----------------------------------------------------------------------------------------------
# BRONZE tables (raw values + metadata). Primary key: (record_key, _source_file)
# ----------------------------------------------------------------------------------------------
_TAIL = [fld("record_key", STR), fld("_raw_json", STR), fld("_source_file", STR),
         fld("ingest_date", DATE), fld("batch_id", STR), fld("load_timestamp", TS)]

BRONZE_SCHEMAS = {
    "alerts": StructType([
        fld("alert_id", STR), fld("area_desc", STR), fld("geocode_ugc", ArrayType(STR)),
        fld("geocode_same", ArrayType(STR)), fld("affected_zones", ArrayType(STR)),
        fld("referenced_alert_ids", ArrayType(STR)),
        fld("sent", STR), fld("effective", STR), fld("onset", STR), fld("expires", STR), fld("ends", STR),
        fld("status", STR), fld("message_type", STR), fld("category", STR), fld("severity", STR),
        fld("certainty", STR), fld("urgency", STR), fld("event", STR), fld("sender_name", STR),
        fld("headline", STR), fld("description", STR), fld("instruction", STR), fld("response", STR),
    ] + _TAIL),
    "forecasts": StructType([
        fld("location_name", STR), fld("forecast_generated_at", STR), fld("forecast_update_time", STR),
        fld("source_ingestion_time", STR), fld("period_number", INT), fld("period_name", STR),
        fld("start_time", STR), fld("end_time", STR), fld("is_daytime", BOOL), fld("temperature", DBL),
        fld("temperature_unit", STR), fld("precip_probability_value", DBL), fld("wind_speed", STR),
        fld("wind_direction", STR), fld("short_forecast", STR), fld("detailed_forecast", STR),
    ] + _TAIL),
    "observations": StructType([
        fld("station_id", STR), fld("observation_timestamp", STR), fld("text_description", STR),
        fld("temperature_value", DBL), fld("dewpoint_value", DBL), fld("wind_direction_value", DBL),
        fld("wind_speed_value", DBL), fld("wind_gust_value", DBL), fld("barometric_pressure_value", DBL),
        fld("visibility_value", DBL), fld("relative_humidity_value", DBL),
        fld("precipitation_last_hour_value", DBL),
    ] + _TAIL),
    "iem_asos": StructType([fld(c, STR) for c in IEM_COLUMNS] + _TAIL),
}

# ----------------------------------------------------------------------------------------------
# SILVER tables (typed, validated). row_hash lets MERGE skip rows that did not change.
# ----------------------------------------------------------------------------------------------
_S_TAIL = [fld("row_hash", STR), fld("load_timestamp", TS)]

SILVER_SCHEMAS = {
    "silver_alerts": StructType([
        fld("alert_id", STR), fld("sent_ts", TS), fld("effective_ts", TS), fld("onset_ts", TS),
        fld("expires_ts", TS), fld("ends_ts", TS), fld("status", STR), fld("message_type", STR),
        fld("category", STR), fld("severity", STR), fld("certainty", STR), fld("urgency", STR),
        fld("event", STR), fld("sender_name", STR), fld("headline", STR), fld("description", STR),
        fld("instruction", STR), fld("response", STR), fld("area_desc", STR),
        fld("ugc_codes", ArrayType(STR)), fld("affected_zones", ArrayType(STR)),
        fld("referenced_alert_ids", ArrayType(STR)),
    ] + _S_TAIL),
    "silver_forecast_periods": StructType([
        fld("location_name", STR), fld("forecast_generated_at", TS), fld("period_number", INT),
        fld("period_name", STR), fld("start_time", TS), fld("end_time", TS), fld("is_daytime", BOOL),
        fld("temperature_f", DBL), fld("precip_probability_pct", DBL), fld("wind_speed_min_mph", DBL),
        fld("wind_speed_max_mph", DBL), fld("wind_direction", STR), fld("short_forecast", STR),
        fld("detailed_forecast", STR), fld("forecast_update_time", TS),
    ] + _S_TAIL),
    "silver_observations": StructType([
        fld("station_id", STR), fld("observation_ts", TS), fld("source", STR),
        fld("text_description", STR), fld("temperature_c", DBL), fld("dewpoint_c", DBL),
        fld("wind_direction_deg", DBL), fld("wind_speed_kmh", DBL), fld("wind_gust_kmh", DBL),
        fld("pressure_pa", DBL), fld("visibility_m", DBL), fld("humidity_pct", DBL),
        fld("precip_last_hour_mm", DBL),
    ] + _S_TAIL),
}
SILVER_KEYS = {
    "silver_alerts": ["alert_id"],
    "silver_forecast_periods": ["location_name", "forecast_generated_at", "period_number"],
    "silver_observations": ["station_id", "observation_ts", "source"],
}

# ----------------------------------------------------------------------------------------------
# Operational tables
# ----------------------------------------------------------------------------------------------
LOG_SCHEMA = StructType([
    fld("run_id", STR), fld("batch_id", STR), fld("layer", STR), fld("source", STR),
    fld("load_type", STR), fld("parameter", STR), fld("start_time", TS), fld("end_time", TS),
    fld("status", STR), fld("rows_inserted", INT), fld("rows_updated", INT),
    fld("rows_quarantined", INT), fld("message", STR),
])
QUARANTINE_SCHEMA = StructType([
    fld("layer", STR), fld("source", STR), fld("record_key", STR), fld("reason", STR),
    fld("raw_payload", STR), fld("batch_id", STR), fld("load_timestamp", TS),
])
QUARANTINE_KEYS = ["layer", "source", "record_key", "reason"]
