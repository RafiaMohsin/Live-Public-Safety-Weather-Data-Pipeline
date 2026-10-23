"""
Local check of the Gold star-schema transformations (no Databricks / Delta needed).

    pip install pyspark==3.5.3
    python tests/test_gold_local.py

Covers: schema conformance, deterministic surrogate keys, unique grain, referential integrity
(every fact foreign key exists in its dimension), daily aggregate maths and hash stability.
MERGE INTO itself needs Delta and is exercised by notebook 04 on Databricks.
"""
import os
import sys
import time
from datetime import datetime

os.environ["TZ"] = "UTC"      # naive datetimes in the test data are UTC, like the pipeline
time.tzset()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pyspark.sql import SparkSession, functions as F  # noqa: E402

from nws_pipeline import gold  # noqa: E402
from nws_pipeline.delta_utils import conform  # noqa: E402
from nws_pipeline.schemas import GOLD_FKS, GOLD_KEYS, GOLD_SCHEMAS, SILVER_SCHEMAS  # noqa: E402

spark = SparkSession.builder.master("local[2]").config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.shuffle.partitions", "2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
T = lambda s: datetime.fromisoformat(s)  # noqa: E731


def silver(name, rows):
    cols = [f.name for f in SILVER_SCHEMAS[name].fields]
    full = [{**dict.fromkeys(cols), **r} for r in rows]
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in full], SILVER_SCHEMAS[name])


OBS = silver("silver_observations", [
    dict(station_id="KMCI", observation_ts=T("2026-10-01 00:00"), source="iem_asos", temperature_c=10.0,
         humidity_pct=50.0, wind_speed_kmh=10.0, wind_gust_kmh=20.0, pressure_pa=101000.0,
         visibility_m=16000.0, precip_last_hour_mm=1.0, row_hash="a"),
    dict(station_id="KMCI", observation_ts=T("2026-10-01 01:00"), source="iem_asos", temperature_c=14.0,
         humidity_pct=70.0, wind_speed_kmh=30.0, wind_gust_kmh=45.0, pressure_pa=101200.0,
         visibility_m=8000.0, precip_last_hour_mm=0.5, row_hash="b"),
    dict(station_id="KMCI", observation_ts=T("2026-10-02 00:00"), source="nws_api", temperature_c=20.0, row_hash="c"),
    dict(station_id="KICT", observation_ts=T("2026-10-01 00:00"), source="nws_api", temperature_c=5.0, row_hash="d"),
])
ALERTS = silver("silver_alerts", [
    dict(alert_id="a1", sent_ts=T("2026-10-01 12:00"), effective_ts=T("2026-10-01 12:00"),
         onset_ts=T("2026-10-01 14:00"), expires_ts=T("2026-10-01 18:00"), message_type="Alert",
         category="Met", severity="Severe", certainty="Likely", urgency="Expected", event="Tornado Warning",
         affected_zones=["z1", "z2"], ugc_codes=["KSC001"], referenced_alert_ids=[], row_hash="x"),
    dict(alert_id="a2", sent_ts=T("2026-10-02 12:00"), effective_ts=T("2026-10-02 12:00"),
         message_type="Update", category="Met", severity="Weird", certainty="Likely", urgency="Expected",
         event="Tornado Warning", affected_zones=None, ugc_codes=None, referenced_alert_ids=["a1"], row_hash="y"),
])
FC = silver("silver_forecast_periods", [
    dict(location_name="Wichita", forecast_generated_at=T("2026-10-01 06:00"), period_number=1,
         start_time=T("2026-10-01 07:00"), temperature_f=68.0, row_hash="f1"),
    dict(location_name="Wichita", forecast_generated_at=T("2026-10-01 06:00"), period_number=2,
         start_time=T("2026-10-02 07:00"), temperature_f=50.0, row_hash="f2"),
])


def finish(name, df):
    return conform(gold._with_hash(df, GOLD_SCHEMAS[name]) if "row_hash" not in df.columns else df,
                   GOLD_SCHEMAS[name])


def test_all_tables_match_contract_and_grain():
    built = {
        "dim_date": gold.build_dim_date(spark, "2026-10-01", "2026-10-03"),
        "dim_station": gold.build_dim_station(OBS),
        "dim_location": gold.build_dim_location(FC),
        "dim_alert_type": gold.build_dim_alert_type(ALERTS),
        "fact_alerts": gold.build_fact_alerts(ALERTS),
        "fact_observations": gold.build_fact_observations(OBS),
        "fact_forecast_periods": gold.build_fact_forecast_periods(FC),
        "fact_daily_weather": gold.build_daily_weather(OBS, OBS),
    }
    tables = {n: finish(n, d).cache() for n, d in built.items()}
    for n, df in tables.items():
        assert [f.name for f in df.schema.fields] == [f.name for f in GOLD_SCHEMAS[n].fields], n
        total = df.count()
        assert total > 0, n
        assert df.select(*GOLD_KEYS[n]).distinct().count() == total, f"{n}: duplicate grain"
    # referential integrity: every foreign key resolves to a dimension row
    dim_keys = {"dim_date": "date_key", "dim_station": "station_key",
                "dim_location": "location_key", "dim_alert_type": "alert_type_key"}
    for fact, fks in GOLD_FKS.items():
        for col, dim in fks.items():
            dk = tables[dim].select(F.col(dim_keys[dim]).alias("_dim_key")).distinct()
            missing = (tables[fact].filter(F.col(col).isNotNull())
                       .join(dk, F.col(col) == F.col("_dim_key"), "left_anti").count())
            assert missing == 0, f"{fact}.{col} has {missing} orphan rows"
    print("ok: contract, grain and referential integrity for", len(tables), "tables")


def test_daily_aggregate_values():
    d = {(r["station_id"], r["date_key"], r["source"]): r
         for r in gold.build_daily_weather(OBS, OBS).collect()}
    r = d[("KMCI", 20261001, "iem_asos")]
    assert r["obs_count"] == 2 and r["avg_temp_c"] == 12.0 and r["max_temp_c"] == 14.0
    assert r["max_wind_gust_kmh"] == 45.0 and r["min_visibility_m"] == 8000.0 and r["precip_sum_mm"] == 1.5
    assert len(d) == 3
    print("ok: daily aggregate values")


def test_incremental_aggregate_recomputes_whole_day():
    """A late row for 2026-10-01 must correct the day using ALL its Silver rows, and touch nothing else."""
    late = OBS.filter("station_id = 'KMCI' AND observation_ts = timestamp'2026-10-01 01:00:00'")
    rows = gold.build_daily_weather(OBS, late).collect()
    assert len(rows) == 1 and rows[0]["obs_count"] == 2 and rows[0]["avg_temp_c"] == 12.0
    print("ok: incremental recompute scope")


def test_keys_and_hash_are_deterministic():
    a = finish("fact_observations", gold.build_fact_observations(OBS)).select("station_key", "observation_ts", "row_hash")
    b = finish("fact_observations", gold.build_fact_observations(OBS)).select("station_key", "observation_ts", "row_hash")
    assert a.exceptAll(b).count() == 0
    k1 = gold.build_dim_station(OBS).select("station_id", "station_key")
    k2 = gold.build_dim_station(OBS.filter("station_id = 'KMCI'")).select("station_id", "station_key")
    assert k2.exceptAll(k1.filter("station_id = 'KMCI'")).count() == 0, "surrogate key must not depend on other rows"
    h1 = finish("dim_station", gold.build_dim_station(OBS)).select("station_key", "row_hash")
    h2 = finish("dim_station", gold.build_dim_station(OBS)).select("station_key", "row_hash")
    assert h1.exceptAll(h2).count() == 0
    print("ok: deterministic keys and hashes (MERGE replay inserts/updates nothing)")


def test_alert_measures():
    r = {x["alert_id"]: x for x in gold.build_fact_alerts(ALERTS).collect()}
    assert r["a1"]["duration_hours"] == 6.0 and r["a1"]["lead_time_hours"] == 2.0
    assert r["a1"]["affected_zone_count"] == 2 and r["a1"]["is_update"] is False
    assert r["a2"]["affected_zone_count"] == 0 and r["a2"]["is_update"] is True
    ranks = {x["severity"]: x["severity_rank"] for x in gold.build_dim_alert_type(ALERTS).collect()}
    assert ranks == {"Severe": 3, "Weird": 0}
    print("ok: alert measures and severity rank")


def test_window_bounds():
    assert gold.window_bounds("full") == (None, None)
    assert gold.window_bounds("backfill", "2026-10-01", "2026-10-02") == ("2026-10-01", "2026-10-03")
    print("ok: window bounds")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
    print(f"\n{len(tests)} Gold tests passed")
