"""
Local check of the Spark transformation logic (no Databricks / Delta needed):
explicit-schema reads, schema-drift detection, quarantine, casting and validation.

    pip install pyspark==3.5.3
    python tests/test_transformations_local.py

The MERGE INTO / audit-log parts need Delta Lake and are exercised by notebooks 00-03 on Databricks.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pyspark.sql import SparkSession, functions as F  # noqa: E402

from nws_pipeline import bronze, silver  # noqa: E402
from nws_pipeline.config import SILVER_TABLES  # noqa: E402
from nws_pipeline.delta_utils import conform  # noqa: E402
from nws_pipeline.schemas import BRONZE_SCHEMAS, IEM_COLUMNS, SILVER_SCHEMAS  # noqa: E402

SAMPLES = os.path.join(os.path.dirname(__file__), "..", "samples")
spark = (SparkSession.builder.master("local[2]").appName("nws-tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
tmp = tempfile.mkdtemp()


def to_bronze(source, path):
    prep = bronze._prepare_iem if source == "iem_asos" else (lambda s, p: bronze._prepare_nws(s, source, p))
    good, bad, drift = prep(spark, path)
    quarantined = bad.count()
    if good is None:
        return None, quarantined, drift
    g = (good.withColumn("_source_file", F.lit(path)).withColumn("ingest_date", F.lit("2026-10-01").cast("date"))
         .withColumn("batch_id", F.lit("b")).withColumn("load_timestamp", F.current_timestamp()))
    return conform(g, BRONZE_SCHEMAS[source]), quarantined, drift


def to_silver(source, b):
    built = silver.BUILD[source](b)
    schema = SILVER_SCHEMAS[SILVER_TABLES[source]]
    good = conform(built.filter(F.col("_reason").isNull()).withColumn("row_hash", F.lit("h"))
                   .withColumn("load_timestamp", F.current_timestamp()), schema)
    reasons = sorted(r[0] for r in built.filter(F.col("_reason").isNotNull()).select("_reason").distinct().collect())
    return good, reasons


def test_real_samples():
    for source, rel, rows in [("alerts", "full_load/alerts/20261001T174648Z.json", 36),
                              ("forecasts", "full_load/forecasts/20261001T174655Z.json", 140),
                              ("observations", "full_load/observations/20261001T174713Z.json", 5000)]:
        b, q, drift = to_bronze(source, os.path.join(SAMPLES, rel))
        assert b.count() == rows and q == 0 and drift == [], (source, b.count(), q, drift)
        g, reasons = to_silver(source, b)
        assert g.count() == rows and reasons == [], (source, g.count(), reasons)
    print("ok  real sample files: 36 alerts, 140 forecast periods, 5000 observations -> Bronze -> Silver")


def test_type_change_and_new_field():
    f = json.load(open(os.path.join(SAMPLES, "incremental/forecasts/20261001T175215Z.json")))
    periods = list(f["grids"].values())[0]["properties"]["periods"]
    periods[0]["temperature"] = "warm"          # integer became a string
    periods[1]["brandNewField"] = 123           # source added a column
    path = os.path.join(tmp, "fc.json")
    json.dump(f, open(path, "w"))
    b, q, drift = to_bronze("forecasts", path)
    assert q == 1 and b.count() == 139 and drift == ["brandNewField"], (q, b.count(), drift)
    print("ok  type change -> 1 record quarantined, 139 loaded; new field detected and preserved")


def test_corrupt_file():
    path = os.path.join(tmp, "broken.json")
    open(path, "w").write('{"stations": {"KICT": {"features": [ <<<<<<< HEAD')
    b, q, _ = to_bronze("observations", path)
    assert b is None and q == 1
    print("ok  invalid JSON file -> whole file quarantined, pipeline continues")


def test_non_actual_alert():
    a = json.load(open(os.path.join(SAMPLES, "incremental/alerts/20261001T175211Z.json")))
    a["features"][0]["properties"]["status"] = "Test"
    path = os.path.join(tmp, "al.json")
    json.dump(a, open(path, "w"))
    b, _, _ = to_bronze("alerts", path)
    g, reasons = to_silver("alerts", b)
    assert g.count() == 1 and reasons == ["test/exercise alert (status is not Actual)"], (g.count(), reasons)
    print("ok  test alert rejected in Silver")


def test_iem_history():
    path = os.path.join(tmp, "iem.csv")
    open(path, "w").write("\n".join([",".join(IEM_COLUMNS),
        "ICT,2025-06-01 00:53,75.0,60.1,60.5,180,10,0.00,1012.3,10.0,M,M",
        "ICT,2025-06-01 01:53,M,M,M,M,M,T,M,M,M,M",
        "ICT,2025-06-01 02:53,warm,60.1,60.5,180,10,0.00,1012.3,10.0,M,M",
        "ICT,not-a-time,70,60,60,180,10,0.00,1012.3,10.0,M,M",
        "ICT,2025-06-01 04:53,200.0,60,60,180,10,0.00,1012.3,10.0,M,M", ""]))
    b, q, _ = to_bronze("iem_asos", path)
    g, reasons = to_silver("iem_asos", b)
    row = g.filter("observation_ts = '2025-06-01 00:53:00'").first()
    assert g.count() == 2 and row.station_id == "KICT" and abs(row.temperature_c - 23.889) < 0.01
    assert reasons == ["missing or unparseable observation timestamp", "non-numeric value in a numeric field",
                       "temperature outside plausible range"], reasons
    print("ok  IEM CSV: F->C conversion, M/T handling, bad timestamp / non-numeric / out-of-range rejected")


def test_iem_header_drift():
    extra = os.path.join(tmp, "extra.csv")
    open(extra, "w").write(",".join(IEM_COLUMNS) + ",newcol\nICT,2025-06-02 00:53,70,60,60,180,10,0.00,1012.3,10.0,M,M,xyz\n")
    b, q, drift = to_bronze("iem_asos", extra)
    assert b.count() == 1 and drift == ["newcol"]
    bad = os.path.join(tmp, "bad.csv")
    open(bad, "w").write("station,valid,TEMP_F\nICT,2025-06-02 00:53,70\n")
    b2, q2, _ = to_bronze("iem_asos", bad)
    assert b2 is None and q2 == 1
    print("ok  CSV with an extra column loads (kept in _raw_json); CSV with a changed header is quarantined")


if __name__ == "__main__":
    for t in (test_real_samples, test_type_change_and_new_field, test_corrupt_file,
              test_non_actual_alert, test_iem_history, test_iem_header_drift):
        t()
    print("\nALL TESTS PASSED")
