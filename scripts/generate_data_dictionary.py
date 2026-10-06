"""Generate docs/data_dictionary.md from the explicit schemas (single source of truth).

    python scripts/generate_data_dictionary.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from nws_pipeline.config import BRONZE_TABLES, LOG_TABLE, QUARANTINE_TABLE  # noqa: E402
from nws_pipeline.schemas import (BRONZE_SCHEMAS, LOG_SCHEMA, QUARANTINE_KEYS,  # noqa: E402
                                  QUARANTINE_SCHEMA, SILVER_KEYS, SILVER_SCHEMAS)

NOTES = {
    "record_key": "SHA-256 of source + business key; identifies the record inside a file",
    "_raw_json": "Original record as received (keeps fields the schema does not know about)",
    "_source_file": "Raw file the record came from",
    "ingest_date": "Date taken from the raw file name (used for backfill windows)",
    "batch_id": "Label of the pipeline run that loaded the record",
    "load_timestamp": "When this record was ingested / processed (UTC)",
    "row_hash": "SHA-256 of the business columns; MERGE updates a row only if it changed",
    "alert_id": "NWS alert identifier (urn:oid:...)",
    "referenced_alert_ids": "Earlier alerts this alert updates (used to resolve update chains)",
    "forecast_generated_at": "When NWS generated the forecast; identifies a forecast version",
    "location_name": "Project location (one of 10 Kansas grid points)",
    "temperature_f": "Forecast temperature in Fahrenheit (converted if source unit was C)",
    "wind_speed_min_mph": "Lower wind speed parsed from text such as '5 to 8 mph'",
    "wind_speed_max_mph": "Upper wind speed parsed from text such as '5 to 8 mph'",
    "observation_ts": "Time of the observation (UTC)",
    "source": "nws_api (live) or iem_asos (historical baseline)",
    "temperature_c": "Degrees Celsius (IEM Fahrenheit values converted)",
    "wind_speed_kmh": "km/h (IEM knots converted)",
    "pressure_pa": "Pascal (IEM millibar converted)",
    "visibility_m": "Metres (IEM miles converted)",
    "precip_last_hour_mm": "Millimetres (IEM inches converted; trace = 0)",
    "reason": "Why the record was rejected",
    "raw_payload": "The rejected record (or file content) as received",
    "rows_inserted": "Rows inserted by this step",
    "rows_updated": "Rows updated by this step",
    "rows_quarantined": "Rows rejected into quarantine_records by this step",
    "parameter": "File path (raw_to_bronze) or date window (bronze_to_silver) that was processed",
    "layer": "raw_to_bronze or bronze_to_silver",
}
BRONZE_DESC = {
    "alerts": "Raw NWS alerts, one row per alert message and source file",
    "forecasts": "Raw NWS forecast periods, one row per grid, forecast version and period",
    "observations": "Raw NWS station observations, one row per station and timestamp",
    "iem_asos": "Raw IEM historical ASOS observations (all values kept as text)",
}


def table_md(title, desc, schema, pk):
    lines = [f"### `{title}`", desc, "", f"**Primary key:** {pk}", "",
             "| Column | Type | Description |", "|---|---|---|"]
    for f in schema.fields:
        lines.append(f"| `{f.name}` | {f.dataType.simpleString()} | {NOTES.get(f.name, '')} |")
    return "\n".join(lines) + "\n"


def build():
    out = ["## Data dictionary\n", "Generated from `nws_pipeline/schemas.py` by `scripts/generate_data_dictionary.py`.\n",
           "### Bronze layer\nRaw values with minimal typing plus metadata columns. "
           "Every table has `load_timestamp`.\n"]
    for src, name in BRONZE_TABLES.items():
        out.append(table_md(name, BRONZE_DESC[src], BRONZE_SCHEMAS[src], "`record_key` + `_source_file`"))
    out.append("### Silver layer\nStrictly typed, validated, one row per business key. Every table has `load_timestamp`.\n")
    desc = {"silver_alerts": "Cleaned NWS alerts; test/exercise alerts are rejected",
            "silver_forecast_periods": "Forecast periods per forecast version; units standardised",
            "silver_observations": "Observations from the live NWS API and the IEM historical baseline in SI units"}
    for name, schema in SILVER_SCHEMAS.items():
        pk = " + ".join(f"`{k}`" for k in SILVER_KEYS[name])
        out.append(table_md(name, desc[name], schema, pk))
    out.append("### Operational tables\n")
    out.append(table_md(LOG_TABLE, "One row per file (raw_to_bronze) or table run (bronze_to_silver)", LOG_SCHEMA,
                        "none (append-only log)"))
    out.append(table_md(QUARANTINE_TABLE, "Records rejected by schema, type or business-rule checks", QUARANTINE_SCHEMA,
                        " + ".join(f"`{k}`" for k in QUARANTINE_KEYS)))
    return "\n".join(out)


if __name__ == "__main__":
    path = os.path.join(os.path.dirname(__file__), "..", "docs", "data_dictionary.md")
    open(path, "w", encoding="utf-8").write(build())
    print("written", os.path.abspath(path))
