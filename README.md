# Live Public-Safety Weather Data Pipeline (NWS API)

Data Analysis and Visualisation semester project. An automated Spark pipeline on Databricks using the
Medallion Architecture (Bronze -> Silver -> Gold) with a Power BI dashboard.

**Team:** Rafia Mohsin, Shanzay Shahid
**Course:** Data Analysis and Visualization
**Phase 1 proposal:** `docs/NWS_Phase1_Proposal_Updated.docx`; **Phase 2:** Bronze + Silver (this repository)

## Project summary
We ingest live weather forecasts, hazardous-weather alerts and station observations for Kansas
(10 grid points, 10 stations) from the U.S. National Weather Service, plus a historical observation baseline.
The goal is to answer: where is weather risk concentrated, how reliable are forecasts at different lead times,
and could forecast data warn of floods before an official alert.

## Data sources
| Source | Used for | Format |
|---|---|---|
| NWS API `https://api.weather.gov` (public domain, no key, User-Agent required) | **Incremental** loads: alerts, forecasts, observations | JSON |
| Iowa Environmental Mesonet ASOS archive (`mesonet.agron.iastate.edu/cgi-bin/request/asos.py`) | **Historical full-load baseline** for observations (same airport stations) | CSV |

Forecasts have no public history, so they are collected live only. Samples are in `samples/` (full and incremental).

## Architecture (Phase 2)
```
raw files (Volume)                Bronze (Delta)                 Silver (Delta)
full/{alerts,forecasts,observations}/*.json  --\
incremental/{alerts,forecasts,observations}/*.json -> bronze_alerts / bronze_forecast_periods / bronze_observations -> silver_alerts / silver_forecast_periods
history/iem_asos/*.csv  ------------------------> bronze_iem_asos -----------------------------------------------> silver_observations (both sources)
                                   \__ quarantine_records (rejected records)      pipeline_execution_logs (every file / table run)
```
How each Phase 2 requirement is met:

| Requirement | Implementation |
|---|---|
| Strict schema-on-read | Every read uses an explicit `StructType` from `nws_pipeline/schemas.py`; no `inferSchema` anywhere |
| Types and casting | Bronze keeps raw values; Silver casts timestamps (`try_cast`), converts units and parses wind text |
| `load_timestamp` everywhere | Present in every Bronze, Silver and quarantine table |
| Idempotency | `MERGE INTO` in Bronze (key `record_key` + `_source_file`) and Silver (business key, `row_hash` skips unchanged rows) |
| Parameterised backfills | `mode`, `start_date`, `end_date`, `sources`, `load_type`, `lookback_days`, `bronze_batch_id` notebook parameters; no hardcoded paths or dates |
| Schema drift | New source fields are detected, logged and preserved in `_raw_json`; type changes, malformed records and unparseable files go to `quarantine_records` without stopping the batch; `autoMerge` is enabled for Delta schema evolution |
| Audit logging | `pipeline_execution_logs`: layer, file/parameter, start/end time, status, rows inserted / updated / quarantined, for every file and table |
| Fault tolerance | A failing file is logged as `Failure` and the run continues with the next file |

## Repository structure
```
.
├── README.md
├── collect_nws.py                 # live NWS collector (full / incremental), runs locally
├── fix_conflicts.py               # helper: removes git conflict markers from JSON files
├── requirements.txt
├── nws_pipeline/                  # PySpark code
│   ├── config.py                  # sources, table names, parameter + file-window helpers
│   ├── schemas.py                 # ALL explicit StructType schemas (raw, Bronze, Silver, ops)
│   ├── bronze.py                  # raw -> Bronze
│   ├── silver.py                  # Bronze -> Silver
│   ├── delta_utils.py             # MERGE INTO, table creation, quarantine
│   ├── audit.py                   # execution logging
│   └── pipeline.py                # orchestration entry points
├── notebooks/                     # Databricks notebooks (import the package above)
│   ├── 00_setup.py   01_raw_to_bronze.py   02_bronze_to_silver.py   03_demo_idempotency_and_audit.py
├── scripts/
│   ├── download_iem_asos.py       # historical baseline downloader
│   └── generate_data_dictionary.py
├── tests/test_transformations_local.py
├── docs/                          # proposal, data_dictionary.md
└── data_raw/{full_load,incremental}/{alerts,forecasts,observations}/
```

## Execution guide

### One-time setup (Databricks)
1. Workspace -> Create -> **Git folder**, paste this repository's URL.
2. Run `notebooks/00_setup` (creates schema `workspace.nws`, Volume `raw`, and all tables from the explicit schemas).
3. Upload raw files to the Volume `/Volumes/workspace/nws/raw/`:
   - `samples/full_load/<type>/*.json` -> `raw/full/<type>/`
   - `samples/incremental/<type>/*.json` -> `raw/incremental/<type>/`
   - IEM history: run `python scripts/download_iem_asos.py --start 2024-10-01 --end 2026-10-01` on your computer, then upload the CSVs to `raw/history/iem_asos/`.
   - Keep the timestamp prefix in each file name (`YYYYMMDDTHHMMSSZ...`): it defines the file's date.

### Standard incremental load
Run `01_raw_to_bronze` then `02_bronze_to_silver` with the defaults: `mode = incremental`, `lookback_days = 1`.
It processes files dated today and yesterday. Because loads are idempotent, overlapping windows are safe.

### Backfill (any historical timeframe)
Set `mode = backfill` plus the window, for example:

| Goal | Parameters |
|---|---|
| Reload everything from 1 Oct 2026 | `mode=backfill`, `start_date=2026-10-01`, `end_date=2026-10-01` |
| Reload only observations for a month | add `sources=observations` (and `load_type=full`) |
| Load the historical IEM files | `sources=iem_asos`, `load_type=history`, `mode=backfill` with a window that covers the download date |
| Re-run Silver for one Bronze batch | notebook 02: `bronze_batch_id=<batch id from pipeline_execution_logs>` |

### Prove idempotency
Run `notebooks/03_demo_idempotency_and_audit`: it re-runs both layers on the same data, asserts that no table
changed, and shows the audit log (rows_inserted and rows_updated are 0 on the re-run).

### Collecting live data (incremental source)
```bash
pip install -r requirements.txt
python collect_nws.py full           # baseline snapshot
python collect_nws.py incremental    # run hourly; writes data_raw/incremental/...
```
Edit the User-Agent email in `collect_nws.py` and `scripts/download_iem_asos.py` first. Upload new files to the Volume,
then run the notebooks.

### Local tests (no Databricks needed)
`pip install pyspark==3.5.3 && python tests/test_transformations_local.py` checks explicit-schema reads, drift detection,
quarantine, casting and validation on the sample files.

## Known limitations
- The NWS observations endpoint returns at most 500 records per station per request (about 1.5 days); the IEM archive provides the historical baseline instead.
- Alert update chains (`referenced_alert_ids`) are stored in Silver; grouping them into events is part of the Gold layer (Phase 3).
- The Gold layer, dashboard and a scheduled Databricks job are Phase 3.

## Security and PII
No PII. The data is public-domain government weather data with no personal records; the alert `sender` field is a shared
agency mailbox, not a person. Free-text alert fields are kept for reference only.

## Infrastructure and FinOps
Databricks Free Edition (serverless), manual or scheduled runs only, one state, small files, no cluster left running.
Estimated volume: about 14 MB/day incremental, a few tens of MB for the historical baseline.

## Data dictionary

Generated from `nws_pipeline/schemas.py` by `scripts/generate_data_dictionary.py`.

### Bronze layer
Raw values with minimal typing plus metadata columns. Every table has `load_timestamp`.

### `bronze_alerts`
Raw NWS alerts, one row per alert message and source file

**Primary key:** `record_key` + `_source_file`

| Column | Type | Description |
|---|---|---|
| `alert_id` | string | NWS alert identifier (urn:oid:...) |
| `area_desc` | string |  |
| `geocode_ugc` | array<string> |  |
| `geocode_same` | array<string> |  |
| `affected_zones` | array<string> |  |
| `referenced_alert_ids` | array<string> | Earlier alerts this alert updates (used to resolve update chains) |
| `sent` | string |  |
| `effective` | string |  |
| `onset` | string |  |
| `expires` | string |  |
| `ends` | string |  |
| `status` | string |  |
| `message_type` | string |  |
| `category` | string |  |
| `severity` | string |  |
| `certainty` | string |  |
| `urgency` | string |  |
| `event` | string |  |
| `sender_name` | string |  |
| `headline` | string |  |
| `description` | string |  |
| `instruction` | string |  |
| `response` | string |  |
| `record_key` | string | SHA-256 of source + business key; identifies the record inside a file |
| `_raw_json` | string | Original record as received (keeps fields the schema does not know about) |
| `_source_file` | string | Raw file the record came from |
| `ingest_date` | date | Date taken from the raw file name (used for backfill windows) |
| `batch_id` | string | Label of the pipeline run that loaded the record |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `bronze_forecast_periods`
Raw NWS forecast periods, one row per grid, forecast version and period

**Primary key:** `record_key` + `_source_file`

| Column | Type | Description |
|---|---|---|
| `location_name` | string | Project location (one of 10 Kansas grid points) |
| `forecast_generated_at` | string | When NWS generated the forecast; identifies a forecast version |
| `forecast_update_time` | string |  |
| `source_ingestion_time` | string |  |
| `period_number` | int |  |
| `period_name` | string |  |
| `start_time` | string |  |
| `end_time` | string |  |
| `is_daytime` | boolean |  |
| `temperature` | double |  |
| `temperature_unit` | string |  |
| `precip_probability_value` | double |  |
| `wind_speed` | string |  |
| `wind_direction` | string |  |
| `short_forecast` | string |  |
| `detailed_forecast` | string |  |
| `record_key` | string | SHA-256 of source + business key; identifies the record inside a file |
| `_raw_json` | string | Original record as received (keeps fields the schema does not know about) |
| `_source_file` | string | Raw file the record came from |
| `ingest_date` | date | Date taken from the raw file name (used for backfill windows) |
| `batch_id` | string | Label of the pipeline run that loaded the record |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `bronze_observations`
Raw NWS station observations, one row per station and timestamp

**Primary key:** `record_key` + `_source_file`

| Column | Type | Description |
|---|---|---|
| `station_id` | string |  |
| `observation_timestamp` | string |  |
| `text_description` | string |  |
| `temperature_value` | double |  |
| `dewpoint_value` | double |  |
| `wind_direction_value` | double |  |
| `wind_speed_value` | double |  |
| `wind_gust_value` | double |  |
| `barometric_pressure_value` | double |  |
| `visibility_value` | double |  |
| `relative_humidity_value` | double |  |
| `precipitation_last_hour_value` | double |  |
| `record_key` | string | SHA-256 of source + business key; identifies the record inside a file |
| `_raw_json` | string | Original record as received (keeps fields the schema does not know about) |
| `_source_file` | string | Raw file the record came from |
| `ingest_date` | date | Date taken from the raw file name (used for backfill windows) |
| `batch_id` | string | Label of the pipeline run that loaded the record |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `bronze_iem_asos`
Raw IEM historical ASOS observations (all values kept as text)

**Primary key:** `record_key` + `_source_file`

| Column | Type | Description |
|---|---|---|
| `station` | string |  |
| `valid` | string |  |
| `tmpf` | string |  |
| `dwpf` | string |  |
| `relh` | string |  |
| `drct` | string |  |
| `sknt` | string |  |
| `p01i` | string |  |
| `mslp` | string |  |
| `vsby` | string |  |
| `gust` | string |  |
| `wxcodes` | string |  |
| `record_key` | string | SHA-256 of source + business key; identifies the record inside a file |
| `_raw_json` | string | Original record as received (keeps fields the schema does not know about) |
| `_source_file` | string | Raw file the record came from |
| `ingest_date` | date | Date taken from the raw file name (used for backfill windows) |
| `batch_id` | string | Label of the pipeline run that loaded the record |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### Silver layer
Strictly typed, validated, one row per business key. Every table has `load_timestamp`.

### `silver_alerts`
Cleaned NWS alerts; test/exercise alerts are rejected

**Primary key:** `alert_id`

| Column | Type | Description |
|---|---|---|
| `alert_id` | string | NWS alert identifier (urn:oid:...) |
| `sent_ts` | timestamp |  |
| `effective_ts` | timestamp |  |
| `onset_ts` | timestamp |  |
| `expires_ts` | timestamp |  |
| `ends_ts` | timestamp |  |
| `status` | string |  |
| `message_type` | string |  |
| `category` | string |  |
| `severity` | string |  |
| `certainty` | string |  |
| `urgency` | string |  |
| `event` | string |  |
| `sender_name` | string |  |
| `headline` | string |  |
| `description` | string |  |
| `instruction` | string |  |
| `response` | string |  |
| `area_desc` | string |  |
| `ugc_codes` | array<string> |  |
| `affected_zones` | array<string> |  |
| `referenced_alert_ids` | array<string> | Earlier alerts this alert updates (used to resolve update chains) |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `silver_forecast_periods`
Forecast periods per forecast version; units standardised

**Primary key:** `location_name` + `forecast_generated_at` + `period_number`

| Column | Type | Description |
|---|---|---|
| `location_name` | string | Project location (one of 10 Kansas grid points) |
| `forecast_generated_at` | timestamp | When NWS generated the forecast; identifies a forecast version |
| `period_number` | int |  |
| `period_name` | string |  |
| `start_time` | timestamp |  |
| `end_time` | timestamp |  |
| `is_daytime` | boolean |  |
| `temperature_f` | double | Forecast temperature in Fahrenheit (converted if source unit was C) |
| `precip_probability_pct` | double |  |
| `wind_speed_min_mph` | double | Lower wind speed parsed from text such as '5 to 8 mph' |
| `wind_speed_max_mph` | double | Upper wind speed parsed from text such as '5 to 8 mph' |
| `wind_direction` | string |  |
| `short_forecast` | string |  |
| `detailed_forecast` | string |  |
| `forecast_update_time` | timestamp |  |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `silver_observations`
Observations from the live NWS API and the IEM historical baseline in SI units

**Primary key:** `station_id` + `observation_ts` + `source`

| Column | Type | Description |
|---|---|---|
| `station_id` | string |  |
| `observation_ts` | timestamp | Time of the observation (UTC) |
| `source` | string | nws_api (live) or iem_asos (historical baseline) |
| `text_description` | string |  |
| `temperature_c` | double | Degrees Celsius (IEM Fahrenheit values converted) |
| `dewpoint_c` | double |  |
| `wind_direction_deg` | double |  |
| `wind_speed_kmh` | double | km/h (IEM knots converted) |
| `wind_gust_kmh` | double |  |
| `pressure_pa` | double | Pascal (IEM millibar converted) |
| `visibility_m` | double | Metres (IEM miles converted) |
| `humidity_pct` | double |  |
| `precip_last_hour_mm` | double | Millimetres (IEM inches converted; trace = 0) |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### Operational tables

### `pipeline_execution_logs`
One row per file (raw_to_bronze) or table run (bronze_to_silver)

**Primary key:** none (append-only log)

| Column | Type | Description |
|---|---|---|
| `run_id` | string |  |
| `batch_id` | string | Label of the pipeline run that loaded the record |
| `layer` | string | raw_to_bronze or bronze_to_silver |
| `source` | string | nws_api (live) or iem_asos (historical baseline) |
| `load_type` | string |  |
| `parameter` | string | File path (raw_to_bronze) or date window (bronze_to_silver) that was processed |
| `start_time` | timestamp |  |
| `end_time` | timestamp |  |
| `status` | string |  |
| `rows_inserted` | int | Rows inserted by this step |
| `rows_updated` | int | Rows updated by this step |
| `rows_quarantined` | int | Rows rejected into quarantine_records by this step |
| `message` | string |  |

### `quarantine_records`
Records rejected by schema, type or business-rule checks

**Primary key:** `layer` + `source` + `record_key` + `reason`

| Column | Type | Description |
|---|---|---|
| `layer` | string | raw_to_bronze or bronze_to_silver |
| `source` | string | nws_api (live) or iem_asos (historical baseline) |
| `record_key` | string | SHA-256 of source + business key; identifies the record inside a file |
| `reason` | string | Why the record was rejected |
| `raw_payload` | string | The rejected record (or file content) as received |
| `batch_id` | string | Label of the pipeline run that loaded the record |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |
