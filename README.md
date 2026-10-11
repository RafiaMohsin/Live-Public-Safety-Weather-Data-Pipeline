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

Forecasts have no public history, so they are collected live only. Repository sample files are under `data_raw/full_load/` and `data_raw/incremental/`. These are local sample-data folders; the Databricks Volume uses the runtime folder names `full/` and `incremental/`.

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
| Fault tolerance | Each file/table operation is audited. An operation failure is logged and processing continues with other units; the notebook checks returned statuses and raises an error if any audited operation reports `Failure`. End-to-end recovery by replaying failed work and verifying completeness has not yet been tested. |

## Repository structure
```
.
├── README.md
├── collect_nws.py                 # live NWS collector (full / incremental), writes to Databricks Volume
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
├── docs/                          # proposal, data_dictionary.md, Phase 2 implementation report
└── data_raw/{full_load,incremental}/{alerts,forecasts,observations}/  # local sample files; Volume folders are full/ and incremental/
```

## Execution guide

### One-time setup (Databricks)
1. Workspace -> Create -> **Git folder**, paste this repository's URL.
2. Run `notebooks/00_setup` (creates schema `workspace.nws`, Volume `raw`, and all tables from the explicit schemas).
3. Upload the repository's sample files to the Databricks Volume `/Volumes/workspace/nws/raw/`:
   - `data_raw/full_load/<type>/*.json` -> `raw/full/<type>/`
   - `data_raw/incremental/<type>/*.json` -> `raw/incremental/<type>/`
   - For historical IEM/ASOS data, run `python scripts/download_iem_asos.py --start 2024-10-01 --end 2026-10-01` in an environment with the required Python dependencies, then upload the resulting CSV files to `raw/history/iem_asos/`.
   - Keep the timestamp prefix in each file name (`YYYYMMDDTHHMMSSZ...`). The pipeline uses this date to select files for a backfill.

   Note: `data_raw/full_load/` and `data_raw/incremental/` are local repository sample-data folders. The corresponding Databricks Volume folders are named `full/` and `incremental/`.

### Standard incremental load

Run `01_raw_to_bronze` and then `02_bronze_to_silver`. Use these widget values in both notebooks:

| Parameter | Raw-to-Bronze (`01_raw_to_bronze`) | Bronze-to-Silver (`02_bronze_to_silver`) |
|---|---|---|
| `mode` | `incremental` | `incremental` |
| `lookback_days` | `1` | `1` |
| `sources` | `all` or a comma-separated list | `all` or a comma-separated list |
| `load_type` | `all`, `full`, `incremental`, or `history` | Not used |
| `start_date`, `end_date` | Leave empty | Leave empty |
| `batch_id` | Optional run label | Optional run label |
| `bronze_batch_id` | Not used | Optional filter for one Bronze batch |

With `lookback_days = 1`, the pipeline selects file dates / Bronze `ingest_date` values from yesterday through today (UTC). Overlapping windows are safe to rerun because the pipeline uses idempotent `MERGE INTO` operations.

Valid `sources` are `alerts`, `forecasts`, `observations`, and `iem_asos`. You can use `all` or a comma-separated selection such as `observations,forecasts`.

### Backfill (any historical timeframe)

Run Raw-to-Bronze first, then Bronze-to-Silver. Set `mode = backfill` and provide both `start_date` and `end_date` in `YYYY-MM-DD` format. The dates are matched against the timestamp date in raw filenames and the `ingest_date` assigned to Bronze rows—not necessarily the observation timestamp inside the data.

**Raw-to-Bronze (`01_raw_to_bronze`)**

| Parameter | Example value | Meaning |
|---|---|---|
| `mode` | `backfill` | Selects an explicit date window |
| `start_date` | `2026-10-01` | First included file date |
| `end_date` | `2026-10-01` | Last included file date |
| `sources` | `observations` or `all` | Source(s) to process; multiple sources are comma-separated |
| `load_type` | `full`, `incremental`, `history`, or `all` | Selects corresponding raw subfolder(s) |
| `batch_id` | `observations-backfill-20261001` | Optional traceable batch label |

**Bronze-to-Silver (`02_bronze_to_silver`)**

| Parameter | Example value | Meaning |
|---|---|---|
| `mode` | `backfill` | Selects an explicit ingestion-date window |
| `start_date` | `2026-10-01` | First included Bronze `ingest_date` |
| `end_date` | `2026-10-01` | Last included Bronze `ingest_date` |
| `sources` | `observations` or `all` | Source(s) to process; multiple sources are comma-separated |
| `batch_id` | `silver-backfill-20261001` | Optional Silver run label |
| `bronze_batch_id` | `observations-backfill-20261001` | Optional filter to one previously loaded Bronze batch |

Examples:
- **Backfill NWS observations for a day:** Raw-to-Bronze: `mode=backfill`, `sources=observations`, `load_type=all`, `start_date=2026-10-01`, `end_date=2026-10-01`. Then Silver: `mode=backfill`, `sources=observations`, with the same date window.
- **Load historical IEM/ASOS files:** Raw-to-Bronze: `mode=backfill`, `sources=iem_asos`, `load_type=history`, and a date window covering the timestamp dates in the CSV filenames. Then Silver: `mode=backfill`, `sources=iem_asos`, with a window covering the resulting Bronze `ingest_date` values. For a targeted replay, also provide `bronze_batch_id=<batch id>` from the audit log.
- **Re-run Silver for one Bronze batch:** notebook 02: set `mode=backfill`, the appropriate `start_date` and `end_date`, `sources=<source>`, and `bronze_batch_id=<batch id from pipeline_execution_logs>`. |

### Failure handling and recovery

Each file operation in Raw-to-Bronze and each source/table operation in Bronze-to-Silver writes an entry to `pipeline_execution_logs`, including status and row counts. Processing is designed to continue after an individual operation fails so other files or sources can still be attempted. At the end of each notebook, a status check raises a notebook error if any returned audited operation has status `Failure`; inspect `pipeline_execution_logs` and `quarantine_records` to investigate.

A successful notebook run does not, by itself, prove that every expected source file was present or that all records were complete. A zero-insert/zero-update replay can mean the data was already processed. End-to-end recovery testing—replaying failed work and verifying final completeness—remains pending.

### Prove idempotency
Run `notebooks/03_demo_idempotency_and_audit`: it re-runs both layers on the same data, asserts that no table
changed, and shows the audit log (rows_inserted and rows_updated are 0 on the re-run).

### Collecting live data (incremental source)

`collect_nws.py` is configured to write to `/Volumes/workspace/nws/raw/full/` or `/Volumes/workspace/nws/raw/incremental/`. Run it in an environment where that Databricks Volume is mounted, or adapt `RAW_BASE` before running it on a regular local computer.

```bash
pip install -r requirements.txt
python collect_nws.py full          # baseline snapshot
python collect_nws.py incremental   # collect newly available data
```

Edit the User-Agent email in `collect_nws.py` and `scripts/download_iem_asos.py` first. After collecting data, run notebooks 01 and 02. For the repository's sample files, upload from `data_raw/full_load/` or `data_raw/incremental/` into the matching Volume folders `raw/full/` or `raw/incremental/`.

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
