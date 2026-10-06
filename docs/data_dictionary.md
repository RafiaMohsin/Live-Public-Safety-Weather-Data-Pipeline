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
