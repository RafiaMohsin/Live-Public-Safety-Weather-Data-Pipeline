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
| `layer` | string | raw_to_bronze, bronze_to_silver or silver_to_gold |
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
| `layer` | string | raw_to_bronze, bronze_to_silver or silver_to_gold |
| `source` | string | nws_api (live) or iem_asos (historical baseline) |
| `record_key` | string | SHA-256 of source + business key; identifies the record inside a file |
| `reason` | string | Why the record was rejected |
| `raw_payload` | string | The rejected record (or file content) as received |
| `batch_id` | string | Label of the pipeline run that loaded the record |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### Gold layer (star schema)
Dimensions and facts joined on deterministic surrogate keys. Full ER diagram in `docs/gold_model.md`.

### `dim_date`
Calendar dimension, one row per day

**Primary key:** `date_key`

| Column | Type | Description |
|---|---|---|
| `date_key` | int | Surrogate key yyyymmdd; joins to dim_date |
| `date` | date |  |
| `year` | int |  |
| `quarter` | int |  |
| `month` | int |  |
| `month_name` | string |  |
| `week_of_year` | int |  |
| `day` | int |  |
| `day_of_week` | int |  |
| `day_name` | string |  |
| `is_weekend` | boolean |  |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `dim_station`
Weather stations (NWS and IEM)

**Primary key:** `station_key`

| Column | Type | Description |
|---|---|---|
| `station_key` | bigint | Surrogate key (xxhash64 of station_id); joins to dim_station |
| `station_id` | string |  |
| `sources` | string |  |
| `first_observation_ts` | timestamp |  |
| `last_observation_ts` | timestamp |  |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `dim_location`
Forecast locations (Kansas grid points)

**Primary key:** `location_key`

| Column | Type | Description |
|---|---|---|
| `location_key` | bigint | Surrogate key (xxhash64 of location_name); joins to dim_location |
| `location_name` | string | Project location (one of 10 Kansas grid points) |
| `first_forecast_ts` | timestamp |  |
| `last_forecast_ts` | timestamp |  |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `dim_alert_type`
Alert event, severity, certainty and urgency

**Primary key:** `alert_type_key`

| Column | Type | Description |
|---|---|---|
| `alert_type_key` | bigint | Surrogate key (xxhash64 of event, category, severity, certainty, urgency); joins to dim_alert_type |
| `event` | string |  |
| `category` | string |  |
| `severity` | string |  |
| `severity_rank` | int | Extreme 4, Severe 3, Moderate 2, Minor 1, other 0 (for sorting in BI) |
| `certainty` | string |  |
| `urgency` | string |  |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `fact_alerts`
One row per alert; measures duration, lead time and affected zones

**Primary key:** `alert_id`  
**Foreign keys:** `alert_type_key` -> `dim_alert_type`, `sent_date_key` -> `dim_date`, `onset_date_key` -> `dim_date`

| Column | Type | Description |
|---|---|---|
| `alert_id` | string | NWS alert identifier (urn:oid:...) |
| `alert_type_key` | bigint | Surrogate key (xxhash64 of event, category, severity, certainty, urgency); joins to dim_alert_type |
| `sent_date_key` | int | Date alert was sent; role of dim_date |
| `onset_date_key` | int | Date the hazard begins; role of dim_date |
| `sent_ts` | timestamp |  |
| `effective_ts` | timestamp |  |
| `onset_ts` | timestamp |  |
| `expires_ts` | timestamp |  |
| `ends_ts` | timestamp |  |
| `status` | string |  |
| `message_type` | string |  |
| `is_update` | boolean | True when the alert updates or cancels an earlier alert |
| `duration_hours` | double | Hours between effective and expires |
| `lead_time_hours` | double | Hours between sent and onset (warning lead time) |
| `affected_zone_count` | int |  |
| `ugc_code_count` | int |  |
| `area_desc` | string |  |
| `headline` | string |  |
| `sender_name` | string |  |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `fact_observations`
One row per station, timestamp and source

**Primary key:** `station_key` + `observation_ts` + `source`  
**Foreign keys:** `station_key` -> `dim_station`, `date_key` -> `dim_date`

| Column | Type | Description |
|---|---|---|
| `station_key` | bigint | Surrogate key (xxhash64 of station_id); joins to dim_station |
| `date_key` | int | Surrogate key yyyymmdd; joins to dim_date |
| `observation_ts` | timestamp | Time of the observation (UTC) |
| `source` | string | nws_api (live) or iem_asos (historical baseline) |
| `observation_hour` | int |  |
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

### `fact_forecast_periods`
One row per location, forecast version and period

**Primary key:** `location_key` + `forecast_generated_at` + `period_number`  
**Foreign keys:** `location_key` -> `dim_location`, `start_date_key` -> `dim_date`

| Column | Type | Description |
|---|---|---|
| `location_key` | bigint | Surrogate key (xxhash64 of location_name); joins to dim_location |
| `forecast_generated_at` | timestamp | When NWS generated the forecast; identifies a forecast version |
| `period_number` | int |  |
| `start_date_key` | int | Date the forecast period starts; role of dim_date |
| `period_name` | string |  |
| `start_time` | timestamp |  |
| `end_time` | timestamp |  |
| `is_daytime` | boolean |  |
| `temperature_f` | double | Forecast temperature in Fahrenheit (converted if source unit was C) |
| `temperature_c` | double | Degrees Celsius (IEM Fahrenheit values converted) |
| `precip_probability_pct` | double |  |
| `wind_speed_min_mph` | double | Lower wind speed parsed from text such as '5 to 8 mph' |
| `wind_speed_max_mph` | double | Upper wind speed parsed from text such as '5 to 8 mph' |
| `wind_direction` | string |  |
| `short_forecast` | string |  |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |

### `fact_daily_weather`
Daily per-station aggregate of observations (aggregate fact)

**Primary key:** `station_key` + `date_key` + `source`  
**Foreign keys:** `station_key` -> `dim_station`, `date_key` -> `dim_date`

| Column | Type | Description |
|---|---|---|
| `station_key` | bigint | Surrogate key (xxhash64 of station_id); joins to dim_station |
| `date_key` | int | Surrogate key yyyymmdd; joins to dim_date |
| `source` | string | nws_api (live) or iem_asos (historical baseline) |
| `obs_count` | int | Observations aggregated into the day |
| `avg_temp_c` | double |  |
| `min_temp_c` | double |  |
| `max_temp_c` | double |  |
| `avg_humidity_pct` | double |  |
| `avg_wind_speed_kmh` | double |  |
| `max_wind_gust_kmh` | double |  |
| `avg_pressure_pa` | double |  |
| `min_visibility_m` | double |  |
| `precip_sum_mm` | double | Sum of hourly precipitation for the day |
| `row_hash` | string | SHA-256 of the business columns; MERGE updates a row only if it changed |
| `load_timestamp` | timestamp | When this record was ingested / processed (UTC) |
