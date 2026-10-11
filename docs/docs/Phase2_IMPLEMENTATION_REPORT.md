# Phase 2 Implementation Report: Core Pipeline Engineering & Data Processing

**Project:** Live Public-Safety & Weather Data Pipeline  
**Environment:** Databricks Free Edition, PySpark, Delta Lake, GitHub  
**Reporting date:** 11 October 2026

## 1. Objective and scope

Phase 2 builds the Bronze and Silver layers of the project Lakehouse. The pipeline processes National Weather Service (NWS) API data—alerts, forecasts, and observations—and historical ASOS observations from the Iowa Environmental Mesonet (IEM). The implementation emphasizes explicit schemas, typed transformations, parameterized loads, deduplication/upserts, schema-drift handling, operational audit records, and repeatable execution.

This report distinguishes **implemented features**, **verified results**, and **remaining tests**.

## 2. Infrastructure, repository, and execution components

- **Cloud environment:** Databricks Free Edition, with project notebooks and Delta tables in the `workspace.nws` catalog/schema.
- **Version control:** Project code and notebooks are maintained in the GitHub repository: [Live Public-Safety & Weather Data Pipeline](https://github.com/RafiaMohsin/Live-Public-Safety-Weather-Data-Pipeline).
- **Pipeline stages:** raw data collection → Raw-to-Bronze ingestion → Bronze-to-Silver transformation.
- **Main notebooks:** `00_setup`, `01_raw_to_bronze`, `02_bronze_to_silver`, and `03_demo_idempotency_and_audit`.
- **Supporting code and assets:** PySpark pipeline modules, local tests, source schemas, data-dictionary generation script, sample raw data, and documentation.
- **Storage:** Raw inputs are stored in a Databricks Volume under `/Volumes/workspace/nws/raw`; Bronze and Silver outputs are Delta tables.

## 3. Data collection and automation

### 3.1 NWS collection

The project includes a collector for NWS API data, including alerts, forecasts, and observations. The collector writes raw data to the configured Databricks Volume. Raw-to-Bronze processing can select source types and load modes rather than depending on one hardcoded daily file.

### 3.2 Historical IEM/ASOS collection

Historical ASOS CSV data was downloaded from the Iowa Environmental Mesonet for 10 Kansas stations: DDC, EMP, GCK, HUT, ICT, LWC, MHK, MKC, SLN, and TOP. The verified dataset consists of 20 CSV files covering 1 October 2024 through 30 September 2026. This historical data supports backfill testing as well as the normal NWS API ingestion path.

### 3.3 Scheduled execution

### Automated Six-Hourly Pipeline Execution

The project includes a Databricks job configured to run the pipeline every six hours. During the testing period, the scheduled pipeline executed approximately four to five times, and all recorded executions were successful. 

The automated pipeline supports the periodic ingestion and processing of weather and public-safety data. The job was paused during a later verification stage.

## 4. Data modeling and schema enforcement

### 4.1 Explicit schemas

The ingestion code defines source schemas using PySpark schema definitions rather than relying on Spark schema inference. Raw JSON and CSV inputs are read according to the applicable source schema, and the pipeline includes handling for invalid input and unexpected schema fields.

### 4.2 Bronze layer

The Raw-to-Bronze stage reads raw JSON/CSV inputs, retains ingestion and batch/source context for traceability, and writes the results to Bronze Delta tables. Operations are parameterized by source and load mode. Raw files are selected by their date-prefixed filenames and the configured load window.

### 4.3 Silver layer

The Bronze-to-Silver stage casts fields into their intended types, standardizes selected values and units, validates records, and applies source-appropriate business keys. Delta `MERGE` operations are used to insert new records and update matching records where supported, preventing replayed inputs from creating duplicate Silver business keys.

### 4.4 Metadata and data dictionary

The pipeline includes ingestion/processing metadata and audit context. The repository also includes a data-dictionary generator based on the schema definitions. The data dictionary should be regenerated after schema changes so that the documented columns and data types remain aligned with the implementation.


## 5. Parameterized processing and backfills

The notebooks expose parameters rather than requiring a single hardcoded processing date.

### Raw-to-Bronze notebook (`01_raw_to_bronze`)

| Parameter | Purpose / verified default |
|---|---|
| `db` | Target catalog/schema; default `workspace.nws` |
| `raw_base` | Raw Volume root; default `/Volumes/workspace/nws/raw` |
| `mode` | `incremental` or `backfill` |
| `sources` | Source selection; default `all` |
| `load_type` | `all`, `full`, `incremental`, or `history` |
| `start_date`, `end_date` | Date window for a backfill, formatted `YYYY-MM-DD` |
| `lookback_days` | Incremental date window; default `1` |
| `batch_id` | Optional batch identifier |

### Bronze-to-Silver notebook (`02_bronze_to_silver`)

| Parameter | Purpose / verified default |
|---|---|
| `db` | Target catalog/schema; default `workspace.nws` |
| `raw_base` | Raw Volume root; default `/Volumes/workspace/nws/raw` |
| `mode` | `incremental` or `backfill` |
| `sources` | Source selection; default `all` |
| `start_date`, `end_date` | Date window for a backfill, formatted `YYYY-MM-DD` |
| `lookback_days` | Incremental date window; default `1` |
| `bronze_batch_id` | Optional filter to a Bronze batch |
| `batch_id` | Optional batch identifier for the Silver operation |

For backfills, both dates are required and the start date must not be later than the end date. Incremental windows are resolved from the current UTC date and `lookback_days`. File selection uses the date encoded in the raw filename; this is distinct from an observation's business timestamp. The IEM/ASOS source is a historical source and is handled through the history load path.

## 6. Idempotency and schema-drift handling

### 6.1 Idempotency

Silver processing uses Delta `MERGE` patterns and business keys to avoid duplicate rows when the same source batch is replayed. The historical IEM/ASOS batch was replayed with the same parameters on 11 October 2026 and produced zero inserts, zero updates, and zero quarantined records. This verifies idempotency for that specific tested batch and parameter set; it does not prove every source, mode, or failure/recovery scenario.

### 6.2 Schema drift and invalid records

The project includes checks for unexpected fields, incompatible types, malformed input, and invalid values. Non-conforming data can be rejected or quarantined where the applicable transformation supports it. Seven local tests exercise representative drift and invalid-input cases (see Section 8).

The implementation does not claim automatic schema evolution with `mergeSchema` as a universal response. The documented strategy is to validate against expected source contracts and handle non-conforming input through the implemented rejection/quarantine paths.

## 7. Auditing and fault tolerance

The pipeline writes operational records to `pipeline_execution_logs`. The audit context records information such as layer/source, operation parameters or file context, start/end timing, status, and operation counts where applicable.

- Individual operations record a success or failure outcome.
- Processing can continue with other operations after one operation fails.
- The notebooks inspect returned operation results and raise a notebook-level error if a result has status `Failure`.
- A successful operation reporting “no raw files found in window” is still an audited successful no-op; it does not mean new records were ingested.
- The current audit summary does not expose a complete error message for every failure, which is a known observability limitation.

The instructor's requirement calls for an audit entry for every file/table operation across full and incremental loads, including layer, processed parameter/file, start/end time, status, and inserted/updated counts. The implementation contains this audit framework; a complete field-by-field audit coverage review across every source and load type remains a recommended final verification rather than an assumed result.

## 8. Verification results

### 8.1 Local tests

**Seven local tests passed.** They cover:

1. Representative source input files.
2. Type drift and quarantine behavior.
3. Invalid JSON input.
4. Rejection of a test alert.
5. IEM conversions and invalid values.
6. Additional IEM columns.
7. Unexpected top-level NWS schema drift.

### 8.2 Historical IEM/ASOS load

| Check | Verified result |
|---|---:|
| Historical input files processed | 20 |
| Stations represented | 10 |
| Bronze IEM/ASOS rows | 210,965 |
| Observation timestamp range | 2024-10-01 to 2026-09-30 |
| Bronze batch quarantined files/records | 0 |
| Silver historical rows inserted | 210,965 |
| Silver historical rows updated | 0 |
| Silver historical rows quarantined | 0 |

The Bronze batch was `iem-history-20261010`; the Silver batch was `iem-silver-history-20261010`.

### 8.3 Silver counts and duplicate checks

| Silver source group | Verified rows |
|---|---:|
| IEM/ASOS observations | 210,965 |
| NWS API data | 10,337 |
| Total across these two reported groups | 221,302 |

The verified distinct business-key counts matched the corresponding Silver row counts. The duplicate-key check returned no duplicates, and the cross-source station/timestamp overlap check returned no rows.

### 8.4 Historical idempotency replay

On 11 October 2026, the same historical Silver backfill parameters were replayed. Result: **0 inserted, 0 updated, 0 quarantined**. This is evidence for the tested historical batch only.

### 8.5 Recent Raw-to-Bronze run

A Raw-to-Bronze run on 11 October 2026 reported all 29 operations as `Success`, with zero inserts, updates, and quarantined records. Some operations reported no raw files in the selected window. This verifies the run outcome for that window, not that new data was ingested during it.

## 9. Remaining verification and limitations

The following will be completed with our phase 3 implementation:

1. **Failure-and-recovery replay:** induce a controlled failure in an isolated test batch, confirm the audit failure and notebook-level failure, correct the cause, replay, and verify row counts, quarantine counts, keys, and audit results.

## 10. Conclusion

Phase 2 has implemented the core Raw-to-Bronze and Bronze-to-Silver processing paths, parameterized load/backfill controls, schema validation and non-conforming-input handling, Delta `MERGE`-based Silver processing, and an operational audit framework. The seven local tests passed, the historical IEM/ASOS load processed 210,965 rows without quarantine, duplicate-key checks were clean, and replaying the tested historical Silver batch made no changes.

