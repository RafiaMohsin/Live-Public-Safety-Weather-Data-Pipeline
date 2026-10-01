# Live Public-Safety Weather Data Pipeline (NWS API)

Data Analysis and Visualisation semester project. An automated Spark pipeline on Databricks using the
Medallion Architecture (Bronze -> Silver -> Gold) with a Power BI dashboard.

**Team:** <Rafia Mohsin>, <Shanzay Khan>
**Course:** <Data Analysis and Visualization>
**Phase 1 proposal:** `docs/NWS_Phase1_Proposal.docx`

## Project summary
We ingest live weather forecasts, hazardous-weather alerts and station observations for the state of Kansas
(10 grid points, 10 stations) from the U.S. National Weather Service. The goal is to answer:
where is weather risk concentrated, how reliable are forecasts at different lead times, and whether forecast data
could warn of floods before an official alert.

## Data source
- National Weather Service API: `https://api.weather.gov` (public domain, no API key, a descriptive `User-Agent` header is required)
- Endpoints used:

| Endpoint | Data |
|---|---|
| `/points/{lat},{lon}` | Lookup of forecast grid and nearest stations |
| `/gridpoints/{office}/{x},{y}/forecast` | 7-day forecast, 14 half-day periods |
| `/alerts/active?area=KS` | Active watches, warnings and advisories |
| `/stations/{id}/observations` | Observed conditions (supports `start` and `end` filters) |

## Load strategy
| Load | What it does | Sample files |
|---|---|---|
| **Full load** (baseline) | All active alerts, a forecast snapshot of 10 grids, and up to 7 days of observations for 10 stations | `samples/full_load/` |
| **Incremental load** | Only new alert IDs, a fresh forecast snapshot, and observations newer than the stored watermark | `samples/incremental/` |

Known limitation: the observations endpoint returned at most 500 records per station, so the sampled full load
covers about 1.5 days instead of 7. Pagination will be added in Phase 2.

## Sample data (collected 1 Oct 2026, 17:46-17:52 UTC)
| File | Type | Load | Size | Contents |
|---|---|---|---|---|
| `samples/full_load/alerts/20261001T174648Z.json` | Alerts | Full | 0.18 MB | 36 active alerts |
| `samples/full_load/forecasts/20261001T174655Z.json` | Forecasts | Full | 0.12 MB | 10 grids x 14 periods = 140 periods |
| `samples/full_load/observations/20261001T174713Z.json` | Observations | Full | 18.2 MB | 5,000 observations (500 per station) |
| `samples/incremental/alerts/20261001T175211Z.json` | Alerts | Incremental | 9.3 KB | 2 new alerts |
| `samples/incremental/forecasts/20261001T175215Z.json` | Forecasts | Incremental | 0.12 MB | 140 periods, unchanged `generatedAt` |
| `samples/incremental/observations/20261001T175222Z.json` | Observations | Incremental | 1.4 KB | 0 new observations |

File names are the UTC ingestion timestamp (`YYYYMMDDTHHMMSSZ`).

## Volume estimates
| | Estimate |
|---|---|
| Full load | about 18.5 MB as sampled (about 80 MB with 7 full days of observations) |
| Incremental, forecasts | about 2.9 MB/day (hourly, 140 periods per poll) |
| Incremental, observations | about 11 MB/day (hourly, 10 stations) |
| Incremental, alerts | 0 to 0.5 MB/day (new alert IDs only) |
| Incremental total | about 14 MB/day at most, about 0.4 GB per 30 days |

## Repository structure
```
.
├── README.md
├── collect_nws.py          # polling script (full / incremental)
├── requirements.txt
├── .gitignore
├── docs/
│   └── NWS_Phase1_Proposal.docx
├── samples/
│   ├── full_load/{alerts,forecasts,observations}/
│   └── incremental/{alerts,forecasts,observations}/
└── notebooks/              # Databricks notebooks (Phase 2)
```

## How to run the collector
```bash
pip install -r requirements.txt
# 1. edit the User-Agent email in collect_nws.py
python collect_nws.py full           # baseline load
python collect_nws.py incremental    # repeat on a schedule (hourly)
```
Raw output is written to `data_raw/<full|incremental>/<alerts|forecasts|observations>/<timestamp>.json`
and a `state.json` stores the watermark and the alert IDs already seen. Both are git-ignored.
Representative files are copied into `samples/`.

## Medallion design (summary)
- **Bronze:** `bronze_alerts`, `bronze_forecast_periods`, `bronze_observations`. Raw rows with source file and `load_timestamp`; non-conforming records are quarantined.
- **Silver:** typed, validated tables `silver_alerts`, `silver_forecast_periods`, `silver_observations`, plus `silver_zones`, `silver_grids`, `silver_stations`. Loaded with MERGE so reruns create no duplicates. Forecast key: grid + `generatedAt` + period number. Alert update chains resolved through `references`.
- **Gold:** facts `fact_alert_event`, `fact_forecast_accuracy`; dimensions `dim_zone`, `dim_grid`, `dim_station`, `dim_date`.
  Core aggregations: alert-days per county, temperature error by lead time, precipitation calibration.
- **Dashboard:** Power BI on the Gold tables.

## Security and PII
No PII. The data is public-domain government weather data with no personal records; the alert `sender` field is a
shared agency mailbox, not a person. `sender` and protocol/broadcast fields are dropped before Silver. Free-text alert
fields are kept for reference only. Gold exposes only aggregated zone- or grid-level metrics.

## Infrastructure and FinOps
Databricks Free Edition with scheduled (not continuous) jobs, one state during development, bounded Bronze retention,
and testing on the small sample files first. Early in Phase 2 we will confirm that Databricks can call
`api.weather.gov`; if outbound access is blocked, the collector runs locally and raw files are uploaded to a Databricks Volume.
