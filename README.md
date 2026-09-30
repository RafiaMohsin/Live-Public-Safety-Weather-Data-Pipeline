# Live Public-Safety Weather Data Pipeline (NWS API)

Semester project: end-to-end Spark pipeline on Databricks using the Medallion Architecture
(Bronze -> Silver -> Gold) with a Power BI dashboard.

**Team:** Rafia and Shanzay  
**Course:** Data Analysis and Visualization  
**Phase 1 proposal:** `docs/proposal.pdf`

## Data source
- National Weather Service API, `https://api.weather.gov` (public domain, no API key, User-Agent header required)
- Endpoints: `/points/{lat},{lon}`, `/gridpoints/{office}/{x},{y}/forecast`, `/alerts/active?area=KS`, `/stations/{id}/observations`

## Load strategy
| Load | What it does | Sample |
|---|---|---|
| Full load | Baseline: all active alerts, forecasts for 10 grid points, 7 days of observations for 10 stations | `samples/full_load/` |
| Incremental load | Only new alert IDs, fresh forecast snapshot, observations since the last watermark | `samples/incremental/` |

## Repository structure
```
.
├── README.md
├── collect_nws.py        # polling script (full / incremental)
├── requirements.txt
├── docs/
│   └── proposal.pdf      # Phase 1 proposal
├── samples/
│   ├── full_load/        # alerts / forecasts / observations JSON
│   └── incremental/      # alerts / forecasts / observations JSON
└── notebooks/            # Databricks notebooks (Phase 2): bronze, silver, gold
```

## How to run
```bash
pip install -r requirements.txt
# edit the User-Agent email in collect_nws.py first
python collect_nws.py full
python collect_nws.py incremental   # repeat on a schedule (hourly)
```
Raw output goes to `data_raw/` (not committed). Representative files are copied to `samples/`.

## Volume estimates
| | Size |
|---|---|
| Full load | <X> MB |
| Incremental (per run) | <Y> MB |
| Incremental (per day) | <Z> MB |

## Security / PII
No PII: the data is public-domain government weather data with no personal records.
Protocol fields are dropped before Silver; free-text alert fields are kept for reference only.

## Infrastructure and FinOps
Databricks Free Edition, scheduled (not continuous) jobs, one state and 10 grid points / stations during development,
bounded Bronze retention.
