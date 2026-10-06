"""Configuration and parameter helpers. No hardcoded dates or paths in the pipeline code."""
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

# source -> raw sub-folders (load types) that can contain its files
SOURCES = {
    "alerts": ("full", "incremental"),
    "forecasts": ("full", "incremental"),
    "observations": ("full", "incremental"),
    "iem_asos": ("history",),          # historical baseline from Iowa Environmental Mesonet (CSV)
}
BRONZE_TABLES = {
    "alerts": "bronze_alerts",
    "forecasts": "bronze_forecast_periods",
    "observations": "bronze_observations",
    "iem_asos": "bronze_iem_asos",
}
# iem_asos and observations both feed the same Silver observations table
SILVER_TABLES = {
    "alerts": "silver_alerts",
    "forecasts": "silver_forecast_periods",
    "observations": "silver_observations",
    "iem_asos": "silver_observations",
}
LOG_TABLE = "pipeline_execution_logs"
QUARANTINE_TABLE = "quarantine_records"


@dataclass
class Config:
    db: str = "workspace.nws"                      # catalog.schema in Databricks (or just a schema name)
    raw_base: str = "/Volumes/workspace/nws/raw"   # folder that contains full/, incremental/, history/

    def t(self, name):
        return f"{self.db}.{name}"


def _check_date(value, label):
    try:
        return date.fromisoformat(value).isoformat()
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be a date like 2026-10-05, got {value!r}")


def resolve_window(mode, start_date=None, end_date=None, lookback_days=1):
    """incremental -> last `lookback_days` days up to today; backfill -> explicit start/end dates."""
    if mode == "backfill":
        if not start_date or not end_date:
            raise ValueError("backfill mode requires start_date and end_date (YYYY-MM-DD)")
        s, e = _check_date(start_date, "start_date"), _check_date(end_date, "end_date")
        if s > e:
            raise ValueError("start_date must not be after end_date")
        return s, e
    if mode == "incremental":
        today = datetime.now(timezone.utc).date()
        return (today - timedelta(days=int(lookback_days))).isoformat(), today.isoformat()
    raise ValueError("mode must be 'incremental' or 'backfill'")


_FILE = re.compile(r"^(\d{8})T\d{6}Z.*\.(json|csv)$")


def list_raw_files(cfg, source, load_type, start_date, end_date):
    """Return [(path, load_type, file_date)] for raw files whose timestamp date is inside the window."""
    kinds = [k for k in SOURCES[source] if load_type in ("all", k)]
    found = []
    for kind in kinds:
        folder = os.path.join(cfg.raw_base, kind, source)
        if not os.path.isdir(folder):
            continue
        for name in sorted(os.listdir(folder)):
            m = _FILE.match(name)
            if not m:
                continue
            d = datetime.strptime(m.group(1), "%Y%m%d").date().isoformat()
            if start_date <= d <= end_date:
                found.append((os.path.join(folder, name), kind, d))
    return found
