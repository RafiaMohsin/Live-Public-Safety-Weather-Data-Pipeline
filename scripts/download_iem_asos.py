"""
Download HISTORICAL observations (the full-load baseline) from the Iowa Environmental Mesonet
ASOS service and save them as landing CSV files that the Bronze notebook can read.

    python scripts/download_iem_asos.py --start 2024-10-01 --end 2026-10-01
    python scripts/download_iem_asos.py --stations ICT TOP --start 2025-01-01 --end 2025-12-31

Output: <out>/<UTC timestamp>_<STATION>_<start>_<end>.csv with a FIXED header (see IEM_COLUMNS),
so the schema-on-read contract in nws_pipeline/schemas.py always matches. Upload the files to the
Databricks Volume folder  raw/history/iem_asos/.

Notes
- IEM has an IP-based rate limit: the script pauses between requests and retries with back-off.
- Check the service documentation for current parameters:
  https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?help
- Missing values arrive as "M" and trace precipitation as "T"; Silver handles both.
"""
import argparse
import csv
import io
import pathlib
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

SERVICE = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
IEM_COLUMNS = ["station", "valid", "tmpf", "dwpf", "relh", "drct", "sknt", "p01i", "mslp",
               "vsby", "gust", "wxcodes"]                      # keep in sync with nws_pipeline/schemas.py
KANSAS_STATIONS = ["DDC", "EMP", "GCK", "HUT", "ICT", "LWC", "MHK", "MKC", "SLN", "TOP"]
HEADERS = {"User-Agent": "(NWS-Medallion-Student-Project, l242595@lhr.nu.edu.pk)"}  # CHANGE THE EMAIL


def build_url(station, start, end, report_types):
    params = [("station", station), ("sts", f"{start}T00:00:00Z"), ("ets", f"{end}T00:00:00Z"),
              ("tz", "Etc/UTC"), ("format", "onlycomma"), ("latlon", "no"), ("missing", "M"), ("trace", "T")]
    params += [("data", c) for c in IEM_COLUMNS[2:]]
    params += [("report_type", r) for r in report_types]
    return SERVICE + "?" + urllib.parse.urlencode(params)


def fetch(url, attempts=5):
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            wait = 5 * (i + 1)
            print(f"  attempt {i + 1} failed ({e}); retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"giving up on {url}")


def normalize(text):
    """Re-order the service's columns into the fixed IEM_COLUMNS layout (missing -> empty)."""
    lines = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
    rows = list(csv.DictReader(lines))
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(IEM_COLUMNS)
    for r in rows:
        w.writerow([r.get(c, "") for c in IEM_COLUMNS])
    return out.getvalue(), len(rows)


def chunks(start, end, days):
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stations", nargs="+", default=KANSAS_STATIONS)
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--chunk-days", type=int, default=365)
    ap.add_argument("--report-types", nargs="+", default=["3", "4"],
                    help="IEM report types (3=routine, 4=special as documented by IEM; verify)")
    ap.add_argument("--out", default="data_raw/history/iem_asos")
    ap.add_argument("--pause", type=float, default=3.0, help="seconds between requests")
    a = ap.parse_args()

    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    start, end = date.fromisoformat(a.start), date.fromisoformat(a.end)
    for station in a.stations:
        for s, e in chunks(start, end, a.chunk_days):
            print(f"{station} {s} -> {e}")
            body, n = normalize(fetch(build_url(station, s, e, a.report_types)))
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            path = out / f"{ts}_{station}_{s:%Y%m%d}_{e:%Y%m%d}.csv"
            path.write_text(body, encoding="utf-8")
            print(f"  saved {path} ({n} rows)")
            time.sleep(a.pause)


if __name__ == "__main__":
    main()
