"""
Collect raw NWS API data for the Bronze layer.

Usage:
    python collect_nws.py full          # baseline: alerts + forecasts + 7 days of observations
    python collect_nws.py incremental   # only NEW alerts, fresh forecasts, observations since last run

Raw JSON is written to data_raw/<mode>/<endpoint>/<UTC timestamp>.json
A small state.json remembers the watermark (last observation time, alert IDs already seen).
"""
import datetime as dt
import json
import pathlib
import sys
import time

import requests

BASE = "https://api.weather.gov"
# NWS asks for a descriptive User-Agent. CHANGE THE EMAIL to a real one.
HEADERS = {
    "User-Agent": "(NWS-Medallion-Student-Project, your_email@example.com)",
    "Accept": "application/geo+json",
}
STATE_CODE = "KS"
# 10 Kansas locations; each is resolved to its NWS forecast grid and nearest station
POINTS = {
    "Kansas City": (39.10, -94.58), "Topeka": (39.05, -95.68),
    "Wichita": (37.69, -97.34), "Dodge City": (37.75, -100.02),
    "Salina": (38.84, -97.61), "Lawrence": (38.97, -95.24),
    "Manhattan": (39.18, -96.57), "Hutchinson": (38.06, -97.93),
    "Garden City": (37.97, -100.87), "Emporia": (38.40, -96.18),
}

ROOT = pathlib.Path(__file__).parent
STATE_FILE = ROOT / "state.json"


def get(url, **params):
    """GET with simple retry; returns parsed JSON."""
    r = None
    for attempt in range(3):
        r = requests.get(url, headers=HEADERS, params=params or None, timeout=30)
        if r.status_code == 200:
            return r.json()
        time.sleep(2 * (attempt + 1))
    r.raise_for_status()


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def load_state():
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}


def save(mode, endpoint, payload):
    folder = ROOT / "data_raw" / mode / endpoint
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{now_utc().strftime('%Y%m%dT%H%M%SZ')}.json"
    path.write_text(json.dumps(payload, indent=2))
    print("saved", path)


def resolve_points(state):
    """Look up forecast grid + nearest station for each location (cached in state.json)."""
    if "grids" in state:
        return
    grids, stations = {}, set()
    for name, (lat, lon) in POINTS.items():
        p = get(f"{BASE}/points/{lat},{lon}")["properties"]
        grids[name] = {"office": p["gridId"], "x": p["gridX"], "y": p["gridY"]}
        st = get(p["observationStations"])["features"]
        if st:
            stations.add(st[0]["properties"]["stationIdentifier"])
    state["grids"], state["stations"] = grids, sorted(stations)


def main(mode):
    state = load_state()
    resolve_points(state)
    run_time = now_utc()

    # 1) ALERTS: full = everything active; incremental = only alert IDs not seen before
    alerts = get(f"{BASE}/alerts/active", area=STATE_CODE)
    seen = set(state.get("seen_alerts", []))
    if mode == "incremental":
        new = [f for f in alerts["features"] if f["id"] not in seen]
        if new:
            save(mode, "alerts", {**alerts, "features": new})
        else:
            print("no new alerts")
    else:
        save(mode, "alerts", alerts)
    state["seen_alerts"] = sorted(seen | {f["id"] for f in alerts["features"]})

    # 2) FORECASTS: a fresh snapshot each run (repeated forecasts are kept on purpose)
    forecasts = {}
    for name, g in state["grids"].items():
        url = f"{BASE}/gridpoints/{g['office']}/{g['x']},{g['y']}/forecast"
        forecasts[name] = get(url)
    save(mode, "forecasts", {"ingestion_time": iso(run_time), "grids": forecasts})

    # 3) OBSERVATIONS: full = last 7 days; incremental = since the stored watermark
    if mode == "full" or "obs_watermark" not in state:
        start = iso(run_time - dt.timedelta(days=7))
    else:
        start = state["obs_watermark"]
    obs = {}
    for sid in state["stations"]:
        obs[sid] = get(f"{BASE}/stations/{sid}/observations", start=start, end=iso(run_time))
    save(mode, "observations", {"ingestion_time": iso(run_time), "start": start, "stations": obs})
    state["obs_watermark"] = iso(run_time)

    STATE_FILE.write_text(json.dumps(state, indent=2))


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("full", "incremental"):
        sys.exit("usage: python collect_nws.py [full|incremental]")
    main(sys.argv[1])
