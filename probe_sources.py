"""
HEADROOM - Phase 1, Step 1: source probe.
Checks whether each source can deliver the 12-week backfill (2026-07-01 -> 2026-10-14)
for PJM / ERCOT / MISO / CAISO / SPP, and saves today's raw pulls under ./raw/.
Run: .\.venv\Scripts\python.exe probe_sources.py
"""

import datetime as dt
import json
import os
import pathlib
import sys

import pandas as pd
import requests

BACKFILL_START = "2026-07-01"
BACKFILL_END = "2026-10-14"
TODAY = dt.date.today().isoformat()
RAW = pathlib.Path("raw")

BAS = {"PJM": "PJM", "ERCOT": "ERCO", "MISO": "MISO", "CAISO": "CISO", "SPP": "SWPP"}

EIA_MIN_COVERAGE = 0.98
EIA_MAX_NULL_SHARE = 0.01
QUEUE_MIN_QUEUE_DATE = 0.90
QUEUE_MIN_WITHDRAWN_DATED = 0.80


def probe_eia930(api_key):
    url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    expected = int((pd.Timestamp(BACKFILL_END) - pd.Timestamp(BACKFILL_START)).total_seconds() // 3600)
    results = []
    for region, ba in BAS.items():
        rows, offset = [], 0
        while True:
            params = {
                "api_key": api_key,
                "frequency": "hourly",
                "data[0]": "value",
                "facets[respondent][]": ba,
                "facets[type][]": "D",
                "start": f"{BACKFILL_START}T00",
                "end": f"{BACKFILL_END}T00",
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
                "offset": offset,
                "length": 5000,
            }
            r = requests.get(url, params=params, timeout=90)
            r.raise_for_status()
            batch = r.json()["response"]["data"]
            rows.extend(batch)
            if len(batch) < 5000:
                break
            offset += 5000

        df = pd.DataFrame(rows)
        n = len(df)
        vals = pd.to_numeric(df["value"], errors="coerce") if n else pd.Series(dtype=float)
        coverage = n / expected if expected else 0
        null_share = float(vals.isna().mean()) if n else 1.0
        results.append({
            "region": region,
            "ba": ba,
            "rows": n,
            "expected_approx": expected,
            "coverage": round(coverage, 4),
            "null_share": round(null_share, 4),
            "first_period": df["period"].min() if n else None,
            "last_period": df["period"].max() if n else None,
            "max_hourly_mw": float(vals.max()) if n else None,
            "verdict": "PASS" if coverage >= EIA_MIN_COVERAGE and null_share <= EIA_MAX_NULL_SHARE else "FAIL",
        })
    return results


def _find(cols, *needles):
    for c in cols:
        lc = c.lower()
        if any(n in lc for n in needles):
            return c
    return None


def probe_queues():
    import gridstatus

    factories = {
        "PJM": lambda: gridstatus.PJM(api_key=os.environ.get("PJM_API_KEY")),
        "ERCOT": gridstatus.Ercot,
        "MISO": gridstatus.MISO,
        "CAISO": gridstatus.CAISO,
        "SPP": gridstatus.SPP,
    }
    results = []
    for region, make in factories.items():
        try:
            q = make().get_interconnection_queue()
        except Exception as e:
            results.append({"region": region, "verdict": "PULL_FAILED", "error": repr(e)[:300]})
            continue

        out_dir = RAW / "queue" / f"region={region}" / f"snapshot_date={TODAY}"
        out_dir.mkdir(parents=True, exist_ok=True)
        q.astype(str).to_parquet(out_dir / "queue.parquet", index=False)

        cols = list(q.columns)
        status_col = "Status" if "Status" in cols else _find(cols, "status")
        qdate_col = "Queue Date" if "Queue Date" in cols else _find(cols, "queue date", "request receive", "receive date")
        wdate_col = "Withdrawn Date" if "Withdrawn Date" in cols else _find(cols, "withdrawn date", "withdrawal date")
        mw_col = "Capacity (MW)" if "Capacity (MW)" in cols else _find(cols, "capacity (mw)", "mw")

        n = len(q)
        qdate_share = float(q[qdate_col].notna().mean()) if qdate_col else 0.0

        if status_col:
            withdrawn = q[q[status_col].astype(str).str.contains("withdr", case=False, na=False)]
            active = q[q[status_col].astype(str).str.contains("active", case=False, na=False)]
        else:
            withdrawn, active = q.iloc[0:0], q.iloc[0:0]

        n_wd = len(withdrawn)
        wd_dated = float(withdrawn[wdate_col].notna().mean()) if (wdate_col and n_wd) else 0.0
        active_mw = float(pd.to_numeric(active[mw_col], errors="coerce").sum()) if mw_col else None

        reconstructable = qdate_share >= QUEUE_MIN_QUEUE_DATE and (n_wd == 0 or wd_dated >= QUEUE_MIN_WITHDRAWN_DATED)
        results.append({
            "region": region,
            "rows": n,
            "status_col": status_col,
            "status_values": q[status_col].astype(str).value_counts().head(8).to_dict() if status_col else None,
            "queue_date_col": qdate_col,
            "queue_date_share": round(qdate_share, 4),
            "withdrawn_date_col": wdate_col,
            "withdrawn_rows": n_wd,
            "withdrawn_dated_share": round(wd_dated, 4),
            "active_mw": active_mw,
            "verdict": "BACKFILL_RECONSTRUCTABLE" if reconstructable else "SNAPSHOT_ONLY",
        })
    return results


def probe_860m():
    months = ["january", "february", "march", "april", "may", "june", "july",
              "august", "september", "october", "november", "december"]
    today = dt.date.today()
    tried = []
    for back in range(1, 5):
        y, m = today.year, today.month - back
        while m <= 0:
            m += 12
            y -= 1
        url = f"https://www.eia.gov/electricity/data/eia860m/xls/{months[m-1]}_generator{y}.xlsx"
        tried.append(url)
        try:
            r = requests.get(url, timeout=120)
            if r.status_code != 200 or len(r.content) < 100_000:
                continue
        except requests.RequestException:
            continue

        out_dir = RAW / "eia860m" / f"snapshot_date={TODAY}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / url.rsplit("/", 1)[-1]
        path.write_bytes(r.content)

        raw = pd.read_excel(path, sheet_name="Operating", header=None, nrows=10)
        hdr = next((i for i, row in raw.iterrows() if row.astype(str).str.contains("Entity ID").any()), 0)
        op = pd.read_excel(path, sheet_name="Operating", header=hdr)
        cols = list(op.columns)
        ret_year = _find(cols, "planned retirement year")
        ret_month = _find(cols, "planned retirement month")
        ba_col = _find(cols, "balancing authority code")
        mw = _find(cols, "nameplate capacity")

        planned = op[pd.to_numeric(op[ret_year], errors="coerce").notna()] if ret_year else op.iloc[0:0]
        by_ba = (
            planned[planned[ba_col].isin(BAS.values())]
            .groupby(ba_col)[mw].apply(lambda s: float(pd.to_numeric(s, errors="coerce").sum()))
            .to_dict()
            if (ba_col and mw and len(planned)) else {}
        )
        return {
            "file": url,
            "operating_rows": len(op),
            "retirement_cols": [ret_year, ret_month],
            "ba_col": ba_col,
            "planned_retirement_rows": len(planned),
            "planned_retirement_mw_by_ba": by_ba,
            "verdict": "PASS" if (ret_year and ba_col and mw) else "FAIL",
        }
    return {"verdict": "DOWNLOAD_FAILED", "tried": tried}


def main():
    key = os.environ.get("EIA_API_KEY")
    if not key:
        sys.exit("Set EIA_API_KEY first.")
    RAW.mkdir(exist_ok=True)

    report = {"run_at": dt.datetime.now().isoformat(timespec="seconds"),
              "window": [BACKFILL_START, BACKFILL_END]}

    print("\n== EIA-930 hourly demand ==")
    report["eia930"] = probe_eia930(key)
    print(pd.DataFrame(report["eia930"]).to_string(index=False))

    print("\n== ISO interconnection queues ==")
    report["queues"] = probe_queues()
    print(pd.DataFrame(report["queues"]).drop(columns=["status_values"], errors="ignore").to_string(index=False))

    print("\n== EIA-860M planned retirements ==")
    report["eia860m"] = probe_860m()
    print(json.dumps(report["eia860m"], indent=2, default=str))

    out = RAW / f"probe_report_{TODAY}.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nSaved {out}")


if __name__ == "__main__":
    main()