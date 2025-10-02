#!/usr/bin/env python3
# push_csv.py — build CSV → git commit/push → Airtable upsert (one script)

import os, re, sys, csv, json, subprocess, requests, argparse
from pathlib import Path
from datetime import datetime
from urllib.parse import quote

# ---------------------------
# Paths & constants
# ---------------------------
REPO_DIR = Path(__file__).resolve().parent
ENV_PATH = REPO_DIR / ".env.local"
CSV_OUT  = REPO_DIR / "data" / "panama_rent_averages.csv"
SNAPSHOT_DIR = REPO_DIR / "data"
SNAPSHOT_NAME_FMT = "panama_rent_averages_{yyyy}-{mm}.csv"  # e.g., ..._2025-10.csv

# ---------------------------
# Env handling
# ---------------------------
def parse_env_file(path: Path):
    if not path.exists():
        return {}
    env = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = re.sub(r"\s+#.*$", "", v).strip().strip('"').strip("'")
    return env

def get_env():
    file_env = parse_env_file(ENV_PATH)
    # process env takes precedence if set
    env = dict(file_env)
    for k in list(file_env.keys()):
        if k in os.environ and os.environ[k]:
            env[k] = os.environ[k]
    return env

# ---------------------------
# CSV BUILD (replace with your real generator if needed)
# ---------------------------
def build_csv_if_needed(skip_build: bool) -> None:
    """
    Default behavior: if --skip-build is passed we do nothing.
    If not skipping and CSV already exists, we keep it (you may replace with your true generator).
    If not skipping and CSV is missing, we create an empty headered CSV (so pipeline still runs).
    Replace this function with your real builder if you want automatic regeneration.
    """
    if skip_build:
        print("⏭️  Skipping CSV build (per flag).")
        return
    if CSV_OUT.exists():
        print(f"✅ CSV exists: {CSV_OUT}")
        return
    print(f"⚠️  CSV not found; creating a header-only file at: {CSV_OUT}")
    CSV_OUT.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "Date","City/Neighborhood","Configuration","Average Price (USD)",
        "Utilities","Groceries","Internet","Cell Phone","Dining","Entertainment","Travel"
    ]
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

# ---------------------------
# Snapshot
# ---------------------------
def write_monthly_snapshot(force_snapshot: bool) -> None:
    yyyy = datetime.now().strftime("%Y")
    mm   = datetime.now().strftime("%m")
    snap_name = SNAPSHOT_NAME_FMT.format(yyyy=yyyy, mm=mm)
    snap_path = SNAPSHOT_DIR / snap_name
    if snap_path.exists() and not force_snapshot:
        print(f"ℹ️ Snapshot exists, skipping: {snap_name} (use --force-snapshot to overwrite)")
        return
    data = CSV_OUT.read_bytes()
    snap_path.write_bytes(data)
    print(f"✅ Wrote snapshot: {snap_path}")

# ---------------------------
# Git helpers
# ---------------------------
def run(cmd, cwd=None, check=True):
    res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, shell=False)
    if check and res.returncode != 0:
        print("❌ Command failed:", " ".join(cmd))
        print(res.stdout)
        print(res.stderr)
        sys.exit(res.returncode)
    return res

def ensure_git_setup():
    run(["git", "--version"])
    # ensure we're in a repo
    run(["git", "rev-parse", "--is-inside-work-tree"], cwd=str(REPO_DIR))

def git_commit_and_push(commit_msg: str):
    run(["git", "add", str(CSV_OUT)], cwd=str(REPO_DIR))
    # include snapshot if present
    for p in SNAPSHOT_DIR.glob("panama_rent_averages_*.csv"):
        run(["git", "add", str(p)], cwd=str(REPO_DIR))
    # commit (no error if nothing to commit)
    res = subprocess.run(["git", "commit", "-m", commit_msg], cwd=str(REPO_DIR), capture_output=True, text=True)
    if "nothing to commit" in (res.stdout + res.stderr).lower():
        print("ℹ️ Nothing to commit.")
    else:
        print(res.stdout.strip() or res.stderr.strip())
    run(["git", "push", "origin", "HEAD"], cwd=str(REPO_DIR))
    print("🚀 Pushed to origin.")

# ---------------------------
# Airtable upsert (auto-detect schema & merge keys)
# ---------------------------
def airtable_upsert_from_csv(env: dict) -> None:
    token = env.get("AIRTABLE_TOKEN")
    base  = env.get("AIRTABLE_BASE")
    table = env.get("AIRTABLE_RENTS_TABLE") or env.get("AIRTABLE_TABLE") or "Rent"

    if not (token and base and table):
        print("⚠️  Skipping Airtable upsert: missing AIRTABLE_TOKEN / AIRTABLE_BASE / table id/name")
        return
    if not CSV_OUT.exists():
        print(f"⚠️  Skipping Airtable upsert: CSV not found at {CSV_OUT}")
        return

    LONG_FIELDS = [
        "Date","City/Neighborhood","Configuration","Average Price (USD)",
        "Utilities","Groceries","Internet","Cell Phone","Dining","Entertainment","Travel"
    ]
    LONG_KEYS = ["Date","City/Neighborhood","Configuration"]
    LONG_NUM = {"Average Price (USD)","Utilities","Groceries","Internet","Cell Phone","Dining","Entertainment","Travel"}

    SHORT_FIELDS = ["City","Bedrooms","Range","USD_Rent"]
    SHORT_KEYS = ["City","Bedrooms","Range"]
    SHORT_NUM = {"USD_Rent"}

    # Load CSV & headers
    with open(CSV_OUT, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        csv_headers = [h.strip() for h in (rows[0].keys() if rows else [])]

    # Probe Airtable table fields (via sample records)
    base_url = f"https://api.airtable.com/v0/{base}/{quote(str(table), safe='')}"
    hdrs = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    probe = requests.get(base_url, headers=hdrs, params={"maxRecords": 3}, timeout=30)
    if probe.status_code >= 300:
        print("❌ Could not probe Airtable table:", probe.status_code, probe.text)
        raise SystemExit(1)

    recs = probe.json().get("records", [])
    table_fields = set()
    for r in recs:
        table_fields.update((r.get("fields") or {}).keys())

    def choose_schema():
        csv_has_long = set(LONG_FIELDS).issubset(set(csv_headers))
        csv_has_short = set(SHORT_FIELDS).issubset(set(csv_headers))
        # prefer long if CSV supports it AND merge keys exist (or table empty)
        if csv_has_long and (not table_fields or set(LONG_KEYS).issubset(table_fields)):
            return "long"
        if csv_has_short and (not table_fields or set(SHORT_KEYS).issubset(table_fields)):
            return "short"
        if not table_fields:
            if csv_has_long: return "long"
            if csv_has_short: return "short"
        return None

    schema = choose_schema()
    if not schema:
        print("❌ CSV ↔ Airtable mismatch.")
        print("   CSV headers:", csv_headers)
        print("   Sample Airtable fields:", sorted(table_fields) if table_fields else "(no rows yet)")
        print("   Expected LONG:", LONG_FIELDS)
        print("   Expected SHORT:", SHORT_FIELDS)
        raise SystemExit(1)

    if schema == "long":
        FIELD_MAP = {k: k for k in LONG_FIELDS}
        PRIMARY_KEY_FIELDS = LONG_KEYS
        NUMERIC_FIELDS = LONG_NUM
    else:
        FIELD_MAP = {k: k for k in SHORT_FIELDS}
        PRIMARY_KEY_FIELDS = SHORT_KEYS
        NUMERIC_FIELDS = SHORT_NUM

    # If table has fields but is missing merge keys, try fallback to short
    if table_fields and not set(PRIMARY_KEY_FIELDS).issubset(table_fields):
        if schema == "long" and set(SHORT_KEYS).issubset(set(csv_headers)) and set(SHORT_KEYS).issubset(table_fields):
            print("ℹ️ Merge keys for LONG schema not found in table; falling back to SHORT schema.")
            FIELD_MAP = {k: k for k in SHORT_FIELDS}
            PRIMARY_KEY_FIELDS = SHORT_KEYS
            NUMERIC_FIELDS = SHORT_NUM
            schema = "short"
        else:
            print("❌ Merge keys not present in Airtable table:", PRIMARY_KEY_FIELDS)
            print("   Table fields:", sorted(table_fields))
            raise SystemExit(1)

    def coerce(name, val):
        if val is None or val == "": return None
        if name == "Date":
            for fmt in ("%m/%d/%Y","%Y-%m-%d","%m/%d/%y"):
                try: return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
                except: pass
            return val
        if name in NUMERIC_FIELDS:
            try: return float(str(val).replace(",", ""))
            except: return val
        return val

    payload_records = []
    for r in rows:
        fields = {}
        for csv_name, at_name in FIELD_MAP.items():
            if csv_name in r:
                fields[at_name] = coerce(at_name, r[csv_name])
        if fields:
            payload_records.append({"fields": fields})

    if not payload_records:
        print("⚠️  No rows to upsert (CSV empty or headers mismatch).")
        return

    upsert_url = f"{base_url}?typecast=true"
    headers_json = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload_template = {"performUpsert": {"fieldsToMergeOn": [{"fieldName": n} for n in PRIMARY_KEY_FIELDS]}}

    sent = 0
    for i in range(0, len(payload_records), 10):
        batch = payload_records[i:i+10]
        payload = dict(payload_template)
        payload["records"] = batch
        res = requests.patch(upsert_url, headers=headers_json, data=json.dumps(payload), timeout=30)
        if res.status_code >= 300:
            print("❌ Airtable error:", res.status_code, res.text)
            print("   Example record:", json.dumps(batch[0], ensure_ascii=False))
            raise SystemExit(1)
        sent += len(batch)

    print(f"✅ [{schema}] Upserted {sent} records into Airtable table '{table}'.")

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Build CSV → git push → Airtable upsert")
    parser.add_argument("--skip-build", action="store_true", help="Skip CSV build step")
    parser.add_argument("--force-snapshot", action="store_true", help="Overwrite this month's snapshot")
    parser.add_argument("--skip-airtable", action="store_true", help="Skip Airtable upsert step")
    parser.add_argument("--commit-msg", default="Update rent CSV and snapshot", help="Custom git commit message")
    args = parser.parse_args()

    print(f"[RUNNING] {__file__}")

    env = get_env()

    # 1) Build/ensure CSV
    build_csv_if_needed(skip_build=args.skip_build)

    # 2) Snapshot (monthly)
    write_monthly_snapshot(force_snapshot=args.force_snapshot)

    # 3) Git push
    ensure_git_setup()
    git_commit_and_push(args.commit_msg)

    # 4) Airtable upsert
    if not args.skip_airtable:
        print("↗️  Pushing latest CSV to Airtable…")
        airtable_upsert_from_csv(env)
    else:
        print("⏭️  Skipping Airtable upsert (per flag).")

if __name__ == "__main__":
    main()







