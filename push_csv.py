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
# Airtable upsert
# ---------------------------
def airtable_upsert_from_csv(env: dict) -> None:
    token = env.get("AIRTABLE_TOKEN")
    base  = env.get("AIRTABLE_BASE")
    # Prefer table ID vars; fall back to name var; then "Rent"
    table = env.get("AIRTABLE_RENTS_TABLE") or env.get("AIRTABLE_TABLE") or "Rent"

    if not (token and base and table):
        print("⚠️  Skipping Airtable upsert: missing AIRTABLE_TOKEN / AIRTABLE_BASE / (AIRTABLE_RENTS_TABLE|AIRTABLE_TABLE)")
        return
    if not CSV_OUT.exists():
        print(f"⚠️  Skipping Airtable upsert: CSV not found at {CSV_OUT}")
        return

    FIELD_MAP = {
        "Date": "Date",
        "City/Neighborhood": "City/Neighborhood",
        "Configuration": "Configuration",
        "Average Price (USD)": "Average Price (USD)",
        "Utilities": "Utilities",
        "Groceries": "Groceries",
        "Internet": "Internet",
        "Cell Phone": "Cell Phone",
        "Dining": "Dining",
        "Entertainment": "Entertainment",
        "Travel": "Travel",
    }
    PRIMARY_KEY_FIELDS = ["Date", "City/Neighborhood", "Configuration"]
    NUMERIC_FIELDS = {"Average Price (USD)","Utilities","Groceries","Internet","Cell Phone","Dining","Entertainment","Travel"}

    def coerce(name, val):
        if val is None or val == "": 
            return None
        if name == "Date":
            for fmt in ("%m/%d/%Y","%Y-%m-%d","%m/%d/%y"):
                try:
                    return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
                except:
                    pass
            return val
        if name in NUMERIC_FIELDS:
            try:
                return float(str(val).replace(",", ""))
            except:
                return val
        return val

    with open(CSV_OUT, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    records = []
    for r in rows:
        fields = {}
        for csv_name, at_name in FIELD_MAP.items():
            if csv_name in r:
                fields[at_name] = coerce(at_name, r[csv_name])
        if fields:
            records.append({"fields": fields})

    if not records:
        print("⚠️  No rows to upsert (CSV empty?).")
        return

    url = f"https://api.airtable.com/v0/{base}/{quote(str(table), safe='')}?typecast=true"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload_template = {
        "performUpsert": {"fieldsToMergeOn": [{"fieldName": k} for k in PRIMARY_KEY_FIELDS]}
    }

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    sent = 0
    for batch in chunks(records, 10):  # Airtable limit per request
        payload = dict(payload_template)
        payload["records"] = batch
        res = requests.patch(url, headers=headers, data=json.dumps(payload), timeout=30)
        if res.status_code >= 300:
            print("❌ Airtable error:", res.status_code, res.text)
            sys.exit(1)
        sent += len(batch)

    print(f"✅ Upserted {sent} records into Airtable table '{table}' (base {base}).")

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

    # Load env
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






