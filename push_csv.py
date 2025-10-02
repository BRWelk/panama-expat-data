#!/usr/bin/env python3
# push_csv.py — build CSV → git commit/push → Airtable update-by-lookup (no writing computed fields)

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
    run(["git", "rev-parse", "--is-inside-work-tree"], cwd=str(REPO_DIR))

def git_commit_and_push(commit_msg: str):
    run(["git", "add", str(CSV_OUT)], cwd=str(REPO_DIR))
    for p in SNAPSHOT_DIR.glob("panama_rent_averages_*.csv"):
        run(["git", "add", str(p)], cwd=str(REPO_DIR))
    res = subprocess.run(["git", "commit", "-m", commit_msg], cwd=str(REPO_DIR), capture_output=True, text=True)
    if "nothing to commit" in (res.stdout + res.stderr).lower():
        print("ℹ️ Nothing to commit.")
    else:
        print(res.stdout.strip() or res.stderr.strip())
    run(["git", "push", "origin", "HEAD"], cwd=str(REPO_DIR))
    print("🚀 Pushed to origin.")

# ---------------------------
# Airtable update-by-lookup (PATCH only editable fields)
# ---------------------------
def airtable_update_by_lookup(env: dict) -> None:
    token = env.get("AIRTABLE_TOKEN")
    base  = env.get("AIRTABLE_BASE")
    table = env.get("AIRTABLE_RENTS_TABLE") or env.get("AIRTABLE_TABLE") or "Rent"

    if not (token and base and table):
        print("⚠️  Skipping Airtable update: missing AIRTABLE_TOKEN / AIRTABLE_BASE / table id/name")
        return
    if not CSV_OUT.exists():
        print(f"⚠️  Skipping Airtable update: CSV not found at {CSV_OUT}")
        return

    # Editable target fields in your table
    TARGET_NUM_FIELD = "average_price_usd"   # numeric
    TARGET_CITY      = "city"                # text
    TARGET_CONFIG    = "config_label"        # likely computed/lookup (do NOT write)
    TARGET_DATE      = "effective_date"      # date
    OPTIONAL_CURRENCY= "currency"            # text
    OPTIONAL_ACTIVE  = "active"              # checkbox/bool

    # Load CSV
    with open(CSV_OUT, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    base_url = f"https://api.airtable.com/v0/{base}/{quote(str(table), safe='')}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    def fmt_date(val: str) -> str:
        if not val: return val
        for fmt in ("%m/%d/%Y","%Y-%m-%d","%m/%d/%y"):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except:
                pass
        return val

    def to_float(v):
        try:
            return float(str(v).replace(",", ""))
        except:
            return v

    total = len(rows)
    updated = 0
    missing = 0
    failures = 0

    for r in rows:
        csv_city   = r.get("City/Neighborhood","")
        csv_cfg    = r.get("Configuration","")
        csv_date   = fmt_date(r.get("Date",""))
        csv_price  = to_float(r.get("Average Price (USD)",""))

        if not (csv_city and csv_cfg and csv_date):
            # Skip incomplete rows
            continue

        # Build filterByFormula to find existing record
        # Compare date as string "YYYY-MM-DD"
        formula = f"AND(" \
                  f"{{{TARGET_CITY}}}='{csv_city.replace(\"'\",\"\\'\")}'," \
                  f"{{{TARGET_CONFIG}}}='{csv_cfg.replace(\"'\",\"\\'\")}'," \
                  f"{{{TARGET_DATE}}}='{csv_date}')"  # Airtable parses ISO date string

        try:
            search = requests.get(
                base_url,
                headers=headers,
                params={"maxRecords": 1, "filterByFormula": formula},
                timeout=30
            )
            if search.status_code >= 300:
                print("❌ Lookup error:", search.status_code, search.text)
                failures += 1
                continue

            data = search.json()
            recs = data.get("records", [])
            if not recs:
                # Cannot create because config_label is non-writable/computed
                print(f"⚠️  No existing record for ({csv_city}, {csv_cfg}, {csv_date}); skipped create.")
                missing += 1
                continue

            rec_id = recs[0]["id"]

            # Prepare PATCH with only editable fields
            patch_fields = { TARGET_NUM_FIELD: csv_price }
            # Optional defaults if the fields exist
            # (We don't know schema perfectly here, so try & ignore 422 due to unknown field)
            patch_fields[OPTIONAL_CURRENCY] = "USD"
            patch_fields[OPTIONAL_ACTIVE]   = True

            patch = requests.patch(
                f"{base_url}/{rec_id}",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                data=json.dumps({"fields": patch_fields, "typecast": True}),
                timeout=30
            )
            if patch.status_code >= 300:
                # Retry without optional fields in case they don't exist
                patch_fields_fallback = { TARGET_NUM_FIELD: csv_price }
                patch2 = requests.patch(
                    f"{base_url}/{rec_id}",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    data=json.dumps({"fields": patch_fields_fallback, "typecast": True}),
                    timeout=30
                )
                if patch2.status_code >= 300:
                    print("❌ Update error:", patch2.status_code, patch2.text)
                    failures += 1
                    continue

            updated += 1

        except Exception as e:
            print("❌ Exception during update:", repr(e))
            failures += 1

    print(f"✅ Airtable update complete. Updated: {updated} | Missing (not created): {missing} | Failures: {failures} | Total CSV rows: {total}")

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Build CSV → git push → Airtable update-by-lookup")
    parser.add_argument("--skip-build", action="store_true", help="Skip CSV build step")
    parser.add_argument("--force-snapshot", action="store_true", help="Overwrite this month's snapshot")
    parser.add_argument("--skip-airtable", action="store_true", help="Skip Airtable update step")
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

    # 4) Airtable update-by-lookup
    if not args.skip_airtable:
        print("↗️  Pushing latest CSV to Airtable…")
        airtable_update_by_lookup(env)
    else:
        print("⏭️  Skipping Airtable update (per flag).")

if __name__ == "__main__":
    main()









