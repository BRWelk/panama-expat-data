#!/usr/bin/env python3
# push_csv.py — commit/push CSV to GitHub, then sync to Airtable with linked records (Cities, Configs → Rents)

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
    env = dict(file_env)
    for k in list(file_env.keys()):
        if k in os.environ and os.environ[k]:
            env[k] = os.environ[k]
    return env

# ---------------------------
# CSV BUILD (no-op unless missing; keep your generator elsewhere)
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
# Airtable helpers
# ---------------------------
def at_base_url(base, table): return f"https://api.airtable.com/v0/{base}/{quote(str(table), safe='')}"
def h_json(token): return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
def h_get(token):  return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def fmt_date(val: str) -> str:
    if not val: return val
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%m/%d/%y"):
        try: return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except: pass
    return val

def to_float(v):
    try: return float(str(v).replace(",", ""))
    except: return v

def find_record_id_by_name(token, base, table, primary_field, name):
    """Find a record by its primary field text equal to name. Return record id or None."""
    url = at_base_url(base, table)
    name_safe = (name or "").replace("'", "''")
    formula = f"{{{primary_field}}}='{name_safe}'"
    r = requests.get(url, headers=h_get(token), params={"maxRecords": 1, "filterByFormula": formula}, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Lookup error [{table}]: {r.status_code} {r.text}")
    recs = r.json().get("records", [])
    return recs[0]["id"] if recs else None

def create_record_by_name(token, base, table, primary_field, name, extra_fields=None):
    """Create a record setting the primary field to name."""
    url = at_base_url(base, table)
    fields = {primary_field: name}
    if extra_fields:
        fields.update(extra_fields)
    r = requests.post(url, headers=h_json(token), data=json.dumps({"fields": fields, "typecast": True}), timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Create error [{table}]: {r.status_code} {r.text}")
    return r.json()["id"]

def get_or_create_linked(token, base, table, primary_field, name, extra_fields=None):
    """Return record id in linked table; create it if missing."""
    rec_id = find_record_id_by_name(token, base, table, primary_field, name)
    if rec_id: return rec_id
    return create_record_by_name(token, base, table, primary_field, name, extra_fields=extra_fields)

# ---------------------------
# Airtable sync with linked records
# ---------------------------
def airtable_sync_linked(env: dict) -> None:
    token = env.get("AIRTABLE_TOKEN")
    base  = env.get("AIRTABLE_BASE")
    rents_table   = env.get("AIRTABLE_RENTS_TABLE")   or env.get("AIRTABLE_TABLE") or "Rents"
    cities_table  = env.get("AIRTABLE_CITIES_TABLE")  or "Cities"
    configs_table = env.get("AIRTABLE_CONFIGS_TABLE") or "Configs"

    # primary field names in those tables (adjust if different)
    CITY_PRIMARY    = env.get("AIRTABLE_CITIES_PRIMARY")  or "city"
    CONFIG_PRIMARY  = env.get("AIRTABLE_CONFIGS_PRIMARY") or "config_label"

    if not (token and base and rents_table and cities_table and configs_table):
        print("⚠️  Skipping Airtable sync: missing AIRTABLE_* env values")
        return
    if not CSV_OUT.exists():
        print(f"⚠️  Skipping Airtable sync: CSV not found at {CSV_OUT}")
        return

    # field names on Rents table
    F_CITY   = "city"             # linked to Cities
    F_CONFIG = "config_label"     # linked to Configs
    F_DATE   = "effective_date"   # date
    F_PRICE  = "average_price_usd"  # number
    F_CURR   = "currency"         # text (optional)
    F_ACTIVE = "active"           # checkbox (optional)

    # cache for lookups to reduce API calls
    city_cache = {}
    config_cache = {}

    # load CSV
    with open(CSV_OUT, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    rents_url = at_base_url(base, rents_table)
    total = len(rows)
    updated = 0
    created = 0
    failures = 0

    for r in rows:
        city_name  = (r.get("City/Neighborhood") or "").strip()
        cfg_label  = (r.get("Configuration") or "").strip()
        eff_date   = fmt_date(r.get("Date") or "")
        price_val  = to_float(r.get("Average Price (USD)"))

        if not (city_name and cfg_label and eff_date):
            continue

        # --- ensure linked City & Config exist, get their record IDs
        try:
            if city_name not in city_cache:
                city_cache[city_name] = get_or_create_linked(token, base, cities_table, CITY_PRIMARY, city_name)
            city_id = city_cache[city_name]

            if cfg_label not in config_cache:
                config_cache[cfg_label] = get_or_create_linked(token, base, configs_table, CONFIG_PRIMARY, cfg_label)
            config_id = config_cache[cfg_label]
        except Exception as e:
            print("❌ Linked record error:", repr(e))
            failures += 1
            continue

        # --- find existing Rents record (match by linked names + date)
        # We can match by the *display* text of linked fields using ARRAYJOIN in formula.
        safe_city = city_name.replace("'", "''")
        safe_cfg  = cfg_label.replace("'", "''")
        formula = (
            f"AND("
            f"ARRAYJOIN({{{F_CITY}}})='{safe_city}',"
            f"ARRAYJOIN({{{F_CONFIG}}})='{safe_cfg}',"
            f"{{{F_DATE}}}='{eff_date}'"
            f")"
        )

        try:
            search = requests.get(
                rents_url, headers=h_get(token),
                params={"maxRecords": 1, "filterByFormula": formula},
                timeout=30
            )
            if search.status_code >= 300:
                print("❌ Lookup error:", search.status_code, search.text)
                failures += 1
                continue

            recs = search.json().get("records", [])
            fields_payload = {
                F_CITY:   [{"id": city_id}],
                F_CONFIG: [{"id": config_id}],
                F_DATE:    eff_date,
                F_PRICE:   price_val,
            }
            # optional defaults if present in schema
            fields_payload[F_CURR] = "USD"
            fields_payload[F_ACTIVE] = True

            if recs:
                # UPDATE existing
                rec_id = recs[0]["id"]
                r_patch = requests.patch(f"{rents_url}/{rec_id}", headers=h_json(token),
                                         data=json.dumps({"fields": fields_payload, "typecast": True}), timeout=30)
                if r_patch.status_code >= 300:
                    # fallback without optional fields
                    minimal = {F_CITY: [{"id": city_id}], F_CONFIG: [{"id": config_id}], F_DATE: eff_date, F_PRICE: price_val}
                    r_patch2 = requests.patch(f"{rents_url}/{rec_id}", headers=h_json(token),
                                              data=json.dumps({"fields": minimal, "typecast": True}), timeout=30)
                    if r_patch2.status_code >= 300:
                        print("❌ Update error:", r_patch2.status_code, r_patch2.text)
                        failures += 1
                        continue
                updated += 1
            else:
                # CREATE new
                r_post = requests.post(rents_url, headers=h_json(token),
                                       data=json.dumps({"fields": fields_payload, "typecast": True}), timeout=30)
                if r_post.status_code >= 300:
                    # fallback without optional fields
                    minimal = {F_CITY: [{"id": city_id}], F_CONFIG: [{"id": config_id}], F_DATE: eff_date, F_PRICE: price_val}
                    r_post2 = requests.post(rents_url, headers=h_json(token),
                                            data=json.dumps({"fields": minimal, "typecast": True}), timeout=30)
                    if r_post2.status_code >= 300:
                        print("❌ Create error:", r_post2.status_code, r_post2.text)
                        failures += 1
                        continue
                created += 1

        except Exception as e:
            print("❌ Exception during create/update:", repr(e))
            failures += 1

    print(f"✅ Airtable sync complete. Updated: {updated} | Created: {created} | Failures: {failures} | Total CSV rows: {total}")

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Build CSV → git push → Airtable sync with linked records")
    parser.add_argument("--skip-build", action="store_true", help="Skip CSV build step")
    parser.add_argument("--force-snapshot", action="store_true", help="Overwrite this month's snapshot")
    parser.add_argument("--skip-airtable", action="store_true", help="Skip Airtable sync step")
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

    # 4) Airtable sync (linked records)
    if not args.skip_airtable:
        print("↗️  Syncing CSV to Airtable (Cities/Configs → Rents)…")
        airtable_sync_linked(env)
    else:
        print("⏭️  Skipping Airtable sync (per flag).")

if __name__ == "__main__":
    main()










