#!/usr/bin/env python3
"""
push_csv.py – Fetch Airtable (Rents + City Overrides) -> build unified CSV -> (optionally) commit & push

Output schema (exact order):
Date, City/Neighborhood, Configuration, Average Price (USD),
Utilities, Groceries, Internet, Cell Phone, Dining, Entertainment, Travel
"""

import os, sys, csv, re, time, unicodedata, subprocess, requests, argparse
from datetime import datetime
from urllib.parse import quote
from pathlib import Path

# ====== Paths ======
REPO_DIR = Path(__file__).resolve().parent
DATA_DIR = REPO_DIR / "data"
CSV_PATH = DATA_DIR / "panama_rent_averages.csv"
HIST_DIR = DATA_DIR / "history"
ENV_PATH = REPO_DIR / ".env.local"
print(f"[RUNNING] {__file__}")

# ====== CSV schema ======
CSV_HEADERS = [
    "Date","City/Neighborhood","Configuration","Average Price (USD)",
    "Utilities","Groceries","Internet","Cell Phone","Dining","Entertainment","Travel",
]
CATEGORIES = ["Utilities","Groceries","Internet","Cell Phone","Dining","Entertainment","Travel"]

# ---- Category normalization map (incoming -> canonical) ----
CAT_MAP = {
    "utilities": "Utilities",
    "utility": "Utilities",
    "groceries": "Groceries",
    "grocery": "Groceries",
    "internet": "Internet",
    "cell phone": "Cell Phone",
    "cellphone": "Cell Phone",
    "mobile": "Cell Phone",
    "dining": "Dining",
    "dining out": "Dining",
    "restaurants": "Dining",
    "entertainment": "Entertainment",
    "travel": "Travel",
    "transportation": "Travel",
}

# ---- Neighborhood → City aliases (keys lowercased/slugged) ----
ALIASES = {
    "avenida balboa": "Panama City",
    "casco viejo": "Panama City",
    # add more if needed, e.g. "costa del este": "Panama City"
}

# ====== env loader ======
def parse_env_file(path: Path):
    if not path.exists():
        sys.exit(f".env.local not found at {path}")
    secrets = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = re.sub(r"\s+#.*$", "", v).strip().strip('"').strip("'")
        secrets[k.strip()] = v
    return secrets

# ====== helpers ======
def normalize_single(v):
    return v[0] if isinstance(v, list) and len(v) == 1 else v

def looks_like_rec_ids(v):
    return isinstance(v, list) and v and all(isinstance(x, str) and x.startswith("rec") for x in v)

def coalesce(*vals):
    for v in vals:
        if v is None: continue
        if isinstance(v, str) and v.strip() == "": continue
        if isinstance(v, list) and len(v) == 0: continue
        return v
    return None

def slug(s: str) -> str:
    if s is None: return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).lower()
    return s

def norm_cat(label: str) -> str | None:
    """Normalize string label to our canonical CSV header."""
    if not label: return None
    key = re.sub(r"\s+", " ", str(label).strip().lower())
    return CAT_MAP.get(key)

def compute_configuration(fields):
    raw = fields.get("config_label")
    if looks_like_rec_ids(raw):
        beds  = normalize_single(fields.get("bedrooms (from config_label)", ""))
        baths = normalize_single(fields.get("bathrooms (from config_label)", ""))
        return f"{beds} BR / {baths} BA" if beds != "" and baths != "" else ""
    return normalize_single(raw) or ""

# ====== Airtable fetch ======
def fetch_all_records(base, table, token, view=None, fields=None):
    url = f"https://api.airtable.com/v0/{base}/{quote(table, safe='')}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {}
    if view: params["view"] = view
    if fields:
        for i, fld in enumerate(fields):
            params[f"fields[{i}]"] = fld
    offset = None
    while True:
        if offset: params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            raise SystemExit(f"Airtable fetch failed {resp.status_code}: {resp.text[:600]}")
        data = resp.json()
        for rec in data.get("records", []):
            yield rec
        offset = data.get("offset")
        if not offset: break

RENTS_FIELDS = [
    "effective_date","city","config_label",
    "bedrooms (from config_label)","bathrooms (from config_label)",
    "average_price_usd",
]

# We cannot rely on 'category' label (it returns rec IDs). Use key field: "key_city_category".
OVERRIDES_FIELDS = [
    "city","city_link",
    "category",                  # rec IDs (ignored for label)
    "key_city_category",         # e.g. "Avenida Balboa | Utilities"
    "final_value_usd","override_usd",
    "active","effective_date",
]

def fetch_rents(env):
    rows = []
    for rec in fetch_all_records(
        env["AIRTABLE_BASE"], env["AIRTABLE_RENTS_TABLE"], env["AIRTABLE_TOKEN"],
        view=(env.get("AIRTABLE_RENTS_VIEW") or None), fields=RENTS_FIELDS
    ):
        f = rec.get("fields", {})
        rows.append({
            "Date":               normalize_single(f.get("effective_date", "")),
            "City/Neighborhood":  str(normalize_single(f.get("city", ""))).strip(),
            "Configuration":      compute_configuration(f),
            "Average Price (USD)":normalize_single(f.get("average_price_usd", 0)) or 0,
        })
    return rows

def build_canonical_city_map(rents_rows):
    return { slug(r["City/Neighborhood"]): str(r["City/Neighborhood"]) for r in rents_rows }

def canonicalize_city(label, canonical_map):
    s = slug(label)
    if not s: return None
    if s in canonical_map:  # exact rents city
        return canonical_map[s]
    if s in ALIASES:        # neighborhood → city
        target = slug(ALIASES[s])
        return canonical_map.get(target, ALIASES[s])
    return None

def parse_category_from_key(key_val: str) -> str | None:
    """
    key_city_category is like "Avenida Balboa | Utilities".
    Return the part after the last ' | '.
    """
    if not key_val:
        return None
    parts = [p.strip() for p in str(key_val).split("|")]
    if not parts:
        return None
    label = parts[-1]  # after last |
    return norm_cat(label)

def fetch_overrides(env, canonical_map, debug=False):
    piv = {}
    total = skipped = 0
    labels_before, labels_after = set(), set()
    raw_cat_seen = set()

    for rec in fetch_all_records(
        env["AIRTABLE_BASE"], env["AIRTABLE_OVERRIDES_TABLE"], env["AIRTABLE_TOKEN"],
        view=(env.get("AIRTABLE_OVERRIDES_VIEW") or None), fields=OVERRIDES_FIELDS
    ):
        total += 1
        f = rec.get("fields", {})

        if "active" in f and not f.get("active"):
            skipped += 1; continue

        raw_city = coalesce(normalize_single(f.get("city")), normalize_single(f.get("city_link")))
        if not raw_city or (isinstance(raw_city, str) and raw_city.startswith("rec")):
            skipped += 1; continue

        labels_before.add(str(raw_city).strip())
        canon = canonicalize_city(raw_city, canonical_map)
        if not canon:
            skipped += 1; continue

        key_val = normalize_single(f.get("key_city_category", "")) or ""
        raw_cat_seen.add(key_val)
        cat = parse_category_from_key(key_val)
        if not cat:
            skipped += 1; continue

        amt = coalesce(f.get("final_value_usd"), f.get("override_usd")) or 0

        labels_after.add(canon)
        if canon not in piv:
            piv[canon] = {c: 0 for c in CATEGORIES}
        piv[canon][cat] = amt

    if debug:
        # Show the unique category keys we parsed, to quickly spot mismatches
        samples = sorted(list(raw_cat_seen))[:10]
        print(f"DEBUG key_city_category samples: {samples}")
    return piv, total, skipped, labels_before, labels_after

def merge_rents_overrides(rents, overrides_by_city):
    merged, applied = [], 0
    for r in rents:
        city = str(r["City/Neighborhood"]).strip()
        o = overrides_by_city.get(city, {})
        if o: applied += 1
        row = dict(r)
        for c in CATEGORIES:
            row[c] = o.get(c, 0)
        merged.append(row)
    return merged, applied

# ====== IO / Git ======
def write_csv_atomic(rows, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_HEADERS)
        w.writeheader(); w.writerows(rows)
    os.replace(tmp, path)
    print(f"✅ Wrote {len(rows)} rows to {path}")

def run(cmd, cwd=None):
    return subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)

def git_commit_and_push(repo_dir: Path, filepaths, message: str, push=True):
    try:
        filepaths = [str(p) for p in (filepaths if isinstance(filepaths, (list,tuple)) else [filepaths])]
        run(["git", "add"] + filepaths, cwd=repo_dir)
        diff = subprocess.run(["git", "status", "--porcelain", "--"] + filepaths, cwd=repo_dir, capture_output=True, text=True)
        if not diff.stdout.strip():
            print("ℹ️ No changes to commit."); return
        run(["git", "commit", "-m", message], cwd=repo_dir)
        if push:
            run(["git", "push"], cwd=repo_dir); print("🚀 Pushed to origin.")
        else:
            print("📝 Committed locally (no push).")
    except subprocess.CalledProcessError as e:
        print("❌ Git error:\n", e.stderr or e.stdout); sys.exit(1)

def snapshot_path_from_rows(rows):
    yyyymm = datetime.today().strftime("%Y-%m")
    dates = []
    for r in rows:
        d = str(r.get("Date","")).strip()
        for fmt in ("%Y-%m-%d","%m/%d/%Y","%Y/%m/%d"):
            try:
                dates.append(datetime.strptime(d, fmt)); break
            except Exception:
                pass
    if dates:
        yyyymm = max(dates).strftime("%Y-%m")
    return HIST_DIR / f"panama_rent_averages_{yyyymm}.csv"

# ====== Main ======
def main():
    env = parse_env_file(ENV_PATH)

    ap = argparse.ArgumentParser(description="Fetch Airtable, merge, write CSV (+ monthly snapshot), commit & push.")
    ap.add_argument("--no-snapshot", action="store_true")
    ap.add_argument("--force-snapshot", action="store_true")
    ap.add_argument("--no-push", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("-m", "--commit-message", default=None)
    args = ap.parse_args()

    rents = fetch_rents(env)
    canonical_map = build_canonical_city_map(rents)
    overrides, total, skipped, before, after = fetch_overrides(env, canonical_map, debug=args.debug)
    rows, applied = merge_rents_overrides(rents, overrides)

    write_csv_atomic(rows, CSV_PATH)
    files_to_commit = [CSV_PATH]

    if not args.no_snapshot:
        snap_path = snapshot_path_from_rows(rows)
        snap_path.parent.mkdir(parents=True, exist_ok=True)
        if snap_path.exists() and not args.force_snapshot:
            print(f"ℹ️ Snapshot exists, skipping: {snap_path.name} (use --force-snapshot to overwrite)")
        else:
            write_csv_atomic(rows, snap_path)
        files_to_commit.append(snap_path)

    if args.debug:
        print(f"DEBUG overrides: total={total}, skipped={skipped}, mapped_cities={len(after)}")
        if before: print("DEBUG sample override labels:", sorted(list(before))[:8])
        if after:  print("DEBUG sample canonical cities:", sorted(list(after))[:8])
        print(f"DEBUG rows with any overrides applied: {applied} / {len(rows)}")
        # show one merged row with non-zero categories
        for r in rows:
            if any(str(r.get(c,'')).strip() not in ('','0','0.0') for c in CATEGORIES):
                print("DEBUG example merged row:", {k: r[k] for k in ['City/Neighborhood','Configuration'] + CATEGORIES})
                break
        else:
            print("DEBUG no non-zero merged rows found.")

    msg = args.commit_message or f"Update rent CSV (+snapshot) ({time.strftime('%Y-%m-%d %H:%M')})"
    git_commit_and_push(REPO_DIR, files_to_commit, msg, push=not args.no_push)

if __name__ == "__main__":
    main()





