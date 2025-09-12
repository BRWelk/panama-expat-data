#!/usr/bin/env python3
import json, re, sys
from pathlib import Path
from urllib.parse import quote
import requests

REPO_DIR = Path(__file__).resolve().parent
ENV_PATH = REPO_DIR / ".env.local"

def parse_env_file(path: Path):
    env = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1)
        env[k.strip()] = re.sub(r"\s+#.*$", "", v).strip().strip('"').strip("'")
    return env

def fetch_all(base, table, token, view=None, fields=None, limit=10):
    url = f"https://api.airtable.com/v0/{base}/{quote(table, safe='')}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {}
    if view: params["view"] = view
    if fields:
        for i, f in enumerate(fields): params[f"fields[{i}]"] = f
    out, offset = [], None
    while True and len(out) < limit:
        if offset: params["offset"] = offset
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            out.append(rec)
            if len(out) >= limit: break
        offset = data.get("offset")
        if not offset or len(out) >= limit: break
    return out

def main():
    env = parse_env_file(ENV_PATH)
    fields = ["city","city_link","category","range_label (from category)",
              "final_value_usd","override_usd","active","effective_date"]
    recs = fetch_all(env["AIRTABLE_BASE"], env["AIRTABLE_OVERRIDES_TABLE"], env["AIRTABLE_TOKEN"],
                     view=(env.get("AIRTABLE_OVERRIDES_VIEW") or None), fields=fields, limit=10)
    for i, rec in enumerate(recs, 1):
        print(f"\n--- OVERRIDE #{i} ---")
        print(json.dumps(rec.get("fields", {}), indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
