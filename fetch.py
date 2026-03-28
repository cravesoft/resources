"""
Fetch raw data for non-renewable natural resources from public APIs and downloads.

Sources:
  EIA International Energy Statistics API  — oil, gas, coal production & reserves
    (free API key at https://www.eia.gov/opendata/register.php)
  USGS Mineral Commodity Summaries 2024    — critical minerals, REEs, metals, uranium
    (public PDF + individual Excel tables, no key needed)
  World Bank Commodity Price Data          — benchmark spot prices
    (public Excel download, no key needed)

Outputs: raw/ directory with JSON, PDF, and Excel files.

Usage:
    uv run python fetch.py               # fetch everything
    uv run python fetch.py --force       # re-fetch ignoring cache
    uv run python fetch.py --no-eia      # skip EIA (no API key)
"""

import argparse
import json
import os
import time
import httpx
from dotenv import load_dotenv

load_dotenv()

EIA_KEY = os.environ.get("EIA_API_KEY", "")
EIA_BASE = "https://api.eia.gov/v2/international/data/"

# EIA product + activity codes
# activityId: 1=production, 3=reserves
EIA_SERIES = [
    {"key": "crude-oil-production",   "productId": "57", "activityId": "1", "unit": "Mb/d"},
    {"key": "crude-oil-reserves",     "productId": "57", "activityId": "3", "unit": "Gb"},
    {"key": "natural-gas-production", "productId": "26", "activityId": "1", "unit": "bcm"},
    {"key": "natural-gas-reserves",   "productId": "26", "activityId": "3", "unit": "tcm"},
    {"key": "coal-production",        "productId": "7",  "activityId": "1", "unit": "Mt"},
    {"key": "coal-reserves",          "productId": "7",  "activityId": "3", "unit": "Mt"},
]

# USGS Mineral Commodity Summaries 2025 — ScienceBase data release
# https://www.sciencebase.gov/catalog/item/677eaf95d34e760b392c4970
USGS_SCIENCEBASE_ID = "677eaf95d34e760b392c4970"
USGS_FILES = [
    # World production + reserves by country for all commodities
    (
        "usgs_world_data.zip",
        "https://www.sciencebase.gov/catalog/file/get/677eaf95d34e760b392c4970"
        "?f=__disk__70%2Fcf%2F36%2F70cf3695ad9405884df4a4758e4b609013e3fb1e",
    ),
    # Salient statistics grouped by commodity (production, trade, price, reserves)
    (
        "usgs_salient_grouped.zip",
        "https://www.sciencebase.gov/catalog/file/get/677eaf95d34e760b392c4970"
        "?f=__disk__e1%2Fab%2F32%2Fe1ab32df12627a6bd9b4f888ecc1d5d048529dde",
    ),
    # Industry trends and statistics
    (
        "usgs_industry_trends.zip",
        "https://www.sciencebase.gov/catalog/file/get/677eaf95d34e760b392c4970"
        "?f=__disk__95%2F04%2F37%2F950437abd793bc931d9f6cfd3c0869b108048b28",
    ),
]

# USGS MCS 2025 full PDF (backup reference, useful for parse stage)
USGS_PDF_URL = "https://pubs.usgs.gov/periodicals/mcs2025/mcs2025.pdf"

# World Bank commodity prices (monthly historical Excel) — updated March 2026
WORLDBANK_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/"
    "CMO-Historical-Data-Monthly.xlsx"
)


def fetch_file(url: str, out_path: str, force: bool = False, binary: bool = True) -> bool:
    """Download url → out_path. Returns True on success."""
    if not force and os.path.exists(out_path):
        size = os.path.getsize(out_path)
        print(f"  CACHED  {out_path} ({size:,} bytes)")
        return True
    try:
        with httpx.stream("GET", url, timeout=120, follow_redirects=True,
                          headers={"User-Agent": "Mozilla/5.0 (research bot)"}) as r:
            r.raise_for_status()
            with open(out_path, "wb" if binary else "w") as f:
                for chunk in r.iter_bytes(chunk_size=65536):
                    f.write(chunk)
        size = os.path.getsize(out_path)
        print(f"  OK      {out_path} ({size:,} bytes)")
        return True
    except Exception as e:
        print(f"  FAILED  {out_path}: {e}")
        if os.path.exists(out_path):
            os.remove(out_path)
        return False


def fetch_eia_series(series: dict, force: bool = False) -> bool:
    """Fetch one EIA international data series and save as JSON."""
    key = series["key"]
    out_path = f"raw/eia_{key}.json"
    if not force and os.path.exists(out_path):
        with open(out_path) as f:
            data = json.load(f)
        n = len(data.get("response", {}).get("data", []))
        print(f"  CACHED  {out_path} ({n} rows)")
        return True
    params = {
        "api_key": EIA_KEY,
        "frequency": "annual",
        "data[0]": "value",
        "facets[activityId][]": series["activityId"],
        "facets[productId][]": series["productId"],
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": "0",
        "length": "5000",
    }
    try:
        r = httpx.get(EIA_BASE, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        n = len(data.get("response", {}).get("data", []))
        with open(out_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"  OK      {out_path} ({n} rows)")
        return True
    except Exception as e:
        print(f"  FAILED  {out_path}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Fetch raw resource data")
    parser.add_argument("--force",  action="store_true", help="Re-fetch ignoring cache")
    parser.add_argument("--no-eia", action="store_true", help="Skip EIA API calls")
    args = parser.parse_args()

    os.makedirs("raw", exist_ok=True)

    # ── EIA API ──────────────────────────────────────────────────────────────
    print("\n=== EIA International Energy Statistics ===")
    if args.no_eia:
        print("  Skipped (--no-eia)")
    elif not EIA_KEY:
        print("  Skipped — EIA_API_KEY not set in .env")
        print("  Register free at: https://www.eia.gov/opendata/register.php")
    else:
        ok = skipped = failed = 0
        for series in EIA_SERIES:
            result = fetch_eia_series(series, force=args.force)
            if result:
                ok += 1
            else:
                failed += 1
            time.sleep(0.5)
        print(f"  {ok} fetched, {failed} failed")

    # ── USGS Mineral Commodity Summaries 2025 ───────────────────────────────
    print("\n=== USGS Mineral Commodity Summaries 2025 (ScienceBase) ===")
    ok = failed = 0
    for name, url in USGS_FILES:
        if fetch_file(url, f"raw/{name}", force=args.force):
            ok += 1
        else:
            failed += 1
        time.sleep(0.5)
    print(f"  {ok} fetched, {failed} failed")

    print("\n=== USGS MCS 2025 Full PDF ===")
    fetch_file(USGS_PDF_URL, "raw/usgs_mcs2025.pdf", force=args.force)

    # ── World Bank commodity prices ──────────────────────────────────────────
    print("\n=== World Bank Commodity Price Data ===")
    fetch_file(WORLDBANK_URL, "raw/worldbank_prices.xlsx", force=args.force)

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n=== Summary ===")
    files = sorted(os.listdir("raw"))
    total_size = sum(os.path.getsize(f"raw/{f}") for f in files)
    print(f"  {len(files)} files in raw/  ({total_size / 1_000_000:.1f} MB total)")
    for f in files:
        size = os.path.getsize(f"raw/{f}")
        print(f"    {f:<45} {size:>10,} bytes")


if __name__ == "__main__":
    main()
