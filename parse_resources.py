"""
Parse raw data into pages/<slug>.md (one Markdown file per resource-country pair).

Reads:
  resources.json                — master list
  raw/eia_*-production.json     — EIA fossil fuel production by country/year
  raw/usgs_world_data.zip       — USGS mine production + reserves (minerals)
  raw/worldbank_prices.xlsx     — monthly commodity prices (World Bank Pink Sheet)

Static data embedded for:
  Fossil fuel proved reserves   — BP Statistical Review of World Energy 2024
  Uranium production + reserves — World Nuclear Association 2024
  Prices not in World Bank      — LME / industry sources (2023 averages)

EIA unit codes used here:
  Crude oil: TBPD  = thousands of barrels per day
  Nat. gas:  BCM   = billion cubic metres per year
  Coal:      MT    = 1000 metric tons per year  (NOT million tonnes!)

Usage:
    uv run python parse_resources.py               # all resources
    uv run python parse_resources.py --start 0 --end 5
    uv run python parse_resources.py --force       # overwrite existing pages
"""

import argparse
import csv
import io
import json
import os
import zipfile

import openpyxl

# ── Static reserves: fossil fuels (BP Statistical Review 2024) ───────────────
OIL_RESERVES_GB = {          # proved reserves, billion barrels
    "USA": 68.8,  "SAU": 267.2, "RUS": 80.0,  "CAN": 170.3,
    "IRQ": 145.0, "ARE": 97.8,  "IRN": 208.6, "KWT": 101.5,
    "BRA": 15.0,  "CHN": 26.0,  "NOR": 8.1,   "KAZ": 30.0,
    "QAT": 25.2,  "MEX": 5.8,   "NGA": 36.9,
}
GAS_RESERVES_TCM = {         # proved reserves, trillion cubic metres
    "USA": 12.6, "RUS": 37.4, "IRN": 34.0, "QAT": 25.0,
    "CHN": 6.1,  "AUS": 4.4,  "CAN": 2.5,  "NOR": 1.5,
    "SAU": 9.4,  "ARE": 5.9,  "TKM": 9.8,
}
COAL_RESERVES_MT = {         # proved reserves, million tonnes
    "USA": 248941, "RUS": 162166, "AUS": 150227, "CHN": 143197,
    "IND": 111052, "IDN": 39947,  "ZAF": 9893,   "DEU": 35900,
    "UKR": 34375,  "KAZ": 25605,  "POL": 28385,
}

# ── Static: uranium (World Nuclear Association 2024) ─────────────────────────
URANIUM_PROD_TU = {          # mine production, tonnes U per year (2023)
    "KAZ": 21227, "CAN": 7000, "NAM": 5613,
    "AUS": 4087,  "UZB": 3500, "RUS": 2984,
}
URANIUM_RESERVES_KTU = {     # identified reserves, kilotonnes U
    "AUS": 1720, "KAZ": 932, "CAN": 588,
    "RUS": 510,  "NAM": 463, "UZB": 130,
}

# ── Static prices (2023 annual averages) not in World Bank Pink Sheet ────────
STATIC_PRICES = {
    "Lithium":    {"price": 22000, "unit": "t (LCE)"},
    "Cobalt":     {"price": 33000, "unit": "t"},
    "Graphite":   {"price": 650,   "unit": "t (natural flake)"},
    "Manganese":  {"price": 4.50,  "unit": "dmtu"},
    "Rare Earths":{"price": 2500,  "unit": "t (mixed REO)"},
    "Uranium":    {"price": 65,    "unit": "lb U₃O₈"},
}

# ── CO₂ intensity (combustion, from IPCC / IEA defaults) ────────────────────
CARBON_INTENSITY = {
    "Coal":        {"value": 2.32,  "unit": "tCO₂/t"},
    "Crude Oil":   {"value": 0.43,  "unit": "tCO₂/bbl"},
    "Natural Gas": {"value": 1.83,  "unit": "tCO₂/t (≈0.05 tCO₂/Mcf)"},
}

# ── World Bank Pink Sheet column indices (0-based) ───────────────────────────
WB_COLS = {
    "Crude Oil":   (1,  "$/bbl"),
    "Coal":        (5,  "$/t"),
    "Natural Gas": (7,  "$/mmbtu"),
    "Copper":      (64, "$/t"),
    "Iron Ore":    (63, "$/t"),      # $/dmtu ≈ $/t at typical 62% Fe grade
    "Nickel":      (67, "$/t"),
    "Zinc":        (68, "$/t"),
    "Gold":        (69, "$/troy oz"),
    "Platinum":    (70, "$/troy oz"),
    "Silver":      (71, "$/troy oz"),
    # Bauxite omitted — use static price (Al price is not a good proxy)
}

# Extend STATIC_PRICES with bauxite
STATIC_PRICES["Bauxite"] = {"price": 35, "unit": "t"}

# ── USGS commodity matching ──────────────────────────────────────────────────
USGS_COMMODITY_PREFIX = {
    "Lithium":    "lithium",    "Cobalt":     "cobalt",
    "Nickel":     "nickel",     "Graphite":   "graphite",
    "Manganese":  "manganese",  "Rare Earths":"rare earths",
    "Copper":     "copper",     "Iron Ore":   "iron ore",
    "Bauxite":    "bauxite",    "Zinc":       "zinc",
    "Gold":       "gold",       "Silver":     "silver",
    "Platinum":   "platinum-group",
}
USGS_TYPE_FILTER = {
    "Lithium":    "mine production, lithium content",
    "Cobalt":     "mine production",
    "Nickel":     "mine production, nickel content",
    "Graphite":   "mine production",
    "Manganese":  "mine production, manganese content",
    "Rare Earths":"mine production",
    "Copper":     "mine production, recoverable",   # "Mine production, recoverable copper content"
    "Iron Ore":   "mine production, usable ore",    # gross weight rows
    "Bauxite":    "mine production",
    "Zinc":       "mine production, zinc content",
    "Gold":       "mine production",
    "Silver":     "mine production, silver content",
    "Platinum":   "mine production",
}
COUNTRY_NAME_MAP = {
    "Dem. Rep. Congo":      "Congo (Kinshasa)",
    "United States":        "United States",
    "New Caledonia":        "New Caledonia",
}

def norm(s): return s.strip().lower()


# ── Data loaders ─────────────────────────────────────────────────────────────

def load_eia(commodity_key: str) -> dict:
    """
    Load EIA production JSON → {(country_id, year): float}.
    Prefers: TBPD for crude oil, BCM for gas, MT for coal.
    Includes both country ('c') and aggregate rows (e.g. WORL).
    """
    path = f"raw/eia_{commodity_key}-production.json"
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        rows = json.load(f)["response"]["data"]
    pref = {"crude-oil": "TBPD", "natural-gas": "BCM", "coal": "MT"}
    unit = pref.get(commodity_key, "MT")
    result = {}
    for row in rows:
        if row["unit"] != unit:
            continue
        try:
            val = float(row["value"])
        except (TypeError, ValueError):
            continue
        result[(row["countryRegionId"], row["period"])] = val
    return result


def load_usgs() -> list[dict]:
    with zipfile.ZipFile("raw/usgs_world_data.zip") as z:
        content = z.read("MCS2025_World_Data.csv").decode("utf-8-sig", errors="replace")
    rows = []
    for row in csv.DictReader(io.StringIO(content)):
        rows.append({k.strip(): v.strip() for k, v in row.items()})
    return rows


def load_wb_prices() -> dict:
    """World Bank 2023 annual average prices."""
    wb_file = openpyxl.load_workbook("raw/worldbank_prices.xlsx", read_only=True, data_only=True)
    sheet = wb_file["Monthly Prices"]
    all_rows = list(sheet.iter_rows(values_only=True))
    rows_2023 = [r for r in all_rows[6:] if r[0] and "2023" in str(r[0])]
    prices = {}
    for commodity, (col, unit) in WB_COLS.items():
        vals = []
        for row in rows_2023:
            try:
                vals.append(float(row[col]))
            except (TypeError, ValueError):
                pass
        if vals:
            prices[commodity] = {"price": round(sum(vals) / len(vals), 2), "unit": unit}
    return prices


# ── USGS helpers ─────────────────────────────────────────────────────────────

def _parse_num(s):
    if not s:
        return None
    s = s.strip().replace(",", "")
    if s in ("W", "--", "NA", "n/a", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def usgs_lookup(usgs_rows, commodity, country_name):
    """(prod_2023, prod_2024_est, reserves_2024, unit_meas) for a country.
    Reserves for Iron Ore are scaled by IRON_ORE_RESERVES_SCALE to reconcile
    the USGS CSV unit inconsistency (reserves in Mt, production in kt)."""
    prefix      = USGS_COMMODITY_PREFIX.get(commodity, "").lower()
    type_filter = USGS_TYPE_FILTER.get(commodity, "mine production").lower()
    target      = COUNTRY_NAME_MAP.get(country_name, country_name).lower()
    for row in usgs_rows:
        if not norm(row.get("COMMODITY", "")).startswith(prefix):
            continue
        if not norm(row.get("TYPE", "")).startswith(type_filter[:20]):
            continue
        if norm(row.get("COUNTRY", "")) != target:
            continue
        reserves = _parse_num(row.get("RESERVES_2024", ""))
        if commodity == "Iron Ore" and reserves is not None:
            reserves = reserves * IRON_ORE_RESERVES_SCALE
        return (
            _parse_num(row.get("PROD_2023", "")),
            _parse_num(row.get("PROD_EST_ 2024", "")),
            reserves,
            row.get("UNIT_MEAS", "").strip(),
        )
    return None, None, None, None


def usgs_world_total(usgs_rows, commodity):
    """(world_prod_2023, world_reserves_2024) from 'World total' row."""
    prefix      = USGS_COMMODITY_PREFIX.get(commodity, "").lower()
    type_filter = USGS_TYPE_FILTER.get(commodity, "mine production").lower()
    for row in usgs_rows:
        if not norm(row.get("COMMODITY", "")).startswith(prefix):
            continue
        if not norm(row.get("TYPE", "")).startswith(type_filter[:20]):
            continue
        if "world total" in norm(row.get("COUNTRY", "")):
            reserves = _parse_num(row.get("RESERVES_2024", ""))
            if commodity == "Iron Ore" and reserves is not None:
                reserves = reserves * IRON_ORE_RESERVES_SCALE
            return (
                _parse_num(row.get("PROD_2023", "")),
                reserves,
            )
    return None, None


# ── Calculations ─────────────────────────────────────────────────────────────
# All EIA production values stored in native EIA units.
# Conversions documented inline.

TROY_OZ_PER_MT = 32_150.75   # troy ounces per metric ton
PRECIOUS_METALS = {"Gold", "Silver", "Platinum"}

# Iron ore USGS CSV quirk: RESERVES_2024 is stored in million metric tons (Mt)
# while PROD_2023 is in thousand metric tons (kt). Apply a ×1000 factor to
# reserves before storing so both are in kt for consistent R/P calculations.
IRON_ORE_RESERVES_SCALE = 1000   # reserves_csv × 1000 → kt


def calc_rp(commodity, prod_raw, reserves, unit_meas=""):
    """Reserve-to-production ratio in years. prod_raw in EIA native / USGS units."""
    if prod_raw is None or reserves is None or prod_raw <= 0:
        return None
    if commodity == "Crude Oil":
        # prod_raw = TBPD; reserves = Gb
        prod_gb_yr = prod_raw * 365 / 1_000_000
        return round(reserves / prod_gb_yr, 1)
    if commodity == "Natural Gas":
        # prod_raw = BCM/yr; reserves = TCM
        return round(reserves * 1000 / prod_raw, 1)
    if commodity == "Coal":
        # prod_raw = 1000 metric tons/yr; reserves = Mt
        prod_mt = prod_raw / 1000
        return round(reserves / prod_mt, 1)
    if commodity == "Uranium":
        # prod_raw = t U/yr; reserves = kt U
        return round(reserves * 1000 / prod_raw, 1)
    # USGS minerals: both in kt after reserve scaling → direct division
    return round(reserves / prod_raw, 1)


def _usgs_prod_in_mt(prod_raw, unit_meas):
    """Convert USGS production to metric tons."""
    if prod_raw is None:
        return None
    u = unit_meas.lower()
    if "thousand" in u:
        return prod_raw * 1_000
    if "kilogram" in u or u == "kg":
        return prod_raw / 1_000
    return prod_raw   # already in metric tons


def calc_prod_value_bn(commodity, prod_raw, price_info, unit_meas=""):
    """Production value in billion USD."""
    if prod_raw is None or price_info is None:
        return None
    p = price_info["price"]
    if commodity == "Crude Oil":
        bbl_yr = prod_raw * 1_000 * 365
        return round(bbl_yr * p / 1e9, 2)
    if commodity == "Natural Gas":
        mmbtu_yr = prod_raw * 35_315_000
        return round(mmbtu_yr * p / 1e9, 2)
    if commodity == "Coal":
        tonnes_yr = prod_raw * 1_000
        return round(tonnes_yr * p / 1e9, 2)
    if commodity == "Uranium":
        lbs_yr = prod_raw * 2_594
        return round(lbs_yr * p / 1e9, 4)
    # USGS minerals
    prod_mt = _usgs_prod_in_mt(prod_raw, unit_meas)
    if prod_mt is None:
        return None
    if commodity in PRECIOUS_METALS:
        # price in $/troy oz → convert production to troy oz
        return round(prod_mt * TROY_OZ_PER_MT * p / 1e9, 2)
    if commodity == "Manganese":
        # price in $/dmtu (per 1% Mn per t); ore is ~44% Mn
        return round(prod_mt * 44 * p / 1e9, 2)
    # General: price in $/t
    return round(prod_mt * p / 1e9, 2)


def display_prod(commodity, prod_raw, unit_meas=""):
    """Human-readable production string for the Markdown page."""
    if prod_raw is None:
        return "data not available"
    if commodity == "Crude Oil":
        mbpd = prod_raw / 1000
        return f"{mbpd:.2f} million barrels/day (Mb/d)"
    if commodity == "Natural Gas":
        return f"{prod_raw:.1f} BCM/yr (billion cubic metres)"
    if commodity == "Coal":
        mt = prod_raw / 1000
        return f"{mt:,.1f} Mt/yr (million tonnes)"
    if commodity == "Uranium":
        return f"{prod_raw:,.0f} t U/yr"
    # USGS minerals
    return f"{prod_raw:,.0f} {unit_meas}"


def display_reserves(commodity, reserves, res_unit=""):
    if reserves is None:
        return "data not available"
    if commodity == "Crude Oil":
        return f"{reserves:.1f} billion barrels (Gb)"
    if commodity == "Natural Gas":
        return f"{reserves:.1f} trillion cubic metres (TCM)"
    if commodity == "Coal":
        return f"{reserves:,.0f} Mt (million tonnes)"
    if commodity == "Uranium":
        return f"{reserves:.0f} kt U (kilotonnes uranium)"
    return f"{reserves:,.0f} {res_unit}"


# ── Markdown generator ────────────────────────────────────────────────────────

def make_markdown(resource, data):
    title     = resource["title"]
    commodity = resource["commodity"]
    country   = resource["country"]
    category  = resource["category"]

    prod_raw    = data.get("prod_raw")
    unit_meas   = data.get("unit_meas", "")
    reserves    = data.get("reserves")
    res_unit    = data.get("res_unit", unit_meas)
    rp          = data.get("rp_ratio")
    price_info  = data.get("price_info")
    prod_value  = data.get("prod_value_bn_usd")
    world_prod  = data.get("world_prod")
    world_res   = data.get("world_reserves")
    share       = data.get("country_share_pct")

    lines = [f"# {title}", "",
             f"**Resource group:** {category.replace('-', ' ').title()}  "
             f"**Country:** {country}  **Commodity:** {commodity}", ""]

    # Production
    lines += ["## Production", "",
              f"Annual production (2023): {display_prod(commodity, prod_raw, unit_meas)}"]
    if world_prod is not None:
        lines.append(f"World total (2023): {display_prod(commodity, world_prod, unit_meas)}")
    if share is not None:
        lines.append(f"{country}'s share of world production: {share:.1f}%")
    lines.append("")

    # Reserves
    lines += ["## Reserves", "",
              f"Proved/identified reserves: {display_reserves(commodity, reserves, res_unit)}"]
    if rp is not None:
        lines.append(f"Reserve-to-production (R/P) ratio: {rp} years")
        lines.append(f"Estimated depletion at current rate: ~{2024 + int(rp)}")
    if world_res is not None:
        lines.append(f"World total reserves: {display_reserves(commodity, world_res, res_unit)}")
    lines.append("")

    # Price & value
    lines += ["## Price & Economic Value", ""]
    if price_info:
        lines.append(f"Benchmark price (2023 avg): ${price_info['price']:,.2f} / {price_info['unit']}")
    if prod_value is not None:
        lines.append(f"Estimated annual production value: ~${prod_value:,.2f} billion USD")
    lines.append("")

    # Environmental
    carbon = CARBON_INTENSITY.get(commodity)
    if carbon:
        lines += ["## Environmental", "",
                  f"CO₂ intensity: {carbon['value']} {carbon['unit']} (combustion)", ""]

    # Context
    lines += ["## Market Context", "",
              f"Category: {category.replace('-', ' ').title()}"]
    if share is not None:
        lines.append(f"{country} accounts for {share:.1f}% of world production")
    lines.append("")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end",   type=int, default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    with open("resources.json") as f:
        resources = json.load(f)
    subset = resources[args.start: args.end or len(resources)]

    os.makedirs("pages", exist_ok=True)

    print("Loading EIA production data...")
    eia = {
        "Crude Oil":   load_eia("crude-oil"),
        "Natural Gas": load_eia("natural-gas"),
        "Coal":        load_eia("coal"),
    }
    print("Loading USGS world data...")
    usgs_rows = load_usgs()
    print("Loading World Bank prices...")
    wb_prices = load_wb_prices()

    print(f"\nGenerating {len(subset)} pages...\n")
    ok = skipped = 0

    for resource in subset:
        slug      = resource["slug"]
        commodity = resource["commodity"]
        country   = resource["country"]
        iso3      = resource["iso3"]
        out_path  = f"pages/{slug}.md"

        if not args.force and os.path.exists(out_path):
            print(f"  CACHED  {slug}")
            skipped += 1
            continue

        data = {}

        # ── Fossil fuels ─────────────────────────────────────────────────────
        if commodity in ("Crude Oil", "Natural Gas", "Coal"):
            eia_data = eia[commodity]
            # Production (prefer 2023, fall back to 2022)
            prod_raw = eia_data.get((iso3, "2023")) or eia_data.get((iso3, "2022"))
            data["prod_raw"]  = prod_raw
            data["unit_meas"] = {"Crude Oil": "TBPD", "Natural Gas": "BCM", "Coal": "1000t"}[commodity]

            # World total
            world_prod = eia_data.get(("WORL", "2023")) or eia_data.get(("WORL", "2022"))
            data["world_prod"] = world_prod
            if prod_raw and world_prod and world_prod > 0:
                data["country_share_pct"] = round(prod_raw / world_prod * 100, 1)

            # Reserves (static BP data)
            res_map  = {"Crude Oil": OIL_RESERVES_GB, "Natural Gas": GAS_RESERVES_TCM,
                        "Coal": COAL_RESERVES_MT}
            data["reserves"] = res_map[commodity].get(iso3)
            data["res_unit"] = {"Crude Oil": "Gb", "Natural Gas": "TCM", "Coal": "Mt"}[commodity]

        # ── Uranium ──────────────────────────────────────────────────────────
        elif commodity == "Uranium":
            prod_raw = URANIUM_PROD_TU.get(iso3)
            data["prod_raw"]       = prod_raw
            data["unit_meas"]      = "t U/yr"
            data["reserves"]       = URANIUM_RESERVES_KTU.get(iso3)
            data["res_unit"]       = "kt U"
            data["world_prod"]     = sum(URANIUM_PROD_TU.values())
            data["world_reserves"] = sum(URANIUM_RESERVES_KTU.values())
            if prod_raw:
                data["country_share_pct"] = round(prod_raw / data["world_prod"] * 100, 1)

        # ── USGS minerals ────────────────────────────────────────────────────
        else:
            p23, p24, res, unit = usgs_lookup(usgs_rows, commodity, country)
            prod_raw = p23 if p23 is not None else p24
            data["prod_raw"]  = prod_raw
            data["unit_meas"] = unit or "metric tons"
            data["reserves"]  = res
            data["res_unit"]  = unit or "metric tons"
            w_prod, w_res = usgs_world_total(usgs_rows, commodity)
            data["world_prod"]     = w_prod
            data["world_reserves"] = w_res
            if prod_raw and w_prod and w_prod > 0:
                data["country_share_pct"] = round(prod_raw / w_prod * 100, 1)

        # ── Shared derived fields ─────────────────────────────────────────────
        unit_meas = data.get("unit_meas", "")
        data["rp_ratio"] = calc_rp(commodity, data.get("prod_raw"), data.get("reserves"), unit_meas)
        price_info       = wb_prices.get(commodity) or STATIC_PRICES.get(commodity)
        data["price_info"]       = price_info
        data["prod_value_bn_usd"] = calc_prod_value_bn(
            commodity, data.get("prod_raw"), price_info, unit_meas
        )

        # Write
        md = make_markdown(resource, data)
        with open(out_path, "w") as f:
            f.write(md)

        share_str = f"{data['country_share_pct']:.0f}%" if data.get("country_share_pct") else "—"
        rp_str    = f"{data['rp_ratio']}yr"            if data.get("rp_ratio") else "—"
        val_str   = f"${data['prod_value_bn_usd']:.1f}B" if data.get("prod_value_bn_usd") else "—"
        print(f"  {slug:<45} share={share_str:>5}  R/P={rp_str:>8}  val={val_str}")
        ok += 1

    n_pages = len([f for f in os.listdir("pages") if f.endswith(".md")])
    print(f"\nDone. {ok} written, {skipped} skipped — {n_pages} total in pages/")


if __name__ == "__main__":
    main()
