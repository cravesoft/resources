"""
Build resources.csv from resources.json and raw data sources.

Reuses parse_resources.py for data loading and calculation.
Writes one row per resource-country pair with all numeric fields
needed by build_site_data.py and the frontend.

Output schema:
  title, category, slug, commodity, country, iso3
  unit, production_2023, world_production_2023
  reserves_proved, world_reserves, rp_ratio, depletion_year_est
  price_usd_per_unit, price_unit, production_value_bn_usd
  country_share_pct, carbon_intensity, carbon_intensity_unit
  data_year, source_citation

Usage:
    uv run python make_csv.py
"""

import csv
import json
import sys

from parse_resources import (
    # Data loaders
    load_eia, load_usgs, load_wb_prices,
    # USGS helpers
    usgs_lookup, usgs_world_total,
    # Calculators
    calc_rp, calc_prod_value_bn,
    # Static lookup tables
    OIL_RESERVES_GB, GAS_RESERVES_TCM, COAL_RESERVES_MT,
    URANIUM_PROD_TU, URANIUM_RESERVES_KTU,
    STATIC_PRICES, CARBON_INTENSITY,
    # Constants
    IRON_ORE_RESERVES_SCALE,
)

FIELDNAMES = [
    "title", "category", "slug", "commodity", "country", "iso3",
    "unit", "production_2023", "world_production_2023",
    "reserves_proved", "world_reserves", "rp_ratio", "depletion_year_est",
    "price_usd_per_unit", "price_unit", "production_value_bn_usd",
    "country_share_pct", "carbon_intensity", "carbon_intensity_unit",
    "data_year", "source_citation",
]

SOURCE_CITATIONS = {
    "Crude Oil":   "EIA International Energy Statistics 2024; BP Statistical Review 2024",
    "Natural Gas": "EIA International Energy Statistics 2024; BP Statistical Review 2024",
    "Coal":        "EIA International Energy Statistics 2024; BP Statistical Review 2024",
    "Uranium":     "World Nuclear Association 2024; IAEA Red Book 2023",
    "Lithium":     "USGS Mineral Commodity Summaries 2025",
    "Cobalt":      "USGS Mineral Commodity Summaries 2025",
    "Nickel":      "USGS Mineral Commodity Summaries 2025",
    "Graphite":    "USGS Mineral Commodity Summaries 2025",
    "Manganese":   "USGS Mineral Commodity Summaries 2025",
    "Rare Earths": "USGS Mineral Commodity Summaries 2025",
    "Copper":      "USGS Mineral Commodity Summaries 2025",
    "Iron Ore":    "USGS Mineral Commodity Summaries 2025",
    "Bauxite":     "USGS Mineral Commodity Summaries 2025",
    "Zinc":        "USGS Mineral Commodity Summaries 2025",
    "Gold":        "USGS Mineral Commodity Summaries 2025",
    "Silver":      "USGS Mineral Commodity Summaries 2025",
    "Platinum":    "USGS Mineral Commodity Summaries 2025",
}

DISPLAY_UNITS = {
    "Crude Oil":   "Mb/d",
    "Natural Gas": "BCM/yr",
    "Coal":        "Mt/yr",
    "Uranium":     "t U/yr",
}


def _fmt(v, decimals=4):
    if v is None:
        return ""
    return str(round(v, decimals)) if isinstance(v, float) else str(v)


def build_row(resource, eia, usgs_rows, wb_prices):
    slug      = resource["slug"]
    commodity = resource["commodity"]
    country   = resource["country"]
    iso3      = resource["iso3"]
    category  = resource["category"]

    row = {k: "" for k in FIELDNAMES}
    row.update({
        "title":            resource["title"],
        "category":         category,
        "slug":             slug,
        "commodity":        commodity,
        "country":          country,
        "iso3":             iso3,
        "data_year":        "2023",
        "source_citation":  SOURCE_CITATIONS.get(commodity, ""),
    })

    prod_raw  = None
    unit_meas = ""
    reserves  = None
    res_unit  = ""
    world_prod = None
    world_res  = None

    # ── Fossil fuels (EIA) ───────────────────────────────────────────────────
    if commodity in ("Crude Oil", "Natural Gas", "Coal"):
        eia_data = eia[commodity]
        prod_raw = eia_data.get((iso3, "2023")) or eia_data.get((iso3, "2022"))
        world_prod = eia_data.get(("WORL", "2023")) or eia_data.get(("WORL", "2022"))

        res_map = {"Crude Oil": OIL_RESERVES_GB, "Natural Gas": GAS_RESERVES_TCM,
                   "Coal": COAL_RESERVES_MT}
        reserves = res_map[commodity].get(iso3)

        # Convert EIA native units to display units
        if commodity == "Crude Oil":
            # TBPD → Mb/d
            unit_meas = "Mb/d"
            prod_display = round(prod_raw / 1000, 3) if prod_raw else None
            world_display = round(world_prod / 1000, 3) if world_prod else None
            res_unit = "Gb"
        elif commodity == "Natural Gas":
            unit_meas = "BCM/yr"
            prod_display = prod_raw
            world_display = world_prod
            res_unit = "TCM"
        else:  # Coal: EIA MT = 1000 metric tons
            unit_meas = "Mt/yr"
            prod_display = round(prod_raw / 1000, 2) if prod_raw else None
            world_display = round(world_prod / 1000, 2) if world_prod else None
            res_unit = "Mt"

        row["unit"]               = unit_meas
        row["production_2023"]    = _fmt(prod_display)
        row["world_production_2023"] = _fmt(world_display)
        row["reserves_proved"]    = _fmt(reserves)
        row["world_reserves"]     = ""   # world totals in reserves are aggregates
        if prod_display and world_display and world_display > 0:
            row["country_share_pct"] = _fmt(round(prod_display / world_display * 100, 1), 1)

        # For R/P and value we still need raw EIA values
        rp = calc_rp(commodity, prod_raw, reserves, unit_meas)
        pv = calc_prod_value_bn(commodity, prod_raw,
                                wb_prices.get(commodity) or STATIC_PRICES.get(commodity),
                                unit_meas)

    # ── Uranium (static WNA) ─────────────────────────────────────────────────
    elif commodity == "Uranium":
        prod_raw   = URANIUM_PROD_TU.get(iso3)
        reserves   = URANIUM_RESERVES_KTU.get(iso3)
        world_prod = sum(URANIUM_PROD_TU.values())
        world_res  = sum(URANIUM_RESERVES_KTU.values())
        unit_meas  = "t U/yr"
        res_unit   = "kt U"

        row["unit"]               = unit_meas
        row["production_2023"]    = _fmt(prod_raw, 0)
        row["world_production_2023"] = _fmt(world_prod, 0)
        row["reserves_proved"]    = _fmt(reserves, 0)
        row["world_reserves"]     = _fmt(world_res, 0)
        if prod_raw and world_prod:
            row["country_share_pct"] = _fmt(round(prod_raw / world_prod * 100, 1), 1)

        rp = calc_rp(commodity, prod_raw, reserves, unit_meas)
        pv = calc_prod_value_bn(commodity, prod_raw, STATIC_PRICES["Uranium"], unit_meas)

    # ── USGS minerals ────────────────────────────────────────────────────────
    else:
        p23, p24, res, unit = usgs_lookup(usgs_rows, commodity, country)
        prod_raw   = p23 if p23 is not None else p24
        reserves   = res
        unit_meas  = unit or "metric tons"
        w_prod, w_res = usgs_world_total(usgs_rows, commodity)
        world_prod = w_prod
        world_res  = w_res

        row["unit"]               = unit_meas
        row["production_2023"]    = _fmt(prod_raw, 0)
        row["world_production_2023"] = _fmt(world_prod, 0)
        row["reserves_proved"]    = _fmt(reserves, 0)
        row["world_reserves"]     = _fmt(world_res, 0)
        if prod_raw and world_prod and world_prod > 0:
            row["country_share_pct"] = _fmt(round(prod_raw / world_prod * 100, 1), 1)

        rp = calc_rp(commodity, prod_raw, reserves, unit_meas)
        price_info = wb_prices.get(commodity) or STATIC_PRICES.get(commodity)
        pv = calc_prod_value_bn(commodity, prod_raw, price_info, unit_meas)

    # ── Shared derived fields ─────────────────────────────────────────────────
    price_info = wb_prices.get(commodity) or STATIC_PRICES.get(commodity)
    carbon     = CARBON_INTENSITY.get(commodity)

    row["rp_ratio"] = _fmt(rp, 1)
    if rp is not None:
        row["depletion_year_est"] = str(2024 + int(rp))

    if price_info:
        row["price_usd_per_unit"] = _fmt(price_info["price"], 2)
        row["price_unit"]         = price_info["unit"]

    row["production_value_bn_usd"] = _fmt(pv, 2)

    if carbon:
        row["carbon_intensity"]      = str(carbon["value"])
        row["carbon_intensity_unit"] = carbon["unit"]

    return row


def main():
    with open("resources.json") as f:
        resources = json.load(f)

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

    print(f"\nBuilding {len(resources)} rows...\n")

    rows = []
    for resource in resources:
        row = build_row(resource, eia, usgs_rows, wb_prices)
        rows.append(row)
        prod  = row["production_2023"] or "—"
        share = (row["country_share_pct"] + "%") if row["country_share_pct"] else "—"
        rp    = (row["rp_ratio"] + "yr")         if row["rp_ratio"] else "—"
        val   = ("$" + row["production_value_bn_usd"] + "B") if row["production_value_bn_usd"] else "—"
        print(f"  {resource['slug']:<45}  prod={prod:>12} {row['unit']:<10}  share={share:>6}  R/P={rp:>8}  val={val}")

    with open("resources.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    # Sanity checks
    complete = sum(1 for r in rows if r["production_2023"] and r["rp_ratio"] and r["production_value_bn_usd"])
    missing_prod = [r["slug"] for r in rows if not r["production_2023"]]
    missing_rp   = [r["slug"] for r in rows if r["production_2023"] and not r["rp_ratio"]]

    print(f"\nWrote {len(rows)} rows to resources.csv")
    print(f"  Fully complete (prod + R/P + value): {complete}/{len(rows)}")
    if missing_prod:
        print(f"  Missing production: {missing_prod}")
    if missing_rp:
        print(f"  Has prod but missing R/P (no reserves): {missing_rp}")


if __name__ == "__main__":
    main()
