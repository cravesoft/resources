"""
Build a compact JSON for the website by merging CSV stats with geopolitical risk scores.

Reads resources.csv (for stats) and resource_scores.json (for geopolitical risk).
Writes site/data.json.

Usage:
    uv run python build_site_data.py
"""

import csv
import json
import os


def main():
    # Load geopolitical risk scores (partial is fine)
    scores = {}
    if os.path.exists("resource_scores.json"):
        with open("resource_scores.json") as f:
            for s in json.load(f):
                scores[s["slug"]] = s

    # Load CSV stats
    with open("resources.csv") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    def _float(v):
        return float(v) if v else None

    def _int(v):
        return int(float(v)) if v else None

    # Build world reserves per commodity: prefer explicit world_reserves field,
    # fall back to sum of reserves_proved across covered countries.
    world_reserves_by_commodity: dict[str, float] = {}
    reserves_sums: dict[str, float] = {}
    for row in rows:
        c = row["commodity"]
        if row["world_reserves"]:
            world_reserves_by_commodity[c] = float(row["world_reserves"])
        if row["reserves_proved"]:
            reserves_sums[c] = reserves_sums.get(c, 0.0) + float(row["reserves_proved"])
    # Fill in missing commodities using sum of covered countries
    for c, s in reserves_sums.items():
        if c not in world_reserves_by_commodity:
            world_reserves_by_commodity[c] = s

    # Merge
    data = []
    for row in rows:
        slug = row["slug"]
        score = scores.get(slug, {})
        data.append({
            "title":              row["title"],
            "slug":               slug,
            "category":           row["category"],
            "commodity":          row["commodity"],
            "country":            row["country"],
            "iso3":               row["iso3"],
            # Treemap area — production value in USD billions
            "value":              _float(row["production_value_bn_usd"]),
            # Numeric metrics
            "rp_ratio":           _float(row["rp_ratio"]),
            "depletion_year":     _int(row["depletion_year_est"]),
            "pay":                _float(row["price_usd_per_unit"]),
            "price_unit":         row["price_unit"],
            "production":         _float(row["production_2023"]),
            "production_unit":    row["unit"],
            "country_share_pct":  _float(row["country_share_pct"]),
            "carbon_intensity":   _float(row["carbon_intensity"]),
            "carbon_unit":        row["carbon_intensity_unit"],
            # Reserves
            "reserves":           _float(row["reserves_proved"]),
            "reserve_share_pct":  round(float(row["reserves_proved"]) / world_reserves_by_commodity[row["commodity"]] * 100, 1)
                                  if row["reserves_proved"] and row["commodity"] in world_reserves_by_commodity else None,
            # Geopolitical risk score (may be None if not yet scored)
            "exposure":           score.get("exposure"),
            "exposure_rationale": score.get("rationale"),
        })

    os.makedirs("site", exist_ok=True)
    with open("site/data.json", "w") as f:
        json.dump(data, f)

    scored = sum(1 for d in data if d["exposure"] is not None)
    total_value = sum(d["value"] for d in data if d["value"])
    print(f"Wrote {len(data)} resources to site/data.json")
    print(f"  Scored: {scored}/{len(data)}")
    print(f"  Total production value: ${total_value:.1f}B")


if __name__ == "__main__":
    main()
