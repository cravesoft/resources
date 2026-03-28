# Global Resource Tracker

An interactive treemap visualizing **99 non-renewable resource × country pairs** — fossil fuels, critical minerals, rare earths, industrial metals, and precious metals. Each rectangle's **area** is proportional to annual production value in USD. **Color** shows the selected metric: reserve-to-production ratio, commodity price, resource category, or LLM-scored geopolitical risk.

## What's here

The visualization covers major producers of 17 commodities across 6 categories: fossil fuels (crude oil, natural gas, coal), nuclear (uranium), critical minerals (lithium, cobalt, nickel, graphite, manganese), rare earth elements, industrial metals (copper, iron ore, bauxite, zinc), and precious metals (gold, silver, platinum). Data as of 2023–2024.

## LLM-powered coloring

The repo includes a data pipeline and LLM scoring stage. The "Geopolitical Risk" layer rates each resource–country pair on a 0–10 scale: how likely is supply to be disrupted, weaponized, or withheld in ways that harm importing economies? The score considers producer stability, market concentration, availability of alternatives, and historical precedent of export weaponization. See `score.py` for the prompt and scoring pipeline.

**What "Geopolitical Risk" is NOT:**
- It does **not** measure price volatility or investment risk.
- It does **not** account for futures contracts, strategic reserves, or demand-side substitution.
- The scores are LLM estimates (Gemini 2.5 Flash), calibrated to publicly available information as of 2024–2025.

## Data sources

| Commodity | Source |
|-----------|--------|
| Crude oil, natural gas, coal | EIA International Energy Statistics 2024 |
| Oil, gas, coal reserves | BP Statistical Review 2024 |
| Uranium production & reserves | World Nuclear Association 2024; IAEA Red Book 2023 |
| All minerals & metals | USGS Mineral Commodity Summaries 2025 |
| Commodity prices | World Bank Pink Sheet (March 2026) |

## Data pipeline

1. **Fetch** (`fetch.py`) — Downloads raw data from EIA API, USGS ScienceBase, and World Bank into `raw/`.
2. **Parse** (`parse_resources.py`) — Converts raw data into structured Markdown descriptions in `pages/` and exposes calculation helpers.
3. **Tabulate** (`make_csv.py`) — Builds `resources.csv` with production, reserves, R/P ratio, price, and production value for all 99 pairs.
4. **Score** (`score.py`) — Sends each resource's Markdown description to an LLM with a geopolitical risk rubric. Results saved to `resource_scores.json`. Resume-safe: skips already-scored entries.
5. **Build site data** (`build_site_data.py`) — Merges CSV stats and scores into `site/data.json`.
6. **Website** (`site/index.html`) — Interactive treemap with four color layers: R/P Ratio, Price, Category, and Geopolitical Risk.

## Key files

| File | Description |
|------|-------------|
| `resources.json` | Master list of 99 resource–country pairs (title, commodity, country, iso3, category, slug) |
| `resources.csv` | Summary stats: production, reserves, R/P ratio, price, production value, world share |
| `resource_scores.json` | Geopolitical risk scores (0–10) with rationales |
| `pages/` | Markdown descriptions used as LLM context for scoring |
| `site/` | Static website (treemap visualization) |

## Setup

```
uv sync
```

Requires API keys in `.env`:
```
EIA_API_KEY=your_key_here       # free at https://www.eia.gov/opendata/register.php
GEMINI_API_KEY=your_key_here    # free at https://aistudio.google.com/apikey
```

## Usage

```bash
# Download raw data (EIA, USGS, World Bank)
uv run python fetch.py

# Generate Markdown descriptions in pages/
uv run python parse_resources.py

# Build resources.csv
uv run python make_csv.py

# Score geopolitical risk (Google AI Studio, resume-safe)
uv run python score.py --delay 8

# Build site/data.json
uv run python build_site_data.py

# Serve locally
cd site && python -m http.server 8000
```
