"""
Score each resource's geopolitical risk using an LLM via Google AI Studio.

Reads Markdown descriptions from pages/, sends each to an LLM with a scoring
rubric, and collects structured scores. Results are cached incrementally to
resource_scores.json so the script can be resumed if interrupted.

Usage:
    uv run python score.py
    uv run python score.py --model gemini-2.0-flash
    uv run python score.py --start 0 --end 5   # test on first 5
"""

import argparse
import json
import os
import time
import httpx
from dotenv import load_dotenv

load_dotenv()

DEFAULT_MODEL = "gemini-2.5-flash"
OUTPUT_FILE = "resource_scores.json"
API_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"

SYSTEM_PROMPT = """\
You are an expert analyst in critical minerals, energy security, and \
geopolitical risk. You will be given a description of a non-renewable natural \
resource produced in a specific country.

Rate the **Geopolitical Risk** of this resource-country pair on a scale from \
0 to 10.

Geopolitical Risk measures how likely it is that supply of this resource from \
this country will be disrupted, weaponized, or withheld in ways that cause \
serious harm to importing economies. Consider:
- The producing country's political stability and governance quality
- Historical precedent of supply disruptions or export weaponization
- Market concentration (is this country a dominant or irreplaceable supplier?)
- Dependence of importing economies (are substitutes or alternative suppliers \
readily available?)
- Alignment with or hostility to major consumer blocs (NATO, EU, US, \
Japan/Korea)
- Strategic importance of the resource to defense, energy, or technology \
industries

Use these anchors to calibrate your score:

- **0–1: Minimal risk.** Stable democratic producer, multiple alternative \
suppliers globally, commodity is easily substituted or stockpiled. \
Examples: iron ore from Australia, natural gas from Norway.

- **2–3: Low risk.** Mostly stable producer with some political risk, or \
moderately concentrated supply but alternatives exist. \
Examples: copper from Chile, uranium from Canada.

- **4–5: Moderate risk.** Producer has significant governance concerns OR \
supply is somewhat concentrated, but not both simultaneously. \
Examples: uranium from Kazakhstan, nickel from Indonesia.

- **6–7: High risk.** Dominant or near-monopoly supplier with a track record \
of politically motivated export restrictions, OR a resource critical to \
defense/tech industries from a country with serious governance instability. \
Examples: cobalt from DRC, natural gas from Russia (post-2022 precedent set).

- **8–9: Very high risk.** Near-monopoly control of a strategically critical \
resource by a country with demonstrated willingness to use supply as \
geopolitical leverage. \
Examples: rare earth elements from China, cobalt from DRC with Chinese \
corporate control of processing.

- **10: Maximum risk.** Complete or effective monopoly of an irreplaceable, \
defense-critical resource combined with active geopolitical confrontation \
between producer and major consumers. Currently hypothetical at pure score 10.

Respond with ONLY a JSON object in this exact format, no other text:
{
  "exposure": <integer 0-10>,
  "rationale": "<2-3 sentences explaining the key risk factors>"
}\
"""


def score_occupation(client, text, model):
    """Send one occupation to the LLM and parse the structured response."""
    response = client.post(
        API_URL,
        headers={
            "Authorization": f"Bearer {os.environ['GEMINI_API_KEY']}",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text},
            ],
            "temperature": 0.2,
        },
        timeout=60,
    )
    response.raise_for_status()
    content = response.json()["choices"][0]["message"]["content"]

    # Strip markdown code fences if present
    content = content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1]  # remove first line
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

    return json.loads(content)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--force", action="store_true",
                        help="Re-score even if already cached")
    args = parser.parse_args()

    with open("resources.json") as f:
        occupations = json.load(f)

    subset = occupations[args.start:args.end]

    # Load existing scores
    scores = {}
    if os.path.exists(OUTPUT_FILE) and not args.force:
        with open(OUTPUT_FILE) as f:
            for entry in json.load(f):
                scores[entry["slug"]] = entry

    print(f"Scoring {len(subset)} resources with {args.model}")
    print(f"Already cached: {len(scores)}")

    errors = []
    client = httpx.Client()

    for i, occ in enumerate(subset):
        slug = occ["slug"]

        if slug in scores:
            continue

        md_path = f"pages/{slug}.md"
        if not os.path.exists(md_path):
            print(f"  [{i+1}] SKIP {slug} (no markdown)")
            continue

        with open(md_path) as f:
            text = f.read()

        print(f"  [{i+1}/{len(subset)}] {occ['title']}...", end=" ", flush=True)

        try:
            result = score_occupation(client, text, args.model)
            scores[slug] = {
                "slug":      slug,
                "title":     occ["title"],
                "commodity": occ["commodity"],
                "country":   occ["country"],
                **result,
            }
            print(f"risk={result['exposure']}")
        except Exception as e:
            print(f"ERROR: {e}")
            errors.append(slug)

        # Save after each one (incremental checkpoint)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(list(scores.values()), f, indent=2)

        if i < len(subset) - 1:
            time.sleep(args.delay)

    client.close()

    print(f"\nDone. Scored {len(scores)} occupations, {len(errors)} errors.")
    if errors:
        print(f"Errors: {errors}")

    # Summary stats
    vals = [s for s in scores.values() if "exposure" in s]
    if vals:
        avg = sum(s["exposure"] for s in vals) / len(vals)
        by_score = {}
        for s in vals:
            bucket = s["exposure"]
            by_score[bucket] = by_score.get(bucket, 0) + 1
        print(f"\nAverage exposure across {len(vals)} occupations: {avg:.1f}")
        print("Distribution:")
        for k in sorted(by_score):
            print(f"  {k}: {'█' * by_score[k]} ({by_score[k]})")


if __name__ == "__main__":
    main()
