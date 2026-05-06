"""
Run the 4 enrichment waves in series on all 829 mutuelles entities.

Wave 1 — RCS (data.gouv recherche-entreprises)  : DG, DGD, President, DAF, Tresorier
Wave 2 — Legal mentions scraper                  : website + DG (publi) + email/phone
Wave 3 — SFCR parser                              : primes, resultat net, fonds propres (= reserves)
Wave 4 — DDG people search (LinkedIn URLs)        : DAF/CIO/Tresorier/DG profiles

Each wave has its own CLI script with --limit / --all. We invoke them as
subprocesses so each one writes back to entities.json after every batch
(checkpoint = if a wave crashes mid-run we keep what was already saved).

Run:
    python scripts/enrich_all_waves.py
    python scripts/enrich_all_waves.py --skip-wave 4   # skip the slow DDG one
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ENRICHMENT = REPO / "scraper" / "enrichment"
ENTITIES_PATH = REPO / "docs" / "data" / "entities.json"

# Default limits aimed to cover the full base (~829 entities)
WAVES = [
    {
        "n": 1,
        "name": "RCS via data.gouv",
        "script": ENRICHMENT / "rcs_enricher.py",
        "args": ["--all"],
        "eta_min": 30,
    },
    {
        "n": 2,
        "name": "Legal mentions scraper",
        "script": ENRICHMENT / "legal_mentions_scraper.py",
        "args": ["--all"],
        "eta_min": 90,
    },
    {
        "n": 3,
        "name": "SFCR parser",
        "script": ENRICHMENT / "sfcr_parser.py",
        "args": ["--limit", "1500"],
        "eta_min": 150,
    },
    {
        "n": 4,
        "name": "People DDG + LinkedIn",
        "script": ENRICHMENT / "people_enricher.py",
        "args": ["--limit", "1500"],
        "eta_min": 240,
    },
]


def coverage_snapshot(label):
    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    ents = data.get("entities", data) if isinstance(data, dict) else data
    n = len(ents)

    def pct(c):
        return f"{c}/{n} ({100 * c // n}%)"

    website = sum(1 for e in ents if e.get("website"))
    people = sum(1 for e in ents if e.get("people"))
    fin = sum(1 for e in ents if e.get("financials"))
    sfcr = sum(1 for e in ents if e.get("sfcr_url") or e.get("sfcr_data"))
    li = sum(
        1
        for e in ents
        for p in (e.get("people") or [])
        if p.get("linkedin")
    )

    # Role breakdown
    roles = {"DG": 0, "DGD": 0, "DAF": 0, "Tresorier": 0, "CIO": 0, "President": 0}
    for e in ents:
        for p in e.get("people", []) or []:
            r = (p.get("role") or "").strip()
            for k in roles:
                if k.lower() in r.lower():
                    roles[k] += 1

    print(f"\n[{label}] coverage of {n} entities:")
    print(f"  website   : {pct(website)}")
    print(f"  people    : {pct(people)}")
    print(f"  financials: {pct(fin)}")
    print(f"  sfcr_url  : {pct(sfcr)}")
    print(f"  linkedin  : {li} URLs")
    print(f"  roles     : DG={roles['DG']} | DGD={roles['DGD']} | DAF={roles['DAF']} | "
          f"Tresorier={roles['Tresorier']} | CIO={roles['CIO']} | President={roles['President']}")


def run_wave(wave):
    print("\n" + "=" * 70)
    print(f"WAVE {wave['n']} — {wave['name']}  (~{wave['eta_min']} min ETA)")
    print(f"  cmd: python {wave['script'].relative_to(REPO)} {' '.join(wave['args'])}")
    print("=" * 70)

    t0 = time.time()
    cmd = [sys.executable, str(wave["script"])] + wave["args"]
    # Each enricher writes its own logs to stdout; let them through.
    result = subprocess.run(cmd, cwd=str(REPO))
    elapsed = time.time() - t0
    print(f"\nWave {wave['n']} finished in {elapsed/60:.1f} min (exit {result.returncode})")
    return result.returncode


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--skip-wave", type=int, action="append", default=[],
                   help="Skip a wave number (can repeat)")
    args = p.parse_args()

    coverage_snapshot("BEFORE")
    overall_t0 = time.time()

    for wave in WAVES:
        if wave["n"] in args.skip_wave:
            print(f"\n*** Skipping wave {wave['n']} ({wave['name']}) ***")
            continue
        rc = run_wave(wave)
        coverage_snapshot(f"AFTER WAVE {wave['n']}")
        if rc != 0:
            print(f"\n!! Wave {wave['n']} returned exit code {rc} — continuing anyway")

    total = time.time() - overall_t0
    print("\n" + "=" * 70)
    print(f"ALL WAVES DONE in {total/60:.1f} min")
    print("=" * 70)
    coverage_snapshot("FINAL")


if __name__ == "__main__":
    main()
