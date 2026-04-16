"""
DAF + CIO enricher - searches LinkedIn via DuckDuckGo for:
  - Directeur Financier / DAF / CFO
  - Directeur des Investissements / CIO / Chief Investment Officer

These roles are NOT in the RCS (internal positions), so we use web search.

Priority: entities with structured_products.status == 'yes' or tier == 1 first.

Run:
    python enrichment/daf_cio_enricher.py --priority        # top ~40 entities only
    python enrichment/daf_cio_enricher.py --limit 100       # first 100 without DAF/CIO
    python enrichment/daf_cio_enricher.py --all              # all entities
"""
import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
DATA = REPO / "docs" / "data"
ENTITIES_PATH = DATA / "entities.json"

DDG_URL = "https://html.duckduckgo.com/html/?q="
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

ROLE_QUERIES = [
    ("Directeur Financier", [
        '"{name}" "directeur financier" site:linkedin.com/in',
        '"{name}" DAF site:linkedin.com/in',
        '"{name}" CFO site:linkedin.com/in',
        '"{name}" "chief financial officer" site:linkedin.com/in',
    ]),
    ("Directeur des Investissements", [
        '"{name}" "directeur des investissements" site:linkedin.com/in',
        '"{name}" CIO investissements site:linkedin.com/in',
        '"{name}" "chief investment officer" site:linkedin.com/in',
        '"{name}" "directeur investissement" site:linkedin.com/in',
    ]),
]


def _search_ddg(query: str) -> list:
    """Search DuckDuckGo HTML and return LinkedIn results."""
    url = DDG_URL + quote_plus(query)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            logger.debug(f"  DDG {r.status_code}")
            return []
    except Exception as e:
        logger.debug(f"  DDG error: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    results = []
    for a in soup.select("a.result__a"):
        href = a.get("href", "")
        m = re.search(r"linkedin\.com/in/[^&\s\"']+", href)
        if not m:
            continue
        link = "https://" + m.group(0)
        title = a.get_text(strip=True)
        # Extract name from LinkedIn title: "Firstname Lastname - Role - Company | LinkedIn"
        name = title.split(" - ")[0].split(" | ")[0].strip()
        # Clean up LinkedIn cruft
        name = re.sub(r'\s*[\|].*$', '', name).strip()
        if name and len(name) > 3 and len(name) < 60:
            results.append({"name": name, "linkedin": link, "title": title})
    return results


def _has_role(entity: dict, role_keyword: str) -> bool:
    """Check if entity already has a person with this role."""
    for p in entity.get("people", []):
        r = (p.get("role") or "").lower()
        if role_keyword.lower() in r:
            return True
    return False


def enrich_entity(entity: dict, delay: float = 2.5) -> int:
    """Search for DAF + CIO on LinkedIn. Returns number of people added."""
    name = entity.get("denomination", "")
    if not name:
        return 0

    # Short name is better for search (remove parenthetical, legal suffixes)
    search_name = entity.get("short_name") or name
    search_name = re.sub(r'\([^)]*\)', '', search_name).strip()
    search_name = re.sub(r'\s+(SA|SAS|SGAM|UMG|SE)\s*$', '', search_name, flags=re.IGNORECASE).strip()
    if len(search_name) > 50:
        search_name = search_name[:50]

    added = 0
    existing_names = {p.get("name", "").lower() for p in entity.get("people", [])}

    for role, queries in ROLE_QUERIES:
        # Skip if already has this role
        role_kw = "financier" if "Financier" in role else "investissement"
        if _has_role(entity, role_kw):
            continue

        found = False
        for q_template in queries:
            query = q_template.replace("{name}", search_name)
            results = _search_ddg(query)
            time.sleep(delay)

            for r in results[:3]:
                n = r["name"]
                if not n or n.lower() in existing_names:
                    continue
                # Basic validation: name should look like a person (not a company)
                if any(kw in n.lower() for kw in ["mutuelle", "assurance", "linkedin", "france", "groupe"]):
                    continue

                entity.setdefault("people", []).append({
                    "name": n,
                    "role": role,
                    "linkedin": r["linkedin"],
                    "email": None,
                    "phone": None,
                    "source": "ddg_linkedin",
                    "confidence": "low",
                    "search_title": r.get("title", "")[:150],
                })
                existing_names.add(n.lower())
                added += 1
                found = True
                break
            if found:
                break

    return added


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--priority", action="store_true",
                   help="Only porteurs structures + feds T1 (~40 entities)")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--all", action="store_true")
    p.add_argument("--delay", type=float, default=2.5,
                   help="Delay between DDG searches (seconds)")
    args = p.parse_args()

    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    entities = data["entities"]

    # Filter entities that need DAF or CIO
    needs_enrichment = []
    for e in entities:
        needs_daf = not _has_role(e, "financier")
        needs_cio = not _has_role(e, "investissement")
        if needs_daf or needs_cio:
            needs_enrichment.append(e)

    if args.priority:
        # Sort: porteurs structures first, then tier 1 feds, then those with people
        def priority_score(e):
            score = 0
            if (e.get("structured_products") or {}).get("status") == "yes":
                score += 100
            if e.get("tier") == 1:
                score += 80
            if e.get("tier") == 2:
                score += 50
            if e.get("people"):
                score += 20
            if e.get("groupe"):
                score += 10
            return -score
        needs_enrichment.sort(key=priority_score)
        needs_enrichment = needs_enrichment[:50]
    elif not args.all:
        needs_enrichment = needs_enrichment[:args.limit]

    logger.info(f"Searching DAF + CIO for {len(needs_enrichment)} entities (delay={args.delay}s)")
    total_added = 0
    start = time.time()

    for i, e in enumerate(needs_enrichment, 1):
        try:
            added = enrich_entity(e, delay=args.delay)
            total_added += added
            e["last_enriched"] = datetime.utcnow().strftime("%Y-%m-%d")
            status = f"+{added}" if added else "skip"
            logger.info(f"[{i}/{len(needs_enrichment)}] {e['denomination'][:40]:40s} {status}")
        except Exception as ex:
            logger.warning(f"  Failed: {ex}")

        # Checkpoint save every 25
        if i % 25 == 0:
            with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"  -> checkpoint ({i} processed, {total_added} added)")

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    elapsed = time.time() - start
    logger.info(f"DONE. {total_added} DAF/CIO added across {len(needs_enrichment)} entities in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
