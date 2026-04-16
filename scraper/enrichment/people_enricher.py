"""
People enricher - finds DG/DGD/DAF/Tresorier/Directeur Investissements + LinkedIn.

Strategy:
- For each entity without people, build search queries
- Use DuckDuckGo HTML (free, no API key) as fallback
- Parse results for LinkedIn profile URLs and names
- Optional: hand-curated seed file (data/people_seed.json) takes precedence

Run:
    python enrichment/people_enricher.py --limit 50
    python enrichment/people_enricher.py --entity ent_xxxxxx
"""
import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from sources.base import fetch  # noqa
from activity_logger import log_event  # noqa

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
DATA = REPO / "docs" / "data"
ENTITIES_PATH = DATA / "entities.json"
SEED_PATH = DATA / "people_seed.json"

ROLE_QUERIES = [
    ("Directeur General",            'directeur general OR DG'),
    ("Directeur Financier",          '"directeur financier" OR DAF'),
    ("Directeur des Investissements", '"directeur des investissements" OR CIO'),
    ("Tresorier",                    'tresorier OR treasurer'),
]

DDG_URL = "https://html.duckduckgo.com/html/?q="


def search_linkedin(query: str) -> list:
    """Search LinkedIn profiles via DuckDuckGo with Playwright browser."""
    from sources.base import fetch_browser
    full = f'site:linkedin.com/in {query}'
    url = DDG_URL + quote_plus(full)
    try:
        html = fetch_browser(url, wait_ms=2500)
    except Exception as e:
        logger.warning(f"DDG search failed: {e}")
        return []
    soup = BeautifulSoup(html, "lxml")
    results = []
    for a in soup.select("a.result__a"):
        href = a.get("href", "")
        # DDG sometimes wraps: /l/?kh=-1&uddg=https%3A%2F%2Flinkedin.com%2Fin%2F...
        m = re.search(r"linkedin\.com/in/[^&\s\"']+", href)
        if not m:
            continue
        link = "https://" + m.group(0)
        title = a.get_text(strip=True)
        # LinkedIn titles are usually "Name - Role - Company | LinkedIn"
        name_part = title.split(" - ")[0].split(" | ")[0].strip()
        results.append({"name": name_part, "linkedin": link, "title": title})
    return results


def enrich_entity(entity: dict) -> int:
    """Add people to entity. Returns number of people added."""
    name = entity.get("denomination", "")
    if not name:
        return 0
    added = 0
    found_names = set(p.get("name", "").lower() for p in entity.get("people", []))
    for role, query in ROLE_QUERIES:
        full_query = f'"{name}" {query}'
        results = search_linkedin(full_query)
        # Take the top match that doesn't duplicate
        for r in results[:2]:
            n = r["name"]
            if not n or n.lower() in found_names:
                continue
            entity.setdefault("people", []).append({
                "name": n,
                "role": role,
                "linkedin": r["linkedin"],
                "email": None,
                "source": "duckduckgo",
                "confidence": "low",
            })
            found_names.add(n.lower())
            added += 1
            break
        time.sleep(1)
    return added


def apply_seed(entities: list) -> int:
    """Overlay manually curated people from people_seed.json."""
    if not SEED_PATH.exists():
        return 0
    with open(SEED_PATH, encoding="utf-8") as f:
        seed = json.load(f)
    by_id = {e["id"]: e for e in entities}
    by_norm = {e.get("denomination_normalized", ""): e for e in entities}
    applied = 0
    for entry in seed.get("people", []):
        target = by_id.get(entry.get("entity_id")) or by_norm.get(entry.get("entity_name_normalized", ""))
        if not target:
            continue
        target.setdefault("people", []).append({
            "name": entry["name"],
            "role": entry.get("role", ""),
            "linkedin": entry.get("linkedin", ""),
            "email": entry.get("email"),
            "source": "seed",
            "confidence": "high",
        })
        applied += 1
    logger.info(f"Applied {applied} seed people")
    return applied


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=20, help="Max entities to enrich")
    p.add_argument("--entity", help="Specific entity id")
    p.add_argument("--seed-only", action="store_true", help="Only apply seed file, no web search")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    entities = data["entities"]

    apply_seed(entities)

    if args.seed_only:
        pass
    else:
        targets = [e for e in entities if not e.get("people")]
        if args.entity:
            targets = [e for e in entities if e["id"] == args.entity]
        targets = targets[: args.limit]
        logger.info(f"Enriching {len(targets)} entities...")
        for i, e in enumerate(targets, 1):
            try:
                old_people = {(p["name"].lower(), p.get("role", "").lower()) for p in (e.get("people") or [])}
                n = enrich_entity(e)
                from datetime import datetime
                e["last_enriched"] = datetime.utcnow().strftime("%Y-%m-%d")
                logger.info(f"[{i}/{len(targets)}] {e['denomination']}: +{n} people")
                # Log activity for new people
                new_people = {(p["name"].lower(), p.get("role", "").lower()) for p in (e.get("people") or [])}
                for name_lower, role_lower in new_people - old_people:
                    p = next((p for p in e["people"] if p["name"].lower() == name_lower), None)
                    if p:
                        log_event("person_joined", e["id"], e["denomination"],
                                  f"{p['name']} - {p.get('role', '')}",
                                  {"name": p["name"], "role": p.get("role", ""),
                                   "linkedin": p.get("linkedin", ""), "source": p.get("source", "")})
            except Exception as ex:
                logger.warning(f"Failed for {e['denomination']}: {ex}")

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Wrote {ENTITIES_PATH}")


if __name__ == "__main__":
    main()
