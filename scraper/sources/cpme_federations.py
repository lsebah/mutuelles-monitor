"""
CPME - Annuaire des federations professionnelles.
https://www.cpme.fr/qui-sommes-nous/annuaire/nos-federations-professionnelles
"""
import logging
import re
from bs4 import BeautifulSoup

from sources.base import fetch, make_entity_dict

logger = logging.getLogger(__name__)
URL = "https://www.cpme.fr/qui-sommes-nous/annuaire/nos-federations-professionnelles"


def scrape_cpme():
    try:
        resp = fetch(URL, delay=2.0)
    except Exception as e:
        logger.error(f"CPME fetch failed: {e}")
        return []
    soup = BeautifulSoup(resp.text, "lxml")
    entities = []
    seen = set()
    # CPME annuaire is a long list of federations - try multiple selector patterns
    candidates = soup.select(".federation, .annuaire-item, .card, article, li")
    if not candidates:
        candidates = soup.find_all(["h2", "h3", "h4"])
    for el in candidates:
        text = el.get_text(" ", strip=True)
        if not text or len(text) < 4 or len(text) > 200:
            continue
        # Heuristic: federation/syndicat/union/confederation
        low = text.lower()
        if not any(k in low for k in ["federation", "fédération", "syndicat", "union", "confederation",
                                       "confédération", "chambre", "association"]):
            continue
        # Get the link as denomination
        link = el.find("a", href=True) if hasattr(el, "find") else None
        name = link.get_text(strip=True) if link else text.split("\n")[0].strip()
        name = re.sub(r"\s+", " ", name).strip()
        if not name or len(name) < 4 or len(name) > 150:
            continue
        if name.lower() in seen:
            continue
        seen.add(name.lower())
        website = link["href"] if link and link.get("href", "").startswith("http") else ""
        ent = make_entity_dict(
            denomination=name,
            type_organisme="federation",
            category="federation_professionnelle",
            website=website,
            source="cpme",
            source_url=URL,
        )
        entities.append(ent)
    logger.info(f"CPME: {len(entities)} federations")
    return entities


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    sys.path.insert(0, "..")
    res = scrape_cpme()
    for r in res[:10]:
        print(" -", r["denomination"])
