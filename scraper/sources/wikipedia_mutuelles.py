"""
Wikipedia - Liste des mutuelles de sante en France.
Scrapes the page and extracts mutuelle names, returns minimal entity dicts
(no address - to be merged with ACPR data).
"""
import logging
from bs4 import BeautifulSoup

from sources.base import fetch, make_entity_dict

logger = logging.getLogger(__name__)
URL = "https://fr.wikipedia.org/wiki/Liste_des_mutuelles_de_sant%C3%A9_en_France"


def scrape_wikipedia_mutuelles():
    try:
        resp = fetch(URL, delay=2.0)
    except Exception as e:
        logger.error(f"Wikipedia fetch failed: {e}")
        return []
    soup = BeautifulSoup(resp.text, "lxml")
    content = soup.find(id="mw-content-text")
    if not content:
        return []
    seen = set()
    entities = []
    # Extract from list items in the article body
    for li in content.find_all("li"):
        text = li.get_text(" ", strip=True)
        # Filter likely mutuelle lines (length + capital letters)
        if len(text) < 4 or len(text) > 200:
            continue
        # Get the first link as denomination if any
        link = li.find("a")
        name = link.get_text(strip=True) if link else text.split("(")[0].split(",")[0].strip()
        name = name.strip()
        if not name or len(name) < 4:
            continue
        # Filter: avoid Wikipedia navigation
        if any(skip in name.lower() for skip in ["modifier", "wikip", "lien", "categori", "voir aussi", "references"]):
            continue
        # Heuristic: must contain a mutuelle-ish keyword
        low = name.lower()
        if not any(k in low for k in ["mutuel", "mut", "harmonie", "mgen", "macif", "matmut", "smerep", "lmde", "mgefi"]):
            continue
        if name.lower() in seen:
            continue
        seen.add(name.lower())
        ent = make_entity_dict(
            denomination=name,
            type_organisme="mutuelle",
            category="mutuelle_sante",
            source="wikipedia",
            source_url=URL,
        )
        entities.append(ent)
    logger.info(f"Wikipedia: {len(entities)} mutuelles")
    return entities


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    sys.path.insert(0, "..")
    res = scrape_wikipedia_mutuelles()
    print(f"Found {len(res)}")
    for r in res[:10]:
        print(" -", r["denomination"])
