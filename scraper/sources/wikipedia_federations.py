"""
Wikipedia categories - Organisations professionnelles en France.
Scrapes the main category + linked sub-categories to build a comprehensive list of:
  - Federations / unions / syndicats professionnels
  - Chambres consulaires
  - Interprofessions
  - Organisations patronales
"""
import logging
import re
import time
from bs4 import BeautifulSoup

from sources.base import fetch, make_entity_dict

logger = logging.getLogger(__name__)

WIKI_BASE = "https://fr.wikipedia.org"

CATEGORIES = [
    "Cat%C3%A9gorie:Organisation_professionnelle_en_France",
    "Cat%C3%A9gorie:Syndicat_professionnel_fran%C3%A7ais",
    "Cat%C3%A9gorie:Syndicat_patronal_fran%C3%A7ais",
    "Cat%C3%A9gorie:Chambre_consulaire_en_France",
    "Cat%C3%A9gorie:F%C3%A9d%C3%A9ration_sportive_fran%C3%A7aise",
    "Cat%C3%A9gorie:Interprofession_en_France",
    "Cat%C3%A9gorie:Confederation_syndicale_en_France",
]

EXCLUDE_TITLES = {
    "arborescence", "graphique", "recherche interne", "petscan",
    "suivi", "syndicat en france", "chambre de commerce en france",
    "comit\u00e9 colbert", "interprofession",
    "organisation de militaires en france",
}


def _is_org_link(text: str) -> bool:
    if not text or len(text) < 4 or len(text) > 200:
        return False
    low = text.lower().strip()
    if low in EXCLUDE_TITLES:
        return False
    # Filter pure noise
    if low.startswith(("aide ", "wikip", "cat\u00e9gorie")):
        return False
    return True


def scrape_wikipedia_federations():
    entities = []
    seen_titles = set()

    for cat in CATEGORIES:
        url = f"{WIKI_BASE}/wiki/{cat}"
        try:
            resp = fetch(url, delay=1.5)
        except Exception as e:
            logger.warning(f"Wikipedia {cat}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        # The actual pages section is #mw-pages
        pages_section = soup.find(id="mw-pages")
        if not pages_section:
            logger.warning(f"  no #mw-pages in {cat}")
            continue

        # Within mw-pages, find all article links
        for a in pages_section.find_all("a", href=True):
            href = a["href"]
            # Skip navigation links (next page, alphabetical jumpers)
            if not href.startswith("/wiki/"):
                continue
            if "Cat%C3%A9gorie" in href or ":" in href.split("/wiki/")[-1]:
                continue
            title = a.get_text(strip=True)
            if not _is_org_link(title):
                continue
            if title.lower() in seen_titles:
                continue
            seen_titles.add(title.lower())

            wiki_url = WIKI_BASE + href
            ent = make_entity_dict(
                denomination=title,
                type_organisme="federation",
                category="federation_professionnelle",
                website=wiki_url,
                source="wikipedia_orgs",
                source_url=url,
            )
            entities.append(ent)

        logger.info(f"  {cat[:40]}: {len(entities)} cumulative")
        time.sleep(1.0)

    logger.info(f"Wikipedia federations: {len(entities)} total")
    return entities


if __name__ == "__main__":
    import sys
    sys.path.insert(0, "..")
    logging.basicConfig(level=logging.INFO)
    res = scrape_wikipedia_federations()
    print(f"\nTotal: {len(res)}")
    for r in res[:25]:
        print(" -", r["denomination"][:70])
