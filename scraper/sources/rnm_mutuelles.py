"""
Registre National des Mutuelles - mutuellefr.info paginated scrape.
The site lists ~600 mutuelles with their RNM number. Pagination is via
URL c685-pN.html (N=1..30 approx).
"""
import logging
import re
from bs4 import BeautifulSoup

from sources.base import fetch, make_entity_dict

logger = logging.getLogger(__name__)

BASE = "https://www.mutuellefr.info/mutuelles-et-ndeg-rnm-registre-national-des-mutuelles-c685-p{}.html"
MAX_PAGES = 30


def scrape_rnm():
    entities = []
    seen_ids = set()
    consecutive_empty = 0
    for page in range(1, MAX_PAGES + 1):
        url = BASE.format(page)
        try:
            resp = fetch(url, delay=2.5)
        except Exception as e:
            logger.warning(f"RNM page {page} failed: {e}")
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        # The page lists rows each with mutuelle name + RNM number; structure varies.
        # Heuristic: look for any text node containing "RNM" or 9-digit number near a link.
        page_added = 0
        # Try common patterns: <li>, <p>, <tr>
        for el in soup.select("li, p, tr"):
            text = el.get_text(" ", strip=True)
            if not text or len(text) > 250:
                continue
            # Extract RNM/SIREN number (9 digits)
            m = re.search(r"\b(\d{9})\b", text)
            if not m:
                continue
            rnm = m.group(1)
            # Get mutuelle name: first link or text before the number
            link = el.find("a")
            name = link.get_text(strip=True) if link else text.split(rnm)[0].strip(" -:|")
            name = re.sub(r"\s+", " ", name).strip()
            if not name or len(name) < 4 or len(name) > 200:
                continue
            if "mutuel" not in name.lower() and "mut" not in name.lower():
                continue
            ent = make_entity_dict(
                denomination=name,
                siren=rnm,
                matricule=rnm,
                type_organisme="mutuelle",
                category="mutuelle_sante",
                source="rnm",
                source_url=url,
            )
            if ent["id"] in seen_ids:
                continue
            seen_ids.add(ent["id"])
            entities.append(ent)
            page_added += 1
        logger.info(f"RNM page {page}: +{page_added} (total {len(entities)})")
        if page_added == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                logger.info("RNM: 3 empty pages, stopping")
                break
        else:
            consecutive_empty = 0
    logger.info(f"RNM total: {len(entities)} mutuelles")
    return entities


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    sys.path.insert(0, "..")
    res = scrape_rnm()
    for r in res[:5]:
        print(" -", r["denomination"], r["siren"])
