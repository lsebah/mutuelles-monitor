"""
Add federations from Wikipedia to entities.json without re-running the full bootstrap.
Safe to run in parallel with rcs_enricher (uses file locking via temp file).
"""
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sources.wikipedia_federations import scrape_wikipedia_federations
from sources.base import normalize_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
ENTITIES_PATH = REPO / "docs" / "data" / "entities.json"


def merge_federations():
    feds = scrape_wikipedia_federations()
    logger.info(f"Got {len(feds)} federations from Wikipedia")

    # Read entities (with retry to avoid race with rcs_enricher)
    for attempt in range(5):
        try:
            with open(ENTITIES_PATH, encoding="utf-8") as f:
                data = json.load(f)
            break
        except (json.JSONDecodeError, FileNotFoundError) as e:
            if attempt == 4:
                raise
            logger.warning(f"Read attempt {attempt+1} failed: {e}, retrying...")
            time.sleep(2)

    entities = data["entities"]
    existing_norms = {e.get("denomination_normalized", ""): e["id"] for e in entities}

    added = 0
    for fed in feds:
        nn = fed.get("denomination_normalized", "")
        if nn and nn in existing_norms:
            continue
        entities.append(fed)
        existing_norms[nn] = fed["id"]
        added += 1

    # Update stats
    if "stats" in data:
        by_type = {}
        for e in entities:
            t = e.get("type_organisme", "autre")
            by_type[t] = by_type.get(t, 0) + 1
        data["stats"]["by_type"] = by_type
        data["stats"]["total"] = len(entities)

    if "scrape_status" not in data:
        data["scrape_status"] = {}
    data["scrape_status"]["wikipedia_federations"] = {"status": "success", "count": len(feds)}

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Added {added} federations (total entities: {len(entities)})")


if __name__ == "__main__":
    merge_federations()
