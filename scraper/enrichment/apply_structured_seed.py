"""
Apply hand-curated structured products seed to entities.json.
Matches by substring on denomination_normalized.

Run:
    python enrichment/apply_structured_seed.py
"""
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sources.base import normalize_name  # noqa

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
DATA = REPO / "docs" / "data"
ENTITIES_PATH = DATA / "entities.json"
SEED_PATH = DATA / "structured_seed.json"


def apply_seed():
    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    entities = data["entities"]

    with open(SEED_PATH, encoding="utf-8") as f:
        seed = json.load(f)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    applied_count = 0
    matched_entities = {}

    for rule in seed.get("porteurs", []):
        match_str = normalize_name(rule["match"])
        if not match_str:
            continue
        for e in entities:
            norm = e.get("denomination_normalized", "")
            if not norm:
                continue
            # Match: rule must be a substring of the entity name (not the reverse)
            if match_str in norm:
                # Don't downgrade an existing 'yes' that was set with high confidence
                current = e.get("structured_products", {}) or {}
                if current.get("status") == "yes" and current.get("confidence") == "high" \
                        and rule["confidence"] != "high":
                    continue
                e["structured_products"] = {
                    "status": rule["status"],
                    "evidence": rule["evidence"],
                    "keywords_found": [],
                    "source_url": "",
                    "confidence": rule["confidence"],
                    "source": "seed_manual",
                    "applied_at": today,
                }
                matched_entities[e["id"]] = e["denomination"]
                applied_count += 1

    # Recompute stats
    sp_yes = sum(1 for e in entities if (e.get("structured_products") or {}).get("status") == "yes")
    sp_no = sum(1 for e in entities if (e.get("structured_products") or {}).get("status") == "no")
    sp_unknown = sum(1 for e in entities if (e.get("structured_products") or {}).get("status") == "unknown")
    if "stats" in data:
        data["stats"]["structured_products"] = {
            "yes": sp_yes,
            "no": sp_no,
            "unknown": sp_unknown,
        }

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Applied seed to {applied_count} entity-rule matches ({len(matched_entities)} unique entities)")
    logger.info(f"  Matched entities: {sp_yes} yes, {sp_no} no, {sp_unknown} unknown")
    print()
    print("=== Entities now flagged as porteurs (yes) ===")
    for e in entities:
        if (e.get("structured_products") or {}).get("status") == "yes":
            sp = e["structured_products"]
            print(f"  [{sp.get('confidence', '?')}] {e['denomination']}")


if __name__ == "__main__":
    apply_seed()
