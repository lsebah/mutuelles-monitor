"""
Apply hand-curated federations seed (Tier 1 + Tier 2) to entities.json.
Adds federations as full entities with people, website, address.
"""
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sources.base import make_entity_dict, normalize_name  # noqa

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
DATA = REPO / "docs" / "data"
ENTITIES_PATH = DATA / "entities.json"
SEED_PATH = DATA / "federations_seed.json"


def apply():
    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    entities = data["entities"]

    with open(SEED_PATH, encoding="utf-8") as f:
        seed = json.load(f)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    norm_to_id = {e.get("denomination_normalized", ""): e["id"] for e in entities}
    by_id = {e["id"]: e for e in entities}

    added = 0
    updated = 0
    for fed in seed.get("federations", []):
        denom = fed["denomination"]
        norm = normalize_name(denom)

        # Check if already exists (by normalized name or short_name)
        existing_id = norm_to_id.get(norm)
        if not existing_id and fed.get("short_name"):
            short_norm = normalize_name(fed["short_name"])
            for nn, eid in norm_to_id.items():
                if nn == short_norm or nn.startswith(short_norm + " ") or nn.endswith(" " + short_norm):
                    existing_id = eid
                    break

        if existing_id and existing_id in by_id:
            ent = by_id[existing_id]
            updated += 1
        else:
            ent = make_entity_dict(
                denomination=denom,
                siren=fed.get("siren", ""),
                type_organisme="federation",
                category=fed.get("category", "federation_professionnelle"),
                address_street=fed.get("address_street", ""),
                postal_code=fed.get("postal_code", ""),
                city=fed.get("city", ""),
                website=fed.get("website", ""),
                source="federations_seed",
                source_url="manual_curation",
            )
            ent["first_seen"] = today
            ent["last_seen"] = today
            entities.append(ent)
            by_id[ent["id"]] = ent
            norm_to_id[normalize_name(denom)] = ent["id"]
            added += 1

        # Add or update tier metadata
        ent["tier"] = fed.get("tier")
        ent["short_name"] = fed.get("short_name", "")
        if fed.get("notes"):
            ent["notes"] = fed["notes"]
        if fed.get("website") and not ent.get("website"):
            ent["website"] = fed["website"]
        ent.setdefault("sources", {})["federations_seed"] = True

        # Merge people from seed (don't duplicate by name)
        existing_names = {p.get("name", "").lower() for p in ent.get("people", [])}
        for p in fed.get("people", []):
            if p.get("name", "").lower() in existing_names:
                continue
            ent.setdefault("people", []).append({
                "name": p["name"],
                "role": p.get("role", ""),
                "linkedin": p.get("linkedin", ""),
                "email": None,
                "phone": None,
                "source": "federations_seed",
                "confidence": p.get("confidence", "medium"),
            })
            existing_names.add(p["name"].lower())

    # Update stats
    by_type = {}
    for e in entities:
        t = e.get("type_organisme", "autre")
        by_type[t] = by_type.get(t, 0) + 1
    if "stats" in data:
        data["stats"]["by_type"] = by_type
        data["stats"]["total"] = len(entities)

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Federations seed: +{added} new, {updated} updated. Total entities: {len(entities)}")
    print()
    print("=== Federations active ===")
    for e in entities:
        if e.get("type_organisme") == "federation":
            tier = e.get("tier", "?")
            print(f"  T{tier} {e['denomination'][:60]:60s} | {len(e.get('people',[]))} people")


if __name__ == "__main__":
    apply()
