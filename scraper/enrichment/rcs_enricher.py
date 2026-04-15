"""
RCS dirigeants enricher (Niveau 1 - free).

Uses recherche-entreprises.api.gouv.fr (official, free, no API key) to fetch
the official dirigeants registered at the RCS for each entity by SIREN.

Maps RCS qualite -> our normalized roles:
  Directeur General -> DG
  Directeur General Delegue -> DGD
  President -> President
  Directeur Financier / DAF -> DAF
  Tresorier -> Tresorier

Run:
    python enrichment/rcs_enricher.py --limit 50
    python enrichment/rcs_enricher.py --all
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
DATA = REPO / "docs" / "data"
ENTITIES_PATH = DATA / "entities.json"

API_BASE = "https://recherche-entreprises.api.gouv.fr/search"
HEADERS = {
    "User-Agent": "mutuelles-monitor/1.0 (https://github.com/lsebah/mutuelles-monitor)",
    "Accept": "application/json",
}

# Map RCS qualite (raw) -> normalized role (kept for known structures-buying roles)
ROLE_MAP = [
    ("directeur general delegue", "Directeur General Delegue"),
    ("directrice generale deleguee", "Directeur General Delegue"),
    ("directeur general", "Directeur General"),
    ("directrice generale", "Directeur General"),
    ("president du conseil", "President du Conseil"),
    ("president directeur general", "President Directeur General"),
    ("president-directeur general", "President Directeur General"),
    ("president", "President"),
    ("directeur financier", "Directeur Financier"),
    ("directrice financiere", "Directeur Financier"),
    ("directeur des investissements", "Directeur des Investissements"),
    ("directrice des investissements", "Directeur des Investissements"),
    ("tresorier", "Tresorier"),
    ("administrateur", "Administrateur"),
    ("membre du directoire", "Membre du Directoire"),
    ("president du directoire", "President du Directoire"),
    ("vice-president", "Vice-President"),
]


def _norm_qualite(q: str) -> str:
    if not q:
        return ""
    s = q.lower()
    # Strip mojibake characters
    for c in "éèêëàâäîïôöûüç":
        s = s.replace(c, c)
    s = (s.replace("é", "e").replace("è", "e").replace("ê", "e")
         .replace("à", "a").replace("â", "a").replace("ô", "o")
         .replace("î", "i").replace("ï", "i").replace("ç", "c").replace("ù", "u")
         .replace("�", ""))
    return s.strip()


def _classify_role(qualite: str) -> str:
    q = _norm_qualite(qualite)
    for kw, role in ROLE_MAP:
        if kw in q:
            return role
    return ""  # Skip non-priority roles


def _fix_mojibake(s: str) -> str:
    """Try to fix the latin1->utf8 double-encode mojibake."""
    if not s:
        return ""
    try:
        return s.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s.replace("�", "")


def fetch_dirigeants(siren: str, denomination: str = ""):
    """Fetch dirigeants from the official API. Returns list of dicts."""
    if not siren:
        return []
    # Strategy 1: search by SIREN exact
    params = {"q": siren}
    try:
        r = requests.get(API_BASE, params=params, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            logger.warning(f"  HTTP {r.status_code} for siren={siren}")
            return []
        data = r.json()
    except Exception as e:
        logger.warning(f"  fetch failed: {e}")
        return []

    results = data.get("results", [])
    if not results:
        # Strategy 2: search by name
        if denomination:
            try:
                r = requests.get(API_BASE, params={"q": denomination}, headers=HEADERS, timeout=15)
                data = r.json()
                results = data.get("results", [])
            except Exception:
                pass
    if not results:
        return []

    # Find best match by SIREN
    match = None
    for res in results:
        if res.get("siren") == siren:
            match = res
            break
    if not match:
        match = results[0]

    return match.get("dirigeants", []) or []


def enrich_entity(entity: dict) -> int:
    siren = entity.get("siren", "")
    if not siren:
        return 0
    dirigeants = fetch_dirigeants(siren, entity.get("denomination", ""))
    if not dirigeants:
        return 0

    # Existing people (avoid duplicates)
    existing_names = set(p.get("name", "").lower() for p in entity.get("people", []))
    added = 0

    # Priority roles only (DG, DGD, President, DAF, Tresorier, Investissements)
    PRIORITY_ROLES = {"Directeur General", "Directeur General Delegue", "President",
                      "President du Directoire", "President Directeur General",
                      "Directeur Financier", "Directeur des Investissements", "Tresorier"}

    for dr in dirigeants:
        qualite = dr.get("qualite", "") or ""
        role = _classify_role(qualite)
        if not role or role not in PRIORITY_ROLES:
            continue
        prenoms = _fix_mojibake(dr.get("prenoms", "") or "")
        nom = _fix_mojibake(dr.get("nom", "") or "")
        full_name = f"{prenoms} {nom}".strip()
        # Strip parenthesized birth name
        if "(" in full_name:
            full_name = full_name.split("(")[0].strip()
        if not full_name or full_name.lower() in existing_names:
            continue
        entity.setdefault("people", []).append({
            "name": full_name,
            "role": role,
            "linkedin": "",
            "email": None,
            "phone": None,
            "source": "rcs_api_gouv",
            "confidence": "high",
            "qualite_raw": qualite,
            "date_naissance": dr.get("date_de_naissance", ""),
        })
        existing_names.add(full_name.lower())
        added += 1
    return added


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--all", action="store_true", help="Process all entities (ignores --limit)")
    p.add_argument("--skip-with-people", action="store_true", default=True,
                   help="Skip entities that already have people from RCS")
    p.add_argument("--delay", type=float, default=0.5, help="Delay between API calls (seconds)")
    args = p.parse_args()

    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    entities = data["entities"]

    targets = entities
    if args.skip_with_people:
        targets = [e for e in entities if not any(
            p.get("source") == "rcs_api_gouv" for p in (e.get("people") or [])
        )]
    if not args.all:
        targets = targets[: args.limit]

    logger.info(f"Enriching {len(targets)} entities (delay={args.delay}s)")
    start = time.time()
    total_added = 0
    saves = 0
    for i, e in enumerate(targets, 1):
        try:
            added = enrich_entity(e)
            total_added += added
            e["last_enriched"] = datetime.utcnow().strftime("%Y-%m-%d")
            if added:
                logger.info(f"[{i}/{len(targets)}] {e['denomination'][:40]:40s} +{added}")
            else:
                logger.debug(f"[{i}/{len(targets)}] {e['denomination'][:40]:40s} (no priority roles)")
        except Exception as ex:
            logger.warning(f"Failed for {e.get('denomination','?')}: {ex}")
        time.sleep(args.delay)
        # Save progressively every 50
        if i % 50 == 0:
            with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            saves += 1
            logger.info(f"  -> checkpoint saved ({i} processed, {total_added} people added)")

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    elapsed = time.time() - start
    logger.info(f"DONE. {total_added} people added across {len(targets)} entities in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
