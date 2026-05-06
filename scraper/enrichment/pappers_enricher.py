"""
Pappers enricher — comptes annuels (CA, résultat, capitaux propres) by SIREN.

Free tier: 100 calls / month → caps to 100 entities by default. Prioritizes
porteurs structurés first, then federation tier 1, so the call budget goes
to the most strategically interesting cabinets.

API key passed via env var PAPPERS_API_KEY.

Coverage caveat: Pappers only knows about RCS-registered entities, so
"mutuelles 45" (Code de la Mutualité) return 404. Société d'assurance SA
and SAS are well covered.

Run:
    PAPPERS_API_KEY=xxx python enrichment/pappers_enricher.py --limit 100
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parent.parent.parent
DATA = REPO / "docs" / "data"
ENTITIES_PATH = DATA / "entities.json"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

API_URL = "https://api.pappers.fr/v2/entreprise"


def _latest_finances(finances):
    """From the Pappers `finances` list, pick the most recent year with at
    least one of CA / résultat / capitaux propres populated."""
    if not finances:
        return None
    sorted_fin = sorted(finances, key=lambda f: f.get("annee") or 0, reverse=True)
    for f in sorted_fin:
        if any(f.get(k) for k in ("chiffre_affaires", "resultat", "capitaux_propres")):
            return f
    return None


def _enrich_one(entity, api_key, delay=0.5):
    """One Pappers call → fill entity['financials_pappers'] if useful."""
    siren = (entity.get("siren") or "").strip()
    if not siren or len(siren) != 9:
        return False

    try:
        resp = requests.get(API_URL,
                            params={"api_token": api_key, "siren": siren},
                            timeout=15)
        time.sleep(delay)
    except Exception as e:
        logger.warning(f"  Pappers fetch failed for {siren}: {e}")
        return False

    if resp.status_code == 404:
        # Mutuelle 45 / radié etc. — expected for ~half the base
        entity.setdefault("financials_pappers", {})["status"] = "not_found"
        return False
    if resp.status_code == 401 or resp.status_code == 403:
        logger.error(f"  Pappers auth error ({resp.status_code}). Check API key.")
        return False
    if resp.status_code == 429:
        logger.warning("  Pappers rate-limited — waiting 30s")
        time.sleep(30)
        return False
    if resp.status_code != 200:
        logger.debug(f"  Pappers {resp.status_code} for {siren}")
        return False

    data = resp.json()
    fin_list = data.get("finances") or []
    latest = _latest_finances(fin_list)
    record = {
        "source": "Pappers",
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%d"),
    }
    if latest:
        record["year"] = latest.get("annee")
        record["chiffre_affaires_eur"] = latest.get("chiffre_affaires")
        record["resultat_eur"] = latest.get("resultat")
        record["capitaux_propres_eur"] = latest.get("capitaux_propres")
        record["effectif"] = latest.get("effectif")
    # Capital social (always present on RCS-registered entities)
    if data.get("capital"):
        record["capital_social_eur"] = data["capital"]
    # Last published comptes year
    comptes = data.get("comptes") or []
    if comptes:
        record["dernier_compte_publie"] = comptes[0].get("annee_cloture")

    entity["financials_pappers"] = record
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=100,
                   help="Max API calls (free tier = 100/month)")
    p.add_argument("--delay", type=float, default=0.5)
    p.add_argument("--all", action="store_true",
                   help="Override --limit (will burn through your monthly quota)")
    args = p.parse_args()

    api_key = os.environ.get("PAPPERS_API_KEY")
    if not api_key:
        logger.error("Set PAPPERS_API_KEY env var first.")
        sys.exit(2)

    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    entities = data["entities"]

    # Candidates = SIREN + not yet Pappers-enriched
    cand = [e for e in entities
            if e.get("siren") and not (e.get("financials_pappers") or {}).get("year")]

    # Priority: structured products yes > tier 1 > tier 2 > rest
    def priority(e):
        sp = (e.get("structured_products") or {}).get("status") == "yes"
        tier = e.get("tier") or 99
        return (-int(sp), tier)
    cand.sort(key=priority)

    if not args.all:
        cand = cand[:args.limit]

    logger.info(f"Pappers enrich: {len(cand)} entities (delay={args.delay}s)")
    found = saved_404 = 0
    start = time.time()

    for i, e in enumerate(cand, 1):
        ok = _enrich_one(e, api_key, delay=args.delay)
        if ok:
            found += 1
        elif (e.get("financials_pappers") or {}).get("status") == "not_found":
            saved_404 += 1
        if i % 25 == 0:
            with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"  [{i}/{len(cand)}] +{found} enriched, {saved_404} 404 — checkpoint saved")

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    elapsed = time.time() - start
    logger.info(f"Pappers DONE in {elapsed/60:.1f} min: "
                f"+{found} enriched, {saved_404} 404 (mutuelles 45 etc.)")


if __name__ == "__main__":
    main()
