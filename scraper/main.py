"""
Mutuelles Monitor - Orchestrator.
Aggregates entities from all sources, dedups, tags groupes, and emits JSON files.

Usage:
    python main.py --bootstrap          # ACPR PDF only
    python main.py --refresh             # ACPR + Wikipedia + RNM + CPME
    python main.py --refresh --no-online # skip slow online scrapers
"""
import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import GROUPES_MUTUALISTES, TYPES_ORGANISME, SOURCES
from merger import merge_all_sources
from sources.acpr_pdf import parse_acpr_pdf
from sources.acpr_xlsx import parse_acpr_xlsx
from sources.base import normalize_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "docs" / "data"

# ACPR sources (prefer 2026 XLSX, fallback to 2015 PDF)
_ACPR_XLSX_REPO = REPO_ROOT / "scraper" / "data" / "acpr_2026.xlsx"
_ACPR_XLSX_LEGACY = REPO_ROOT.parent / "20260414_liste_des_organismes_d_assurances_actifs_xlsx.xlsx"
_ACPR_PDF_REPO = REPO_ROOT / "scraper" / "data" / "acpr_2015.pdf"
_ACPR_PDF_LEGACY = REPO_ROOT.parent / "20150101-listes-organismes-assurance-actifs-et-des-groupes.pdf"

ACPR_XLSX = _ACPR_XLSX_REPO if _ACPR_XLSX_REPO.exists() else _ACPR_XLSX_LEGACY
ACPR_PDF = _ACPR_PDF_REPO if _ACPR_PDF_REPO.exists() else _ACPR_PDF_LEGACY


def tag_groupes(entities: list) -> list:
    """Attach a 'groupe' tag to entities matching known mutualist groups.

    Strict matching: the member key must appear as a *whole word* in the
    entity's normalized name, and must be at least 4 chars to avoid spurious
    matches like 'sma' -> 'smacl'.
    """
    import re as _re
    name_to_groupe = []  # list of (key, groupe_name)
    for g in GROUPES_MUTUALISTES:
        for member in g.get("members", []):
            k = normalize_name(member)
            if k and len(k) >= 4:
                name_to_groupe.append((k, g["name"]))
        gk = normalize_name(g["name"])
        if gk and len(gk) >= 4:
            name_to_groupe.append((gk, g["name"]))

    tagged = 0
    for e in entities:
        if e.get("groupe"):
            continue
        norm = e.get("denomination_normalized", "")
        if not norm:
            continue
        for key, gname in name_to_groupe:
            # Word-boundary match
            if _re.search(rf"\b{_re.escape(key)}\b", norm):
                e["groupe"] = gname
                tagged += 1
                break
    logger.info(f"Tagged {tagged} entities with a groupe")
    return entities


def compute_stats(entities: list) -> dict:
    by_type = {}
    by_dept = {}
    by_groupe = {}
    by_region = {}
    sp_yes = 0
    sp_no = 0
    sp_unknown = 0
    with_people = 0
    with_financials = 0

    for e in entities:
        t = e.get("type_organisme", "autre")
        by_type[t] = by_type.get(t, 0) + 1
        d = e.get("address", {}).get("department", "")
        if d:
            by_dept[d] = by_dept.get(d, 0) + 1
        r = e.get("address", {}).get("region", "")
        if r:
            by_region[r] = by_region.get(r, 0) + 1
        g = e.get("groupe", "")
        if g:
            by_groupe[g] = by_groupe.get(g, 0) + 1
        sp = e.get("structured_products", {}).get("status", "unknown")
        if sp == "yes":
            sp_yes += 1
        elif sp == "no":
            sp_no += 1
        else:
            sp_unknown += 1
        if e.get("people"):
            with_people += 1
        if e.get("financials"):
            with_financials += 1

    return {
        "total": len(entities),
        "by_type": by_type,
        "by_department": dict(sorted(by_dept.items(), key=lambda x: -x[1])[:30]),
        "by_region": by_region,
        "by_groupe": dict(sorted(by_groupe.items(), key=lambda x: -x[1])),
        "structured_products": {
            "yes": sp_yes,
            "no": sp_no,
            "unknown": sp_unknown,
        },
        "enrichment": {
            "with_people": with_people,
            "with_financials": with_financials,
        },
    }


def write_outputs(entities: list, scrape_status: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Stamp first_seen on new ones + detect new entities
    existing_path = DATA_DIR / "entities.json"
    existing_first_seen = {}
    existing_ids = set()
    if existing_path.exists():
        try:
            with open(existing_path, encoding="utf-8") as f:
                old = json.load(f)
            for e in old.get("entities", []):
                existing_ids.add(e["id"])
                if e.get("first_seen"):
                    existing_first_seen[e["id"]] = e["first_seen"]
        except Exception as ex:
            logger.warning(f"Could not read existing entities.json: {ex}")

    # Log new entities to activity feed
    try:
        sys.path.insert(0, str(Path(__file__).parent / "enrichment"))
        from activity_logger import log_event
        for e in entities:
            if e["id"] not in existing_ids and existing_ids:
                log_event("new_entity", e["id"], e.get("denomination", ""),
                          f"Nouvelle entite ({e.get('type_organisme', '')})",
                          {"type_organisme": e.get("type_organisme", ""),
                           "city": e.get("address", {}).get("city", ""),
                           "source": list(e.get("sources", {}).keys())[:1]})
    except Exception as ex:
        logger.debug(f"Activity logging skipped: {ex}")

    for e in entities:
        if not e.get("first_seen"):
            e["first_seen"] = existing_first_seen.get(e["id"], today)
        e["last_seen"] = today

    stats = compute_stats(entities)
    payload = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "scrape_status": scrape_status,
        "stats": stats,
        "types_labels": TYPES_ORGANISME,
        "sources": {k: v["full_name"] for k, v in SOURCES.items()},
        "entities": entities,
    }
    with open(existing_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"Wrote {existing_path} ({len(entities)} entities)")

    with open(DATA_DIR / "stats.json", "w", encoding="utf-8") as f:
        json.dump({"last_updated": payload["last_updated"], **stats}, f, ensure_ascii=False, indent=2)


def run(bootstrap=False, refresh=False, no_online=False):
    sources_data = []
    scrape_status = {}

    # ACPR 2026 XLSX (primary, REGAFI export)
    if ACPR_XLSX.exists():
        try:
            acpr_xlsx_entities = parse_acpr_xlsx(str(ACPR_XLSX))
            sources_data.append(acpr_xlsx_entities)
            scrape_status["acpr_xlsx_2026"] = {"status": "success", "count": len(acpr_xlsx_entities)}
        except Exception as e:
            logger.error(f"ACPR XLSX failed: {e}")
            scrape_status["acpr_xlsx_2026"] = {"status": "error", "error": str(e)}
    elif ACPR_PDF.exists():
        # Fallback to legacy 2015 PDF only if XLSX missing
        try:
            acpr_pdf_entities = parse_acpr_pdf(str(ACPR_PDF))
            sources_data.append(acpr_pdf_entities)
            scrape_status["acpr_pdf_2015"] = {"status": "success", "count": len(acpr_pdf_entities)}
        except Exception as e:
            logger.error(f"ACPR PDF failed: {e}")
            scrape_status["acpr_pdf_2015"] = {"status": "error", "error": str(e)}

    if refresh and not no_online:
        # Phase 2 sources
        try:
            from sources.wikipedia_federations import scrape_wikipedia_federations
            wf = scrape_wikipedia_federations()
            sources_data.append(wf)
            scrape_status["wikipedia_federations"] = {"status": "success", "count": len(wf)}
        except Exception as e:
            logger.error(f"Wikipedia federations failed: {e}")
            scrape_status["wikipedia_federations"] = {"status": "error", "error": str(e)}
        try:
            from sources.wikipedia_mutuelles import scrape_wikipedia_mutuelles
            wiki = scrape_wikipedia_mutuelles()
            sources_data.append(wiki)
            scrape_status["wikipedia_mutuelles"] = {"status": "success", "count": len(wiki)}
        except Exception as e:
            logger.error(f"Wikipedia mutuelles failed: {e}")
            scrape_status["wikipedia_mutuelles"] = {"status": "error", "error": str(e)}
        try:
            from sources.rnm_mutuelles import scrape_rnm
            rnm = scrape_rnm()
            sources_data.append(rnm)
            scrape_status["rnm"] = {"status": "success", "count": len(rnm)}
        except Exception as e:
            logger.error(f"RNM failed: {e}")
            scrape_status["rnm"] = {"status": "error", "error": str(e)}

    merged = merge_all_sources(*sources_data)
    merged = tag_groupes(merged)
    write_outputs(merged, scrape_status)

    # Re-apply structured products seed (manual overrides)
    try:
        from enrichment.apply_structured_seed import apply_seed
        apply_seed()
    except Exception as e:
        logger.warning(f"Could not apply structured seed: {e}")

    logger.info("DONE")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", action="store_true", help="ACPR PDF only")
    p.add_argument("--refresh", action="store_true", help="All sources")
    p.add_argument("--no-online", action="store_true", help="Skip online scrapers")
    args = p.parse_args()
    if not args.bootstrap and not args.refresh:
        args.bootstrap = True
    run(bootstrap=args.bootstrap, refresh=args.refresh, no_online=args.no_online)
