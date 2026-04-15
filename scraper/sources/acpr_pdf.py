"""
Mutuelles Monitor - ACPR PDF parser.
Parses the 2015 ACPR list of insurance organisations and groups
to bootstrap the directory.
"""
import logging
import re
from pathlib import Path

import pdfplumber

from sources.base import make_entity_dict

logger = logging.getLogger(__name__)

# Mapping ACPR raw label -> normalized type code
TYPE_MAP = {
    "mutuelle": "mutuelle",
    "entreprise d'assurance non vie": "assurance_non_vie",
    "entreprise d assurance non vie": "assurance_non_vie",
    "entreprise dassurance non vie": "assurance_non_vie",
    "entreprise d'assurance vie": "assurance_vie",
    "entreprise d assurance vie": "assurance_vie",
    "entreprise dassurance vie": "assurance_vie",
    "entreprise d'assurance mixte": "assurance_mixte",
    "entreprise d assurance mixte": "assurance_mixte",
    "entreprise dassurance mixte": "assurance_mixte",
    "entreprise de reassurance": "reassurance",
    "entreprise de réassurance": "reassurance",
    "institution de prevoyance": "institution_prevoyance",
    "institution de prévoyance": "institution_prevoyance",
    "groupe": "groupe",
    "groupe prudentiel": "groupe",
    "systeme federal de garantie": "groupe",
    "système fédéral de garantie": "groupe",
}


def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\n", " ").replace("\u2019", "'").replace("\xad", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _norm_type_key(s: str) -> str:
    s = _norm(s).lower()
    s = s.replace("é", "e").replace("è", "e").replace("ê", "e")
    return s


def parse_acpr_pdf(pdf_path: str) -> list:
    """
    Parse the ACPR list PDF. Returns a list of normalized entity dicts.
    """
    p = Path(pdf_path)
    if not p.exists():
        logger.error(f"ACPR PDF not found: {pdf_path}")
        return []

    entities = []
    seen_ids = set()

    with pdfplumber.open(str(p)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            for table in tables:
                if not table:
                    continue
                # Skip header row if present
                start = 0
                if table[0] and table[0][0] and "SIREN" in table[0][0].upper():
                    start = 1
                for row in table[start:]:
                    if not row or len(row) < 7:
                        continue
                    siren_raw = (row[0] or "").strip()
                    matricule = (row[1] or "").strip()
                    denomination = _norm(row[2] or "")
                    type_raw = _norm(row[3] or "")
                    address = _norm(row[4] or "")
                    city = _norm(row[5] or "")
                    cp = _norm(row[6] or "")

                    if not denomination or not siren_raw:
                        continue
                    if not re.match(r"^\d{9}", siren_raw.replace(" ", "")):
                        continue

                    type_key = _norm_type_key(type_raw)
                    type_norm = "autre"
                    for k, v in TYPE_MAP.items():
                        if k in type_key:
                            type_norm = v
                            break

                    category = "groupe" if type_norm == "groupe" else type_norm

                    ent = make_entity_dict(
                        denomination=denomination,
                        siren=siren_raw,
                        matricule=matricule,
                        type_organisme=type_norm,
                        category=category,
                        address_street=address,
                        postal_code=cp,
                        city=city,
                        source="acpr_pdf_2015",
                        source_url="https://acpr.banque-france.fr/",
                    )
                    if ent["id"] in seen_ids:
                        continue
                    seen_ids.add(ent["id"])
                    entities.append(ent)

            logger.debug(f"Page {page_num}: cumulative {len(entities)} entities")

    logger.info(f"ACPR PDF parsed: {len(entities)} entities")
    return entities


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    path = sys.argv[1] if len(sys.argv) > 1 else "../20150101-listes-organismes-assurance-actifs-et-des-groupes.pdf"
    ents = parse_acpr_pdf(path)
    print(f"Parsed {len(ents)} entities")
    types = {}
    for e in ents:
        types[e["type_organisme"]] = types.get(e["type_organisme"], 0) + 1
    for t, c in sorted(types.items(), key=lambda x: -x[1]):
        print(f"  {t}: {c}")
    print("\nFirst 3:")
    for e in ents[:3]:
        print(f"  {e['denomination']} | {e['type_organisme']} | {e['address']['city']} ({e['address']['department']})")
