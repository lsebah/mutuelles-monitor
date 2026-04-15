"""
Mutuelles Monitor - Folk CRM CSV export.
One row per (entity, key person). Entities without people get one row with no person.
"""
import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

FOLK_HEADERS = [
    "First Name", "Last Name", "Job Title", "Company",
    "Email", "Phone", "LinkedIn",
    "Address", "City", "Postal Code", "Department", "Region",
    "Website", "Type", "Groupe", "SIREN",
    "Primes 2024 (EUR)", "Resultat net (EUR)",
    "Produits structures",
    "Notes",
]


def _split_name(full_name: str):
    if not full_name:
        return "", ""
    parts = full_name.strip().split(None, 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def export_to_folk_csv(entities: list, output_path: Path):
    """Write one row per (entity, person) pair."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        w.writerow(FOLK_HEADERS)

        for e in entities:
            addr = e.get("address", {}) or {}
            fin = e.get("financials") or {}
            sp = e.get("structured_products") or {}
            sources = ", ".join((e.get("sources") or {}).keys())
            base = {
                "Company": e.get("denomination", ""),
                "Address": addr.get("street", ""),
                "City": addr.get("city", ""),
                "Postal Code": addr.get("postal_code", ""),
                "Department": addr.get("department_name", ""),
                "Region": addr.get("region", ""),
                "Website": e.get("website", ""),
                "Type": e.get("type_organisme", ""),
                "Groupe": e.get("groupe", ""),
                "SIREN": e.get("siren", ""),
                "Primes 2024 (EUR)": fin.get("primes_eur", "") if fin.get("year") == 2024 else "",
                "Resultat net (EUR)": fin.get("resultat_net_eur", "") if fin.get("year") == 2024 else "",
                "Produits structures": sp.get("status", "unknown"),
                "Notes": f"Sources: {sources}. {sp.get('evidence', '')}".strip(),
            }
            people = e.get("people") or []
            if not people:
                row = [
                    "", "", "", base["Company"],
                    e.get("email", ""), e.get("phone", ""), "",
                    base["Address"], base["City"], base["Postal Code"], base["Department"], base["Region"],
                    base["Website"], base["Type"], base["Groupe"], base["SIREN"],
                    base["Primes 2024 (EUR)"], base["Resultat net (EUR)"],
                    base["Produits structures"], base["Notes"],
                ]
                w.writerow(row)
                rows_written += 1
            else:
                for p in people:
                    fn, ln = _split_name(p.get("name", ""))
                    row = [
                        fn, ln, p.get("role", ""), base["Company"],
                        p.get("email", "") or e.get("email", ""),
                        e.get("phone", ""), p.get("linkedin", ""),
                        base["Address"], base["City"], base["Postal Code"], base["Department"], base["Region"],
                        base["Website"], base["Type"], base["Groupe"], base["SIREN"],
                        base["Primes 2024 (EUR)"], base["Resultat net (EUR)"],
                        base["Produits structures"], base["Notes"],
                    ]
                    w.writerow(row)
                    rows_written += 1

    logger.info(f"Folk CSV: {rows_written} rows -> {output_path}")
    return rows_written


if __name__ == "__main__":
    import json
    import sys
    logging.basicConfig(level=logging.INFO)
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("../docs/data/entities.json")
    dst = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("../docs/data/folk_import.csv")
    with open(src, encoding="utf-8") as f:
        data = json.load(f)
    export_to_folk_csv(data["entities"], dst)
