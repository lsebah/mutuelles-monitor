"""
Apply financial seed data to entities.json.
Sources: public annual reports and SFCR filings (2024).

Run:
    python enrichment/apply_financials_seed.py
"""
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from activity_logger import log_event, format_eur  # noqa

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

REPO = Path(__file__).resolve().parent.parent.parent
ENTITIES_PATH = REPO / "docs" / "data" / "entities.json"

# Financial seed - verified from public sources (press releases, SFCR 2024)
# primes and resultat_net in EUR, sp_ratio and rendement in %
FINANCIALS_SEED = [
    # --- CNP Assurances group ---
    {"match": "CNP ASSURANCES", "groupe": "CNP Assurances", "exact_denom": True,
     "year": 2024, "primes_eur": 37_400_000_000, "resultat_net_eur": 1_582_000_000,
     "source": "Rapport annuel 2024"},
    # --- Credit Agricole Assurances ---
    {"match": "PREDICA", "groupe": "Credit Agricole Assurances",
     "year": 2024, "primes_eur": 43_600_000_000, "resultat_net_eur": 1_959_000_000,
     "sp_ratio": 70.2, "source": "CA Assurances resultats 12M 2024"},
    {"match": "SPIRICA", "groupe": "Credit Agricole Assurances",
     "year": 2024, "primes_eur": 3_015_000_000, "source": "SFCR Spirica 2024"},
    {"match": "PACIFICA", "groupe": "Credit Agricole Assurances",
     "year": 2024, "primes_eur": 5_900_000_000, "source": "SFCR Pacifica 2024"},
    # --- BNP Paribas Cardif ---
    {"match": "CARDIF ASSURANCE VIE", "groupe": "BNP Paribas Cardif",
     "year": 2024, "primes_eur": 36_400_000_000, "resultat_net_eur": 1_600_000_000,
     "source": "BNP Paribas Cardif resultats 2024"},
    # --- Allianz France ---
    {"match": "ALLIANZ VIE", "groupe": "Allianz",
     "year": 2024, "primes_eur": 13_100_000_000, "source": "Allianz France chiffres cles 2024"},
    # --- Generali ---
    {"match": "GENERALI VIE", "groupe": "Generali",
     "year": 2024, "primes_eur": 12_892_000_000, "resultat_net_eur": 421_000_000,
     "source": "RSSF Generali Vie 2024"},
    # --- Covea ---
    {"match": "COVEA COOPERATIONS", "groupe": "Covea",
     "year": 2024, "primes_eur": 27_700_000_000, "resultat_net_eur": 1_200_000_000,
     "source": "Rapport annuel Covea 2024"},
    # --- Groupama ---
    {"match": "GROUPAMA GAN VIE", "groupe": "Groupama",
     "year": 2024, "primes_eur": 18_500_000_000, "resultat_net_eur": 961_000_000,
     "source": "Groupama resultats annuels 2024"},
    # --- AG2R La Mondiale ---
    {"match": "AG2R PREVOYANCE", "groupe": "AG2R La Mondiale",
     "year": 2024, "primes_eur": 12_800_000_000, "resultat_net_eur": 183_000_000,
     "source": "AG2R La Mondiale resultats annuels 2024"},
    # --- Groupe VYV ---
    {"match": "MGEN", "groupe": "VYV", "exact_denom": True,
     "year": 2024, "primes_eur": 3_000_000_000, "resultat_net_eur": 96_000_000,
     "source": "MGEN resultats 2024"},
    {"match": "HARMONIE MUTUELLE", "groupe": "VYV",
     "year": 2024, "primes_eur": 4_300_000_000, "resultat_net_eur": 99_000_000,
     "source": "Harmonie Mutuelle resultats consolides 2024"},
    # --- Malakoff Humanis ---
    {"match": "MALAKOFF HUMANIS", "groupe": "Malakoff Humanis",
     "year": 2024, "primes_eur": 7_520_000_000, "resultat_net_eur": 211_000_000,
     "source": "Malakoff Humanis resultats 2024"},
    # --- AESIO / AEMA / Macif ---
    {"match": "AESIO MUTUELLE", "groupe": "AESIO/AEMA",
     "year": 2024, "primes_eur": 16_100_000_000, "resultat_net_eur": 211_000_000,
     "source": "AEMA Groupe resultats 2024"},
    # --- MACSF ---
    {"match": "MACSF SGAM", "groupe": "MACSF",
     "year": 2024, "primes_eur": 3_040_000_000, "resultat_net_eur": 300_000_000,
     "source": "MACSF resultats annuels 2024"},
    {"match": "MACSF PREVOYANCE", "groupe": "MACSF",
     "year": 2024, "primes_eur": 3_040_000_000, "resultat_net_eur": 300_000_000,
     "source": "MACSF resultats annuels 2024"},
    # --- Matmut ---
    {"match": "SGAM MATMUT", "groupe": "Matmut",
     "year": 2024, "primes_eur": 3_176_000_000, "resultat_net_eur": 104_000_000,
     "source": "Matmut resultats 2024"},
    {"match": "MATMUT MUTUALITE", "groupe": "Matmut",
     "year": 2024, "primes_eur": 3_176_000_000, "resultat_net_eur": 104_000_000,
     "source": "Matmut resultats 2024"},
    # --- Apicil ---
    {"match": "APICIL PREVOYANCE", "groupe": "Apicil",
     "year": 2024, "primes_eur": 3_900_000_000, "resultat_net_eur": 58_000_000,
     "source": "Apicil resultats 2024"},
    # --- Sogecap / SG Assurances ---
    {"match": "SOGECAP", "groupe": "",
     "year": 2024, "primes_eur": 20_300_000_000, "resultat_net_eur": 393_000_000,
     "source": "SG Assurances resultats 2024"},
    # --- Klesia ---
    {"match": "KLESIA", "groupe": "Klesia", "exact_denom": False,
     "year": 2024, "primes_eur": 2_800_000_000, "source": "SFCR Klesia 2024"},
]


def main():
    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    entities = data["entities"]

    applied = 0
    for seed in FINANCIALS_SEED:
        match_str = seed["match"].upper()
        exact = seed.get("exact_denom", False)

        # Find matching entity
        target = None
        for e in entities:
            denom = (e.get("denomination") or "").upper()
            grp = (e.get("groupe") or "")
            if exact:
                if denom == match_str:
                    target = e
                    break
            else:
                if match_str in denom:
                    # Prefer entities from the right groupe
                    if seed.get("groupe") and grp == seed["groupe"]:
                        target = e
                        break
                    elif not target:
                        target = e

        if not target:
            print(f"  SKIP (not found): {seed['match']}")
            continue

        fin = {
            "year": seed.get("year", 2024),
            "primes_eur": seed.get("primes_eur"),
            "resultat_net_eur": seed.get("resultat_net_eur"),
            "sp_ratio": seed.get("sp_ratio"),
            "rendement_actifs_pct": seed.get("rendement_actifs_pct"),
            "source": seed.get("source", "public"),
        }
        target["financials"] = fin
        applied += 1
        print(f"  OK: {target['denomination'][:45]:45s} primes={fin['primes_eur']} rn={fin.get('resultat_net_eur')}")

        # Log activity event
        parts = []
        if fin["primes_eur"]:
            parts.append(f"Primes {fin['year']}: {format_eur(fin['primes_eur'])}")
        if fin.get("resultat_net_eur") is not None:
            parts.append(f"R. net: {format_eur(fin['resultat_net_eur'])}")
        if fin.get("sp_ratio") is not None:
            parts.append(f"S/P: {fin['sp_ratio']}%")
        if fin.get("rendement_actifs_pct") is not None:
            parts.append(f"Rdt: {fin['rendement_actifs_pct']}%")
        log_event(
            "financial_update",
            target["id"],
            target["denomination"],
            " | ".join(parts),
            {"year": fin["year"], "primes_eur": fin["primes_eur"],
             "resultat_net_eur": fin.get("resultat_net_eur"),
             "sp_ratio": fin.get("sp_ratio"),
             "rendement_actifs_pct": fin.get("rendement_actifs_pct"),
             "source": fin.get("source")},
        )

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\nApplied financials to {applied} entities. Wrote {ENTITIES_PATH}")


if __name__ == "__main__":
    main()
