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

    # --- AXA France ---
    {"match": "AXA FRANCE VIE", "groupe": "AXA",
     "year": 2024, "primes_eur": 15_200_000_000, "resultat_net_eur": 1_220_000_000,
     "source": "AXA France comptes 2024"},
    {"match": "AXA FRANCE IARD", "groupe": "AXA",
     "year": 2024, "primes_eur": 6_500_000_000, "resultat_net_eur": 430_000_000,
     "source": "AXA France comptes 2024"},
    {"match": "AXA CORPORATE SOLUTIONS", "groupe": "AXA",
     "year": 2024, "primes_eur": 2_100_000_000, "source": "SFCR AXA CS 2024"},
    {"match": "AXA ASSURANCES IARD MUTUELLE", "groupe": "AXA",
     "year": 2024, "primes_eur": 4_200_000_000, "source": "SFCR AXA AIM 2024"},

    # --- Swisslife France ---
    {"match": "SWISSLIFE ASSURANCE ET PATRIMOINE", "groupe": "",
     "year": 2024, "primes_eur": 5_500_000_000, "resultat_net_eur": 182_000_000,
     "source": "Swiss Life France resultats 2024"},
    {"match": "SWISSLIFE PREVOYANCE ET SANTE", "groupe": "",
     "year": 2024, "primes_eur": 1_230_000_000, "source": "SFCR SLPS 2024"},
    {"match": "SWISSLIFE ASSURANCES DE BIENS", "groupe": "",
     "year": 2024, "primes_eur": 480_000_000, "source": "SFCR SLAB 2024"},

    # --- Abeille (ex-Aviva) ---
    {"match": "ABEILLE VIE", "groupe": "",
     "year": 2024, "primes_eur": 4_100_000_000, "resultat_net_eur": 98_000_000,
     "source": "Abeille Assurances resultats 2024"},
    {"match": "ABEILLE IARD & SANTE", "groupe": "",
     "year": 2024, "primes_eur": 3_050_000_000, "source": "SFCR Abeille IARD 2024"},
    {"match": "ABEILLE EPARGNE RETRAITE", "groupe": "",
     "year": 2024, "primes_eur": 890_000_000, "source": "SFCR Abeille ER 2024"},

    # --- Suravenir (Arkea) ---
    {"match": "SURAVENIR", "groupe": "", "exact_denom": True,
     "year": 2024, "primes_eur": 3_100_000_000, "resultat_net_eur": 52_000_000,
     "source": "Credit Mutuel Arkea resultats 2024"},
    {"match": "SURAVENIR ASSURANCES", "groupe": "",
     "year": 2024, "primes_eur": 620_000_000, "source": "SFCR Suravenir A 2024"},

    # --- HSBC ---
    {"match": "HSBC ASSURANCES VIE", "groupe": "",
     "year": 2024, "primes_eur": 1_450_000_000, "source": "SFCR HSBC Vie 2024"},

    # --- Natixis / BPCE Assurances ---
    {"match": "NATIXIS ASSURANCES", "groupe": "",
     "year": 2024, "primes_eur": 2_900_000_000, "source": "BPCE Assurances 2024"},
    {"match": "BPCE VIE", "groupe": "",
     "year": 2024, "primes_eur": 13_200_000_000, "resultat_net_eur": 470_000_000,
     "source": "BPCE Assurances resultats 2024"},
    {"match": "BPCE ASSURANCES IARD", "groupe": "",
     "year": 2024, "primes_eur": 1_500_000_000, "source": "SFCR BPCE IARD 2024"},
    {"match": "BPCE IARD", "groupe": "",
     "year": 2024, "primes_eur": 950_000_000, "source": "SFCR BPCE IARD 2024"},

    # --- La Mondiale (AG2R) ---
    {"match": "LA MONDIALE", "groupe": "AG2R La Mondiale", "exact_denom": True,
     "year": 2024, "primes_eur": 7_400_000_000, "resultat_net_eur": 245_000_000,
     "source": "AG2R LM resultats 2024"},
    {"match": "LA MONDIALE PARTENAIRE", "groupe": "AG2R La Mondiale",
     "year": 2024, "primes_eur": 4_200_000_000, "source": "SFCR LMP 2024"},
    {"match": "ARIAL CNP ASSURANCES", "groupe": "CNP Assurances",
     "year": 2024, "primes_eur": 1_250_000_000, "source": "SFCR Arial 2024"},

    # --- Covéa subsidiaries ---
    {"match": "MAAF ASSURANCES", "groupe": "Covea",
     "year": 2024, "primes_eur": 4_580_000_000, "source": "Covea comptes 2024"},
    {"match": "MAAF VIE", "groupe": "Covea",
     "year": 2024, "primes_eur": 1_820_000_000, "source": "Covea comptes 2024"},
    {"match": "MMA IARD", "groupe": "Covea",
     "year": 2024, "primes_eur": 3_450_000_000, "source": "Covea comptes 2024"},
    {"match": "MMA VIE", "groupe": "Covea",
     "year": 2024, "primes_eur": 1_520_000_000, "source": "Covea comptes 2024"},
    {"match": "GMF ASSURANCES", "groupe": "Covea",
     "year": 2024, "primes_eur": 2_530_000_000, "source": "Covea comptes 2024"},
    {"match": "GMF VIE", "groupe": "Covea",
     "year": 2024, "primes_eur": 810_000_000, "source": "Covea comptes 2024"},

    # --- MAIF ---
    {"match": "FILIA-MAIF", "groupe": "",
     "year": 2024, "primes_eur": 4_200_000_000, "resultat_net_eur": 152_000_000,
     "source": "MAIF resultats 2024"},
    {"match": "PARNASSE MAIF", "groupe": "",
     "year": 2024, "primes_eur": 480_000_000, "source": "SFCR Parnasse MAIF 2024"},

    # --- Macif (Aéma) entities ---
    {"match": "MACIF VIE SE", "groupe": "AESIO/AEMA",
     "year": 2024, "primes_eur": 2_800_000_000, "source": "AEMA resultats 2024"},
    {"match": "MACIF MUTUALITE", "groupe": "AESIO/AEMA",
     "year": 2024, "primes_eur": 4_650_000_000, "source": "AEMA resultats 2024"},
    {"match": "MACIFILIA", "groupe": "AESIO/AEMA",
     "year": 2024, "primes_eur": 580_000_000, "source": "SFCR Macifilia 2024"},

    # --- CNP subsidiaries ---
    {"match": "CNP ASSURANCES IARD", "groupe": "CNP Assurances",
     "year": 2024, "primes_eur": 2_450_000_000, "source": "CNP groupe 2024"},
    {"match": "CNP RETRAITE", "groupe": "CNP Assurances",
     "year": 2024, "primes_eur": 1_500_000_000, "source": "CNP groupe 2024"},
    {"match": "CNP CAUTION", "groupe": "CNP Assurances",
     "year": 2024, "primes_eur": 890_000_000, "source": "SFCR CNP Caution 2024"},
    {"match": "CNP ASSURANCES SANTE INDIVIDUELLE", "groupe": "CNP Assurances",
     "year": 2024, "primes_eur": 620_000_000, "source": "SFCR CNP SI 2024"},

    # --- Crédit Mutuel Assurances (ACM) ---
    {"match": "ASSURANCES DU CREDIT MUTUEL IARD", "groupe": "",
     "year": 2024, "primes_eur": 2_050_000_000, "source": "ACM resultats 2024"},
    {"match": "ASSURANCES DU CREDIT MUTUEL VIE", "groupe": "",
     "year": 2024, "primes_eur": 3_200_000_000, "source": "ACM resultats 2024"},

    # --- Réassureurs majeurs (FR) ---
    {"match": "SCOR SE", "groupe": "",
     "year": 2024, "primes_eur": 17_800_000_000, "resultat_net_eur": 780_000_000,
     "source": "SCOR annual report 2024"},
    {"match": "CAISSE CENTRALE DE REASSURANCE", "groupe": "",
     "year": 2024, "primes_eur": 2_100_000_000, "resultat_net_eur": 185_000_000,
     "source": "CCR resultats 2024"},

    # --- Mutuelles importantes ---
    {"match": "MATMUT VIE", "groupe": "Matmut",
     "year": 2024, "primes_eur": 720_000_000, "source": "Matmut comptes 2024"},
    {"match": "MNT", "groupe": "VYV", "exact_denom": True,
     "year": 2024, "primes_eur": 620_000_000, "source": "MNT resultats 2024"},
    {"match": "SMACL ASSURANCES", "groupe": "VYV",
     "year": 2024, "primes_eur": 280_000_000, "source": "SMACL resultats 2024"},
    {"match": "UNEO", "groupe": "", "exact_denom": True,
     "year": 2024, "primes_eur": 420_000_000, "source": "Uneo resultats 2024"},
    {"match": "MUTUELLE GENERALE", "groupe": "",
     "year": 2024, "primes_eur": 1_500_000_000, "source": "La Mutuelle Generale 2024"},
    {"match": "INTERIALE", "groupe": "",
     "year": 2024, "primes_eur": 350_000_000, "source": "Interiale resultats 2024"},
    {"match": "MGEFI", "groupe": "VYV", "exact_denom": True,
     "year": 2024, "primes_eur": 290_000_000, "source": "MGEFI resultats 2024"},

    # --- Apicil additional ---
    {"match": "APICIL EPARGNE", "groupe": "Apicil",
     "year": 2024, "primes_eur": 1_800_000_000, "source": "Apicil resultats 2024"},
    {"match": "APICIL ASSURANCES", "groupe": "Apicil",
     "year": 2024, "primes_eur": 1_200_000_000, "source": "Apicil resultats 2024"},

    # --- Klesia subsidiaries ---
    {"match": "KLESIA PREVOYANCE", "groupe": "Klesia",
     "year": 2024, "primes_eur": 1_800_000_000, "source": "Klesia resultats 2024"},
    {"match": "KLESIA MUT", "groupe": "Klesia",
     "year": 2024, "primes_eur": 420_000_000, "source": "Klesia resultats 2024"},

    # --- Autres ---
    {"match": "GENERALI IARD", "groupe": "Generali",
     "year": 2024, "primes_eur": 3_150_000_000, "source": "Generali France 2024"},
    {"match": "ALLIANZ IARD", "groupe": "Allianz",
     "year": 2024, "primes_eur": 5_200_000_000, "source": "Allianz France 2024"},
    {"match": "ALBINGIA", "groupe": "",
     "year": 2024, "primes_eur": 980_000_000, "resultat_net_eur": 95_000_000,
     "source": "Albingia comptes 2024"},
    {"match": "THELEM", "groupe": "",
     "year": 2024, "primes_eur": 340_000_000, "source": "Thelem resultats 2024"},
    {"match": "SMABTP", "groupe": "",
     "year": 2024, "primes_eur": 1_240_000_000, "source": "SMABTP resultats 2024"},
    {"match": "SMA SA", "groupe": "",
     "year": 2024, "primes_eur": 620_000_000, "source": "SMA comptes 2024"},
    {"match": "MUTEX", "groupe": "",
     "year": 2024, "primes_eur": 310_000_000, "source": "Mutex resultats 2024"},
    {"match": "PREVOIR VIE", "groupe": "",
     "year": 2024, "primes_eur": 520_000_000, "source": "Prevoir resultats 2024"},
    {"match": "MILLEIS VIE", "groupe": "",
     "year": 2024, "primes_eur": 850_000_000, "source": "Milleis resultats 2024"},
    {"match": "UNOFI-ASSURANCES", "groupe": "",
     "year": 2024, "primes_eur": 380_000_000, "source": "Unofi resultats 2024"},
    {"match": "MONCEAU", "groupe": "",
     "year": 2024, "primes_eur": 720_000_000, "source": "Monceau Assurances 2024"},
    {"match": "CORUM LIFE", "groupe": "",
     "year": 2024, "primes_eur": 480_000_000, "source": "Corum L'Epargne 2024"},
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
