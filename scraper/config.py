"""
Mutuelles Monitor - Configuration
French departments, source URLs, type taxonomy, structured products keywords.
"""

DEPARTMENTS = {
    "01": "Ain", "02": "Aisne", "03": "Allier", "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes", "06": "Alpes-Maritimes", "07": "Ardeche", "08": "Ardennes",
    "09": "Ariege", "10": "Aube", "11": "Aude", "12": "Aveyron",
    "13": "Bouches-du-Rhone", "14": "Calvados", "15": "Cantal", "16": "Charente",
    "17": "Charente-Maritime", "18": "Cher", "19": "Correze", "2A": "Corse-du-Sud",
    "2B": "Haute-Corse", "21": "Cote-d'Or", "22": "Cotes-d'Armor", "23": "Creuse",
    "24": "Dordogne", "25": "Doubs", "26": "Drome", "27": "Eure",
    "28": "Eure-et-Loir", "29": "Finistere", "30": "Gard", "31": "Haute-Garonne",
    "32": "Gers", "33": "Gironde", "34": "Herault", "35": "Ille-et-Vilaine",
    "36": "Indre", "37": "Indre-et-Loire", "38": "Isere", "39": "Jura",
    "40": "Landes", "41": "Loir-et-Cher", "42": "Loire", "43": "Haute-Loire",
    "44": "Loire-Atlantique", "45": "Loiret", "46": "Lot", "47": "Lot-et-Garonne",
    "48": "Lozere", "49": "Maine-et-Loire", "50": "Manche", "51": "Marne",
    "52": "Haute-Marne", "53": "Mayenne", "54": "Meurthe-et-Moselle", "55": "Meuse",
    "56": "Morbihan", "57": "Moselle", "58": "Nievre", "59": "Nord",
    "60": "Oise", "61": "Orne", "62": "Pas-de-Calais", "63": "Puy-de-Dome",
    "64": "Pyrenees-Atlantiques", "65": "Hautes-Pyrenees", "66": "Pyrenees-Orientales",
    "67": "Bas-Rhin", "68": "Haut-Rhin", "69": "Rhone", "70": "Haute-Saone",
    "71": "Saone-et-Loire", "72": "Sarthe", "73": "Savoie", "74": "Haute-Savoie",
    "75": "Paris", "76": "Seine-Maritime", "77": "Seine-et-Marne", "78": "Yvelines",
    "79": "Deux-Sevres", "80": "Somme", "81": "Tarn", "82": "Tarn-et-Garonne",
    "83": "Var", "84": "Vaucluse", "85": "Vendee", "86": "Vienne",
    "87": "Haute-Vienne", "88": "Vosges", "89": "Yonne", "90": "Territoire de Belfort",
    "91": "Essonne", "92": "Hauts-de-Seine", "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne", "95": "Val-d'Oise",
    "971": "Guadeloupe", "972": "Martinique", "973": "Guyane",
    "974": "La Reunion", "976": "Mayotte",
}

DEPT_TO_REGION = {
    "75": "Ile-de-France", "77": "Ile-de-France", "78": "Ile-de-France",
    "91": "Ile-de-France", "92": "Ile-de-France", "93": "Ile-de-France",
    "94": "Ile-de-France", "95": "Ile-de-France",
    "13": "Provence-Alpes-Cote d'Azur", "83": "Provence-Alpes-Cote d'Azur",
    "84": "Provence-Alpes-Cote d'Azur", "04": "Provence-Alpes-Cote d'Azur",
    "05": "Provence-Alpes-Cote d'Azur", "06": "Provence-Alpes-Cote d'Azur",
    "69": "Auvergne-Rhone-Alpes", "01": "Auvergne-Rhone-Alpes",
    "03": "Auvergne-Rhone-Alpes", "07": "Auvergne-Rhone-Alpes",
    "15": "Auvergne-Rhone-Alpes", "26": "Auvergne-Rhone-Alpes",
    "38": "Auvergne-Rhone-Alpes", "42": "Auvergne-Rhone-Alpes",
    "43": "Auvergne-Rhone-Alpes", "63": "Auvergne-Rhone-Alpes",
    "73": "Auvergne-Rhone-Alpes", "74": "Auvergne-Rhone-Alpes",
    "31": "Occitanie", "09": "Occitanie", "11": "Occitanie",
    "12": "Occitanie", "30": "Occitanie", "32": "Occitanie",
    "34": "Occitanie", "46": "Occitanie", "48": "Occitanie",
    "65": "Occitanie", "66": "Occitanie", "81": "Occitanie", "82": "Occitanie",
    "33": "Nouvelle-Aquitaine", "16": "Nouvelle-Aquitaine",
    "17": "Nouvelle-Aquitaine", "19": "Nouvelle-Aquitaine",
    "23": "Nouvelle-Aquitaine", "24": "Nouvelle-Aquitaine",
    "40": "Nouvelle-Aquitaine", "47": "Nouvelle-Aquitaine",
    "64": "Nouvelle-Aquitaine", "79": "Nouvelle-Aquitaine",
    "86": "Nouvelle-Aquitaine", "87": "Nouvelle-Aquitaine",
    "44": "Pays de la Loire", "49": "Pays de la Loire",
    "53": "Pays de la Loire", "72": "Pays de la Loire", "85": "Pays de la Loire",
    "35": "Bretagne", "22": "Bretagne", "29": "Bretagne", "56": "Bretagne",
    "59": "Hauts-de-France", "02": "Hauts-de-France", "60": "Hauts-de-France",
    "62": "Hauts-de-France", "80": "Hauts-de-France",
    "67": "Grand Est", "68": "Grand Est", "10": "Grand Est",
    "08": "Grand Est", "51": "Grand Est", "52": "Grand Est",
    "54": "Grand Est", "55": "Grand Est", "57": "Grand Est",
    "88": "Grand Est",
    "21": "Bourgogne-Franche-Comte", "25": "Bourgogne-Franche-Comte",
    "39": "Bourgogne-Franche-Comte", "58": "Bourgogne-Franche-Comte",
    "70": "Bourgogne-Franche-Comte", "71": "Bourgogne-Franche-Comte",
    "89": "Bourgogne-Franche-Comte", "90": "Bourgogne-Franche-Comte",
    "76": "Normandie", "14": "Normandie", "27": "Normandie",
    "50": "Normandie", "61": "Normandie",
    "18": "Centre-Val de Loire", "28": "Centre-Val de Loire",
    "36": "Centre-Val de Loire", "37": "Centre-Val de Loire",
    "41": "Centre-Val de Loire", "45": "Centre-Val de Loire",
    "2A": "Corse", "2B": "Corse",
    "971": "Guadeloupe", "972": "Martinique", "973": "Guyane",
    "974": "La Reunion", "976": "Mayotte",
}

# Type taxonomy (normalized)
TYPES_ORGANISME = {
    "mutuelle": "Mutuelle",
    "assurance_vie": "Entreprise d'assurance Vie",
    "assurance_non_vie": "Entreprise d'assurance Non Vie",
    "assurance_mixte": "Entreprise d'assurance Mixte",
    "reassurance": "Entreprise de reassurance",
    "institution_prevoyance": "Institution de prevoyance",
    "groupe": "Groupe d'assurance",
    "federation": "Federation professionnelle",
    "autre": "Autre",
}

# Major mutualist / insurance groups (for tagging entities to a parent group)
GROUPES_MUTUALISTES = [
    {"name": "VYV", "full_name": "Groupe VYV", "members": ["MGEN", "Harmonie Mutuelle", "MNT", "MGEFI", "SMACL"]},
    {"name": "Malakoff Humanis", "full_name": "Malakoff Humanis", "members": ["Malakoff Mederic", "Humanis Prevoyance"]},
    {"name": "AESIO/AEMA", "full_name": "Groupe Aema (AESIO + Macif)", "members": ["AESIO Mutuelle", "Macif", "Adrea", "Apreva", "Eovi"]},
    {"name": "AG2R La Mondiale", "full_name": "AG2R La Mondiale", "members": ["AG2R Prevoyance", "La Mondiale", "AG2R Macif Prevoyance"]},
    {"name": "Covea", "full_name": "Covea (MAAF, MMA, GMF)", "members": ["MAAF", "MMA", "GMF"]},
    {"name": "Groupama", "full_name": "Groupama Assurances Mutuelles", "members": ["Groupama Gan Vie", "Gan Assurances", "Gan Patrimoine"]},
    {"name": "Credit Agricole Assurances", "full_name": "CAA / Predica / Pacifica", "members": ["Predica", "Pacifica", "Spirica"]},
    {"name": "BNP Paribas Cardif", "full_name": "BNP Paribas Cardif", "members": ["Cardif Assurance Vie", "Cardif IARD"]},
    {"name": "CNP Assurances", "full_name": "CNP Assurances (Caisse des Depots)", "members": ["CNP Assurances", "CNP IAM"]},
    {"name": "Klesia", "full_name": "Klesia", "members": ["Klesia Prevoyance", "Klesia Mut"]},
    {"name": "Apicil", "full_name": "Groupe Apicil", "members": ["Apicil Prevoyance", "Apicil Vie"]},
    {"name": "Allianz", "full_name": "Allianz France", "members": ["Allianz Vie", "Allianz IARD"]},
    {"name": "AXA", "full_name": "AXA France", "members": ["AXA France Vie", "AXA France IARD", "AXA Assurances IARD Mutuelle"]},
    {"name": "Generali", "full_name": "Generali France", "members": ["Generali Vie", "Generali IARD"]},
    {"name": "MACSF", "full_name": "MACSF (professions de sante)", "members": ["MACSF Assurances", "MACSF Prevoyance"]},
    {"name": "Matmut", "full_name": "Groupe Matmut", "members": ["Matmut Mutualite"]},
]

# Keywords for SFCR structured products detection (case-insensitive)
KEYWORDS_STRUCTURED = [
    "produits structures",
    "produit structure",
    "titres de creance structures",
    "titres structures",
    "autocall",
    "fonds a formule",
    "fonds a formules",
    "emtn",
    "euro medium term note",
    "structured notes",
    "obligations structurees",
    "produits derives complexes",
    "phoenix",
    "athena",
]

# Source configurations
SOURCES = {
    "acpr_pdf": {
        "name": "ACPR PDF 2015",
        "full_name": "ACPR - Liste organismes assurance et groupes 2015",
        "url": "https://acpr.banque-france.fr/",
        "local_pdf": "20150101-listes-organismes-assurance-actifs-et-des-groupes.pdf",
    },
    "acpr_online": {
        "name": "ACPR online",
        "full_name": "ACPR - Registre officiel des assurances",
        "url": "https://acpr.banque-france.fr/registre-officiel-des-organismes-dassurance",
    },
    "rnm": {
        "name": "RNM",
        "full_name": "Registre National des Mutuelles",
        "url": "https://www.mutuellefr.info/mutuelles-et-ndeg-rnm-registre-national-des-mutuelles-c685-p1.html",
    },
    "wikipedia": {
        "name": "Wikipedia",
        "full_name": "Wikipedia - Liste mutuelles de sante",
        "url": "https://fr.wikipedia.org/wiki/Liste_des_mutuelles_de_sant%C3%A9_en_France",
    },
    "cpme": {
        "name": "CPME",
        "full_name": "CPME - Annuaire federations professionnelles",
        "url": "https://www.cpme.fr/qui-sommes-nous/annuaire/nos-federations-professionnelles",
    },
}

# Target roles for people enrichment
TARGET_ROLES = [
    "Directeur General",
    "Directeur General Delegue",
    "Directeur Financier",
    "Tresorier",
    "Directeur des Investissements",
    "President",
]
