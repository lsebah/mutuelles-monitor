"""
Mutuelles Monitor - Base scraper utilities.
Shared HTTP session, rate limiting, normalization helpers.
"""
import hashlib
import logging
import re
import time
import unicodedata

import requests

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html, application/json, */*",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

_last_request_time = 0
MIN_DELAY = 1.0


def rate_limit(delay=None):
    global _last_request_time
    d = delay or MIN_DELAY
    elapsed = time.time() - _last_request_time
    if elapsed < d:
        time.sleep(d - elapsed)
    _last_request_time = time.time()


def fetch(url, method="GET", max_retries=3, delay=None, **kwargs):
    rate_limit(delay)
    for attempt in range(max_retries):
        try:
            if method.upper() == "POST":
                resp = SESSION.post(url, timeout=20, **kwargs)
            else:
                resp = SESSION.get(url, timeout=20, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    return None


def make_entity_id(denomination: str, siren: str = "", city: str = "") -> str:
    """Generate a stable unique ID for a directory entity."""
    if siren and len(siren) >= 9:
        raw = f"siren|{siren[:9]}"
    else:
        raw = f"name|{normalize_name(denomination)}|{normalize_city(city)}"
    return "ent_" + hashlib.md5(raw.encode()).hexdigest()[:10]


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = strip_accents(name.lower().strip())
    for suffix in ["sas", "sarl", "eurl", "sa", "sasu", "sci", "scp",
                   "selarl", "selurl", "selafa", "selas"]:
        name = re.sub(rf'\b{suffix}\b', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    name = re.sub(r'[^\w\s]', '', name)
    return name.strip()


def normalize_city(city: str) -> str:
    if not city:
        return ""
    return strip_accents(city.lower().strip())


def strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))


def clean_siren(siren: str) -> str:
    if not siren:
        return ""
    cleaned = re.sub(r'[\s\-\.]', '', str(siren))
    if re.match(r'^\d{9,14}$', cleaned):
        return cleaned[:9]
    return ""


def clean_phone(phone: str) -> str:
    if not phone:
        return ""
    cleaned = re.sub(r'[^\d+]', '', phone.strip())
    if cleaned and not cleaned.startswith('+'):
        if cleaned.startswith('0') and len(cleaned) == 10:
            cleaned = '+33' + cleaned[1:]
    return cleaned


def clean_email(email: str) -> str:
    if not email:
        return ""
    email = email.strip().lower()
    if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        return email
    return ""


def extract_department(postal_code: str) -> str:
    if not postal_code:
        return ""
    pc = str(postal_code).strip()
    if len(pc) >= 2:
        if pc.startswith("97"):
            return pc[:3]
        if pc.startswith("20"):
            try:
                code = int(pc[:5]) if len(pc) >= 5 else int(pc)
                return "2A" if code < 20200 else "2B"
            except ValueError:
                return "20"
        return pc[:2]
    return ""


def make_entity_dict(
    denomination="",
    siren="",
    matricule="",
    type_organisme="",
    category="",
    groupe="",
    address_street="",
    postal_code="",
    city="",
    phone="",
    email="",
    website="",
    source="",
    source_url="",
):
    """Create a normalized entity dictionary for the directory."""
    dept = extract_department(postal_code)
    siren = clean_siren(siren)

    from config import DEPARTMENTS, DEPT_TO_REGION

    entity = {
        "id": make_entity_id(denomination, siren, city),
        "denomination": (denomination or "").strip(),
        "denomination_normalized": normalize_name(denomination),
        "siren": siren,
        "matricule": (matricule or "").strip(),
        "type_organisme": type_organisme or "",
        "category": category or "",
        "groupe": groupe or "",
        "address": {
            "street": (address_street or "").strip(),
            "postal_code": str(postal_code or "").strip(),
            "city": (city or "").strip(),
            "department": dept,
            "department_name": DEPARTMENTS.get(dept, ""),
            "region": DEPT_TO_REGION.get(dept, ""),
        },
        "phone": clean_phone(phone),
        "email": clean_email(email),
        "website": (website or "").strip(),
        "people": [],
        "financials": None,
        "structured_products": {
            "status": "unknown",
            "evidence": "",
            "keywords_found": [],
            "source_url": "",
        },
        "sources": {},
        "source_urls": {},
        "first_seen": "",
        "last_seen": "",
        "last_enriched": "",
    }

    if source:
        entity["sources"][source] = True
        if source_url:
            entity["source_urls"][source] = source_url

    return entity
