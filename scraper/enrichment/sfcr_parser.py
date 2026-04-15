"""
SFCR parser - downloads SFCR PDFs and detects structured products + financials.

Strategy:
- For each entity, find the SFCR URL via DuckDuckGo search ("<name>" SFCR 2024 filetype:pdf)
- Download the PDF (cached locally)
- Extract text with pypdf
- Search for KEYWORDS_STRUCTURED in the text
- Extract primes brutes (gross written premiums) and resultat net via regex
- Update the entity in entities.json

Run:
    python enrichment/sfcr_parser.py --limit 5
    python enrichment/sfcr_parser.py --entity-name "VYV"
    python enrichment/sfcr_parser.py --pdf-url https://example.com/sfcr.pdf --entity-id ent_xxx
"""
import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from pypdf import PdfReader

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sources.base import fetch  # noqa
from config import KEYWORDS_STRUCTURED  # noqa

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
DATA = REPO / "docs" / "data"
ENTITIES_PATH = DATA / "entities.json"
SFCR_CACHE = REPO / ".sfcr_cache"

DDG_URL = "https://html.duckduckgo.com/html/?q="


def find_sfcr_url(entity_name: str) -> str:
    """Search DuckDuckGo for the SFCR PDF URL."""
    q = f'"{entity_name}" SFCR 2024 filetype:pdf'
    try:
        resp = fetch(DDG_URL + quote_plus(q), delay=2.5)
    except Exception as e:
        logger.warning(f"DDG search failed: {e}")
        return ""
    soup = BeautifulSoup(resp.text, "lxml")
    for a in soup.select("a.result__a"):
        href = a.get("href", "")
        m = re.search(r"https?://[^&\s\"']+\.pdf", href)
        if m:
            return m.group(0)
    return ""


def download_pdf(url: str, entity_id: str) -> Path:
    SFCR_CACHE.mkdir(parents=True, exist_ok=True)
    dst = SFCR_CACHE / f"{entity_id}.pdf"
    if dst.exists() and dst.stat().st_size > 1000:
        logger.info(f"Cache hit: {dst.name}")
        return dst
    logger.info(f"Downloading {url}")
    resp = fetch(url, delay=3.0)
    dst.write_bytes(resp.content)
    return dst


def extract_text(pdf_path: Path) -> str:
    try:
        r = PdfReader(str(pdf_path))
        text = []
        for p in r.pages:
            try:
                text.append(p.extract_text() or "")
            except Exception:
                pass
        return "\n".join(text)
    except Exception as e:
        logger.warning(f"PDF parse failed: {e}")
        return ""


def detect_structured(text: str) -> dict:
    """Search for structured products keywords. Returns dict with status, evidence, keywords_found."""
    if not text:
        return {"status": "unknown", "evidence": "", "keywords_found": []}
    low = text.lower()
    found = []
    for kw in KEYWORDS_STRUCTURED:
        if kw in low:
            found.append(kw)
    if not found:
        return {"status": "no", "evidence": "Aucun mot-cle structures detecte dans le SFCR", "keywords_found": []}
    # Extract a context snippet around the first match
    idx = low.find(found[0])
    snippet = text[max(0, idx - 100): idx + 200].replace("\n", " ").strip()
    snippet = re.sub(r"\s+", " ", snippet)[:300]
    return {
        "status": "yes",
        "evidence": snippet,
        "keywords_found": found,
    }


def extract_financials(text: str) -> dict:
    """Try to extract primes brutes emises and resultat net from SFCR text."""
    fin = {"year": 2024, "primes_eur": None, "resultat_net_eur": None, "source": "SFCR"}
    if not text:
        return fin
    # Look for "primes brutes" patterns
    # Format examples: "Primes brutes acquises : 1 234 567 (en milliers d'euros)"
    primes_pat = re.search(
        r"primes\s+(?:brutes\s+)?(?:acquises|emises)[^\d]{0,80}([\d\s\u00a0,.]{4,20})",
        text, re.IGNORECASE)
    if primes_pat:
        n = _parse_number(primes_pat.group(1))
        if n is not None:
            # SFCR usually in milliers d'euros
            in_thousands = "millier" in text[max(0, primes_pat.start() - 200): primes_pat.end() + 200].lower()
            in_millions = "million" in text[max(0, primes_pat.start() - 200): primes_pat.end() + 200].lower()
            if in_thousands:
                n *= 1000
            elif in_millions:
                n *= 1_000_000
            fin["primes_eur"] = int(n)

    rn_pat = re.search(
        r"r[ée]sultat\s+net[^\d]{0,80}([\d\s\u00a0,.\-]{3,20})",
        text, re.IGNORECASE)
    if rn_pat:
        n = _parse_number(rn_pat.group(1))
        if n is not None:
            in_thousands = "millier" in text[max(0, rn_pat.start() - 200): rn_pat.end() + 200].lower()
            in_millions = "million" in text[max(0, rn_pat.start() - 200): rn_pat.end() + 200].lower()
            if in_thousands:
                n *= 1000
            elif in_millions:
                n *= 1_000_000
            fin["resultat_net_eur"] = int(n)
    return fin


def _parse_number(s: str):
    if not s:
        return None
    s = s.strip().replace("\u00a0", "").replace(" ", "")
    # French decimal: "1.234,56" or "1234,56"
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def process_entity(entity: dict) -> bool:
    name = entity.get("denomination", "")
    if not name:
        return False
    url = find_sfcr_url(name)
    if not url:
        logger.info(f"No SFCR found for {name}")
        return False
    try:
        pdf = download_pdf(url, entity["id"])
    except Exception as e:
        logger.warning(f"Download failed for {name}: {e}")
        return False
    text = extract_text(pdf)
    if not text:
        return False
    sp = detect_structured(text)
    sp["source_url"] = url
    entity["structured_products"] = sp
    fin = extract_financials(text)
    if fin.get("primes_eur") or fin.get("resultat_net_eur"):
        fin["source_url"] = url
        entity["financials"] = fin
    entity["last_enriched"] = datetime.utcnow().strftime("%Y-%m-%d")
    logger.info(f"  -> structures={sp['status']} primes={fin.get('primes_eur')} rn={fin.get('resultat_net_eur')}")
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--entity-name", help="Substring match")
    p.add_argument("--entity-id")
    p.add_argument("--pdf-url", help="Skip search, use this URL")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    entities = data["entities"]

    if args.entity_id:
        targets = [e for e in entities if e["id"] == args.entity_id]
    elif args.entity_name:
        sub = args.entity_name.lower()
        targets = [e for e in entities if sub in e.get("denomination", "").lower()]
    else:
        # Priority: those without structured_products status and large groups
        targets = [e for e in entities if e.get("structured_products", {}).get("status") == "unknown"]
        targets = sorted(targets, key=lambda e: -len(e.get("groupe", "") or ""))[: args.limit]

    if args.pdf_url and targets:
        e = targets[0]
        pdf = download_pdf(args.pdf_url, e["id"])
        text = extract_text(pdf)
        sp = detect_structured(text)
        sp["source_url"] = args.pdf_url
        e["structured_products"] = sp
        fin = extract_financials(text)
        if fin.get("primes_eur"):
            fin["source_url"] = args.pdf_url
            e["financials"] = fin
        e["last_enriched"] = datetime.utcnow().strftime("%Y-%m-%d")
    else:
        for i, e in enumerate(targets, 1):
            logger.info(f"[{i}/{len(targets)}] {e['denomination']}")
            process_entity(e)

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Wrote {ENTITIES_PATH}")


if __name__ == "__main__":
    main()
