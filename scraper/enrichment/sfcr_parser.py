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
sys.path.insert(0, str(Path(__file__).resolve().parent))
from sources.base import fetch  # noqa
from config import KEYWORDS_STRUCTURED  # noqa
from activity_logger import log_event, format_eur  # noqa

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
DATA = REPO / "docs" / "data"
ENTITIES_PATH = DATA / "entities.json"
SFCR_CACHE = REPO / ".sfcr_cache"

DDG_URL = "https://html.duckduckgo.com/html/?q="


def find_sfcr_url(entity_name: str) -> str:
    """Search DuckDuckGo for the SFCR PDF URL using Playwright browser."""
    from sources.base import fetch_browser
    q = f'"{entity_name}" SFCR 2024 filetype:pdf'
    try:
        html = fetch_browser(DDG_URL + quote_plus(q), wait_ms=2500)
    except Exception as e:
        logger.warning(f"DDG search failed: {e}")
        return ""
    from urllib.parse import unquote
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a.result__a"):
        href = unquote(a.get("href", ""))
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


def _detect_unit_multiplier(text: str, match_start: int, match_end: int) -> int:
    """Detect if surrounding text indicates milliers or millions d'euros."""
    context = text[max(0, match_start - 300): match_end + 300].lower()
    if "millier" in context or "k€" in context or "en k" in context:
        return 1_000
    if "million" in context or "m€" in context or "en m" in context:
        return 1_000_000
    if "milliard" in context or "md€" in context or "md " in context:
        return 1_000_000_000
    return 1


def extract_financials(text: str) -> dict:
    """Extract financial KPIs from SFCR text: primes, resultat net, S/P ratio, rendement des actifs."""
    fin = {
        "year": 2024,
        "primes_eur": None,
        "resultat_net_eur": None,
        "fonds_propres_eur": None,
        "scr_ratio_pct": None,
        "sp_ratio": None,
        "rendement_actifs_pct": None,
        "source": "SFCR",
    }
    if not text:
        return fin

    # --- 1. Primes brutes emises ---
    primes_pat = re.search(
        r"primes\s+(?:brutes\s+)?(?:acquises|emises|ecrites)[^\d]{0,80}([\d\s\u00a0,.]{4,20})",
        text, re.IGNORECASE)
    if primes_pat:
        n = _parse_number(primes_pat.group(1))
        if n is not None:
            mult = _detect_unit_multiplier(text, primes_pat.start(), primes_pat.end())
            fin["primes_eur"] = int(n * mult)

    # --- 2. Resultat net ---
    rn_pat = re.search(
        r"r[ée]sultat\s+net[^\d]{0,80}([-\u2212]?\s*[\d\s\u00a0,.]{3,20})",
        text, re.IGNORECASE)
    if rn_pat:
        n = _parse_number(rn_pat.group(1))
        if n is not None:
            mult = _detect_unit_multiplier(text, rn_pat.start(), rn_pat.end())
            fin["resultat_net_eur"] = int(n * mult)

    # --- 3. Ratio S/P (sinistres sur primes, aka combined/loss ratio) ---
    # SFCR patterns: "ratio S/P : 85,3%", "ratio sinistres/primes", "ratio combiné",
    # "taux de sinistralité : 72%", "loss ratio"
    sp_patterns = [
        r"ratio\s+(?:s\s*/\s*p|sinistres?\s*/\s*primes?|combin[ée])[^\d]{0,60}([\d\s\u00a0,.]{2,8})\s*%",
        r"taux\s+de\s+sinistralit[ée][^\d]{0,60}([\d\s\u00a0,.]{2,8})\s*%",
        r"(?:loss|combined)\s+ratio[^\d]{0,60}([\d\s\u00a0,.]{2,8})\s*%",
        r"s\s*/\s*p[^\d]{0,40}([\d\s\u00a0,.]{2,8})\s*%",
    ]
    for pat in sp_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            n = _parse_number(m.group(1))
            if n is not None and 10 < n < 300:  # sanity check: ratio should be 10%-300%
                fin["sp_ratio"] = round(n, 1)
                break

    # --- 3b. Fonds propres (Solvency II "own funds", proxy for "réserves") ---
    # SFCR patterns: "fonds propres éligibles : 1 234 M€", "fonds propres totaux",
    # "eligible own funds", "fonds propres de base"
    fp_patterns = [
        r"fonds\s+propres\s+(?:[ée]ligibles|totaux|de\s+base)[^\d]{0,80}([\d\s ,.]{4,20})",
        r"fonds\s+propres\s+couvrant\s+(?:le\s+)?scr[^\d]{0,80}([\d\s ,.]{4,20})",
        r"eligible\s+own\s+funds[^\d]{0,80}([\d\s ,.]{4,20})",
        r"total\s+(?:eligible\s+)?own\s+funds[^\d]{0,80}([\d\s ,.]{4,20})",
    ]
    for pat in fp_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            n = _parse_number(m.group(1))
            if n is not None and n > 0:
                mult = _detect_unit_multiplier(text, m.start(), m.end())
                fin["fonds_propres_eur"] = int(n * mult)
                break

    # --- 3c. Ratio de couverture du SCR (Solvency II coverage ratio) ---
    # SFCR patterns: "ratio de couverture du SCR : 215%", "SCR coverage ratio",
    # "ratio de solvabilité"
    scr_patterns = [
        r"ratio\s+de\s+couverture\s+(?:du\s+)?scr[^\d]{0,60}([\d\s ,.]{2,8})\s*%",
        r"ratio\s+de\s+solvabilit[ée][^\d]{0,60}([\d\s ,.]{2,8})\s*%",
        r"scr\s+coverage\s+ratio[^\d]{0,60}([\d\s ,.]{2,8})\s*%",
        r"taux\s+de\s+couverture\s+(?:du\s+)?scr[^\d]{0,60}([\d\s ,.]{2,8})\s*%",
    ]
    for pat in scr_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            n = _parse_number(m.group(1))
            if n is not None and 50 < n < 1000:  # SCR ratio plausible band
                fin["scr_ratio_pct"] = round(n, 0)
                break

    # --- 4. Rendement des actifs (investment return / yield on assets) ---
    # SFCR patterns: "rendement des actifs : 3,2%", "rendement financier",
    # "taux de rendement des placements", "rendement des investissements"
    rdt_patterns = [
        r"rendement\s+(?:des\s+)?(?:actifs|placements|investissements|portefeuille)[^\d]{0,60}([-\u2212]?\s*[\d\s\u00a0,.]{1,8})\s*%",
        r"taux\s+de\s+rendement\s+(?:des\s+)?(?:actifs|placements|investissements)[^\d]{0,60}([-\u2212]?\s*[\d\s\u00a0,.]{1,8})\s*%",
        r"rendement\s+financier[^\d]{0,60}([-\u2212]?\s*[\d\s\u00a0,.]{1,8})\s*%",
        r"performance\s+(?:des\s+)?(?:actifs|placements)[^\d]{0,60}([-\u2212]?\s*[\d\s\u00a0,.]{1,8})\s*%",
    ]
    for pat in rdt_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            n = _parse_number(m.group(1))
            if n is not None and -20 < n < 30:  # sanity check: yield between -20% and +30%
                fin["rendement_actifs_pct"] = round(n, 2)
                break

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
    if any(fin.get(k) for k in ("primes_eur", "resultat_net_eur",
                                 "fonds_propres_eur", "scr_ratio_pct")):
        fin["source_url"] = url
        entity["financials"] = fin
    entity["last_enriched"] = datetime.utcnow().strftime("%Y-%m-%d")
    logger.info(
        f"  -> structures={sp['status']} primes={fin.get('primes_eur')} "
        f"rn={fin.get('resultat_net_eur')} fp={fin.get('fonds_propres_eur')} "
        f"scr={fin.get('scr_ratio_pct')}%"
    )

    # Log activity events
    if sp["status"] in ("yes", "no"):
        log_event("structured_update", entity["id"], name,
                  f"Produits structures: {sp['status']}",
                  {"status": sp["status"], "keywords_found": sp.get("keywords_found", [])})
    if any(fin.get(k) for k in ("primes_eur", "resultat_net_eur",
                                 "fonds_propres_eur", "scr_ratio_pct")):
        parts = []
        if fin.get("primes_eur"):
            parts.append(f"Primes: {format_eur(fin['primes_eur'])}")
        if fin.get("resultat_net_eur"):
            parts.append(f"R. net: {format_eur(fin['resultat_net_eur'])}")
        if fin.get("fonds_propres_eur"):
            parts.append(f"FP: {format_eur(fin['fonds_propres_eur'])}")
        if fin.get("scr_ratio_pct"):
            parts.append(f"SCR: {fin['scr_ratio_pct']}%")
        log_event("financial_update", entity["id"], name,
                  " | ".join(parts),
                  {"year": fin.get("year"), "primes_eur": fin.get("primes_eur"),
                   "resultat_net_eur": fin.get("resultat_net_eur"),
                   "fonds_propres_eur": fin.get("fonds_propres_eur"),
                   "scr_ratio_pct": fin.get("scr_ratio_pct"),
                   "source": "SFCR"})
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
