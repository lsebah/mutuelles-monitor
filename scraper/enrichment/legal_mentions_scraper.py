"""
Scrape "mentions legales" pages of company websites to extract:
  - Directeur de publication (often the DG)
  - Email contact generique (contact@, info@, communication@)
  - Phone (siege)

Strategy:
  1. Find the entity's website (existing or via DDG search)
  2. Fetch / and look for "mentions legales" link
  3. Fetch the mentions legales page
  4. Extract: directeur publication name, email, phone

Run:
    python enrichment/legal_mentions_scraper.py --limit 30
"""
import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sources.base import fetch  # noqa

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
ENTITIES_PATH = REPO / "docs" / "data" / "entities.json"

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r"(?:\+33\s?|0)[1-9](?:[\s.-]?\d{2}){4}")
DIRPUB_REGEX = re.compile(
    r"directeur\s+(?:de\s+(?:la\s+)?publication|de\s+publication)\s*[:\-]?\s*([A-Z][A-Za-zéèêà'\- ]+)",
    re.IGNORECASE
)


def _find_mentions_url(home_html: str, base_url: str) -> str:
    """Look for 'mentions legales' link in homepage."""
    soup = BeautifulSoup(home_html, "lxml")
    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True).lower()
        href = a["href"]
        if any(kw in text for kw in ["mentions l", "legal", "informations l", "infos l"]):
            return urljoin(base_url, href)
    return ""


def scrape_entity_legal(website: str) -> dict:
    """Returns {'email': ..., 'phone': ..., 'dirpub': ...} or empty dict."""
    if not website:
        return {}
    if not website.startswith("http"):
        website = "https://" + website
    try:
        r = requests.get(website, timeout=15,
                         headers={"User-Agent": "Mozilla/5.0 (mutuelles-monitor)"})
        if r.status_code != 200:
            return {}
        home_html = r.text
    except Exception as e:
        logger.debug(f"  homepage fetch failed: {e}")
        return {}

    mentions_url = _find_mentions_url(home_html, website)
    if not mentions_url:
        # Try common paths directly
        for path in ["/mentions-legales", "/mentions-legales/", "/legal", "/informations-legales"]:
            try:
                test = website.rstrip("/") + path
                tr = requests.get(test, timeout=10,
                                  headers={"User-Agent": "Mozilla/5.0 (mutuelles-monitor)"})
                if tr.status_code == 200 and len(tr.text) > 1000:
                    mentions_url = test
                    break
            except Exception:
                pass
    if not mentions_url:
        return {}

    try:
        mr = requests.get(mentions_url, timeout=15,
                          headers={"User-Agent": "Mozilla/5.0 (mutuelles-monitor)"})
        if mr.status_code != 200:
            return {}
        text = BeautifulSoup(mr.text, "lxml").get_text(" ", strip=True)
    except Exception as e:
        logger.debug(f"  mentions fetch failed: {e}")
        return {}

    out = {"source_url": mentions_url}
    em = EMAIL_REGEX.findall(text)
    # Filter generic ones first
    emails = [e for e in em if any(p in e.lower() for p in
              ["contact", "info", "communication", "service", "client", "presse"])]
    if emails:
        out["email"] = emails[0]
    elif em:
        out["email"] = em[0]

    ph = PHONE_REGEX.findall(text)
    if ph:
        out["phone"] = ph[0].replace(" ", "").replace(".", "").replace("-", "")

    dp = DIRPUB_REGEX.search(text)
    if dp:
        out["dirpub"] = dp.group(1).strip().split("\n")[0][:80]
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--all", action="store_true")
    p.add_argument("--delay", type=float, default=1.0)
    args = p.parse_args()

    with open(ENTITIES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    entities = data["entities"]

    targets = [e for e in entities if e.get("website") and not e.get("email")]
    if not args.all:
        targets = targets[: args.limit]

    logger.info(f"Scraping mentions legales for {len(targets)} entities")
    success = 0
    for i, e in enumerate(targets, 1):
        try:
            res = scrape_entity_legal(e.get("website", ""))
            if res:
                if res.get("email") and not e.get("email"):
                    e["email"] = res["email"]
                if res.get("phone") and not e.get("phone"):
                    e["phone"] = res["phone"]
                if res.get("dirpub"):
                    # Add as a person if not already there
                    e.setdefault("people", []).append({
                        "name": res["dirpub"],
                        "role": "Directeur de Publication",
                        "linkedin": "",
                        "email": res.get("email"),
                        "source": "mentions_legales",
                        "confidence": "medium",
                    })
                success += 1
                logger.info(f"[{i}/{len(targets)}] {e['denomination'][:35]:35s} email={bool(res.get('email'))} tel={bool(res.get('phone'))} dirpub={bool(res.get('dirpub'))}")
        except Exception as ex:
            logger.warning(f"  failed: {ex}")
        time.sleep(args.delay)
        if i % 25 == 0:
            with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    with open(ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"DONE. {success}/{len(targets)} successfully enriched")


if __name__ == "__main__":
    main()
