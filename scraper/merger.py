"""
Mutuelles Monitor - Cross-source deduplication and merging.
Merges entities from ACPR, RNM, Wikipedia, CPME into a unified directory.
"""
import logging
from difflib import SequenceMatcher

from sources.base import normalize_name, normalize_city

logger = logging.getLogger(__name__)


def merge_all_sources(*source_lists):
    """
    Merge entity lists from multiple sources into a single deduplicated list.

    Strategy:
    1. Exact SIREN match -> merge
    2. Exact normalized name + same city -> merge
    3. Fuzzy name match (>0.85) + same department -> merge
    4. No match -> new entry
    """
    merged = {}
    siren_index = {}
    name_city_index = {}
    name_dept_index = {}

    total_input = 0
    merge_count = 0

    for entities in source_lists:
        for entity in entities:
            total_input += 1
            siren = entity.get("siren", "")
            norm_name = entity.get("denomination_normalized") or normalize_name(entity.get("denomination", ""))
            city = normalize_city(entity.get("address", {}).get("city", ""))
            dept = entity.get("address", {}).get("department", "")

            matched_id = None

            if siren and siren in siren_index:
                matched_id = siren_index[siren]

            if not matched_id and norm_name and city:
                key = (norm_name, city)
                if key in name_city_index:
                    matched_id = name_city_index[key]

            if not matched_id and norm_name and dept:
                if dept in name_dept_index:
                    for candidate_id in name_dept_index[dept]:
                        candidate = merged[candidate_id]
                        candidate_name = candidate.get("denomination_normalized", "")
                        if _fuzzy_match(norm_name, candidate_name, threshold=0.88):
                            matched_id = candidate_id
                            break

            if matched_id:
                _merge_entity(merged[matched_id], entity)
                merge_count += 1
            else:
                eid = entity["id"]
                merged[eid] = entity.copy()
                if siren:
                    siren_index[siren] = eid
                if norm_name and city:
                    name_city_index[(norm_name, city)] = eid
                if dept:
                    name_dept_index.setdefault(dept, []).append(eid)

    logger.info(f"Merger: {total_input} input -> {len(merged)} unique ({merge_count} merged)")
    return list(merged.values())


def _merge_entity(existing, new):
    for source, val in new.get("sources", {}).items():
        existing.setdefault("sources", {})[source] = val
    for source, url in new.get("source_urls", {}).items():
        if source not in existing.get("source_urls", {}):
            existing.setdefault("source_urls", {})[source] = url

    for f in ["siren", "matricule", "type_organisme", "category", "groupe",
              "phone", "email", "website"]:
        if not existing.get(f) and new.get(f):
            existing[f] = new[f]

    existing_addr = existing.get("address", {})
    new_addr = new.get("address", {})
    for key in ["street", "postal_code", "city", "department", "department_name", "region"]:
        if not existing_addr.get(key) and new_addr.get(key):
            existing_addr[key] = new_addr[key]
    existing["address"] = existing_addr

    # Merge people (avoid dup by name)
    existing_people = {p.get("name", "").lower(): p for p in existing.get("people", [])}
    for p in new.get("people", []):
        key = p.get("name", "").lower()
        if key and key not in existing_people:
            existing_people[key] = p
    existing["people"] = list(existing_people.values())

    # Financials: take new if existing is null
    if not existing.get("financials") and new.get("financials"):
        existing["financials"] = new["financials"]

    # Structured products: 'yes' wins over 'unknown'
    new_sp = new.get("structured_products", {})
    if new_sp.get("status") == "yes":
        existing["structured_products"] = new_sp
    elif existing.get("structured_products", {}).get("status") == "unknown" and new_sp.get("status") == "no":
        existing["structured_products"] = new_sp


def _fuzzy_match(name1, name2, threshold=0.88):
    if not name1 or not name2:
        return False
    return SequenceMatcher(None, name1, name2).ratio() >= threshold
