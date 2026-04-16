"""
Activity logger - writes enrichment events to docs/data/activity.json.
Used by all enrichment scripts to build the dashboard activity feed.

Events are deduplicated by deterministic ID, pruned to max_days.
"""
import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
ACTIVITY_PATH = REPO / "docs" / "data" / "activity.json"


def load_activity() -> dict:
    if ACTIVITY_PATH.exists():
        with open(ACTIVITY_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"last_updated": None, "events": []}


def save_activity(data: dict) -> None:
    data["last_updated"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    # Sort events newest first
    data["events"].sort(key=lambda e: e.get("date", ""), reverse=True)
    ACTIVITY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ACTIVITY_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _make_event_id(event_type: str, entity_id: str, details: dict) -> str:
    today = datetime.utcnow().strftime("%Y%m%d")
    raw = f"{today}_{event_type}_{entity_id}_{json.dumps(details, sort_keys=True)}"
    h = hashlib.md5(raw.encode()).hexdigest()[:8]
    return f"evt_{today}_{event_type[:3]}_{entity_id}_{h}"


def log_event(
    event_type: str,
    entity_id: str,
    entity_name: str,
    summary: str,
    details: dict | None = None,
) -> None:
    """Append an event to activity.json. Idempotent via deterministic ID."""
    details = details or {}
    data = load_activity()
    evt_id = _make_event_id(event_type, entity_id, details)

    # Deduplicate
    existing_ids = {e["id"] for e in data["events"]}
    if evt_id in existing_ids:
        return

    event = {
        "id": evt_id,
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "type": event_type,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "summary": summary,
        "details": details,
    }
    data["events"].append(event)
    save_activity(data)
    logger.debug(f"Activity: {event_type} | {entity_name} | {summary}")


def prune_old_events(max_days: int = 90) -> int:
    """Remove events older than max_days. Returns number of pruned events."""
    data = load_activity()
    cutoff = (datetime.utcnow() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    original = len(data["events"])
    data["events"] = [e for e in data["events"] if e.get("date", "") >= cutoff]
    pruned = original - len(data["events"])
    if pruned > 0:
        save_activity(data)
        logger.info(f"Pruned {pruned} events older than {max_days} days")
    return pruned


def format_eur(n) -> str:
    """Format EUR amount for summary text."""
    if n is None:
        return ""
    if abs(n) >= 1e9:
        return f"{n / 1e9:.2f} Md€"
    if abs(n) >= 1e6:
        return f"{n / 1e6:.1f} M€"
    if abs(n) >= 1e3:
        return f"{n / 1e3:.0f} k€"
    return f"{n} €"
