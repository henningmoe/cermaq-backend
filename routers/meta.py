"""
Meta endpoints – sites, units, and sync triggers.
"""

from fastapi import APIRouter, BackgroundTasks
import aiosqlite
from db import DB_PATH
from scaleaq_client import get_scaleaq_client
from sync import sync_feed_data
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/sites")
async def list_sites():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM sites ORDER BY name")
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


@router.get("/units")
async def list_units(site_id: str | None = None):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if site_id:
            cur = await db.execute(
                "SELECT * FROM units WHERE site_id = ? ORDER BY name", (site_id,)
            )
        else:
            cur = await db.execute("SELECT * FROM units ORDER BY site_id, name")
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


@router.post("/sync-meta")
async def sync_meta():
    """
    Fetch company meta from ScaleAQ and populate sites + units tables.
    Returns raw response if structure is unexpected.
    """
    client = get_scaleaq_client()
    data   = await client.get_company_meta()

    # Log full response so we can see structure
    logger.info(f"ScaleAQ meta type: {type(data)}")
    logger.info(f"ScaleAQ meta (500 chars): {str(data)[:500]}")

    # Try all known response shapes
    sites = []
    if isinstance(data, list):
        sites = data
    elif isinstance(data, dict):
        for key in ["sites", "localities", "companies", "units"]:
            if data.get(key):
                sites = data[key]
                break
        if not sites and data.get("data"):
            inner = data["data"]
            for key in ["sites", "localities", "companies"]:
                if isinstance(inner, dict) and inner.get(key):
                    sites = inner[key]
                    break
            if not sites and isinstance(inner, list):
                sites = inner

    if not sites:
        # Return raw so we can inspect
        return {
            "error": "Could not find sites in response",
            "response_type": str(type(data)),
            "response_keys": list(data.keys()) if isinstance(data, dict) else None,
            "raw": str(data)[:2000],
        }

    async with aiosqlite.connect(DB_PATH) as db:
        for site in sites:
            sid   = str(site.get("id") or site.get("siteId") or site.get("localityId") or "")
            sname = site.get("name") or site.get("siteName") or site.get("localityName") or sid
            if not sid:
                continue
            await db.execute(
                "INSERT INTO sites(site_id, name) VALUES(?,?) "
                "ON CONFLICT(site_id) DO UPDATE SET name=excluded.name",
                (sid, sname),
            )
            for key in ["units", "pens", "cages", "merds"]:
                units = site.get(key, [])
                if units:
                    break
            for unit in units:
                uid   = str(unit.get("id") or unit.get("unitId") or unit.get("penId") or "")
                uname = unit.get("name") or unit.get("unitName") or unit.get("penName") or uid
                if not uid:
                    continue
                await db.execute(
                    "INSERT INTO units(unit_id, site_id, name) VALUES(?,?,?) "
                    "ON CONFLICT(unit_id) DO UPDATE SET name=excluded.name",
                    (uid, sid, uname),
                )
        await db.commit()

    return {
        "synced_sites": len(sites),
        "site_names": [
            s.get("name") or s.get("siteName") or s.get("localityName", "")
            for s in sites
        ],
    }


@router.post("/sync-now")
async def trigger_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(sync_feed_data)
    return {"status": "sync started"}


@router.post("/backfill")
async def trigger_backfill(background_tasks: BackgroundTasks, days: int = 40):
    """Hent historiske data fra ScaleAQ for angitt antall dager tilbake."""
    from sync import sync_feed_data_from
    background_tasks.add_task(sync_feed_data_from, days)
    return {"status": "backfill started", "days": days}
