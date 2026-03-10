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
    """Return all known sites."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM sites ORDER BY name")
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


@router.get("/units")
async def list_units(site_id: str | None = None):
    """Return all units, optionally filtered by site."""
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
    Run once after deploy to seed the DB with unit IDs.
    """
    client = get_scaleaq_client()
    data   = await client.get_company_meta()

    # ScaleAQ meta structure: { sites: [ { id, name, units: [{id, name}] } ] }
    sites = data.get("sites", data.get("data", {}).get("sites", []))

    async with aiosqlite.connect(DB_PATH) as db:
        for site in sites:
            sid  = str(site.get("id",   site.get("siteId",   "")))
            sname = site.get("name", site.get("siteName", sid))
            await db.execute(
                "INSERT INTO sites(site_id, name) VALUES(?,?) "
                "ON CONFLICT(site_id) DO UPDATE SET name=excluded.name",
                (sid, sname),
            )
            for unit in site.get("units", []):
                uid   = str(unit.get("id",   unit.get("unitId",   "")))
                uname = unit.get("name", unit.get("unitName", uid))
                await db.execute(
                    "INSERT INTO units(unit_id, site_id, name) VALUES(?,?,?) "
                    "ON CONFLICT(unit_id) DO UPDATE SET name=excluded.name",
                    (uid, sid, uname),
                )
        await db.commit()

    return {"synced_sites": len(sites)}


@router.post("/sync-now")
async def trigger_sync(background_tasks: BackgroundTasks):
    """Manually trigger a feed data sync (runs in background)."""
    background_tasks.add_task(sync_feed_data)
    return {"status": "sync started"}
