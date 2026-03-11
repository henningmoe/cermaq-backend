"""
routers/aquabyte.py
"""

import sqlite3
import os
import asyncio
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from datetime import date, timedelta
from typing import Optional

router = APIRouter(prefix="/api/aquabyte", tags=["aquabyte"])

DB_PATH = os.environ.get("DB_PATH", "/data/cermaq.db")

def get_db():
    return sqlite3.connect(DB_PATH)

def rows_to_dicts(cursor):
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def default_from(days=30):
    return (date.today() - timedelta(days=days)).isoformat()

def default_to():
    return date.today().isoformat()

# ---------------------------------------------------------------------------
# Pen management — legg til / deaktiver pens uten kodeendring
# ---------------------------------------------------------------------------

class PenCreate(BaseModel):
    pen_id:    str
    unit_name: str
    site_name: str = "Horsvågen"

@router.get("/pens")
def list_pens():
    """Vis alle registrerte pens."""
    with get_db() as conn:
        cur = conn.execute("SELECT pen_id, unit_name, site_name, active, added_at FROM aq_pens ORDER BY added_at")
        return {"pens": rows_to_dicts(cur)}

@router.post("/pens")
def add_pen(pen: PenCreate):
    """
    Legg til ny pen. Vil bli synkronisert neste gang scheduler kjører (09:00 / 23:00).
    For å trigge umiddelbart: POST /api/aquabyte/sync-now
    """
    with get_db() as conn:
        existing = conn.execute("SELECT pen_id FROM aq_pens WHERE pen_id = ?", (pen.pen_id,)).fetchone()
        if existing:
            # Reaktiver hvis den var deaktivert
            conn.execute("UPDATE aq_pens SET active = 1, unit_name = ?, site_name = ? WHERE pen_id = ?",
                         (pen.unit_name, pen.site_name, pen.pen_id))
            return {"status": "reaktivert", "pen_id": pen.pen_id, "unit_name": pen.unit_name}
        conn.execute(
            "INSERT INTO aq_pens (pen_id, unit_name, site_name) VALUES (?, ?, ?)",
            (pen.pen_id, pen.unit_name, pen.site_name)
        )
    return {"status": "lagt til", "pen_id": pen.pen_id, "unit_name": pen.unit_name,
            "info": "Kjør POST /api/aquabyte/sync-now for å hente data umiddelbart"}

@router.delete("/pens/{pen_id}")
def deactivate_pen(pen_id: str):
    """Deaktiver pen — stoppes fra å synkroniseres, data beholdes."""
    with get_db() as conn:
        conn.execute("UPDATE aq_pens SET active = 0 WHERE pen_id = ?", (pen_id,))
    return {"status": "deaktivert", "pen_id": pen_id}

@router.post("/sync-now")
async def sync_now(lookback_days: int = 14):
    """Trigger manuell sync for alle aktive pens umiddelbart."""
    from aquabyte_sync import sync_all
    asyncio.create_task(sync_all(lookback_days=lookback_days))
    return {"status": "startet", "lookback_days": lookback_days}

@router.post("/backfill")
async def backfill(days: int = 40):
    """Backfill historiske data for alle aktive pens."""
    from aquabyte_sync import sync_all
    asyncio.create_task(sync_all(lookback_days=days))
    return {"status": "startet", "lookback_days": days}

# ---------------------------------------------------------------------------
# Data-endepunkter
# ---------------------------------------------------------------------------

@router.get("/biomass/{pen_id}")
def get_biomass(pen_id: str,
                from_date: str = Query(default=None),
                to_date:   str = Query(default=None)):
    from_date = from_date or default_from(30)
    to_date   = to_date   or default_to()
    with get_db() as conn:
        cur = conn.execute("""
            SELECT date, avg_weight, k_factor, cv, sample_size FROM aq_biomass
            WHERE pen_id=? AND date>=? AND date<=? ORDER BY date
        """, (pen_id, from_date, to_date))
        return {"pen_id": pen_id, "biomass": rows_to_dicts(cur)}

@router.get("/lice/{pen_id}")
def get_lice(pen_id: str,
             from_date: str = Query(default=None),
             to_date:   str = Query(default=None)):
    from_date = from_date or default_from(30)
    to_date   = to_date   or default_to()
    with get_db() as conn:
        cur = conn.execute("""
            SELECT date, adult_female, mobile, stationary FROM aq_lice
            WHERE pen_id=? AND date>=? AND date<=? ORDER BY date
        """, (pen_id, from_date, to_date))
        return {"pen_id": pen_id, "lice": rows_to_dicts(cur)}

@router.get("/welfare/{pen_id}")
def get_welfare(pen_id: str,
                from_date: str = Query(default=None),
                to_date:   str = Query(default=None)):
    from_date = from_date or default_from(30)
    to_date   = to_date   or default_to()
    with get_db() as conn:
        cur = conn.execute("""
            SELECT date, score, category FROM aq_welfare
            WHERE pen_id=? AND date>=? AND date<=? ORDER BY date
        """, (pen_id, from_date, to_date))
        return {"pen_id": pen_id, "welfare": rows_to_dicts(cur)}

@router.get("/swim-speed/{pen_id}")
def get_swim_speed(pen_id: str,
                   from_date: str = Query(default=None),
                   to_date:   str = Query(default=None)):
    from_date = from_date or default_from(30)
    to_date   = to_date   or default_to()
    with get_db() as conn:
        cur = conn.execute("""
            SELECT date, speed_bls FROM aq_swim_speed
            WHERE pen_id=? AND date>=? AND date<=? ORDER BY date
        """, (pen_id, from_date, to_date))
        return {"pen_id": pen_id, "swim_speed": rows_to_dicts(cur)}

@router.get("/breathing/{pen_id}")
def get_breathing(pen_id: str,
                  from_date: str = Query(default=None),
                  to_date:   str = Query(default=None)):
    from_date = from_date or default_from(30)
    to_date   = to_date   or default_to()
    with get_db() as conn:
        cur = conn.execute("""
            SELECT date, breathing_index FROM aq_breathing
            WHERE pen_id=? AND date>=? AND date<=? ORDER BY date
        """, (pen_id, from_date, to_date))
        return {"pen_id": pen_id, "breathing": rows_to_dicts(cur)}

@router.get("/dashboard/{pen_id}")
def get_dashboard(pen_id: str,
                  from_date: str = Query(default=None),
                  to_date:   str = Query(default=None)):
    """Alt-i-ett endepunkt dashboardet bruker."""
    from_date = from_date or default_from(30)
    to_date   = to_date   or default_to()
    with get_db() as conn:
        biomass   = rows_to_dicts(conn.execute("SELECT date, avg_weight, k_factor, cv FROM aq_biomass WHERE pen_id=? AND date>=? AND date<=? ORDER BY date", (pen_id, from_date, to_date)))
        lice      = rows_to_dicts(conn.execute("SELECT date, adult_female, mobile, stationary FROM aq_lice WHERE pen_id=? AND date>=? AND date<=? ORDER BY date", (pen_id, from_date, to_date)))
        welfare   = rows_to_dicts(conn.execute("SELECT date, score, category FROM aq_welfare WHERE pen_id=? AND date>=? AND date<=? ORDER BY date", (pen_id, from_date, to_date)))
        swim      = rows_to_dicts(conn.execute("SELECT date, speed_bls FROM aq_swim_speed WHERE pen_id=? AND date>=? AND date<=? ORDER BY date", (pen_id, from_date, to_date)))
        breathing = rows_to_dicts(conn.execute("SELECT date, breathing_index FROM aq_breathing WHERE pen_id=? AND date>=? AND date<=? ORDER BY date", (pen_id, from_date, to_date)))
    return {"pen_id": pen_id, "from_date": from_date, "to_date": to_date,
            "biomass": biomass, "lice": lice, "welfare": welfare,
            "swim_speed": swim, "breathing": breathing}
