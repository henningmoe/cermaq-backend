import os
import sqlite3
import httpx
from datetime import date, timedelta

AQUABYTE_BASE_URL = "https://api.aquabyte.ai/v3"
AQUABYTE_API_KEY  = os.environ.get("AQUABYTE_API_KEY", "")
DB_PATH           = os.environ.get("DB_PATH", "/data/cermaq.db")

def _headers():
    return {"apikey": AQUABYTE_API_KEY}

def _date_range(lookback_days: int):
    today = date.today()
    return (today - timedelta(days=lookback_days)).isoformat(), today.isoformat()

# ---------------------------------------------------------------------------
# PEN_MAP — lagres i databasen, ikke hardkodet
# Legg til nye pens via POST /api/aquabyte/pens uten å endre kode
# ---------------------------------------------------------------------------

def get_pen_map() -> dict:
    """Hent alle aktive pens fra databasen. Returnerer {pen_id: unit_name}."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT pen_id, unit_name FROM aq_pens WHERE active = 1"
            ).fetchall()
            return {r[0]: r[1] for r in rows}
    except Exception:
        return {"5607": "Fram01"}  # fallback

def init_pen_table():
    """Opprett aq_pens-tabell og seed med Fram01 hvis tom."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS aq_pens (
                pen_id      TEXT PRIMARY KEY,
                unit_name   TEXT NOT NULL,
                site_name   TEXT DEFAULT 'Horsvågen',
                active      INTEGER DEFAULT 1,
                added_at    TEXT DEFAULT (datetime('now'))
            )
        """)
        existing = conn.execute("SELECT COUNT(*) FROM aq_pens").fetchone()[0]
        if existing == 0:
            conn.execute("""
                INSERT INTO aq_pens (pen_id, unit_name, site_name)
                VALUES ('5607', 'Fram01', 'Horsvågen')
            """)

# ---------------------------------------------------------------------------
# Fetch-funksjoner
# ---------------------------------------------------------------------------

async def fetch_biomass(pen_id: str, from_date: str, to_date: str, bucket_size: int = 1000):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{AQUABYTE_BASE_URL}/biomass", headers=_headers(),
            params={"penId": pen_id, "fromdate": from_date, "todate": to_date, "bucketsize": bucket_size})
        r.raise_for_status()
        return r.json().get("biomass", [])

async def fetch_lice(pen_id: str, from_date: str, to_date: str):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{AQUABYTE_BASE_URL}/liceCount", headers=_headers(),
            params={"penId": pen_id, "fromDate": from_date, "toDate": to_date})
        r.raise_for_status()
        return r.json()

async def fetch_welfare(pen_id: str, from_date: str, to_date: str):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{AQUABYTE_BASE_URL}/welfareScores", headers=_headers(),
            params={"penId": pen_id, "fromDate": from_date, "toDate": to_date})
        r.raise_for_status()
        return r.json()

async def fetch_swim_speed(pen_id: str, from_date: str, to_date: str):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{AQUABYTE_BASE_URL}/behaviour/swimSpeed", headers=_headers(),
            params={"penId": pen_id, "fromTime": f"{from_date}T00:00:00Z",
                    "toTime": f"{to_date}T00:00:00Z", "period": "D"})
        r.raise_for_status()
        return r.json()

async def fetch_breathing(pen_id: str, from_date: str, to_date: str):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{AQUABYTE_BASE_URL}/behaviour/breathingIndex", headers=_headers(),
            params={"penId": pen_id, "fromTime": f"{from_date}T00:00:00Z",
                    "toTime": f"{to_date}T00:00:00Z"})
        r.raise_for_status()
        return r.json()
