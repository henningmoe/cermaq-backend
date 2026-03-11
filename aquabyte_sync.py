"""
aquabyte_sync.py — henter data for alle aktive pens i aq_pens-tabellen.
Kjører kl 09:00 og 23:00 via scheduler i main.py.
For å legge til en ny pen: POST /api/aquabyte/pens — da plukkes den opp automatisk neste sync.
"""

import os
import sqlite3
import json
import logging
from datetime import date, timedelta

from aquabyte_client import (
    DB_PATH, _date_range, get_pen_map, init_pen_table,
    fetch_biomass, fetch_lice, fetch_welfare,
    fetch_swim_speed, fetch_breathing,
)

logger = logging.getLogger("aquabyte_sync")

def get_db():
    return sqlite3.connect(DB_PATH)

def init_aquabyte_tables():
    init_pen_table()
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS aq_biomass (
                pen_id      TEXT,
                date        TEXT,
                avg_weight  REAL,
                k_factor    REAL,
                cv          REAL,
                sample_size INTEGER,
                fetched_at  TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (pen_id, date)
            );
            CREATE TABLE IF NOT EXISTS aq_lice (
                pen_id       TEXT,
                date         TEXT,
                adult_female REAL,
                mobile       REAL,
                stationary   REAL,
                fetched_at   TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (pen_id, date)
            );
            CREATE TABLE IF NOT EXISTS aq_welfare (
                pen_id     TEXT,
                date       TEXT,
                score      REAL,
                category   TEXT,
                raw_json   TEXT,
                fetched_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (pen_id, date)
            );
            CREATE TABLE IF NOT EXISTS aq_swim_speed (
                pen_id     TEXT,
                date       TEXT,
                speed_bls  REAL,
                fetched_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (pen_id, date)
            );
            CREATE TABLE IF NOT EXISTS aq_breathing (
                pen_id          TEXT,
                date            TEXT,
                breathing_index REAL,
                fetched_at      TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (pen_id, date)
            );
        """)
    logger.info("Aquabyte-tabeller OK")

async def sync_pen(pen_id: str, unit_name: str, lookback_days: int = 14):
    from_date, to_date = _date_range(lookback_days)
    logger.info(f"Synkroniserer {unit_name} (penId={pen_id}) {from_date} → {to_date}")

    with get_db() as conn:
        try:
            rows = await fetch_biomass(pen_id, from_date, to_date)
            for r in rows:
                conn.execute("""
                    INSERT OR REPLACE INTO aq_biomass (pen_id, date, avg_weight, k_factor, cv, sample_size)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (pen_id, r["date"], r["avgWeight"], r.get("kFactor"), r.get("cv"), r.get("sampleSize")))
            logger.info(f"  [{unit_name}] biomass: {len(rows)} rader")
        except Exception as e:
            logger.error(f"  [{unit_name}] biomass feil: {e}")

        try:
            data = await fetch_lice(pen_id, from_date, to_date)
            rows = data if isinstance(data, list) else data.get("liceCounts", [])
            for r in rows:
                conn.execute("""
                    INSERT OR REPLACE INTO aq_lice (pen_id, date, adult_female, mobile, stationary)
                    VALUES (?, ?, ?, ?, ?)
                """, (pen_id, r.get("date"), r.get("adultFemale"), r.get("mobile"), r.get("stationary")))
            logger.info(f"  [{unit_name}] lice: {len(rows)} rader")
        except Exception as e:
            logger.error(f"  [{unit_name}] lice feil: {e}")

        try:
            data = await fetch_welfare(pen_id, from_date, to_date)
            rows = data if isinstance(data, list) else data.get("welfareScores", [])
            for r in rows:
                conn.execute("""
                    INSERT OR REPLACE INTO aq_welfare (pen_id, date, score, category, raw_json)
                    VALUES (?, ?, ?, ?, ?)
                """, (pen_id, r.get("date"), r.get("score"), r.get("category"), json.dumps(r)))
            logger.info(f"  [{unit_name}] welfare: {len(rows)} rader")
        except Exception as e:
            logger.error(f"  [{unit_name}] welfare feil: {e}")

        try:
            data = await fetch_swim_speed(pen_id, from_date, to_date)
            rows = data if isinstance(data, list) else data.get("swimSpeeds", [])
            for r in rows:
                d = r.get("date") or r.get("fromTime", "")[:10]
                conn.execute("""
                    INSERT OR REPLACE INTO aq_swim_speed (pen_id, date, speed_bls)
                    VALUES (?, ?, ?)
                """, (pen_id, d, r.get("swimSpeed") or r.get("speed")))
            logger.info(f"  [{unit_name}] swim speed: {len(rows)} rader")
        except Exception as e:
            logger.error(f"  [{unit_name}] swim speed feil: {e}")

        try:
            data = await fetch_breathing(pen_id, from_date, to_date)
            rows = data if isinstance(data, list) else data.get("breathingIndexes", [])
            for r in rows:
                d = r.get("date") or r.get("fromTime", "")[:10]
                conn.execute("""
                    INSERT OR REPLACE INTO aq_breathing (pen_id, date, breathing_index)
                    VALUES (?, ?, ?)
                """, (pen_id, d, r.get("breathingIndex") or r.get("value")))
            logger.info(f"  [{unit_name}] breathing: {len(rows)} rader")
        except Exception as e:
            logger.error(f"  [{unit_name}] breathing feil: {e}")

    logger.info(f"Ferdig med {unit_name}")

async def sync_all(lookback_days: int = 14):
    """Henter pen_map fra DB — plukker automatisk opp nye pens."""
    init_aquabyte_tables()
    pen_map = get_pen_map()
    logger.info(f"Synkroniserer {len(pen_map)} aktive pens: {list(pen_map.values())}")
    for pen_id, unit_name in pen_map.items():
        await sync_pen(pen_id, unit_name, lookback_days)
