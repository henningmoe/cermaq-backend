"""
Database layer – uses SQLite via aiosqlite for zero-config startup.
To switch to PostgreSQL on Railway, set DATABASE_URL env var and
uncomment the asyncpg sections below.
"""

import aiosqlite
import os
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "/data/cermaq.db")


async def get_db():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        yield db


async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS sites (
                site_id     TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                updated_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS units (
                unit_id     TEXT PRIMARY KEY,
                site_id     TEXT NOT NULL,
                name        TEXT NOT NULL,
                updated_at  TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (site_id) REFERENCES sites(site_id)
            );

            -- 10-minute feed buckets (raw from ScaleAQ aggregate endpoint)
            CREATE TABLE IF NOT EXISTS feed_10min (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_id         TEXT NOT NULL,
                bucket_time     TEXT NOT NULL,   -- ISO8601 UTC e.g. 2026-03-10T08:00:00Z
                feed_kg         REAL,
                intensity       REAL,
                fetched_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(unit_id, bucket_time)
            );

            -- Hourly rollup (pre-aggregated for fast dashboard queries)
            CREATE TABLE IF NOT EXISTS feed_hourly (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_id         TEXT NOT NULL,
                hour_time       TEXT NOT NULL,   -- e.g. 2026-03-10T08:00:00Z
                feed_kg         REAL,
                intensity_avg   REAL,
                fetched_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(unit_id, hour_time)
            );

            -- Daily rollup
            CREATE TABLE IF NOT EXISTS feed_daily (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_id         TEXT NOT NULL,
                date            TEXT NOT NULL,   -- YYYY-MM-DD
                feed_kg         REAL,
                feed_sessions   INTEGER,
                first_feed      TEXT,
                last_feed       TEXT,
                fetched_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(unit_id, date)
            );

            CREATE INDEX IF NOT EXISTS idx_feed_10min_unit_time
                ON feed_10min(unit_id, bucket_time);
            CREATE INDEX IF NOT EXISTS idx_feed_hourly_unit_time
                ON feed_hourly(unit_id, hour_time);
            CREATE INDEX IF NOT EXISTS idx_feed_daily_unit_date
                ON feed_daily(unit_id, date);
        """)
        await db.commit()
    logger.info(f"Database ready at {DB_PATH}")
