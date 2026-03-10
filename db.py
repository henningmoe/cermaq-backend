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
            CREATE TABLE IF NOT EXISTS feed_10min (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_id         TEXT NOT NULL,
                bucket_time     TEXT NOT NULL,
                feed_kg         REAL,
                intensity       REAL,
                fetched_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(unit_id, bucket_time)
            );
            CREATE TABLE IF NOT EXISTS feed_hourly (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_id         TEXT NOT NULL,
                hour_time       TEXT NOT NULL,
                feed_kg         REAL,
                intensity_avg   REAL,
                fetched_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(unit_id, hour_time)
            );
            CREATE TABLE IF NOT EXISTS feed_daily (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_id         TEXT NOT NULL,
                date            TEXT NOT NULL,
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
