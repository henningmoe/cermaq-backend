"""
Sync job – runs every 10 minutes via APScheduler.
Fetches ScaleAQ 10-min feed buckets and writes to DB,
then rebuilds hourly + daily rollups.
"""

import aiosqlite
import logging
import os
from datetime import datetime, timezone, timedelta

from db import DB_PATH
from scaleaq_client import get_scaleaq_client

logger = logging.getLogger(__name__)

# How many days back to fetch on each sync
LOOKBACK_DAYS = int(os.getenv("SYNC_LOOKBACK_DAYS", "2"))


# ------------------------------------------------------------------ #
#  Helpers                                                             #
# ------------------------------------------------------------------ #

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: str) -> datetime:
    """Parse ISO8601 UTC string to aware datetime."""
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: {s!r}")


# ------------------------------------------------------------------ #
#  Parse ScaleAQ aggregate response                                    #
# ------------------------------------------------------------------ #

def parse_aggregate_rows(raw_items: list[dict]) -> list[dict]:
    """
    ScaleAQ aggregate response is a list of objects.
    Each object typically looks like:
    {
        "unitId": "abc123",
        "time":   "2026-03-10T08:00:00Z",
        "dataType": "FeedAmount",
        "value":  45.3
    }
    OR nested:
    {
        "unitId": "abc123",
        "buckets": [
            { "time": "...", "FeedAmount": 45.3, "Intensity": 0.8 }
        ]
    }
    We handle both shapes.
    """
    rows = []

    for item in raw_items:
        unit_id = item.get("unitId") or item.get("unit_id", "")

        # Shape 1: flat rows per dataType
        if "dataType" in item:
            rows.append({
                "unit_id":    unit_id,
                "bucket_time": item.get("time", ""),
                "data_type":  item["dataType"],
                "value":      item.get("value", 0.0),
            })

        # Shape 2: buckets list
        elif "buckets" in item:
            for b in item["buckets"]:
                t = b.get("time", "")
                rows.append({
                    "unit_id":    unit_id,
                    "bucket_time": t,
                    "data_type":  "FeedAmount",
                    "value":      b.get("FeedAmount", 0.0),
                })
                rows.append({
                    "unit_id":    unit_id,
                    "bucket_time": t,
                    "data_type":  "Intensity",
                    "value":      b.get("Intensity", 0.0),
                })

        # Shape 3: already combined per bucket
        elif "FeedAmount" in item or "feedAmount" in item:
            rows.append({
                "unit_id":    unit_id,
                "bucket_time": item.get("time", ""),
                "data_type":  "FeedAmount",
                "value":      item.get("FeedAmount", item.get("feedAmount", 0.0)),
            })
            rows.append({
                "unit_id":    unit_id,
                "bucket_time": item.get("time", ""),
                "data_type":  "Intensity",
                "value":      item.get("Intensity", item.get("intensity", 0.0)),
            })

    return rows


# ------------------------------------------------------------------ #
#  Write to DB                                                         #
# ------------------------------------------------------------------ #

async def upsert_10min(db: aiosqlite.Connection, rows: list[dict]):
    """Upsert 10-min buckets, pairing FeedAmount + Intensity per (unit, time)."""
    # Group by (unit_id, bucket_time)
    buckets: dict[tuple, dict] = {}
    for r in rows:
        key = (r["unit_id"], r["bucket_time"])
        if key not in buckets:
            buckets[key] = {"feed_kg": None, "intensity": None}
        if r["data_type"] == "FeedAmount":
            buckets[key]["feed_kg"]   = r["value"]
        elif r["data_type"] == "Intensity":
            buckets[key]["intensity"] = r["value"]

    await db.executemany(
        """
        INSERT INTO feed_10min (unit_id, bucket_time, feed_kg, intensity)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(unit_id, bucket_time) DO UPDATE SET
            feed_kg   = excluded.feed_kg,
            intensity = excluded.intensity,
            fetched_at = datetime('now')
        """,
        [
            (uid, bt, v["feed_kg"], v["intensity"])
            for (uid, bt), v in buckets.items()
        ],
    )
    await db.commit()
    logger.info(f"Upserted {len(buckets)} 10-min buckets")


async def rebuild_hourly(db: aiosqlite.Connection, unit_ids: list[str], since: str):
    """Rebuild hourly rollup from 10-min data."""
    for uid in unit_ids:
        await db.execute(
            """
            INSERT INTO feed_hourly (unit_id, hour_time, feed_kg, intensity_avg)
            SELECT
                unit_id,
                strftime('%Y-%m-%dT%H:00:00Z', bucket_time) AS hour_time,
                SUM(COALESCE(feed_kg,   0))                  AS feed_kg,
                AVG(COALESCE(intensity, 0))                  AS intensity_avg
            FROM feed_10min
            WHERE unit_id = ? AND bucket_time >= ?
            GROUP BY unit_id, hour_time
            ON CONFLICT(unit_id, hour_time) DO UPDATE SET
                feed_kg       = excluded.feed_kg,
                intensity_avg = excluded.intensity_avg,
                fetched_at    = datetime('now')
            """,
            (uid, since),
        )
    await db.commit()
    logger.info(f"Rebuilt hourly rollup for {len(unit_ids)} units since {since}")


async def rebuild_daily(db: aiosqlite.Connection, unit_ids: list[str], since: str):
    """Rebuild daily rollup from 10-min data."""
    for uid in unit_ids:
        await db.execute(
            """
            INSERT INTO feed_daily (unit_id, date, feed_kg, feed_sessions, first_feed, last_feed)
            SELECT
                unit_id,
                substr(bucket_time, 1, 10)           AS date,
                SUM(COALESCE(feed_kg, 0))            AS feed_kg,
                COUNT(CASE WHEN feed_kg > 0 THEN 1 END) AS feed_sessions,
                MIN(bucket_time)                     AS first_feed,
                MAX(bucket_time)                     AS last_feed
            FROM feed_10min
            WHERE unit_id = ? AND bucket_time >= ? AND COALESCE(feed_kg, 0) > 0
            GROUP BY unit_id, date
            ON CONFLICT(unit_id, date) DO UPDATE SET
                feed_kg       = excluded.feed_kg,
                feed_sessions = excluded.feed_sessions,
                first_feed    = excluded.first_feed,
                last_feed     = excluded.last_feed,
                fetched_at    = datetime('now')
            """,
            (uid, since),
        )
    await db.commit()
    logger.info(f"Rebuilt daily rollup for {len(unit_ids)} units since {since}")


# ------------------------------------------------------------------ #
#  Main sync entry point                                               #
# ------------------------------------------------------------------ #

async def sync_feed_data():
    """
    Called every 10 minutes by APScheduler.
    Fetches LOOKBACK_DAYS of 10-min data and rebuilds rollups.
    """
    client   = get_scaleaq_client()
    now      = utc_now()
    from_dt  = now - timedelta(days=LOOKBACK_DAYS)
    from_str = iso(from_dt)
    to_str   = iso(now)

    logger.info(f"Syncing ScaleAQ feed data {from_str} → {to_str}")

    try:
        raw = await client.get_feed_aggregate(from_time=from_str, to_time=to_str)
    except Exception as e:
        logger.error(f"ScaleAQ fetch failed: {e}")
        return

    if not raw:
        logger.warning("ScaleAQ returned no data")
        return

    rows     = parse_aggregate_rows(raw)
    unit_ids = list({r["unit_id"] for r in rows})

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await upsert_10min(db, rows)
        await rebuild_hourly(db, unit_ids, from_str)
        await rebuild_daily(db, unit_ids, from_str)

    logger.info(f"Sync complete – {len(rows)} rows, {len(unit_ids)} units")


# ------------------------------------------------------------------ #
#  Historical backfill (run once manually if needed)                  #
# ------------------------------------------------------------------ #

async def backfill(days: int = 30):
    """
    Run manually to fill historical data:
        python -c "import asyncio; from sync import backfill; asyncio.run(backfill(30))"
    """
    client  = get_scaleaq_client()
    now     = utc_now()
    chunk   = timedelta(days=1)

    for i in range(days, 0, -1):
        day_start = now - timedelta(days=i)
        day_end   = day_start + chunk
        from_str  = iso(day_start.replace(hour=0, minute=0, second=0))
        to_str    = iso(day_end.replace(  hour=0, minute=0, second=0))

        logger.info(f"Backfilling {from_str[:10]}…")
        try:
            raw  = await client.get_feed_aggregate(from_time=from_str, to_time=to_str)
            rows = parse_aggregate_rows(raw)
            if not rows:
                continue
            unit_ids = list({r["unit_id"] for r in rows})
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                await upsert_10min(db, rows)
                await rebuild_hourly(db, unit_ids, from_str)
                await rebuild_daily(db, unit_ids, from_str)
        except Exception as e:
            logger.error(f"Backfill failed for {from_str[:10]}: {e}")

    logger.info("Backfill complete")
