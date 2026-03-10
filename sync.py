"""
Sync job – runs every 10 minutes via APScheduler.
"""

import aiosqlite
import logging
import os
from datetime import datetime, timezone, timedelta

from db import DB_PATH
from scaleaq_client import get_scaleaq_client

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = int(os.getenv("SYNC_LOOKBACK_DAYS", "2"))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


async def get_unit_ids(db: aiosqlite.Connection) -> list[str]:
    cur = await db.execute("SELECT unit_id FROM units")
    rows = await cur.fetchall()
    return [r[0] for r in rows]


def parse_aggregate_rows(raw_items: list[dict]) -> list[dict]:
    rows = []
    for item in raw_items:
        unit_id = str(item.get("unitId") or item.get("unit_id") or "")
        if not unit_id:
            logger.warning(f"Skipping item with no unitId: {list(item.keys())}")
            continue

        # Shape 1: flat rows per dataType
        if "dataType" in item:
            rows.append({
                "unit_id":     unit_id,
                "bucket_time": item.get("time", ""),
                "data_type":   item["dataType"],
                "value":       float(item.get("value") or 0),
            })

        # Shape 2: buckets list
        elif "buckets" in item:
            for b in item["buckets"]:
                t = b.get("time", "")
                rows.append({"unit_id": unit_id, "bucket_time": t, "data_type": "FeedAmount", "value": float(b.get("FeedAmount") or 0)})
                rows.append({"unit_id": unit_id, "bucket_time": t, "data_type": "Intensity",  "value": float(b.get("Intensity")  or 0)})

        # Shape 3: combined per bucket
        elif "FeedAmount" in item or "feedAmount" in item:
            t = item.get("time", "")
            rows.append({"unit_id": unit_id, "bucket_time": t, "data_type": "FeedAmount", "value": float(item.get("FeedAmount") or item.get("feedAmount") or 0)})
            rows.append({"unit_id": unit_id, "bucket_time": t, "data_type": "Intensity",  "value": float(item.get("Intensity")  or item.get("intensity")  or 0)})

        else:
            logger.warning(f"Unknown item shape, keys: {list(item.keys())}, sample: {str(item)[:200]}")

    return rows


async def upsert_10min(db: aiosqlite.Connection, rows: list[dict]):
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
            feed_kg    = excluded.feed_kg,
            intensity  = excluded.intensity,
            fetched_at = datetime('now')
        """,
        [(uid, bt, v["feed_kg"], v["intensity"]) for (uid, bt), v in buckets.items()],
    )
    await db.commit()
    logger.info(f"Upserted {len(buckets)} 10-min buckets")


async def rebuild_hourly(db: aiosqlite.Connection, unit_ids: list[str], since: str):
    for uid in unit_ids:
        await db.execute(
            """
            INSERT INTO feed_hourly (unit_id, hour_time, feed_kg, intensity_avg)
            SELECT unit_id,
                   strftime('%Y-%m-%dT%H:00:00Z', bucket_time) AS hour_time,
                   SUM(COALESCE(feed_kg, 0)),
                   AVG(COALESCE(intensity, 0))
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
    logger.info(f"Rebuilt hourly rollup for {len(unit_ids)} units")


async def rebuild_daily(db: aiosqlite.Connection, unit_ids: list[str], since: str):
    for uid in unit_ids:
        await db.execute(
            """
            INSERT INTO feed_daily (unit_id, date, feed_kg, feed_sessions, first_feed, last_feed)
            SELECT unit_id,
                   substr(bucket_time, 1, 10) AS date,
                   SUM(COALESCE(feed_kg, 0)),
                   COUNT(CASE WHEN feed_kg > 0 THEN 1 END),
                   MIN(bucket_time),
                   MAX(bucket_time)
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
    logger.info(f"Rebuilt daily rollup for {len(unit_ids)} units")


async def sync_feed_data():
    client  = get_scaleaq_client()
    now     = utc_now()
    from_dt = now - timedelta(days=LOOKBACK_DAYS)
    from_str = iso(from_dt)
    to_str   = iso(now)

    logger.info(f"Syncing ScaleAQ feed data {from_str} → {to_str}")

    # Hent unit-IDer fra DB
    async with aiosqlite.connect(DB_PATH) as db:
        unit_ids = await get_unit_ids(db)

    if not unit_ids:
        logger.warning("No units in DB – run /api/meta/sync-meta first")
        return

    logger.info(f"Fetching data for {len(unit_ids)} units: {unit_ids}")

    try:
        raw = await client.get_feed_aggregate(
            from_time=from_str,
            to_time=to_str,
            unit_ids=unit_ids,
        )
    except Exception as e:
        logger.error(f"ScaleAQ fetch failed: {e}")
        return

    if not raw:
        logger.warning("ScaleAQ returned no data")
        return

    # Log first item so we can see the shape
    logger.info(f"First item from ScaleAQ: {str(raw[0])[:300]}")

    rows     = parse_aggregate_rows(raw)
    unit_ids_with_data = list({r["unit_id"] for r in rows})

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await upsert_10min(db, rows)
        await rebuild_hourly(db, unit_ids_with_data, from_str)
        await rebuild_daily(db, unit_ids_with_data, from_str)

    logger.info(f"Sync complete – {len(rows)} rows, {len(unit_ids_with_data)} units")
