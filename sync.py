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
    """
    ScaleAQ nested structure:
    [ { siteId, items: [ { unitId, items: [ { type, items: [ { dateTime, averageValue, measurement } ] } ] } ] } ]

    NB: FeedAmount er kumulativ (akkumulert siden oppstart av fôringsøkt).
    Vi lagrer råverdiene og beregner daglig total som MAX(feed_kg) per dag i rebuild_daily.
    """
    rows = []

    for site_item in raw_items:
        for unit_item in site_item.get("items", []):
            unit_id = str(unit_item.get("unitId", ""))
            if not unit_id:
                continue

            for type_item in unit_item.get("items", []):
                data_type = type_item.get("type", "")

                for bucket in type_item.get("items", []):
                    val = bucket.get("averageValue") or bucket.get("sum") or 0
                    val = float(val)

                    # ScaleAQ returnerer FeedAmount i gram — konverter til kg
                    if data_type == "FeedAmount" and bucket.get("measurement") == "g":
                        val = val / 1000.0

                    rows.append({
                        "unit_id":     unit_id,
                        "bucket_time": bucket.get("dateTime", ""),
                        "data_type":   data_type,
                        "value":       val,
                    })

    logger.info(f"Parsed {len(rows)} rows from {len(raw_items)} site items")
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
    """
    Timesprofil: for hver time, ta MAX(feed_kg) som representerer toppen av
    den kumulative kurven i den timen — dvs. total fôret frem til slutten av timen.
    For å få kg PER time bruker vi MAX i timen minus MAX i forrige time.
    Her lagrer vi MAX per time som et mellomsteg; dashboard bruker dette til timesprofil.
    """
    for uid in unit_ids:
        await db.execute(
            """
            INSERT INTO feed_hourly (unit_id, hour_time, feed_kg, intensity_avg)
            SELECT unit_id,
                   strftime('%Y-%m-%dT%H:00:00Z', bucket_time) AS hour_time,
                   MAX(COALESCE(feed_kg, 0)),
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
    """
    Daglig total = MAX(feed_kg) per dag, siden FeedAmount er kumulativ.
    Siste bucket på dagen har høyeste verdi = total fôret den dagen.
    """
    for uid in unit_ids:
        await db.execute(
            """
            INSERT INTO feed_daily (unit_id, date, feed_kg, feed_sessions, first_feed, last_feed)
            SELECT unit_id,
                   substr(bucket_time, 1, 10) AS date,
                   MAX(COALESCE(feed_kg, 0)),
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
    client   = get_scaleaq_client()
    now      = utc_now()
    from_dt  = now - timedelta(days=LOOKBACK_DAYS)
    from_str = iso(from_dt)
    to_str   = iso(now)

    logger.info(f"Syncing ScaleAQ feed data {from_str} → {to_str}")

    async with aiosqlite.connect(DB_PATH) as db:
        unit_ids = await get_unit_ids(db)

    if not unit_ids:
        logger.warning("No units in DB – run /api/meta/sync-meta first")
        return

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

    rows = parse_aggregate_rows(raw)
    unit_ids_with_data = list({r["unit_id"] for r in rows})

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await upsert_10min(db, rows)
        await rebuild_hourly(db, unit_ids_with_data, from_str)
        await rebuild_daily(db, unit_ids_with_data, from_str)

    logger.info(f"Sync complete – {len(rows)} rows, {len(unit_ids_with_data)} units")


async def sync_feed_data_from(days: int = 40):
    """Backfill: hent data fra ScaleAQ fra `days` dager tilbake til nå."""
    client   = get_scaleaq_client()
    now      = utc_now()
    from_dt  = now - timedelta(days=days)
    from_str = iso(from_dt)
    to_str   = iso(now)

    logger.info(f"Backfill: fetching ScaleAQ data {from_str} → {to_str} ({days} days)")

    async with aiosqlite.connect(DB_PATH) as db:
        unit_ids = await get_unit_ids(db)

    if not unit_ids:
        logger.warning("No units in DB – run /api/meta/sync-meta first")
        return

    try:
        raw = await client.get_feed_aggregate(
            from_time=from_str,
            to_time=to_str,
            unit_ids=unit_ids,
        )
    except Exception as e:
        logger.error(f"ScaleAQ backfill fetch failed: {e}")
        return

    if not raw:
        logger.warning("ScaleAQ returned no data for backfill")
        return

    rows = parse_aggregate_rows(raw)
    unit_ids_with_data = list({r["unit_id"] for r in rows})

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await upsert_10min(db, rows)
        await rebuild_hourly(db, unit_ids_with_data, from_str)
        await rebuild_daily(db, unit_ids_with_data, from_str)

    logger.info(f"Backfill complete – {len(rows)} rows, {len(unit_ids_with_data)} units, {days} days")
