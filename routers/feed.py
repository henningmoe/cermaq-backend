"""
Feed data endpoints consumed by the dashboard.
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
import aiosqlite
from db import DB_PATH

router = APIRouter()


# ------------------------------------------------------------------ #
#  10-minute buckets                                                   #
# ------------------------------------------------------------------ #

@router.get("/10min/{unit_id}")
async def feed_10min(
    unit_id:   str,
    date:      str = Query(..., description="YYYY-MM-DD"),
):
    """
    Return all 10-minute feed buckets for a unit on a given date.
    Dashboard uses this to render the 10-min chart.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT bucket_time, feed_kg, intensity
            FROM   feed_10min
            WHERE  unit_id    = ?
              AND  substr(bucket_time, 1, 10) = ?
            ORDER  BY bucket_time
            """,
            (unit_id, date),
        )
        rows = await cursor.fetchall()

    if not rows:
        raise HTTPException(404, f"No 10-min data for unit={unit_id} date={date}")

    return {
        "unit_id": unit_id,
        "date":    date,
        "buckets": [
            {
                "time":      r["bucket_time"],
                "feed_kg":   r["feed_kg"],
                "intensity": r["intensity"],
            }
            for r in rows
        ],
    }


# ------------------------------------------------------------------ #
#  Hourly profile                                                      #
# ------------------------------------------------------------------ #

@router.get("/hourly/{unit_id}")
async def feed_hourly(
    unit_id:   str,
    from_date: str = Query(..., description="YYYY-MM-DD"),
    to_date:   str = Query(..., description="YYYY-MM-DD"),
):
    """
    Return hourly feed totals for a date range.
    Used to draw the hourly bar chart in the Analyzer.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT hour_time,
                   feed_kg,
                   intensity_avg,
                   substr(hour_time, 1, 10)                     AS date,
                   CAST(substr(hour_time, 12, 2) AS INTEGER)    AS hour
            FROM   feed_hourly
            WHERE  unit_id   = ?
              AND  substr(hour_time, 1, 10) BETWEEN ? AND ?
            ORDER  BY hour_time
            """,
            (unit_id, from_date, to_date),
        )
        rows = await cursor.fetchall()

    # Group by date → { date: { hour: kg } }
    by_date: dict[str, dict] = {}
    totals:  dict[str, float] = {}

    for r in rows:
        d = r["date"]
        h = r["hour"]
        kg = r["feed_kg"] or 0.0
        if d not in by_date:
            by_date[d]  = {}
            totals[d]   = 0.0
        by_date[d][h]   = round(kg, 2)
        totals[d]       += kg

    return {
        "unit_id":  unit_id,
        "from":     from_date,
        "to":       to_date,
        "dates":    sorted(by_date.keys()),
        "profiles": by_date,
        "totals":   {d: round(totals[d], 2) for d in sorted(totals)},
    }


# ------------------------------------------------------------------ #
#  Daily summary                                                       #
# ------------------------------------------------------------------ #

@router.get("/daily/{unit_id}")
async def feed_daily(
    unit_id:   str,
    from_date: str = Query(..., description="YYYY-MM-DD"),
    to_date:   str = Query(..., description="YYYY-MM-DD"),
):
    """
    Return daily feed totals for a date range.
    Used to populate the KPI strip and time-series charts.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT date, feed_kg, feed_sessions, first_feed, last_feed
            FROM   feed_daily
            WHERE  unit_id = ?
              AND  date BETWEEN ? AND ?
            ORDER  BY date
            """,
            (unit_id, from_date, to_date),
        )
        rows = await cursor.fetchall()

    return {
        "unit_id": unit_id,
        "from":    from_date,
        "to":      to_date,
        "days": [
            {
                "date":          r["date"],
                "feed_kg":       round(r["feed_kg"] or 0, 2),
                "feed_sessions": r["feed_sessions"],
                "first_feed":    r["first_feed"],
                "last_feed":     r["last_feed"],
            }
            for r in rows
        ],
    }


# ------------------------------------------------------------------ #
#  Dashboard combined payload (one fetch for entire dashboard)        #
# ------------------------------------------------------------------ #

@router.get("/dashboard/{unit_id}")
async def feed_dashboard(
    unit_id:   str,
    from_date: str = Query(..., description="YYYY-MM-DD"),
    to_date:   str = Query(..., description="YYYY-MM-DD"),
):
    """
    Combined endpoint: returns everything the dashboard needs for one unit
    in a single HTTP request.

    Response shape matches the existing HV_FEED_DATA constant in the HTML
    so the dashboard can swap hardcoded data with a simple fetch().
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Daily totals
        cur = await db.execute(
            """
            SELECT date, feed_kg
            FROM   feed_daily
            WHERE  unit_id = ? AND date BETWEEN ? AND ?
            ORDER  BY date
            """,
            (unit_id, from_date, to_date),
        )
        daily_rows = await cur.fetchall()

        # Hourly profiles
        cur2 = await db.execute(
            """
            SELECT substr(hour_time,1,10)                    AS date,
                   CAST(substr(hour_time,12,2) AS INTEGER)  AS hour,
                   feed_kg
            FROM   feed_hourly
            WHERE  unit_id = ? AND substr(hour_time,1,10) BETWEEN ? AND ?
            ORDER  BY hour_time
            """,
            (unit_id, from_date, to_date),
        )
        hourly_rows = await cur2.fetchall()

        # 10-min for today
        from datetime import date as _date
        today = str(_date.today())
        cur3 = await db.execute(
            """
            SELECT bucket_time, feed_kg
            FROM   feed_10min
            WHERE  unit_id = ? AND substr(bucket_time,1,10) = ?
            ORDER  BY bucket_time
            """,
            (unit_id, today),
        )
        tenmin_rows = await cur3.fetchall()

    # Build profile dict { date: { hour: kg } }
    profiles: dict = {}
    for r in hourly_rows:
        d = r["date"]
        if d not in profiles:
            profiles[d] = {}
        profiles[d][r["hour"]] = round(r["feed_kg"] or 0, 2)

    # Build 10-min dict { time: kg }
    tenmin: dict = {}
    for r in tenmin_rows:
        t = r["bucket_time"][11:16]   # HH:MM
        tenmin[t] = round(r["feed_kg"] or 0, 2)

    dates   = [r["date"] for r in daily_rows]
    totals  = [round(r["feed_kg"] or 0, 2) for r in daily_rows]

    return {
        "unit_id":  unit_id,
        "dates":    dates,
        "totals":   totals,
        "profiles": profiles,
        "today_10min": tenmin,
    }


# ------------------------------------------------------------------ #
#  Latest sync status                                                  #
# ------------------------------------------------------------------ #

@router.get("/sync-status")
async def sync_status():
    """Return when data was last fetched per unit."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT unit_id,
                   MAX(fetched_at) AS last_fetched,
                   MAX(bucket_time) AS latest_data
            FROM   feed_10min
            GROUP  BY unit_id
            ORDER  BY unit_id
            """
        )
        rows = await cur.fetchall()
    return [dict(r) for r in rows]
