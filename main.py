"""
main.py — cermaq-backend
"""
import os
import logging
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from routers import meta, feed
from routers.aquabyte import router as aquabyte_router
from db import init_db
from aquabyte_sync import sync_all, init_aquabyte_tables
from sync import sync_feed_data, sync_feed_data_from

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

scheduler = AsyncIOScheduler(timezone="Europe/Oslo")


async def scheduled_aquabyte_sync():
    now = datetime.now().strftime("%H:%M")
    logger.info(f"[{now}] Kjorer planlagt Aquabyte-sync...")
    await sync_all(lookback_days=14)


async def scheduled_scaleaq_sync():
    now = datetime.now().strftime("%H:%M")
    logger.info(f"[{now}] Kjorer planlagt ScaleAQ-sync...")
    await sync_feed_data()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    init_aquabyte_tables()

    # Backfill ved oppstart
    asyncio.create_task(sync_all(lookback_days=40))
    asyncio.create_task(sync_feed_data_from(days=3))

    # Aquabyte: 09:00 og 23:00
    scheduler.add_job(scheduled_aquabyte_sync, "cron", hour=9,  minute=0)
    scheduler.add_job(scheduled_aquabyte_sync, "cron", hour=23, minute=0)

    # ScaleAQ: hvert 10. minutt
    scheduler.add_job(scheduled_scaleaq_sync, "interval", minutes=10)

    scheduler.start()
    logger.info("Scheduler startet — ScaleAQ sync hvert 10 min, Aquabyte kl 09:00 og 23:00")
    yield
    scheduler.shutdown()


app = FastAPI(
    title="Cermaq Backend",
    version="2.2",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(meta.router, prefix="/api/meta", tags=["meta"])
app.include_router(feed.router, prefix="/api/feed", tags=["feed"])
app.include_router(aquabyte_router)


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}
