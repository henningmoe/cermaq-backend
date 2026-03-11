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

# Eksisterende routers
from routers import meta, feed

# Ny Aquabyte router
from routers.aquabyte import router as aquabyte_router

# DB init
from db import init_db

# Aquabyte sync
from aquabyte_sync import sync_all, init_aquabyte_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

scheduler = AsyncIOScheduler(timezone="Europe/Oslo")


async def scheduled_aquabyte_sync():
    now = datetime.now().strftime("%H:%M")
    logger.info(f"[{now}] Kjører planlagt Aquabyte-sync...")
    await sync_all(lookback_days=14)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Init ScaleAQ-tabeller
    await init_db()
    # Init Aquabyte-tabeller
    init_aquabyte_tables()
    # Backfill Aquabyte ved oppstart
    asyncio.create_task(sync_all(lookback_days=40))
    # Planlegg 09:00 og 23:00 norsk tid
    scheduler.add_job(scheduled_aquabyte_sync, "cron", hour=9,  minute=0)
    scheduler.add_job(scheduled_aquabyte_sync, "cron", hour=23, minute=0)
    scheduler.start()
    logger.info("Scheduler startet — Aquabyte sync kjører kl 09:00 og 23:00")
    yield
    scheduler.shutdown()


app = FastAPI(
    title="Cermaq Backend",
    version="2.1",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers med riktig prefix
app.include_router(meta.router, prefix="/api/meta", tags=["meta"])
app.include_router(feed.router, prefix="/api/feed", tags=["feed"])
app.include_router(aquabyte_router)  # har allerede prefix="/api/aquabyte"


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}
