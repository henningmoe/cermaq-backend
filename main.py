"""
main.py — legg dette inn i ditt eksisterende main.py i henningmoe/cermaq-backend
(eller erstatt hele filen med dette)
"""

import os
import logging
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Eksisterende routers
from routers import meta, feed

# Ny Aquabyte router
from routers.aquabyte import router as aquabyte_router

# Aquabyte sync
from aquabyte_sync import sync_all, init_aquabyte_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

API_KEY = os.environ.get("API_KEY", "")

def verify_key(x_api_key: str = Depends(lambda x_api_key: x_api_key)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Ugyldig API-nøkkel")

scheduler = AsyncIOScheduler(timezone="Europe/Oslo")

async def scheduled_aquabyte_sync():
    now = datetime.now().strftime("%H:%M")
    logger.info(f"[{now}] Kjører planlagt Aquabyte-sync...")
    await sync_all(lookback_days=14)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Init tabeller
    init_aquabyte_tables()

    # Kjør en gang ved oppstart for å fylle data
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
    version="2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Eksisterende routers
app.include_router(meta.router)
app.include_router(feed.router)

# Aquabyte
app.include_router(aquabyte_router)

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}
