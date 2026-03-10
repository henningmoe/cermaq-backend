"""
Cermaq Horsvågen – Backend API
Railway deployment
"""

from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import logging

from db import init_db
from scaleaq_client import ScaleAQClient
from sync import sync_feed_data
from routers import feed, meta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Initialising database…")
    await init_db()

    logger.info("Running initial sync…")
    try:
        await sync_feed_data()
    except Exception as e:
        logger.warning(f"Initial sync failed (will retry on schedule): {e}")

    # Schedule sync every 10 minutes
    scheduler.add_job(sync_feed_data, "interval", minutes=10, id="feed_sync")
    scheduler.start()
    logger.info("Scheduler started – syncing every 10 minutes")

    yield

    # Shutdown
    scheduler.shutdown()


app = FastAPI(
    title="Cermaq Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # Restrict to your Netlify URL in production
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(feed.router, prefix="/api/feed", tags=["Feed"])
app.include_router(meta.router, prefix="/api/meta", tags=["Meta"])


@app.get("/health")
async def health():
    return {"status": "ok"}
