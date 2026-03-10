"""
Cermaq Horsvågen – Backend API
Railway deployment
"""

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging

from db import init_db
from sync import sync_feed_data
from auth import require_api_key
from routers import feed, meta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initialising database…")
    await init_db()

    logger.info("Running initial sync…")
    try:
        await sync_feed_data()
    except Exception as e:
        logger.warning(f"Initial sync failed (will retry on schedule): {e}")

    scheduler.add_job(sync_feed_data, "interval", minutes=10, id="feed_sync")
    scheduler.start()
    logger.info("Scheduler started – syncing every 10 minutes")

    yield

    scheduler.shutdown()


app = FastAPI(
    title="Cermaq Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Sett til Netlify-URL i produksjon
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Alle /api/* endepunkter krever gyldig X-API-Key header
app.include_router(
    feed.router,
    prefix="/api/feed",
    tags=["Feed"],
    dependencies=[Depends(require_api_key)],
)
app.include_router(
    meta.router,
    prefix="/api/meta",
    tags=["Meta"],
    dependencies=[Depends(require_api_key)],
)


# /health er åpen (brukes av Railway for helsesjekk)
@app.get("/health")
async def health():
    return {"status": "ok"}
