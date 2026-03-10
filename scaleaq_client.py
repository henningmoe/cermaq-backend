"""
ScaleAQ API client
Handles OAuth token acquisition, refresh, and all API calls.
"""

import httpx
import os
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)

SCALEAQ_BASE    = "https://api.scaleaq.com"
SCALEAQ_VERSION = "2025-01-01"


class ScaleAQClient:
    def __init__(self):
        self.username    = os.environ["SCALEAQ_USERNAME"]
        self.password    = os.environ["SCALEAQ_PASSWORD"]
        self._token: Optional[str] = None
        self._token_expiry: float  = 0

    # ------------------------------------------------------------------ #
    #  Auth                                                                #
    # ------------------------------------------------------------------ #

    async def _get_token(self) -> str:
        """Return a valid access token, fetching a new one if needed."""
        if self._token and time.time() < self._token_expiry - 60:
            return self._token

        logger.info("Fetching new ScaleAQ token…")
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{SCALEAQ_BASE}/auth/token",
                headers={
                    "Content-Type":  "application/json",
                    "Scale-Version": SCALEAQ_VERSION,
                },
                json={
                    "username": self.username,
                    "password": self.password,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

        # ScaleAQ returns { access_token, expires_in, token_type }
        self._token        = data["access_token"]
        expires_in         = int(data.get("expires_in", 3600))
        self._token_expiry = time.time() + expires_in
        logger.info(f"Token acquired, expires in {expires_in}s")
        return self._token

    def _headers(self, token: str) -> dict:
        return {
            "Authorization": f"Bearer {token}",
            "Scale-Version": SCALEAQ_VERSION,
            "Content-Type":  "application/json",
        }

    # ------------------------------------------------------------------ #
    #  Meta                                                                #
    # ------------------------------------------------------------------ #

    async def get_company_meta(self) -> dict:
        """Return full company meta: sites, units, silos, etc."""
        token = await self._get_token()
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{SCALEAQ_BASE}/meta/company",
                params={"include": "all"},
                headers=self._headers(token),
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()

    # ------------------------------------------------------------------ #
    #  Feed data – 10-minute aggregated buckets                           #
    # ------------------------------------------------------------------ #

    async def get_feed_aggregate(
        self,
        from_time: str,
        to_time:   str,
        unit_ids:  list[str] | None = None,
        site_ids:  list[str] | None = None,
    ) -> list[dict]:
        """
        Fetch aggregated FeedAmount + Intensity in 10-minute buckets.

        from_time / to_time: ISO8601 UTC strings
            e.g. "2026-03-10T00:00:00Z"
        """
        token = await self._get_token()
        payload = {
            "fromTime":    from_time,
            "toTime":      to_time,
            "siteids":     site_ids  or [],
            "unitIds":     unit_ids  or [],
            "dataTypes":   ["FeedAmount", "Intensity"],
            "depth":       None,
            "depthVariance": 1,
            "bucketSize":  "0.00:10:00",
            "feedTypeId":  None,
        }

        results = []
        page    = 0

        async with httpx.AsyncClient() as client:
            while True:
                payload["pageIndex"] = page
                resp = await client.post(
                    f"{SCALEAQ_BASE}/time-series/retrieve/units/aggregate",
                    headers=self._headers(token),
                    json=payload,
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()

                # ScaleAQ returns either a list or { items: [...], ... }
                items = data if isinstance(data, list) else data.get("items", data.get("data", []))
                if not items:
                    break
                results.extend(items)

                # Stop if we got fewer than a full page
                if len(items) < payload.get("pageSize", 10000):
                    break
                page += 1

        logger.info(f"Fetched {len(results)} 10-min buckets from ScaleAQ")
        return results

    # ------------------------------------------------------------------ #
    #  Raw time-series (optional, for detailed drill-down)                #
    # ------------------------------------------------------------------ #

    async def get_raw_timeseries(
        self,
        from_time: str,
        to_time:   str,
        unit_ids:  list[str] | None = None,
        data_types: list[str] | None = None,
        page_size: int = 10000,
    ) -> list[dict]:
        token   = await self._get_token()
        payload = {
            "fromTime":       from_time,
            "toTime":         to_time,
            "siteIds":        [],
            "unitIds":        unit_ids   or [],
            "siloIds":        [],
            "dataTypes":      data_types or [],
            "depth":          None,
            "depthVariance":  0,
            "pageSize":       page_size,
            "pageIndex":      0,
            "searchAfter":    None,
            "sortDirection":  "asc",
            "trackTotalHits": False,
        }

        results = []
        async with httpx.AsyncClient() as client:
            while True:
                resp = await client.post(
                    f"{SCALEAQ_BASE}/time-series/retrieve",
                    headers=self._headers(token),
                    json=payload,
                    timeout=60,
                )
                resp.raise_for_status()
                data  = resp.json()
                items = data if isinstance(data, list) else data.get("items", [])
                if not items:
                    break
                results.extend(items)
                if len(items) < page_size:
                    break
                payload["pageIndex"] += 1

        return results


# Singleton reused across sync jobs
_client: Optional[ScaleAQClient] = None


def get_scaleaq_client() -> ScaleAQClient:
    global _client
    if _client is None:
        _client = ScaleAQClient()
    return _client
