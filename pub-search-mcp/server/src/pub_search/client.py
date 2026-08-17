"""HTTP client for pub-search API."""

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx

# configurable via env var, defaults to the edge proxy: agent traffic gets the
# 60s edge cache and the per-IP rate-limit guard instead of hitting fly raw
API_URL = os.getenv("LEAFLET_SEARCH_API_URL", "https://pub-search.waow.tech/api")


@asynccontextmanager
async def get_http_client() -> AsyncIterator[httpx.AsyncClient]:
    """Get an async HTTP client for API requests."""
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=30.0,
        headers={"Accept": "application/json"},
    ) as client:
        yield client
