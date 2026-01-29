"""DexScreener REST enrichment client."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DexScreenerPairData:
    price_usd: Optional[float]
    liquidity_usd: Optional[float]
    fdv: Optional[float]
    market_cap: Optional[float]
    change_m5: Optional[float]
    change_h1: Optional[float]
    change_h24: Optional[float]
    created_at: Optional[int]
    pair_address: Optional[str] = None
    chain: Optional[str] = None
    dex_id: Optional[str] = None


def age_days_from_created_at(
    created_at: Optional[int], now_ts: Optional[float] = None
) -> Optional[float]:
    if created_at is None:
        return None
    timestamp = _normalize_epoch_seconds(created_at)
    if timestamp is None:
        return None
    now = now_ts or time.time()
    age_seconds = max(0.0, now - timestamp)
    return age_seconds / 86400


def parse_dexscreener_response(
    payload: Dict[str, Any],
    token_address: str,
    pair_address: Optional[str] = None,
) -> Optional[DexScreenerPairData]:
    pairs = payload.get("pairs") or []
    if not isinstance(pairs, list) or not pairs:
        logger.debug("No pairs found in response for token=%s", token_address)
        return None

    selected: Optional[Dict[str, Any]] = None
    if pair_address:
        for pair in pairs:
            if pair.get("pairAddress") == pair_address:
                selected = pair
                break

    if selected is None:
        selected = max(
            pairs,
            key=lambda pair: _safe_float(pair.get("liquidity", {}).get("usd")) or 0.0,
        )

    price_usd = _safe_float(selected.get("priceUsd"))
    liquidity_usd = _safe_float(selected.get("liquidity", {}).get("usd"))
    fdv = _safe_float(selected.get("fdv"))
    market_cap = _safe_float(selected.get("marketCap"))
    price_change = selected.get("priceChange") or {}

    return DexScreenerPairData(
        price_usd=price_usd,
        liquidity_usd=liquidity_usd,
        fdv=fdv,
        market_cap=market_cap,
        change_m5=_safe_float(price_change.get("m5")),
        change_h1=_safe_float(price_change.get("h1")),
        change_h24=_safe_float(price_change.get("h24")),
        created_at=_normalize_epoch_seconds(
            selected.get("pairCreatedAt") or selected.get("createdAt")
        ),
        pair_address=selected.get("pairAddress"),
        chain=selected.get("chainId"),
        dex_id=selected.get("dexId"),
    )


class DexScreenerRestClient:
    """Async REST client with caching, rate limiting, and retries."""

    def __init__(
        self,
        cache_ttl_s: int = 60,
        rate_limit_s: float = 0.3,
        max_retries: int = 3,
        timeout_s: int = 10,
        base_url: str = "https://api.dexscreener.com/latest/dex/tokens",
    ) -> None:
        self._cache_ttl_s = cache_ttl_s
        self._rate_limit_s = rate_limit_s
        self._max_retries = max_retries
        self._timeout_s = timeout_s
        self._base_url = base_url.rstrip("/")
        self._cache: Dict[Tuple[str, str], Tuple[float, Optional[DexScreenerPairData]]] = (
            {}
        )
        self._last_request = 0.0
        self._lock = asyncio.Lock()

    async def get_pair_data(
        self, token_address: str, pair_address: Optional[str] = None
    ) -> Optional[DexScreenerPairData]:
        cache_key = (token_address.lower(), pair_address or "")
        now = time.time()
        cached = self._cache.get(cache_key)
        if cached and cached[0] >= now:
            return cached[1]

        payload = await self._fetch_json(token_address)
        if payload is None:
            self._cache[cache_key] = (now + self._cache_ttl_s, None)
            return None

        pair_data = parse_dexscreener_response(payload, token_address, pair_address)
        self._cache[cache_key] = (now + self._cache_ttl_s, pair_data)
        return pair_data

    async def _fetch_json(self, token_address: str) -> Optional[Dict[str, Any]]:
        url = f"{self._base_url}/{token_address}"
        for attempt in range(self._max_retries + 1):
            try:
                await self._rate_limit()
                return await asyncio.to_thread(self._get_json_sync, url)
            except Exception as exc:
                if attempt >= self._max_retries:
                    logger.warning(
                        "DexScreener REST failed token=%s: %s", token_address, exc
                    )
                    break
                delay = 0.5 * (2**attempt)
                logger.debug(
                    "DexScreener REST retry token=%s attempt=%s delay=%.2fs",
                    token_address,
                    attempt + 1,
                    delay,
                )
                await asyncio.sleep(delay)
        return None

    async def _rate_limit(self) -> None:
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_request
            if elapsed < self._rate_limit_s:
                await asyncio.sleep(self._rate_limit_s - elapsed)
            self._last_request = time.time()

    def _get_json_sync(self, url: str) -> Dict[str, Any]:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; DexScraper/1.0)",
            "Accept": "application/json",
        }
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=self._timeout_s) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Unexpected payload type")
        return payload


def _normalize_epoch_seconds(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    if timestamp > 10**12:
        timestamp = timestamp // 1000
    return timestamp


def _safe_float(value: Optional[Any]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
