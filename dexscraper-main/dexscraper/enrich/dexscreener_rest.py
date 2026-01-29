"""DexScreener REST enrichment client."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import aiohttp

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
    symbol: Optional[str] = None


@dataclass(frozen=True)
class DexScreenerFetchResult:
    data: Optional[DexScreenerPairData]
    reason: str


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

    base_token = selected.get("baseToken") or {}
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
        symbol=base_token.get("symbol"),
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
        self._cache: Dict[
            Tuple[str, str], Tuple[float, DexScreenerFetchResult]
        ] = {}
        self._last_request = 0.0
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "DexScreenerRestClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def get_pair_data(
        self, token_address: str, pair_address: Optional[str] = None
    ) -> DexScreenerFetchResult:
        cache_key = (token_address.lower(), pair_address or "")
        now = time.time()
        cached = self._cache.get(cache_key)
        if cached and cached[0] >= now:
            return cached[1]

        payload, fetch_reason = await self._fetch_json(token_address)
        if payload is None:
            result = DexScreenerFetchResult(data=None, reason=fetch_reason)
            self._cache[cache_key] = (now + self._cache_ttl_s, result)
            return result

        pair_data = parse_dexscreener_response(payload, token_address, pair_address)
        if pair_data is None:
            result = DexScreenerFetchResult(data=None, reason="rest_no_pairs")
        elif pair_data.created_at is None:
            result = DexScreenerFetchResult(data=None, reason="missing_created_at")
        else:
            result = DexScreenerFetchResult(data=pair_data, reason="")
        self._cache[cache_key] = (now + self._cache_ttl_s, result)
        return result

    async def _fetch_json(
        self, token_address: str
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        url = f"{self._base_url}/{token_address}"
        await self._ensure_session()
        last_reason = "rest_error"
        for attempt in range(self._max_retries + 1):
            try:
                await self._rate_limit()
                assert self._session is not None
                async with self._session.get(url, timeout=self._timeout_s) as response:
                    response.raise_for_status()
                    payload = await response.json()
                if not isinstance(payload, dict):
                    raise ValueError("Unexpected payload type")
                return payload, ""
            except asyncio.TimeoutError:
                last_reason = "rest_timeout"
            except (aiohttp.ClientError, ValueError) as exc:
                last_reason = "rest_error"
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
        return None, last_reason

    async def _rate_limit(self) -> None:
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_request
            if elapsed < self._rate_limit_s:
                await asyncio.sleep(self._rate_limit_s - elapsed)
            self._last_request = time.time()

    async def _ensure_session(self) -> None:
        if self._session is None or self._session.closed:
            headers = {
                "User-Agent": "Mozilla/5.0 (compatible; DexScraper/1.0)",
                "Accept": "application/json",
            }
            self._session = aiohttp.ClientSession(headers=headers)


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
