import time

import pytest
from aioresponses import aioresponses

from dexscraper.enrich.dexscreener_rest import DexScreenerRestClient
from dexscraper.models import ExtractedTokenBatch, TokenProfile
from dexscraper.signal_bot import (
    DetectorConfig,
    PumpDumpDetector,
    TimeSeriesStore,
    process_batch,
)


class FakeNotifier:
    def __init__(self) -> None:
        self.messages = []

    async def send(self, message: str) -> None:
        self.messages.append(message)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_enrich_normalize_detect_telegram() -> None:
    store = TimeSeriesStore()
    detector = PumpDumpDetector(
        DetectorConfig(
            pump_pct_5m=0,
            dump_pct_5m=0,
            pump_pct_1h=0,
            dump_pct_1h=0,
            min_liquidity_usd=1000,
            min_age_days=0,
            cooldown_s=0,
            min_points_window=3,
        )
    )
    notifier = FakeNotifier()
    rest_client = DexScreenerRestClient()
    url = "https://api.dexscreener.com/latest/dex/tokens/contract"
    payload = {
        "pairs": [
            {
                "pairAddress": "pair",
                "priceUsd": "1.0",
                "liquidity": {"usd": "25000"},
                "marketCap": "150000",
                "priceChange": {"m5": "5.0", "h1": "5.0", "h24": "1.0"},
                "pairCreatedAt": int((time.time() - 7200) * 1000),
                "chainId": "solana",
                "dexId": "pumpfun",
                "baseToken": {"symbol": "AAA"},
            }
        ]
    }
    now = time.time()
    tokens = [
        TokenProfile(
            symbol="AAA",
            chain="solana",
            protocol="pumpfun",
            pair_address="pair",
            token_address="contract",
            price=1.0,
            liquidity=20000,
            market_cap=100000,
            timestamp=int(now - 240),
        ),
        TokenProfile(
            symbol="AAA",
            chain="solana",
            protocol="pumpfun",
            pair_address="pair",
            token_address="contract",
            price=1.0,
            liquidity=20000,
            market_cap=100000,
            timestamp=int(now - 120),
        ),
        TokenProfile(
            symbol="AAA",
            chain="solana",
            protocol="pumpfun",
            pair_address="pair",
            token_address="contract",
            price=1.0,
            liquidity=20000,
            market_cap=100000,
            timestamp=int(now),
        ),
    ]
    batch = ExtractedTokenBatch(tokens=tokens)
    async with rest_client:
        with aioresponses() as mocked:
            mocked.get(url, payload=payload)
            signals = await process_batch(
                batch,
                store,
                detector,
                notifier,
                min_points_window=3,
                rest_client=rest_client,
            )
            assert signals
            assert notifier.messages
