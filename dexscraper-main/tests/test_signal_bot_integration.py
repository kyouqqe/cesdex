import time

import pytest

from dexscraper.enrich.dexscreener_rest import DexScreenerPairData
from dexscraper.models import ExtractedTokenBatch, TokenProfile
from dexscraper.signal_bot import (
    DetectorConfig,
    PumpDumpDetector,
    TimeSeriesStore,
    process_batch,
)


class FakeRestClient:
    async def get_pair_data(self, token_address: str, pair_address: str):
        return DexScreenerPairData(
            price_usd=1.0,
            liquidity_usd=25000,
            fdv=None,
            market_cap=150000,
            change_m5=None,
            change_h1=None,
            change_h24=None,
            created_at=int(time.time()) - 7200,
            pair_address=pair_address,
            chain="solana",
            dex_id="pumpfun",
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
    rest_client = FakeRestClient()
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
