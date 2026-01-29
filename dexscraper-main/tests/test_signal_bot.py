import time

import pytest
from aioresponses import aioresponses

from dexscraper.enrich.dexscreener_rest import DexScreenerRestClient
from dexscraper.models import ExtractedTokenBatch, TokenProfile
from dexscraper.signal_bot import (
    DetectorConfig,
    NormalizedEvent,
    PumpDumpDetector,
    Signal,
    TimeSeriesStore,
    format_signal_message,
    normalize,
    process_batch,
)


def _make_event(ts: float, change_5m: float, change_1h: float) -> NormalizedEvent:
    return NormalizedEvent(
        symbol="TEST",
        chain="solana",
        dex="pumpfun",
        pair="pair1",
        contract="contract1",
        price_usd=1.0,
        liquidity_usd=20000,
        mcap_or_fdv=100000,
        age_days=2.0,
        change_5m=change_5m,
        change_1h=change_1h,
        tools_links={"DexScreener": "https://example.com"},
        ts=ts,
    )


def test_compute_pct_requires_points() -> None:
    store = TimeSeriesStore()
    key = ("solana", "pumpfun", "pair1")
    now = time.time()
    store.add(key, now - 200, 1.0)
    store.add(key, now - 100, 1.1)
    assert store.compute_pct(key, 300, 3, now) is None
    store.add(key, now, 1.2)
    pct = store.compute_pct(key, 300, 3, now)
    assert pct is not None
    assert pct > 0


def test_detector_dedup_and_cooldown() -> None:
    config = DetectorConfig(
        pump_pct_5m=5,
        dump_pct_5m=5,
        pump_pct_1h=5,
        dump_pct_1h=5,
        min_liquidity_usd=1000,
        min_age_days=1,
        cooldown_s=600,
        min_points_window=3,
        time_bucket_s=300,
    )
    detector = PumpDumpDetector(config)
    now = time.time()
    event = _make_event(now, 10, 10)
    assert detector.evaluate(event) is not None
    assert detector.evaluate(event) is None
    later = _make_event(now + 100, 10, 10)
    assert detector.evaluate(later) is None


def test_format_signal_message() -> None:
    event = _make_event(time.time(), 5.5, 7.2)
    signal = Signal(event=event, signal_type="pump")
    message = format_signal_message(signal)
    assert "TEST | SOLANA | 5m ▲5.50%" in message
    assert "Liquidity:" in message
    assert "Tools:" in message


@pytest.mark.asyncio
async def test_normalize_sanity_checks() -> None:
    token = TokenProfile(
        symbol="AAA",
        chain="solana",
        protocol="pumpfun",
        pair_address="pair",
        token_address="contract",
        age="1h",
        timestamp=int(time.time()),
    )
    url = "https://api.dexscreener.com/latest/dex/tokens/contract"
    payload = {
        "pairs": [
            {
                "pairAddress": "pair",
                "priceUsd": "0.5",
                "liquidity": {"usd": "20000"},
                "marketCap": "100000",
                "priceChange": {"m5": "1.0", "h1": "2.0", "h24": "3.0"},
                "pairCreatedAt": int(time.time() * 1000),
                "chainId": "solana",
                "dexId": "pumpfun",
                "baseToken": {"symbol": "AAA"},
            }
        ]
    }
    async with DexScreenerRestClient() as rest_client:
        with aioresponses() as mocked:
            mocked.get(url, payload=payload)
            event = await normalize(token, rest_client=rest_client)
    assert event is not None


@pytest.mark.asyncio
async def test_process_batch_triggers_send() -> None:
    class DummyNotifier:
        def __init__(self) -> None:
            self.messages = []

        async def send(self, message: str) -> None:
            self.messages.append(message)

    store = TimeSeriesStore()
    detector = PumpDumpDetector(
        DetectorConfig(
            pump_pct_5m=5,
            dump_pct_5m=5,
            pump_pct_1h=5,
            dump_pct_1h=5,
            min_liquidity_usd=1000,
            min_age_days=0,
            cooldown_s=0,
            min_points_window=3,
            time_bucket_s=60,
        )
    )
    notifier = DummyNotifier()
    url = "https://api.dexscreener.com/latest/dex/tokens/contract"
    payload = {
        "pairs": [
            {
                "pairAddress": "pair",
                "priceUsd": "1.2",
                "liquidity": {"usd": "20000"},
                "marketCap": "100000",
                "priceChange": {"m5": "10.0", "h1": "10.0", "h24": "1.0"},
                "pairCreatedAt": int((time.time() - 3600) * 1000),
                "chainId": "solana",
                "dexId": "pumpfun",
                "baseToken": {"symbol": "AAA"},
            }
        ]
    }
    base_time = time.time()
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
            timestamp=int(base_time - 240),
        ),
        TokenProfile(
            symbol="AAA",
            chain="solana",
            protocol="pumpfun",
            pair_address="pair",
            token_address="contract",
            price=1.1,
            liquidity=20000,
            market_cap=100000,
            timestamp=int(base_time - 120),
        ),
        TokenProfile(
            symbol="AAA",
            chain="solana",
            protocol="pumpfun",
            pair_address="pair",
            token_address="contract",
            price=1.2,
            liquidity=20000,
            market_cap=100000,
            timestamp=int(base_time),
        ),
    ]
    batch = ExtractedTokenBatch(tokens=tokens)
    async with DexScreenerRestClient() as rest_client:
        with aioresponses() as mocked:
            mocked.get(url, payload=payload)
            await process_batch(
                batch, store, detector, notifier, 3, rest_client=rest_client
            )
    assert notifier.messages
