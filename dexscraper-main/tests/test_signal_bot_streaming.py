import asyncio
import time

from dexscraper.models import ExtractedTokenBatch, TokenProfile
from dexscraper.signal_bot import (
    DetectorConfig,
    PumpDumpDetector,
    SignalBotConfig,
    StreamConfig,
    TelegramConfig,
    TimeSeriesStore,
    stream_signal_bot,
)


class FakeWebSocket:
    def __init__(self, messages):
        self._messages = list(messages)
        self.close_called = False
        self.close_code = 1000
        self.close_reason = "normal"

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def close(self):
        if self._messages:
            raise AssertionError("WebSocket closed before messages exhausted")
        self.close_called = True


def _make_batch(now: float) -> ExtractedTokenBatch:
    token = TokenProfile(
        symbol="AAA",
        chain="solana",
        protocol="pumpfun",
        pair_address="pair",
        token_address="contract",
        price=1.0,
        liquidity=20000,
        market_cap=100000,
        age="1h",
        timestamp=int(now),
    )
    return ExtractedTokenBatch(tokens=[token])


def test_stream_does_not_close_after_first_message() -> None:
    async def _run() -> None:
        now = time.time()
        websocket = FakeWebSocket([b"msg1", b"msg2"])

        class DummyScraper:
            async def extract_tokens_from_message(self, message, max_tokens):
                return _make_batch(now)

        async def connect():
            return websocket

        config = SignalBotConfig(
            detector=DetectorConfig(
                pump_pct_5m=0,
                dump_pct_5m=0,
                pump_pct_1h=0,
                dump_pct_1h=0,
                min_liquidity_usd=0,
                min_age_days=0,
                cooldown_s=0,
                min_points_window=1,
            ),
            stream=StreamConfig(max_tokens=50, max_retries=1),
            telegram=TelegramConfig(enabled=False),
        )
        store = TimeSeriesStore()
        detector = PumpDumpDetector(config.detector)

        await stream_signal_bot(
            DummyScraper(),
            config,
            store,
            detector,
            notifier=None,
            connect_fn=connect,
            max_messages=2,
        )
        assert websocket.close_called is True

    asyncio.run(_run())


def test_stream_uses_configured_max_tokens() -> None:
    async def _run() -> None:
        now = time.time()
        websocket = FakeWebSocket([b"msg1"])
        captured = {}

        class DummyScraper:
            async def extract_tokens_from_message(self, message, max_tokens):
                captured["max_tokens"] = max_tokens
                return _make_batch(now)

        async def connect():
            return websocket

        config = SignalBotConfig(
            detector=DetectorConfig(
                pump_pct_5m=0,
                dump_pct_5m=0,
                pump_pct_1h=0,
                dump_pct_1h=0,
                min_liquidity_usd=0,
                min_age_days=0,
                cooldown_s=0,
                min_points_window=1,
            ),
            stream=StreamConfig(max_tokens=5, max_retries=1),
            telegram=TelegramConfig(enabled=False),
        )
        store = TimeSeriesStore()
        detector = PumpDumpDetector(config.detector)

        await stream_signal_bot(
            DummyScraper(),
            config,
            store,
            detector,
            notifier=None,
            connect_fn=connect,
            max_messages=1,
        )
        assert captured["max_tokens"] == 5

    asyncio.run(_run())
