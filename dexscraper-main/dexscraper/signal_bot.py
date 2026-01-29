"""Pump/dump signal bot built on DexScreener ingestion."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass, field
from statistics import median
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import websockets
import yaml

from .config import Chain, DEX, Filters, Order, RankBy, ScrapingConfig, Timeframe
from .enrich.dexscreener_rest import DexScreenerPairData, DexScreenerRestClient
from .models import ExtractedTokenBatch, TokenProfile
from .scraper import DexScraper

logger = logging.getLogger(__name__)


@dataclass
class NormalizedEvent:
    """Normalized signal event derived from raw token profiles."""

    symbol: str
    chain: str
    dex: str
    pair: str
    contract: str
    price_usd: float
    liquidity_usd: float
    mcap_or_fdv: float
    age_days: float
    change_5m: Optional[float]
    change_1h: Optional[float]
    tools_links: Dict[str, str]
    ts: float


@dataclass
class Signal:
    """Signal payload emitted by the detector."""

    event: NormalizedEvent
    signal_type: str


@dataclass
class DetectorConfig:
    pump_pct_5m: float
    dump_pct_5m: float
    pump_pct_1h: float
    dump_pct_1h: float
    min_liquidity_usd: float
    min_age_days: float
    cooldown_s: int
    min_points_window: int
    time_bucket_s: int = 300


@dataclass
class StreamConfig:
    poll_interval_s: int = 5
    max_tokens: int = 50
    rate_limit: float = 4.0
    max_retries: int = 5
    backoff_base: float = 1.0
    use_cloudflare_bypass: bool = False
    transport: str = "websockets"
    timeframe: str = "h24"
    rank_by: str = "trendingScoreH6"
    order: str = "desc"
    filters: Dict[str, object] = field(default_factory=dict)


@dataclass
class TelegramConfig:
    enabled: bool
    rate_limit_s: float = 1.0
    max_retries: int = 3
    backoff_base: float = 1.0


@dataclass
class SignalBotConfig:
    detector: DetectorConfig
    stream: StreamConfig
    telegram: TelegramConfig


class TimeSeriesStore:
    """In-memory price history with rolling windows."""

    def __init__(
        self,
        retention_s: int = 7200,
        outlier_window: int = 20,
        clamp_ratio: float = 0.5,
    ) -> None:
        self._retention_s = retention_s
        self._outlier_window = outlier_window
        self._clamp_ratio = clamp_ratio
        self._series: Dict[Tuple[str, str, str], List[Tuple[float, float]]] = {}

    def add(self, key: Tuple[str, str, str], ts: float, price: float) -> None:
        series = self._series.setdefault(key, [])
        series.append((ts, price))
        cutoff = ts - self._retention_s
        if series and series[0][0] < cutoff:
            self._series[key] = [(t, p) for t, p in series if t >= cutoff]

    def compute_pct(
        self,
        key: Tuple[str, str, str],
        window_s: int,
        min_points: int,
        as_of: float,
    ) -> Optional[float]:
        series = self._series.get(key)
        if not series:
            return None
        window_start = as_of - window_s
        window_points = [(t, p) for t, p in series if t >= window_start]
        if len(window_points) < min_points:
            return None
        filtered_prices = self._filter_outliers([p for _, p in window_points])
        if len(filtered_prices) < min_points:
            return None
        start = filtered_prices[0]
        end = filtered_prices[-1]
        if start <= 0:
            return None
        pct = ((end - start) / start) * 100
        logger.debug(
            "pct computed from history key=%s window=%ss points=%s start=%.10f end=%.10f pct=%.4f",
            key,
            window_s,
            len(filtered_prices),
            start,
            end,
            pct,
        )
        return pct

    def _filter_outliers(self, prices: List[float]) -> List[float]:
        if not prices:
            return []
        tail = prices[-self._outlier_window :]
        baseline = median(tail)
        if baseline <= 0:
            return prices
        lower = baseline * (1 - self._clamp_ratio)
        upper = baseline * (1 + self._clamp_ratio)
        return [min(max(price, lower), upper) for price in prices]


class PumpDumpDetector:
    """Evaluate normalized events for pump/dump signals with dedup/cooldown."""

    def __init__(self, config: DetectorConfig) -> None:
        self._config = config
        self._cooldowns: Dict[Tuple[str, str], float] = {}
        self._dedup: Dict[Tuple[str, str, int], float] = {}

    def evaluate(self, event: NormalizedEvent) -> Optional[Signal]:
        if event.liquidity_usd < self._config.min_liquidity_usd:
            return None
        if event.age_days < self._config.min_age_days:
            return None
        if event.change_5m is None or event.change_1h is None:
            return None

        signal_type = self._classify(event)
        if signal_type is None:
            return None

        time_bucket = int(event.ts // self._config.time_bucket_s)
        dedup_key = (event.pair, signal_type, time_bucket)
        cooldown_key = (event.pair, signal_type)
        last_sent = self._cooldowns.get(cooldown_key)
        if last_sent is not None and event.ts - last_sent < self._config.cooldown_s:
            return None
        if dedup_key in self._dedup:
            return None

        self._cooldowns[cooldown_key] = event.ts
        self._dedup[dedup_key] = event.ts
        return Signal(event=event, signal_type=signal_type)

    def _classify(self, event: NormalizedEvent) -> Optional[str]:
        if (
            event.change_5m >= self._config.pump_pct_5m
            and event.change_1h >= self._config.pump_pct_1h
        ):
            return "pump"
        if (
            event.change_5m <= -self._config.dump_pct_5m
            and event.change_1h <= -self._config.dump_pct_1h
        ):
            return "dump"
        return None


class TelegramNotifier:
    """Async Telegram sender with queueing and backoff."""

    def __init__(
        self,
        token: str,
        chat_id: str,
        rate_limit_s: float = 1.0,
        max_retries: int = 3,
        backoff_base: float = 1.0,
    ) -> None:
        self._token = token
        self._chat_id = chat_id
        self._rate_limit_s = rate_limit_s
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._queue: "asyncio.Queue[Optional[str]]" = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task[None]] = None
        self._last_send = 0.0

    async def start(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        await self._queue.put(None)
        if self._worker_task:
            await self._worker_task

    async def send(self, message: str) -> None:
        await self._queue.put(message)

    async def _worker(self) -> None:
        while True:
            message = await self._queue.get()
            if message is None:
                self._queue.task_done()
                break
            await self._rate_limit()
            await self._send_with_retry(message)
            self._queue.task_done()

    async def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_send
        if elapsed < self._rate_limit_s:
            await asyncio.sleep(self._rate_limit_s - elapsed)
        self._last_send = time.time()

    async def _send_with_retry(self, message: str) -> None:
        for attempt in range(self._max_retries):
            try:
                await asyncio.to_thread(self._post_message, message)
                return
            except Exception as exc:
                delay = self._backoff_base * (2**attempt)
                logger.warning("Telegram send failed: %s (retry in %.2fs)", exc, delay)
                await asyncio.sleep(delay)
        logger.error("Telegram send failed after retries")

    def _post_message(self, message: str) -> None:
        import urllib.parse
        import urllib.request

        payload = urllib.parse.urlencode(
            {
                "chat_id": self._chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
        ).encode("utf-8")
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        request = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(request, timeout=10) as response:
            data = response.read()
            parsed = json.loads(data)
            if not parsed.get("ok"):
                raise RuntimeError(f"Telegram API error: {parsed}")


def parse_age_days(age_value: Optional[str]) -> Optional[float]:
    if not age_value:
        return None
    value = age_value.strip().lower()
    if not value:
        return None
    unit = value[-1]
    try:
        amount = float(value[:-1])
    except ValueError:
        return None
    if unit == "m":
        return amount / (60 * 24)
    if unit == "h":
        return amount / 24
    if unit == "d":
        return amount
    if unit == "w":
        return amount * 7
    return None


def parse_age_days_from_created_at(created_at: Optional[int]) -> Optional[float]:
    from .enrich.dexscreener_rest import age_days_from_created_at

    return age_days_from_created_at(created_at)


def _build_tools_links(chain: str, pair: str, contract: str) -> Dict[str, str]:
    chain_key = chain.lower()
    links = {
        "DexScreener": f"https://dexscreener.com/{chain_key}/{pair}",
        "GeckoTerminal": f"https://www.geckoterminal.com/{chain_key}/pools/{pair}",
    }
    dextools_chain = {
        "solana": "solana",
        "ethereum": "ether",
        "base": "base",
        "bsc": "bsc",
        "polygon": "polygon",
        "arbitrum": "arbitrum",
        "optimism": "optimism",
        "avalanche": "avalanche",
    }.get(chain_key)
    if dextools_chain:
        links[
            "DexTools"
        ] = f"https://www.dextools.io/app/en/{dextools_chain}/pair-explorer/{pair}"
    if chain_key == "solana":
        links[
            "Birdeye"
        ] = f"https://birdeye.so/token/{contract}?chain=solana"
    return links


async def normalize(raw_pair: TokenProfile) -> Optional[NormalizedEvent]:
    event, _ = await normalize_with_reason(raw_pair)
    return event


async def normalize_with_reason(
    raw_pair: TokenProfile,
    rest_client: Optional[DexScreenerRestClient] = None,
) -> Tuple[Optional[NormalizedEvent], str]:
    """Validate & normalize raw pair data into a detector-friendly event.

    Returns: (event or None, drop_reason)
    """
    if not raw_pair.pair_address or not raw_pair.token_address:
        return None, "missing_addresses"

    rest_data: Optional[DexScreenerPairData] = None
    if rest_client is not None:
        rest_data = await rest_client.get_pair_data(
            raw_pair.token_address, raw_pair.pair_address
        )

    chain = raw_pair.chain or (rest_data.chain if rest_data else None)
    protocol = raw_pair.protocol or (rest_data.dex_id if rest_data else None)
    if not raw_pair.symbol or not chain or not protocol:
        return None, "missing_identity"

    price = (
        rest_data.price_usd
        if rest_data and rest_data.price_usd is not None
        else raw_pair.price
    )
    liquidity = (
        rest_data.liquidity_usd
        if rest_data and rest_data.liquidity_usd is not None
        else raw_pair.liquidity
    )
    market_cap = None
    if rest_data:
        market_cap = rest_data.market_cap or rest_data.fdv
    if market_cap is None:
        market_cap = raw_pair.market_cap

    if price is None or liquidity is None:
        return None, "missing_price_or_liquidity"
    if market_cap is None:
        return None, "missing_mcap"

    if rest_data:
        raw_pair.price = price
        raw_pair.liquidity = liquidity
        raw_pair.market_cap = market_cap
        if rest_data.change_m5 is not None:
            raw_pair.change_5m = rest_data.change_m5
        if rest_data.change_h1 is not None:
            raw_pair.change_1h = rest_data.change_h1
        if rest_data.change_h24 is not None:
            raw_pair.change_24h = rest_data.change_h24

    # Numeric sanity
    if not all(
        math.isfinite(value) for value in (price, liquidity, market_cap)
    ):
        return None, "non_finite_values"
    if price <= 0 or liquidity <= 0 or market_cap <= 0:
        return None, "non_positive_values"
    # Heuristic: absurdly tiny price with large liquidity is usually a quote/unit mismatch
    if price < 1e-9 and liquidity >= 10_000:
        return None, "price_liquidity_mismatch"

    age_days: Optional[float] = None
    if rest_data and rest_data.created_at is not None:
        age_days = parse_age_days_from_created_at(rest_data.created_at)
    elif rest_client is None:
        age_days = parse_age_days(raw_pair.age)
    if age_days is None:
        return None, "missing_created_at"

    tools_links = _build_tools_links(
        chain, raw_pair.pair_address, raw_pair.token_address
    )

    event = NormalizedEvent(
        symbol=raw_pair.symbol.upper(),
        chain=chain,
        dex=protocol,
        pair=raw_pair.pair_address,
        contract=raw_pair.token_address,
        price_usd=price,
        liquidity_usd=liquidity,
        mcap_or_fdv=market_cap,
        age_days=age_days,
        change_5m=None,
        change_1h=None,
        tools_links=tools_links,
        ts=float(raw_pair.timestamp or time.time()),
    )
    return event, ""


def format_signal_message(signal: Signal) -> str:
    event = signal.event
    pct_5m = event.change_5m or 0.0
    pct_1h = event.change_1h or 0.0
    arrow_5m = "▲" if pct_5m >= 0 else "▼"
    arrow_1h = "▲" if pct_1h >= 0 else "▼"
    emoji = "🚀" if signal.signal_type == "pump" else "🩸"
    header = (
        f"{event.symbol} | {event.chain.upper()} | 5m {arrow_5m}{pct_5m:.2f}% | "
        f"1h {arrow_1h}{pct_1h:.2f}% {emoji}"
    )
    tools_links = " | ".join(
        f'<a href="{url}">{name}</a>' for name, url in event.tools_links.items()
    )
    lines = [
        header,
        f"Source: {event.dex} / {event.chain}",
        f"Pool: {event.pair}",
        f"Price: ${event.price_usd:,.8f}",
        f"Days: {event.age_days:.2f}",
        f"Liquidity: ${event.liquidity_usd:,.0f}",
        f"MCap/FDV: ${event.mcap_or_fdv:,.0f}",
        f"Contract: <code>{event.contract}</code>",
        f"Tools: {tools_links}",
    ]
    return "\n".join(lines)


async def process_batch(
    batch: ExtractedTokenBatch,
    store: TimeSeriesStore,
    detector: PumpDumpDetector,
    notifier: Optional[TelegramNotifier],
    min_points_window: int,
    rest_client: Optional[DexScreenerRestClient] = None,
) -> List[Signal]:
    signals: List[Signal] = []
    for token in batch.tokens:
        event, reason = await normalize_with_reason(token, rest_client=rest_client)
        if event is None:
            logger.debug(
                "[filter] drop reason=%s chain=%s dex=%s symbol=%s liquidity=%s",
                reason,
                token.chain,
                token.protocol,
                token.symbol,
                token.liquidity,
            )
            continue
        key = (event.chain, event.dex, event.pair)
        store.add(key, event.ts, event.price_usd)
        event.change_5m = store.compute_pct(key, 300, min_points_window, event.ts)
        event.change_1h = store.compute_pct(key, 3600, min_points_window, event.ts)
        signal = detector.evaluate(event)
        logger.info(
            "[detector] tick chain=%s dex=%s symbol=%s change_5m=%s change_1h=%s triggered=%s",
            event.chain,
            event.dex,
            event.symbol,
            None if event.change_5m is None else f"{event.change_5m:.4f}",
            None if event.change_1h is None else f"{event.change_1h:.4f}",
            bool(signal),
        )
        if signal:
            signals.append(signal)
            message = format_signal_message(signal)
            logger.info("Signal detected: %s %s", signal.signal_type, event.pair)
            if notifier:
                await notifier.send(message)
            else:
                logger.info("Telegram disabled. Message: %s", message)
    return signals


def _parse_filters(raw_filters: Dict[str, object]) -> Filters:
    chain_ids = [Chain(chain) for chain in raw_filters.get("chain_ids", [])]
    dex_ids = [DEX(dex) for dex in raw_filters.get("dex_ids", [])]
    return Filters(
        chain_ids=chain_ids or [Chain.SOLANA],
        dex_ids=dex_ids,
        liquidity_min=raw_filters.get("liquidity_min"),
        liquidity_max=raw_filters.get("liquidity_max"),
        volume_h24_min=raw_filters.get("volume_h24_min"),
        volume_h24_max=raw_filters.get("volume_h24_max"),
        volume_h6_min=raw_filters.get("volume_h6_min"),
        volume_h6_max=raw_filters.get("volume_h6_max"),
        volume_h1_min=raw_filters.get("volume_h1_min"),
        volume_h1_max=raw_filters.get("volume_h1_max"),
        txns_h24_min=raw_filters.get("txns_h24_min"),
        txns_h24_max=raw_filters.get("txns_h24_max"),
        txns_h6_min=raw_filters.get("txns_h6_min"),
        txns_h6_max=raw_filters.get("txns_h6_max"),
        txns_h1_min=raw_filters.get("txns_h1_min"),
        txns_h1_max=raw_filters.get("txns_h1_max"),
        pair_age_min=raw_filters.get("pair_age_min"),
        pair_age_max=raw_filters.get("pair_age_max"),
        price_change_h24_min=raw_filters.get("price_change_h24_min"),
        price_change_h24_max=raw_filters.get("price_change_h24_max"),
        price_change_h6_min=raw_filters.get("price_change_h6_min"),
        price_change_h6_max=raw_filters.get("price_change_h6_max"),
        price_change_h1_min=raw_filters.get("price_change_h1_min"),
        price_change_h1_max=raw_filters.get("price_change_h1_max"),
        fdv_min=raw_filters.get("fdv_min"),
        fdv_max=raw_filters.get("fdv_max"),
        market_cap_min=raw_filters.get("market_cap_min"),
        market_cap_max=raw_filters.get("market_cap_max"),
        enhanced_token_info=raw_filters.get("enhanced_token_info", False),
        active_boosts_min=raw_filters.get("active_boosts_min"),
        recent_purchased_impressions_min=raw_filters.get(
            "recent_purchased_impressions_min"
        ),
        max_age=raw_filters.get("max_age"),
        profile=raw_filters.get("profile"),
        max_launchpad_progress=raw_filters.get("max_launchpad_progress"),
    )


def _build_scraper_config(stream: StreamConfig) -> ScrapingConfig:
    filters = _parse_filters(stream.filters)
    return ScrapingConfig(
        timeframe=Timeframe(stream.timeframe),
        rank_by=RankBy(stream.rank_by),
        order=Order(stream.order),
        filters=filters,
    )


def load_config(path: str) -> SignalBotConfig:
    import yaml

    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    detector = DetectorConfig(**raw["detector"])
    stream = StreamConfig(**raw["stream"])
    telegram_raw = raw.get("telegram", {})
    telegram = TelegramConfig(
        enabled=telegram_raw.get("enabled", False),
        rate_limit_s=telegram_raw.get("rate_limit_s", 1.0),
        max_retries=telegram_raw.get("max_retries", 3),
        backoff_base=telegram_raw.get("backoff_base", 1.0),
    )
    return SignalBotConfig(detector=detector, stream=stream, telegram=telegram)


async def run_signal_bot(config: SignalBotConfig) -> None:
    from .scraper import DexScraper

    scraper_config = _build_scraper_config(config.stream)
    scraper = DexScraper(
        rate_limit=config.stream.rate_limit,
        max_retries=config.stream.max_retries,
        backoff_base=config.stream.backoff_base,
        use_cloudflare_bypass=config.stream.use_cloudflare_bypass,
        config=scraper_config,
        transport=config.stream.transport,
    )
    store = TimeSeriesStore()
    detector = PumpDumpDetector(config.detector)
    notifier: Optional[TelegramNotifier] = None
    rest_client = DexScreenerRestClient()

    if config.telegram.enabled:
        token = _require_env("TELEGRAM_BOT_TOKEN")
        chat_id = _require_env("TELEGRAM_CHAT_ID")
        notifier = TelegramNotifier(
            token=token,
            chat_id=chat_id,
            rate_limit_s=config.telegram.rate_limit_s,
            max_retries=config.telegram.max_retries,
            backoff_base=config.telegram.backoff_base,
        )
        await notifier.start()

    try:
        await stream_signal_bot(
            scraper,
            config,
            store,
            detector,
            notifier,
            rest_client=rest_client,
        )
    finally:
        if notifier:
            await notifier.stop()


def _require_env(name: str) -> str:
    import os

    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="DexScraper pump/dump signal bot")
    parser.add_argument("--config", required=True, help="Path to config YAML")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    config = load_config(args.config)
    asyncio.run(run_signal_bot(config))


async def stream_signal_bot(
    scraper: "DexScraper",
    config: SignalBotConfig,
    store: TimeSeriesStore,
    detector: PumpDumpDetector,
    notifier: Optional[TelegramNotifier],
    rest_client: Optional[DexScreenerRestClient] = None,
    connect_fn: Optional[Callable[[], Awaitable[object]]] = None,
    max_messages: Optional[int] = None,
) -> None:
    connect = connect_fn or scraper._connect
    retry_count = 0
    max_retries = config.stream.max_retries
    backoff_base = config.stream.backoff_base
    message_limit = config.stream.max_tokens
    while True:
        websocket = await connect()
        if websocket is None:
            retry_count = await _handle_stream_retry(
                retry_count, max_retries, backoff_base, config.stream.poll_interval_s
            )
            if retry_count < 0:
                return
            continue
        logger.info("[stream] connection established")
        retry_count = 0
        messages_seen = 0
        try:
            async for message in websocket:
                # DexScreener WebSocket присылает BINARY frames
                if not isinstance(message, (bytes, bytearray)):
                    logger.debug(
                        "[stream] non-bytes message type=%s", type(message)
                    )
                    continue

                batch = await scraper.extract_tokens_from_message(
                    message,
                    message_limit,
                )

                logger.info(
                    "[stream] tokens_extracted=%d",
                    len(batch.tokens),
                )

                if not batch.tokens:
                    continue

                _log_sample_tokens(batch)

                await process_batch(
                    batch,
                    store,
                    detector,
                    notifier,
                    config.detector.min_points_window,
                    rest_client=rest_client,
                )
                messages_seen += 1
                if max_messages is not None and messages_seen >= max_messages:
                    break

            await websocket.close()
            if max_messages is not None and messages_seen >= max_messages:
                return
        except websockets.exceptions.ConnectionClosed as exc:
            logger.warning(
                "[stream] ws closed code=%s reason=%r",
                getattr(exc, "code", None),
                getattr(exc, "reason", None),
            )
        except Exception as exc:
            logger.exception("[stream] ws loop error: %r", exc)
        finally:
            if websocket:
                await websocket.close()



async def _handle_stream_retry(
    retry_count: int, max_retries: int, backoff_base: float, min_delay_s: float
) -> int:
    retry_count += 1
    if max_retries > 0 and retry_count > max_retries:
        logger.error("[stream] max_retries exceeded, stopping")
        return -1
    delay = max(min_delay_s, backoff_base * (2 ** max(retry_count - 1, 0)))
    logger.info("[stream] reconnecting in %.2fs", delay)
    await asyncio.sleep(delay)
    return retry_count


def _log_sample_tokens(batch: ExtractedTokenBatch, limit: int = 3) -> None:
    for token in batch.tokens[:limit]:
        logger.info(
            "[stream] sample chain=%s dex=%s symbol=%s addr=%s event_id=%s",
            token.chain,
            token.protocol,
            token.symbol,
            token.token_address or token.pair_address,
            token.record_position or token.timestamp,
        )


if __name__ == "__main__":
    main()
