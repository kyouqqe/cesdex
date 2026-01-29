"""DexScreener WebSocket scraper package for real-time cryptocurrency data."""

__version__ = "0.1.0"
__author__ = "Vincent Koc"

from .config import (
    Chain,
    DEX,
    Filters,
    Order,
    PresetConfigs,
    RankBy,
    ScrapingConfig,
    Timeframe,
)
from .models import (
    ExtractedTokenBatch,
    LiquidityData,
    OHLCData,
    PriceData,
    TokenProfile,
    TradingPair,
    VolumeData,
)
from .scraper import DexScraper

__all__ = [
    "DexScraper",
    "TradingPair",
    "PriceData",
    "LiquidityData",
    "VolumeData",
    "OHLCData",
    "TokenProfile",
    "ExtractedTokenBatch",
    "ScrapingConfig",
    "PresetConfigs",
    "Chain",
    "Timeframe",
    "RankBy",
    "Order",
    "DEX",
    "Filters",
]


def __getattr__(name: str):
    if name == "DexScraper":
        from .scraper import DexScraper

        return DexScraper
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
