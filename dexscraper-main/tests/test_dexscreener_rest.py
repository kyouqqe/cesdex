import time

from dexscraper.enrich.dexscreener_rest import (
    age_days_from_created_at,
    parse_dexscreener_response,
)


def test_age_days_from_created_at_seconds_and_ms() -> None:
    now = int(time.time())
    one_day_ago = now - 86400
    assert age_days_from_created_at(one_day_ago, now_ts=now) == 1.0
    assert age_days_from_created_at(one_day_ago * 1000, now_ts=now) == 1.0


def test_parse_dexscreener_response_selects_pair() -> None:
    payload = {
        "pairs": [
            {
                "pairAddress": "pair1",
                "priceUsd": "1.23",
                "liquidity": {"usd": "1000"},
                "fdv": "5000",
                "marketCap": "6000",
                "priceChange": {"m5": "1", "h1": "-2", "h24": "10"},
                "pairCreatedAt": 1700000000000,
                "chainId": "solana",
                "dexId": "pumpfun",
            },
            {
                "pairAddress": "pair2",
                "priceUsd": "2.34",
                "liquidity": {"usd": "5000"},
                "fdv": "7000",
                "marketCap": "8000",
                "priceChange": {"m5": "2", "h1": "-3", "h24": "20"},
                "pairCreatedAt": 1700000000000,
                "chainId": "solana",
                "dexId": "raydium",
            },
        ]
    }

    parsed = parse_dexscreener_response(payload, "token", pair_address="pair1")
    assert parsed is not None
    assert parsed.price_usd == 1.23
    assert parsed.liquidity_usd == 1000.0
    assert parsed.market_cap == 6000.0
    assert parsed.fdv == 5000.0
    assert parsed.change_m5 == 1.0
    assert parsed.change_h1 == -2.0
    assert parsed.change_h24 == 10.0
    assert parsed.pair_address == "pair1"
    assert parsed.chain == "solana"
    assert parsed.dex_id == "pumpfun"


def test_parse_dexscreener_response_falls_back_to_liquidity() -> None:
    payload = {
        "pairs": [
            {"pairAddress": "pair1", "liquidity": {"usd": "100"}},
            {"pairAddress": "pair2", "liquidity": {"usd": "500"}},
        ]
    }

    parsed = parse_dexscreener_response(payload, "token")
    assert parsed is not None
    assert parsed.pair_address == "pair2"
