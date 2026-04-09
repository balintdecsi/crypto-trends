from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable


def _parse_coins(value: str) -> list[tuple[str, str]]:
    """
    Parse coins from env:
      COINS="bitcoin:btc,ethereum:eth,solana:sol"
      COINS="bitcoin,ethereum"  # symbol defaults to id
    """
    raw = [part.strip() for part in value.split(",") if part.strip()]
    coins: list[tuple[str, str]] = []
    for item in raw:
        if ":" in item:
            coin_id, symbol = item.split(":", 1)
            coin_id = coin_id.strip()
            symbol = symbol.strip()
        else:
            coin_id = item.strip()
            symbol = coin_id
        if not coin_id:
            continue
        if not symbol:
            symbol = coin_id
        coins.append((coin_id, symbol))
    return coins


@dataclass(frozen=True)
class CollectorConfig:
    provider: str
    coingecko_base_url: str
    coins: list[tuple[str, str]]
    vs_currency: str
    duckdb_path: str


def load_collector_config(environ: dict[str, str] | None = None) -> CollectorConfig:
    env = environ or os.environ

    provider = env.get("PROVIDER", "coingecko").strip() or "coingecko"
    base_url = env.get("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3").strip()
    vs_currency = env.get("VS_CURRENCY", "usd").strip() or "usd"
    duckdb_path = env.get("DUCKDB_PATH", "/data/warehouse.duckdb").strip() or "/data/warehouse.duckdb"

    coins_env = env.get("COINS", "bitcoin:btc,ethereum:eth")
    coins = _parse_coins(coins_env)

    if not coins:
        raise ValueError("COINS is empty. Example: COINS='bitcoin:btc,ethereum:eth'")

    return CollectorConfig(
        provider=provider,
        coingecko_base_url=base_url,
        coins=coins,
        vs_currency=vs_currency,
        duckdb_path=duckdb_path,
    )

