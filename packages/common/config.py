from __future__ import annotations

import os
from dataclasses import dataclass


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
    interval_seconds: int
    initial_backfill_days: int
    # Bucket size (minutes) for downsampling initial market_chart (15 → 15-minute candles).
    initial_backfill_interval_minutes: int
    skip_initial_backfill: bool


@dataclass(frozen=True)
class WarehouseConfig:
    duckdb_path: str


@dataclass(frozen=True)
class AlerterConfig:
    duckdb_path: str
    drop_threshold_pct: float


def load_warehouse_config(environ: dict[str, str] | None = None) -> WarehouseConfig:
    env = environ or os.environ
    path = env.get("DUCKDB_PATH", "/data/warehouse.duckdb").strip() or "/data/warehouse.duckdb"
    return WarehouseConfig(duckdb_path=path)


def load_alerter_config(environ: dict[str, str] | None = None) -> AlerterConfig:
    env = environ or os.environ
    path = env.get("DUCKDB_PATH", "/data/warehouse.duckdb").strip() or "/data/warehouse.duckdb"
    raw = env.get("ALERT_DROP_THRESHOLD_PCT", "5.0").strip() or "5.0"
    try:
        thr = float(raw)
    except ValueError:
        thr = 5.0
    if thr <= 0:
        thr = 5.0
    return AlerterConfig(duckdb_path=path, drop_threshold_pct=thr)


def load_collector_config(environ: dict[str, str] | None = None) -> CollectorConfig:
    env = environ or os.environ

    provider = env.get("PROVIDER", "coingecko").strip() or "coingecko"
    base_url = env.get("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3").strip()
    vs_currency = env.get("VS_CURRENCY", "usd").strip() or "usd"
    duckdb_path = env.get("DUCKDB_PATH", "/data/warehouse.duckdb").strip() or "/data/warehouse.duckdb"

    coins_env = env.get("COINS", "bitcoin:btc,ethereum:eth,solana:sol,cardano:ada")
    coins = _parse_coins(coins_env)

    if not coins:
        raise ValueError("COINS is empty. Example: COINS='bitcoin:btc,ethereum:eth'")

    raw_interval = env.get("COLLECTOR_INTERVAL_SECONDS", "0").strip() or "0"
    try:
        interval_seconds = int(raw_interval)
    except ValueError:
        interval_seconds = 0

    raw_days = env.get("INITIAL_BACKFILL_DAYS", "1").strip() or "1"
    try:
        initial_backfill_days = int(raw_days)
    except ValueError:
        initial_backfill_days = 1
    if initial_backfill_days < 1:
        initial_backfill_days = 1

    skip_initial_backfill = env.get("SKIP_INITIAL_BACKFILL", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )

    raw_im = env.get("INITIAL_BACKFILL_INTERVAL_MINUTES", "15").strip() or "15"
    try:
        initial_backfill_interval_minutes = int(raw_im)
    except ValueError:
        initial_backfill_interval_minutes = 15
    if initial_backfill_interval_minutes < 1:
        initial_backfill_interval_minutes = 15

    return CollectorConfig(
        provider=provider,
        coingecko_base_url=base_url,
        coins=coins,
        vs_currency=vs_currency,
        duckdb_path=duckdb_path,
        interval_seconds=interval_seconds,
        initial_backfill_days=initial_backfill_days,
        initial_backfill_interval_minutes=initial_backfill_interval_minutes,
        skip_initial_backfill=skip_initial_backfill,
    )

