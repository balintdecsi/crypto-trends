from __future__ import annotations

import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

from packages.common.config import CollectorConfig, load_collector_config  # noqa: E402
from packages.common.db import BRONZE_TICKS_TABLE, connect_duckdb, ensure_bronze_schema  # noqa: E402
from packages.common.providers.coingecko import CoinGeckoClient  # noqa: E402

# CoinGecko demo tier: avoid burst limits when requesting one chart per coin.
_INTER_COIN_DELAY_S = 1.2


@dataclass(frozen=True)
class BronzeTickRow:
    ingested_at: datetime
    provider: str
    request_id: str
    coin_id: str
    symbol: str | None
    currency: str
    price: float
    asof_ts: datetime | None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _to_utc_ts(unix_s: int | None) -> datetime | None:
    if unix_s is None:
        return None
    return datetime.fromtimestamp(unix_s, tz=timezone.utc).replace(microsecond=0)


def _validate_rows(rows: list[BronzeTickRow]) -> None:
    if not rows:
        raise RuntimeError("No rows to write (API returned empty batch).")
    for r in rows:
        if not r.coin_id:
            raise ValueError("Validation failed: coin_id is empty.")
        if r.price is None:
            raise ValueError(f"Validation failed: price is null for {r.coin_id}.")
        if r.price < 0:
            raise ValueError(f"Validation failed: negative price for {r.coin_id}: {r.price}")


def _rows_from_simple_prices(
    cfg: CollectorConfig,
    client: CoinGeckoClient,
    *,
    request_id: str,
    ingested_at: datetime,
    symbols: dict[str, str],
) -> list[BronzeTickRow]:
    coin_ids = [cid for cid, _sym in cfg.coins]
    quotes = client.fetch_simple_prices(coin_ids=coin_ids, vs_currency=cfg.vs_currency)
    rows: list[BronzeTickRow] = []
    for q in quotes:
        rows.append(
            BronzeTickRow(
                ingested_at=ingested_at,
                provider=cfg.provider,
                request_id=request_id,
                coin_id=q.coin_id,
                symbol=symbols.get(q.coin_id),
                currency=q.vs_currency,
                price=q.price,
                asof_ts=_to_utc_ts(q.last_updated_at) or ingested_at,
            )
        )
    return rows


def _resample_prices_to_interval(
    points: list[tuple[datetime, float]],
    interval_minutes: int,
) -> list[tuple[datetime, float]]:
    """Take last price in each fixed-length UTC bucket (e.g. 15-minute OHLC-style close)."""
    if interval_minutes <= 0 or len(points) == 0:
        return points
    rule = f"{interval_minutes}min"
    df = pd.DataFrame(points, columns=["asof_ts", "price"])
    df["asof_ts"] = pd.to_datetime(df["asof_ts"], utc=True)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["asof_ts", "price"])
    if df.empty:
        return []
    df = df.sort_values("asof_ts").set_index("asof_ts")
    df = df.resample(rule, label="right", closed="right").last()
    df = df.dropna(subset=["price"])
    out: list[tuple[datetime, float]] = []
    for idx, row in df.iterrows():
        if pd.isna(row["price"]):
            continue
        ts = idx.to_pydatetime()  # type: ignore[union-attr]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc).replace(microsecond=0)
        out.append((ts, float(row["price"])))
    return out


def _rows_from_market_chart_24h(
    cfg: CollectorConfig,
    client: CoinGeckoClient,
    *,
    request_id: str,
    ingested_at: datetime,
    symbols: dict[str, str],
) -> list[BronzeTickRow]:
    """One CoinGecko call per coin: `/coins/{id}/market_chart`; downsample to N-minute buckets."""
    rows: list[BronzeTickRow] = []
    for i, (coin_id, _sym) in enumerate(cfg.coins):
        if i > 0:
            time.sleep(_INTER_COIN_DELAY_S)
        points = client.fetch_market_chart_prices(
            coin_id=coin_id,
            vs_currency=cfg.vs_currency,
            days=cfg.initial_backfill_days,
        )
        points = _resample_prices_to_interval(points, cfg.initial_backfill_interval_minutes)
        for asof_ts, price in points:
            rows.append(
                BronzeTickRow(
                    ingested_at=ingested_at,
                    provider=cfg.provider,
                    request_id=request_id,
                    coin_id=coin_id,
                    symbol=symbols.get(coin_id),
                    currency=cfg.vs_currency,
                    price=price,
                    asof_ts=asof_ts,
                )
            )
    return rows


def run_once(cfg: CollectorConfig) -> int:
    symbols = {cid: sym for cid, sym in cfg.coins}

    request_id = str(uuid.uuid4())
    ingested_at = _utcnow()

    if cfg.provider != "coingecko":
        raise ValueError(f"Unsupported PROVIDER={cfg.provider!r}. Only 'coingecko' is implemented.")

    client = CoinGeckoClient(cfg.coingecko_base_url)

    con = connect_duckdb(cfg.duckdb_path)
    try:
        ensure_bronze_schema(con)
        bronze_count = con.execute(f"SELECT COUNT(*) FROM {BRONZE_TICKS_TABLE}").fetchone()[0]
        use_initial_backfill = (
            int(bronze_count) == 0
            and not cfg.skip_initial_backfill
        )

        if use_initial_backfill:
            rows = _rows_from_market_chart_24h(
                cfg, client, request_id=request_id, ingested_at=ingested_at, symbols=symbols
            )
            load_mode = (
                f"initial_market_chart_days={cfg.initial_backfill_days} "
                f"interval={cfg.initial_backfill_interval_minutes}min"
            )
        else:
            rows = _rows_from_simple_prices(
                cfg, client, request_id=request_id, ingested_at=ingested_at, symbols=symbols
            )
            load_mode = "simple_price_snapshot"

        _validate_rows(rows)

        con.executemany(
            f"""
            INSERT INTO {BRONZE_TICKS_TABLE} (
                ingested_at,
                provider,
                request_id,
                coin_id,
                symbol,
                currency,
                price,
                asof_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r.ingested_at,
                    r.provider,
                    r.request_id,
                    r.coin_id,
                    r.symbol,
                    r.currency,
                    r.price,
                    r.asof_ts,
                )
                for r in rows
            ],
        )

        total = con.execute(f"SELECT COUNT(*) FROM {BRONZE_TICKS_TABLE}").fetchone()[0]
    finally:
        con.close()

    print(
        "\n".join(
            [
                "collector run complete",
                f"- provider: {cfg.provider}",
                f"- load_mode: {load_mode}",
                f"- rows_appended: {len(rows)}",
                f"- warehouse: {cfg.duckdb_path}",
                f"- bronze_table: {BRONZE_TICKS_TABLE} (total_rows={total})",
                f"- request_id: {request_id}",
                f"- ingested_at: {ingested_at.isoformat()}",
            ]
        )
    )
    return len(rows)


def main() -> None:
    cfg = load_collector_config()
    while True:
        run_once(cfg)
        if cfg.interval_seconds <= 0:
            break
        time.sleep(cfg.interval_seconds)


if __name__ == "__main__":
    main()
