from __future__ import annotations

import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

from packages.common.config import CollectorConfig, load_collector_config  # noqa: E402
from packages.common.db import BRONZE_TICKS_TABLE, connect_duckdb, ensure_bronze_schema  # noqa: E402
from packages.common.providers.coingecko import CoinGeckoClient  # noqa: E402


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


def run_once(cfg: CollectorConfig) -> int:
    coin_ids = [cid for cid, _sym in cfg.coins]
    symbols = {cid: sym for cid, sym in cfg.coins}

    request_id = str(uuid.uuid4())
    ingested_at = _utcnow()

    if cfg.provider != "coingecko":
        raise ValueError(f"Unsupported PROVIDER={cfg.provider!r}. Only 'coingecko' is implemented.")

    client = CoinGeckoClient(cfg.coingecko_base_url)
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

    _validate_rows(rows)

    con = connect_duckdb(cfg.duckdb_path)
    try:
        ensure_bronze_schema(con)
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
                f"- coins: {len(coin_ids)}",
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
    run_once(cfg)


if __name__ == "__main__":
    main()

