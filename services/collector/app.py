from __future__ import annotations

import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

from packages.common.config import CollectorConfig, load_collector_config  # noqa: E402
from packages.common.db import BRONZE_TICKS_TABLE, connect_duckdb, ensure_bronze_schema  # noqa: E402
from packages.common.providers.coingecko import CoinGeckoClient  # noqa: E402

# CoinGecko demo tier: avoid burst limits when requesting one chart per coin.
_INTER_COIN_DELAY_S = 1.2


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # region agent log
    try:
        log_path = "/home/balintdecsi/repos/.cursor/debug-babcd5.log"
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "sessionId": "babcd5",
            "runId": "collector-pre-fix",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "id": f"log_{uuid.uuid4().hex[:12]}",
        }
        line = json.dumps(payload, separators=(",", ":")) + "\n"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line)
        # Also write to mounted data path for dockerized runs.
        for extra_path in ("/data/debug-babcd5.log", "data/debug-babcd5.log"):
            try:
                Path(extra_path).parent.mkdir(parents=True, exist_ok=True)
                with open(extra_path, "a", encoding="utf-8") as ef:
                    ef.write(line)
            except Exception:
                pass
    except Exception:
        pass
    # endregion


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


def _needs_recent_history_backfill(con, cfg: CollectorConfig, now_utc: datetime) -> bool:
    """
    Decide whether we should seed recent history even when bronze isn't empty.

    If there are no rows in the last 24 hours for configured coins, or if each coin has too few
    distinct timestamps, charts appear flat/constant. In that case, run market_chart backfill.
    """
    coin_ids = [cid for cid, _ in cfg.coins]
    if not coin_ids:
        return False
    lookback_start = now_utc - timedelta(hours=24)
    placeholders = ",".join(["?"] * len(coin_ids))
    stats = con.execute(
        f"""
        SELECT
            coin_id,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT asof_ts) AS n_distinct_asof
        FROM {BRONZE_TICKS_TABLE}
        WHERE coin_id IN ({placeholders})
          AND asof_ts IS NOT NULL
          AND asof_ts >= ?
        GROUP BY coin_id
        """,
        [*coin_ids, lookback_start],
    ).fetchall()
    if not stats:
        return True
    by_coin = {row[0]: int(row[2]) for row in stats}
    # 24h @ 15m gives ~96 points; require at least a few points to avoid flat lines.
    min_points_for_ok_chart = 4
    for cid in coin_ids:
        if by_coin.get(cid, 0) < min_points_for_ok_chart:
            return True
    return False


def run_once(cfg: CollectorConfig) -> int:
    symbols = {cid: sym for cid, sym in cfg.coins}

    request_id = str(uuid.uuid4())
    ingested_at = _utcnow()

    if cfg.provider != "coingecko":
        raise ValueError(f"Unsupported PROVIDER={cfg.provider!r}. Only 'coingecko' is implemented.")

    # region agent log
    _debug_log(
        "H5",
        "services/collector/app.py:run_once",
        "collector_config",
        {
            "coins": [cid for cid, _ in cfg.coins],
            "vs_currency": cfg.vs_currency,
            "initial_backfill_days": int(cfg.initial_backfill_days),
            "initial_backfill_interval_minutes": int(cfg.initial_backfill_interval_minutes),
            "skip_initial_backfill": bool(cfg.skip_initial_backfill),
        },
    )
    # endregion

    client = CoinGeckoClient(cfg.coingecko_base_url)

    con = connect_duckdb(cfg.duckdb_path)
    try:
        ensure_bronze_schema(con)
        bronze_count = con.execute(f"SELECT COUNT(*) FROM {BRONZE_TICKS_TABLE}").fetchone()[0]
        needs_history_backfill = _needs_recent_history_backfill(con, cfg, ingested_at)
        use_initial_backfill = (
            int(bronze_count) == 0 or needs_history_backfill
        ) and not cfg.skip_initial_backfill
        # region agent log
        _debug_log(
            "H6",
            "services/collector/app.py:run_once",
            "load_mode_decision",
            {
                "bronze_count": int(bronze_count),
                "needs_history_backfill": bool(needs_history_backfill),
                "use_initial_backfill": bool(use_initial_backfill),
            },
        )
        # endregion

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
        # region agent log
        per_coin_counts: dict[str, int] = {}
        for r in rows:
            per_coin_counts[r.coin_id] = per_coin_counts.get(r.coin_id, 0) + 1
        _debug_log(
            "H7",
            "services/collector/app.py:run_once",
            "rows_per_coin_before_insert",
            {"rows_total": int(len(rows)), "rows_per_coin": per_coin_counts},
        )
        # endregion

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
