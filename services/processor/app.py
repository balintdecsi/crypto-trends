from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

from packages.common.config import load_warehouse_config  # noqa: E402
from packages.common.db import (  # noqa: E402
    BRONZE_TICKS_TABLE,
    GOLD_LATEST_TABLE,
    GOLD_RETURNS_TABLE,
    GOLD_SUMMARY_TABLE,
    GOLD_TRENDS_TABLE,
    SILVER_TICKS_TABLE,
    connect_duckdb,
    ensure_bronze_schema,
)

SILVER_SQL = f"""
CREATE OR REPLACE TABLE {SILVER_TICKS_TABLE} AS
WITH ranked AS (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY b.provider, b.coin_id, b.asof_ts
            ORDER BY b.ingested_at DESC
        ) AS rn
    FROM {BRONZE_TICKS_TABLE} b
)
SELECT
    provider,
    coin_id,
    symbol,
    currency,
    asof_ts,
    price
FROM ranked
WHERE rn = 1
    AND coin_id IS NOT NULL
    AND price >= 0
"""

GOLD_LATEST_SQL = f"""
CREATE OR REPLACE TABLE {GOLD_LATEST_TABLE} AS
SELECT provider, coin_id, symbol, currency, price, asof_ts
FROM (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.provider, s.coin_id
            ORDER BY s.asof_ts DESC
        ) AS rn
    FROM {SILVER_TICKS_TABLE} s
) t
WHERE rn = 1
"""

GOLD_RETURNS_SQL = f"""
CREATE OR REPLACE TABLE {GOLD_RETURNS_TABLE} AS
WITH ordered AS (
    SELECT
        provider,
        coin_id,
        symbol,
        currency,
        asof_ts,
        price,
        LAG(price) OVER (
            PARTITION BY provider, coin_id
            ORDER BY asof_ts
        ) AS prior_price,
        LAG(asof_ts) OVER (
            PARTITION BY provider, coin_id
            ORDER BY asof_ts
        ) AS prior_asof_ts
    FROM {SILVER_TICKS_TABLE}
)
SELECT
    provider,
    coin_id,
    symbol,
    currency,
    asof_ts,
    price,
    prior_price,
    prior_asof_ts,
    CASE
        WHEN prior_price IS NOT NULL AND prior_price > 0
        THEN (price - prior_price) / prior_price * 100.0
        ELSE NULL
    END AS pct_change_from_prior
FROM ordered
"""

GOLD_TRENDS_SQL = f"""
CREATE OR REPLACE TABLE {GOLD_TRENDS_TABLE} AS
SELECT
    provider,
    coin_id,
    symbol,
    asof_ts,
    price,
    AVG(price) OVER (
        PARTITION BY provider, coin_id
        ORDER BY asof_ts
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma7
FROM {SILVER_TICKS_TABLE}
"""

GOLD_SUMMARY_SQL = f"""
CREATE OR REPLACE TABLE {GOLD_SUMMARY_TABLE} AS
SELECT *
FROM {GOLD_RETURNS_TABLE}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY provider, coin_id
    ORDER BY asof_ts DESC
) = 1
"""


def _validate_silver(con) -> None:
    n = con.execute(f"SELECT COUNT(*) FROM {SILVER_TICKS_TABLE}").fetchone()[0]
    if n == 0:
        return
    d = con.execute(
        f"""
        SELECT COUNT(DISTINCT (provider || '|' || coin_id || '|' || CAST(asof_ts AS VARCHAR)))
        FROM {SILVER_TICKS_TABLE}
        """
    ).fetchone()[0]
    assert int(n) == int(d), f"silver_price_ticks: duplicate (provider,coin_id,asof_ts): rows={n} distinct={d}"


def run_once() -> None:
    cfg = load_warehouse_config()
    con = connect_duckdb(cfg.duckdb_path)
    try:
        ensure_bronze_schema(con)
        bronze_n = con.execute(f"SELECT COUNT(*) FROM {BRONZE_TICKS_TABLE}").fetchone()[0]
        if bronze_n == 0:
            print("processor: bronze empty; skipping transforms")
            return

        con.execute(SILVER_SQL)
        _validate_silver(con)

        con.execute(GOLD_LATEST_SQL)
        con.execute(GOLD_RETURNS_SQL)
        con.execute(GOLD_TRENDS_SQL)
        con.execute(GOLD_SUMMARY_SQL)

        sn = con.execute(f"SELECT COUNT(*) FROM {SILVER_TICKS_TABLE}").fetchone()[0]
        gn = con.execute(f"SELECT COUNT(*) FROM {GOLD_LATEST_TABLE}").fetchone()[0]
        print(f"processor complete: silver_rows={sn} gold_latest_rows={gn} warehouse={cfg.duckdb_path}")
    finally:
        con.close()


def main() -> None:
    run_once()


if __name__ == "__main__":
    main()
