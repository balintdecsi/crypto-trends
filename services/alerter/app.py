from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

from packages.common.config import load_alerter_config  # noqa: E402
from packages.common.db import (  # noqa: E402
    ALERTS_DROPS_TABLE,
    GOLD_RETURNS_TABLE,
    connect_duckdb,
    ensure_alerts_schema,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def run_once() -> None:
    cfg = load_alerter_config()
    thr = cfg.drop_threshold_pct
    if thr <= 0:
        raise ValueError("ALERT_DROP_THRESHOLD_PCT must be positive")

    created_at = _utcnow()
    con = connect_duckdb(cfg.duckdb_path)
    try:
        ensure_alerts_schema(con)
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {GOLD_RETURNS_TABLE}").fetchone()[0]
        except Exception:
            print("alerter: gold_returns missing; run processor first")
            return

        if n == 0:
            print("alerter: gold_returns empty")
            return

        con.execute(
            f"""
            INSERT INTO {ALERTS_DROPS_TABLE} (
                alert_id,
                provider,
                coin_id,
                symbol,
                currency,
                asof_ts,
                drop_pct,
                prior_price,
                current_price,
                threshold_pct,
                created_at,
                status
            )
            WITH latest AS (
                SELECT
                    provider,
                    coin_id,
                    symbol,
                    currency,
                    asof_ts,
                    price AS current_price,
                    prior_price,
                    prior_asof_ts,
                    pct_change_from_prior,
                    md5(concat(
                        provider, '|', coin_id, '|',
                        cast(epoch(asof_ts) AS BIGINT), '|',
                        coalesce(cast(epoch(prior_asof_ts) AS BIGINT), 0)
                    )) AS alert_id
                FROM {GOLD_RETURNS_TABLE}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY provider, coin_id
                    ORDER BY asof_ts DESC
                ) = 1
            )
            SELECT
                l.alert_id,
                l.provider,
                l.coin_id,
                l.symbol,
                l.currency,
                l.asof_ts,
                ABS(l.pct_change_from_prior) AS drop_pct,
                l.prior_price,
                l.current_price,
                CAST(? AS DOUBLE) AS threshold_pct,
                CAST(? AS TIMESTAMP) AS created_at,
                'active' AS status
            FROM latest l
            WHERE l.pct_change_from_prior IS NOT NULL
                AND l.pct_change_from_prior <= -CAST(? AS DOUBLE)
                AND l.prior_price IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM {ALERTS_DROPS_TABLE} a
                    WHERE a.alert_id = l.alert_id
                )
            """,
            [thr, created_at, thr],
        )

        ac = con.execute(f"SELECT COUNT(*) FROM {ALERTS_DROPS_TABLE}").fetchone()[0]
        print(f"alerter complete: alerts_total={ac} threshold={thr}% warehouse={cfg.duckdb_path}")
    finally:
        con.close()


def main() -> None:
    run_once()


if __name__ == "__main__":
    main()
