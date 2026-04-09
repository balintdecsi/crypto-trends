from __future__ import annotations

import duckdb


BRONZE_TICKS_TABLE = "bronze_price_ticks"
SILVER_TICKS_TABLE = "silver_price_ticks"
GOLD_LATEST_TABLE = "gold_latest_prices"
GOLD_RETURNS_TABLE = "gold_returns"
GOLD_TRENDS_TABLE = "gold_trends"
GOLD_SUMMARY_TABLE = "gold_market_summary"
ALERTS_DROPS_TABLE = "alerts_price_drops"


def connect_duckdb(path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(path)


def ensure_bronze_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TICKS_TABLE} (
            ingested_at TIMESTAMP NOT NULL,
            provider VARCHAR NOT NULL,
            request_id VARCHAR NOT NULL,
            coin_id VARCHAR NOT NULL,
            symbol VARCHAR,
            currency VARCHAR NOT NULL,
            price DOUBLE NOT NULL,
            asof_ts TIMESTAMP
        )
        """
    )


def ensure_alerts_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ALERTS_DROPS_TABLE} (
            alert_id VARCHAR PRIMARY KEY,
            provider VARCHAR NOT NULL,
            coin_id VARCHAR NOT NULL,
            symbol VARCHAR,
            currency VARCHAR NOT NULL,
            asof_ts TIMESTAMP NOT NULL,
            drop_pct DOUBLE NOT NULL,
            prior_price DOUBLE NOT NULL,
            current_price DOUBLE NOT NULL,
            threshold_pct DOUBLE NOT NULL,
            created_at TIMESTAMP NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'active'
        )
        """
    )

