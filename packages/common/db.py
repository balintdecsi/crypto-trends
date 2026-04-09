from __future__ import annotations

import duckdb


BRONZE_TICKS_TABLE = "bronze_price_ticks"


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

