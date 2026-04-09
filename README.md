# crypto-trends
A minimal application that follows crypto prices regularly and monitors sudden changes.

## What’s implemented (collector slice)
This repo currently implements the **Collector** part of the planned architecture (`ARCHITECTURE.md`):
- Fetches **batch** prices for a configured list of coins from a **public API** (CoinGecko).
- Stores each batch into a **DuckDB warehouse file** (not SQLite) as an append-only **bronze** table: `bronze_price_ticks`.
- Runs locally via **Docker**.

## Data source
- **Provider**: CoinGecko API (no API key required for basic usage).
- **Endpoint used**: `/simple/price` with `include_last_updated_at=true`.

## DuckDB warehouse
- **File**: `data/warehouse.duckdb` (created on first run)
- **Table**: `bronze_price_ticks`
- **Schema**:
  - `ingested_at` (TIMESTAMP, not null)
  - `provider` (VARCHAR, not null)
  - `request_id` (VARCHAR, not null) — UUID per batch run
  - `coin_id` (VARCHAR, not null)
  - `symbol` (VARCHAR, nullable)
  - `currency` (VARCHAR, not null)
  - `price` (DOUBLE, not null)
  - `asof_ts` (TIMESTAMP, nullable) — provider timestamp if available, else `ingested_at`

### Ingestion-time validations
- `coin_id` must be present
- `price` must be numeric and \( \ge 0 \)

## Configuration
Set these environment variables (Docker examples below):
- `COINS`: comma-separated list of coin IDs, optionally with symbol mapping.
  - Example: `bitcoin:btc,ethereum:eth,solana:sol`
  - Example: `bitcoin,ethereum` (symbol defaults to id)
- `VS_CURRENCY`: quote currency, default `usd`
- `DUCKDB_PATH`: DuckDB file path, default `/data/warehouse.duckdb` in the container
- `COINGECKO_BASE_URL`: default `https://api.coingecko.com/api/v3`

## Run (Docker Compose)
From the `crypto-trends/` directory:

```bash
mkdir -p data
docker compose up --build
```

You should see a summary like:
- rows appended
- DuckDB path
- total rows in `bronze_price_ticks`

### If Docker isn’t installed (or you’re on WSL without Docker integration)
You can run the collector locally using `uv`:

```bash
mkdir -p data
uv venv
. .venv/bin/activate
uv pip install -r requirements.txt
COINS="bitcoin:btc,ethereum:eth,solana:sol" DUCKDB_PATH="./data/warehouse.duckdb" python services/collector/app.py
```

## Inspect the warehouse
Option A: DuckDB CLI (if installed locally):

```bash
duckdb data/warehouse.duckdb "select * from bronze_price_ticks order by ingested_at desc limit 10;"
```

Option B: Python one-liner:

```bash
python -c "import duckdb; con=duckdb.connect('data/warehouse.duckdb'); print(con.execute('select count(*) from bronze_price_ticks').fetchone()[0])"
```

## Decisions / rationale
- **DuckDB**: satisfies “not SQLite”, is file-based, fast, and easy to ship in Docker; aligns with your `ceu/ds-2` patterns.
- **Bronze table first**: preserves raw-ish ingested data and makes later silver/gold processing reproducible.
- **CoinGecko**: simplest public crypto price API for demos (no key); kept behind a small client so it can be swapped later.

