from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from packages.common.providers.coingecko import CoinGeckoClient
from packages.common.config import CollectorConfig
from services.collector.app import _rows_from_market_chart_24h


class TestCoinGeckoFetchMarketChartPrices(unittest.TestCase):
    @patch("packages.common.providers.coingecko.requests.get")
    def test_fetch_market_chart_prices_parses_non_constant_series(self, mock_get: Mock) -> None:
        mock_resp = Mock()
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {
            "prices": [
                [1712700000000, 70123.12],
                [1712700900000, 70210.55],
                [1712701800000, 70080.01],
            ]
        }
        mock_get.return_value = mock_resp

        client = CoinGeckoClient("https://api.coingecko.com/api/v3")
        points = client.fetch_market_chart_prices(coin_id="bitcoin", vs_currency="usd", days=1)

        self.assertEqual(len(points), 3)
        self.assertEqual(points[0][0], datetime.fromtimestamp(1712700000, tz=timezone.utc))
        self.assertEqual(points[1][0], datetime.fromtimestamp(1712700900, tz=timezone.utc))
        self.assertEqual(points[2][0], datetime.fromtimestamp(1712701800, tz=timezone.utc))
        self.assertEqual(points[0][1], 70123.12)
        self.assertEqual(points[1][1], 70210.55)
        self.assertEqual(points[2][1], 70080.01)
        self.assertNotEqual(len({p for _, p in points}), 1, "Fetched prices should not all be constant")

    @patch("packages.common.providers.coingecko.requests.get")
    def test_fetch_market_chart_prices_filters_invalid_points(self, mock_get: Mock) -> None:
        mock_resp = Mock()
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {
            "prices": [
                [1712700000000, 10.0],
                [1712700900000, -5.0],   # filtered (negative)
                ["bad", 12.0],           # filtered (bad timestamp)
                [1712701800000, "bad"],  # filtered (bad price)
            ]
        }
        mock_get.return_value = mock_resp

        client = CoinGeckoClient("https://api.coingecko.com/api/v3")
        points = client.fetch_market_chart_prices(coin_id="bitcoin", vs_currency="usd", days=1)

        self.assertEqual(points, [(datetime.fromtimestamp(1712700000, tz=timezone.utc), 10.0)])


class TestCoinGeckoLiveFetch(unittest.TestCase):
    @unittest.skipUnless(
        os.getenv("RUN_LIVE_FETCH_TEST", "").strip().lower() in {"1", "true", "yes"},
        "Set RUN_LIVE_FETCH_TEST=1 to run live API fetch and save output.",
    )
    def test_live_fetch_prints_and_saves_data(self) -> None:
        client = CoinGeckoClient("https://api.coingecko.com/api/v3")
        points = client.fetch_market_chart_prices(coin_id="bitcoin", vs_currency="usd", days=1)

        self.assertGreater(len(points), 0, "Live fetch returned no points.")

        # Print the actual fetched points to terminal output.
        print(f"Fetched {len(points)} points from CoinGecko.")
        for ts, price in points[:10]:
            print(f"{ts.isoformat()}  {price}")
        if len(points) > 10:
            print(f"... ({len(points) - 10} more rows)")

        # Save complete fetched data for inspection.
        out_dir = Path("tests/artifacts")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "coingecko_bitcoin_24h.csv"
        df = pd.DataFrame(points, columns=["asof_ts", "price"])
        df.to_csv(out_path, index=False)
        print(f"Saved fetched data to: {out_path}")


class TestCollectorBackfillFetch(unittest.TestCase):
    @unittest.skipUnless(
        os.getenv("RUN_LIVE_FETCH_TEST", "").strip().lower() in {"1", "true", "yes"},
        "Set RUN_LIVE_FETCH_TEST=1 to run live collector backfill fetch and save output.",
    )
    def test_collector_market_chart_backfill_24h_15m(self) -> None:
        cfg = CollectorConfig(
            provider="coingecko",
            coingecko_base_url="https://api.coingecko.com/api/v3",
            coins=[("bitcoin", "btc")],
            vs_currency="usd",
            duckdb_path="/tmp/not-used-for-this-test.duckdb",
            interval_seconds=0,
            initial_backfill_days=1,
            initial_backfill_interval_minutes=15,
            skip_initial_backfill=False,
        )
        client = CoinGeckoClient(cfg.coingecko_base_url)
        ingested_at = datetime.now(timezone.utc).replace(microsecond=0)

        rows = _rows_from_market_chart_24h(
            cfg,
            client,
            request_id="test-live-backfill",
            ingested_at=ingested_at,
            symbols={"bitcoin": "btc"},
        )

        self.assertGreater(len(rows), 40, "Expected many 15-minute rows over 24h.")
        self.assertTrue(all(r.coin_id == "bitcoin" for r in rows))
        self.assertTrue(all(r.asof_ts is not None for r in rows))

        ts = [r.asof_ts for r in rows if r.asof_ts is not None]
        prices = [r.price for r in rows]
        self.assertEqual(ts, sorted(ts), "Timestamps should be sorted.")
        self.assertNotEqual(len(set(prices)), 1, "Backfill prices should not be constant.")

        # Check 15-minute bucketing and ~24h window.
        for t in ts:
            self.assertEqual(t.minute % 15, 0, f"Timestamp not aligned to 15m bucket: {t.isoformat()}")
            self.assertEqual(t.second, 0, f"Timestamp has non-zero seconds: {t.isoformat()}")
        self.assertLessEqual(ts[-1] - ts[0], timedelta(hours=25))
        self.assertGreaterEqual(ts[-1] - ts[0], timedelta(hours=20))

        # Print sample + save full rows for manual inspection.
        print(f"Collector backfill produced {len(rows)} rows.")
        for r in rows[:10]:
            assert r.asof_ts is not None
            print(f"{r.asof_ts.isoformat()}  {r.price}")
        if len(rows) > 10:
            print(f"... ({len(rows) - 10} more rows)")

        out_dir = Path("tests/artifacts")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "collector_bitcoin_24h_15m.csv"
        pd.DataFrame(
            [{"asof_ts": r.asof_ts, "price": r.price, "coin_id": r.coin_id} for r in rows]
        ).to_csv(out_path, index=False)
        print(f"Saved collector backfill data to: {out_path}")


if __name__ == "__main__":
    unittest.main()
