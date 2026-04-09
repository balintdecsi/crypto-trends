from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

from packages.common.config import load_collector_config, load_warehouse_config  # noqa: E402
from packages.common.db import (  # noqa: E402
    ALERTS_DROPS_TABLE,
    BRONZE_TICKS_TABLE,
    GOLD_LATEST_TABLE,
    GOLD_RETURNS_TABLE,
    GOLD_TRENDS_TABLE,
    SILVER_TICKS_TABLE,
)
from packages.common.providers.coingecko import CoinGeckoClient  # noqa: E402


def _warehouse_path() -> str:
    return load_warehouse_config().duckdb_path


@st.cache_data(ttl=30)
def _read_sql(path: str, sql: str) -> pd.DataFrame | None:
    if not Path(path).exists():
        return None
    try:
        con = duckdb.connect(path, read_only=True)
        try:
            return con.execute(sql).df()
        finally:
            con.close()
    except Exception:
        return None


def _load_price_history_for_chart(path: str) -> pd.DataFrame | None:
    """Prefer silver; if empty/missing, use bronze (deduped) so charts work before processor runs."""
    silver = _read_sql(
        path,
        f"SELECT coin_id, asof_ts, price FROM {SILVER_TICKS_TABLE} ORDER BY asof_ts, coin_id",
    )
    if silver is not None and not silver.empty:
        return silver
    return _read_sql(
        path,
        f"""
        WITH ranked AS (
            SELECT
                coin_id,
                asof_ts,
                price,
                ROW_NUMBER() OVER (
                    PARTITION BY provider, coin_id, asof_ts
                    ORDER BY ingested_at DESC
                ) AS rn
            FROM {BRONZE_TICKS_TABLE}
        )
        SELECT coin_id, asof_ts, price
        FROM ranked
        WHERE rn = 1
        ORDER BY asof_ts, coin_id
        """,
    )


def _prepare_line_chart_wide(df: pd.DataFrame) -> pd.DataFrame:
    """
    Streamlit line_chart is picky about dtypes and timezone-aware datetimes; normalize so y-values
    are plain floats and the time index is UTC-na (wall-clock) for the chart API.
    """
    out = df.copy()
    out["asof_ts"] = pd.to_datetime(out["asof_ts"], utc=True)
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out.dropna(subset=["asof_ts", "price"])
    if out.empty:
        return pd.DataFrame()
    wide = out.pivot_table(
        index="asof_ts", columns="coin_id", values="price", aggfunc="last"
    ).sort_index()
    wide = wide.astype("float64")
    # Naive UTC datetime index avoids empty/flat charts with some Streamlit versions.
    idx = pd.DatetimeIndex(wide.index)
    if getattr(idx, "tz", None) is not None:
        wide.index = idx.tz_convert("UTC").tz_localize(None)
    wide.index.name = "asof_ts_utc"
    wide = wide.sort_index()
    return wide


def _live_snapshot_df() -> pd.DataFrame | None:
    try:
        cfg = load_collector_config()
    except Exception:
        return None
    if cfg.provider != "coingecko":
        return None
    coin_ids = [c for c, _ in cfg.coins]
    try:
        quotes = CoinGeckoClient(cfg.coingecko_base_url).fetch_simple_prices(
            coin_ids=coin_ids, vs_currency=cfg.vs_currency
        )
    except Exception:
        return None
    rows = []
    symbols = {cid: sym for cid, sym in cfg.coins}
    for q in quotes:
        rows.append(
            {
                "coin_id": q.coin_id,
                "symbol": symbols.get(q.coin_id, q.coin_id),
                "currency": q.vs_currency,
                "price": q.price,
                "source": "live_api",
            }
        )
    return pd.DataFrame(rows) if rows else None


def main() -> None:
    st.set_page_config(page_title="Crypto Trends", layout="wide")
    path = _warehouse_path()

    st.title("Crypto Trends")
    st.caption("Batch prices → DuckDB warehouse → charts, trends, and drop alerts.")

    tab_charts, tab_latest, tab_alerts, tab_arch = st.tabs(
        ["Price charts", "Latest & trends", "Alerts", "Architecture"]
    )

    silver = _read_sql(path, f"SELECT * FROM {SILVER_TICKS_TABLE} ORDER BY asof_ts, coin_id")
    chart_df = _load_price_history_for_chart(path)
    latest = _read_sql(path, f"SELECT * FROM {GOLD_LATEST_TABLE} ORDER BY coin_id")
    trends = _read_sql(
        path,
        f"""
        SELECT * FROM {GOLD_TRENDS_TABLE}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY provider, coin_id ORDER BY asof_ts DESC) <= 50
        ORDER BY coin_id, asof_ts
        """,
    )
    returns_snap = _read_sql(
        path,
        f"""
        SELECT * FROM {GOLD_RETURNS_TABLE}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY provider, coin_id ORDER BY asof_ts DESC) = 1
        ORDER BY coin_id
        """,
    )
    alerts = _read_sql(path, f"SELECT * FROM {ALERTS_DROPS_TABLE} ORDER BY created_at DESC LIMIT 100")

    with tab_charts:
        st.subheader("Price history (USD)")
        if chart_df is not None and not chart_df.empty:
            coins = sorted(chart_df["coin_id"].dropna().unique().tolist())
            pick = st.multiselect("Coins", coins, default=coins[: min(5, len(coins))])
            sub = chart_df[chart_df["coin_id"].isin(pick)] if pick else chart_df
            wide = _prepare_line_chart_wide(sub)
            if wide.empty:
                st.warning("Price series empty after cleaning; check warehouse data.")
            else:
                st.line_chart(wide, height=420)
                with st.expander("Chart source (rows used)"):
                    st.caption(
                        "Uses `silver_price_ticks` when present; otherwise deduped `bronze_price_ticks`."
                    )
                    st.dataframe(sub.sort_values(["asof_ts", "coin_id"]), use_container_width=True)
        else:
            st.warning(
                "No price history in the warehouse yet. Run the collector, or use the live snapshot below."
            )
            live = _live_snapshot_df()
            if live is not None:
                st.info("Live snapshot from CoinGecko (single point per coin — run the pipeline for history).")
                st.bar_chart(live.set_index("coin_id")["price"])
                st.dataframe(live, use_container_width=True)
            else:
                st.error("Could not load warehouse or live quotes. Check COINS and network.")

    with tab_latest:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Latest prices (gold)")
            if latest is not None and not latest.empty:
                st.dataframe(latest, use_container_width=True)
            else:
                live = _live_snapshot_df()
                if live is not None:
                    st.dataframe(live, use_container_width=True)
                else:
                    st.write("No data.")
        with c2:
            st.subheader("Latest move vs prior tick")
            if returns_snap is not None and not returns_snap.empty:
                show = returns_snap[
                    ["coin_id", "symbol", "price", "prior_price", "pct_change_from_prior", "asof_ts"]
                ].copy()
                st.dataframe(show, use_container_width=True)
            else:
                st.write("Need at least two silver points per coin.")

        st.subheader("Moving average (7 ticks) — recent window")
        if trends is not None and not trends.empty:
            coins_t = sorted(trends["coin_id"].dropna().unique().tolist())
            if coins_t:
                one = st.selectbox("Coin for MA7 chart", coins_t, index=0)
                tsub = trends[trends["coin_id"] == one].copy()
                tsub["asof_ts"] = pd.to_datetime(tsub["asof_ts"], utc=True)
                tsub = tsub.set_index("asof_ts")[["price", "ma7"]].astype("float64")
                idx = pd.DatetimeIndex(tsub.index)
                if getattr(idx, "tz", None) is not None:
                    tsub.index = idx.tz_convert("UTC").tz_localize(None)
                st.line_chart(tsub.sort_index(), height=320)
        else:
            st.write("No trend data yet.")

    with tab_alerts:
        st.subheader("Price drop alerts")
        st.caption("Alerts fire when the latest tick drops vs the prior tick by at least the threshold.")
        if alerts is not None and not alerts.empty:
            st.dataframe(alerts, use_container_width=True)
        else:
            st.write("No alerts yet (or alerter not run).")

    with tab_arch:
        st.markdown(
            """
            ### System overview

            **Public API → Collector → DuckDB (bronze) → Processor (silver/gold) → Alerter → Streamlit**

            Full detail: see `ARCHITECTURE.md` in the repo.
            """
        )
        mermaid = """
flowchart LR
  api[(CoinGecko_API)] --> collector[Collector]
  collector --> bronze[(DuckDB_bronze)]
  bronze --> processor[Processor]
  processor --> silver_gold[(DuckDB_silver_gold)]
  silver_gold --> alerter[Alerter]
  alerter --> alerts[(alerts_table)]
  silver_gold --> ui[Streamlit_UI]
  alerts --> ui
"""
        components.html(
            f"""
<!DOCTYPE html>
<html><body style="background:white;">
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<pre class="mermaid">{mermaid}</pre>
<script>mermaid.initialize({{startOnLoad:true, theme: "neutral"}});</script>
</body></html>
            """,
            height=360,
        )


if __name__ == "__main__":
    main()
