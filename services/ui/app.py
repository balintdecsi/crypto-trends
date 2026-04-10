from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import altair as alt
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


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # region agent log
    try:
        log_path = "/home/balintdecsi/repos/.cursor/debug-babcd5.log"
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "sessionId": "babcd5",
            "runId": "ui-pre-fix",
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
        # region agent log
        _debug_log(
            "H1",
            "services/ui/app.py:_load_price_history_for_chart",
            "using_silver_history",
            {
                "rows": int(len(silver)),
                "coins": int(silver["coin_id"].nunique()) if "coin_id" in silver else -1,
                "asof_distinct": int(silver["asof_ts"].nunique()) if "asof_ts" in silver else -1,
            },
        )
        # endregion
        return silver
    bronze = _read_sql(
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
    # region agent log
    _debug_log(
        "H1",
        "services/ui/app.py:_load_price_history_for_chart",
        "using_bronze_fallback_history",
        {
            "rows": int(len(bronze)) if bronze is not None else -1,
            "coins": int(bronze["coin_id"].nunique()) if bronze is not None and "coin_id" in bronze else -1,
            "asof_distinct": int(bronze["asof_ts"].nunique()) if bronze is not None and "asof_ts" in bronze else -1,
        },
    )
    # endregion
    return bronze


def _prepare_line_chart_wide(df: pd.DataFrame) -> pd.DataFrame:
    """
    Streamlit line_chart is picky about dtypes and timezone-aware datetimes; normalize so y-values
    are plain floats and the time index is UTC-na (wall-clock) for the chart API.
    """
    out = df.copy()
    # region agent log
    _debug_log(
        "H2",
        "services/ui/app.py:_prepare_line_chart_wide",
        "input_to_prepare_line_chart",
        {
            "rows": int(len(out)),
            "coins": int(out["coin_id"].nunique()) if "coin_id" in out else -1,
            "asof_distinct": int(out["asof_ts"].nunique()) if "asof_ts" in out else -1,
        },
    )
    # endregion
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
    # region agent log
    _debug_log(
        "H2",
        "services/ui/app.py:_prepare_line_chart_wide",
        "output_from_prepare_line_chart",
        {
            "rows": int(len(wide)),
            "cols": int(len(wide.columns)),
            "index_distinct": int(wide.index.nunique()) if len(wide) > 0 else 0,
        },
    )
    # endregion
    return wide


def _prepare_coin_chart_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["asof_ts"] = pd.to_datetime(out["asof_ts"], utc=True)
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out.dropna(subset=["asof_ts", "price"]).sort_values("asof_ts")
    if out.empty:
        return pd.DataFrame(columns=["asof_ts", "price"])
    idx = pd.DatetimeIndex(out["asof_ts"])
    if getattr(idx, "tz", None) is not None:
        out["asof_ts"] = idx.tz_convert("UTC").tz_localize(None)
    return out[["asof_ts", "price"]]


def _render_coin_chart(df: pd.DataFrame, height: int = 260) -> None:
    if df.empty:
        st.warning("No chartable data.")
        return
    y_min = float(df["price"].min())
    y_max = float(df["price"].max())
    if y_min == y_max:
        pad = max(abs(y_min) * 0.01, 1e-6)
    else:
        pad = (y_max - y_min) * 0.05
    domain_min = y_min - pad
    domain_max = y_max + pad
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("asof_ts:T", title="Time (UTC)"),
            y=alt.Y(
                "price:Q",
                title="Price",
                scale=alt.Scale(domain=[domain_min, domain_max], nice=False, zero=False),
            ),
            tooltip=[alt.Tooltip("asof_ts:T", title="Time"), alt.Tooltip("price:Q", title="Price")],
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


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
            # region agent log
            _debug_log(
                "H3",
                "services/ui/app.py:main.tab_charts",
                "chart_df_present",
                {
                    "rows": int(len(chart_df)),
                    "coins": int(chart_df["coin_id"].nunique()) if "coin_id" in chart_df else -1,
                    "asof_distinct": int(chart_df["asof_ts"].nunique()) if "asof_ts" in chart_df else -1,
                },
            )
            # endregion
            coins = sorted(chart_df["coin_id"].dropna().unique().tolist())
            # Show one chart per coin so each chart has its own y-axis scale.
            cols = st.columns(2)
            for i, coin in enumerate(coins):
                with cols[i % 2]:
                    st.markdown(f"**{coin.upper()}**")
                    coin_df = chart_df[chart_df["coin_id"] == coin].copy()
                    # region agent log
                    _debug_log(
                        "H4",
                        "services/ui/app.py:main.tab_charts",
                        "per_coin_chart_subset",
                        {
                            "coin": coin,
                            "rows": int(len(coin_df)),
                            "asof_distinct": int(coin_df["asof_ts"].nunique()) if "asof_ts" in coin_df else -1,
                        },
                    )
                    # endregion
                    chart_points = _prepare_coin_chart_frame(coin_df)
                    if chart_points.empty:
                        st.warning(f"No chartable data for {coin}.")
                    else:
                        _render_coin_chart(chart_points, height=260)
            with st.expander("Chart source (rows used)"):
                st.caption(
                    "Uses `silver_price_ticks` when present; otherwise deduped `bronze_price_ticks`."
                )
                st.dataframe(chart_df.sort_values(["asof_ts", "coin_id"]), use_container_width=True)
        else:
            # region agent log
            _debug_log(
                "H3",
                "services/ui/app.py:main.tab_charts",
                "chart_df_missing_or_empty",
                {"is_none": chart_df is None},
            )
            # endregion
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
