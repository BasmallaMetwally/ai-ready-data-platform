"""Streamlit dashboard over the analytics marts.

Every query here is a plain SELECT against a mart. That is the point: all the
logic lives in dbt and is version-controlled and tested, so the BI layer stays
thin and no business rule is ever reimplemented in the presentation tier.
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Crypto Market Warehouse", page_icon="📊", layout="wide")

DSN = (
    f"postgresql+psycopg://{os.getenv('PG_USER', 'warehouse')}:"
    f"{os.getenv('PG_PASSWORD', 'warehouse')}@{os.getenv('PG_HOST', 'localhost')}:"
    f"{os.getenv('PG_PORT', '5432')}/{os.getenv('PG_DATABASE', 'market')}"
)


@st.cache_resource
def get_engine():
    return create_engine(DSN, pool_pre_ping=True)


@st.cache_data(ttl=300)
def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def format_usd(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    for threshold, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(value) >= threshold:
            return f"${value / threshold:,.2f}{suffix}"
    return f"${value:,.2f}"


st.title("Crypto Market Warehouse")
st.caption("Binance OHLCV → S3 bronze → Spark silver → PostgreSQL → dbt marts")

# ------------------------------------------------------------------- overview
try:
    overview = query(
        "SELECT * FROM analytics.mart_market_overview ORDER BY market_cap_rank NULLS LAST"
    )
except Exception as exc:  # noqa: BLE001 - any failure here must render as UI, not a stack trace
    st.error(
        "Could not reach the warehouse. Is the stack up, and has the "
        f"`crypto_market_daily` DAG run at least once?\n\n```\n{exc}\n```"
    )
    st.stop()

if overview.empty:
    st.warning("The marts are empty. Trigger `crypto_market_daily` in Airflow, then refresh.")
    st.stop()

as_of = overview["as_of_date"].max()
st.markdown(f"**Data as of {as_of}** · {len(overview)} assets tracked")

cols = st.columns(4)
cols[0].metric("Assets", len(overview))
cols[1].metric("Total 24h volume", format_usd(overview["volume_usd_24h"].sum()))
cols[2].metric(
    "Median 30d volatility",
    f"{overview['annualised_volatility_30d'].median():.1%}"
    if overview["annualised_volatility_30d"].notna().any()
    else "—",
)
cols[3].metric("Bullish regime", f"{(overview['trend_regime'] == 'bullish').sum()}/{len(overview)}")

st.divider()

# ------------------------------------------------------------------ selection
left, right = st.columns([1, 3])
with left:
    pair = st.selectbox("Asset", overview["trading_pair"].tolist())
    window = st.select_slider("History", options=[30, 90, 180, 365, 1095], value=180)

row = overview[overview["trading_pair"] == pair].iloc[0]

with right:
    kpi = st.columns(4)
    kpi[0].metric(
        row["asset_name"] or pair,
        format_usd(row["close_price"]),
        f"{row['daily_return']:.2%}" if pd.notna(row["daily_return"]) else None,
    )
    kpi[1].metric("7d return", f"{row['return_7d']:.2%}" if pd.notna(row["return_7d"]) else "—")
    kpi[2].metric("30d return", f"{row['return_30d']:.2%}" if pd.notna(row["return_30d"]) else "—")
    kpi[3].metric(
        "Drawdown from peak",
        f"{row['drawdown_from_peak']:.1%}" if pd.notna(row["drawdown_from_peak"]) else "—",
    )

# ------------------------------------------------------------------ price view
history = query(
    """
    SELECT f.date_key, f.open_price, f.high_price, f.low_price, f.close_price,
           f.quote_volume_usd, m.sma_30d, m.sma_200d, m.annualised_volatility_30d
    FROM analytics.fct_ohlcv_daily f
    LEFT JOIN analytics.fct_asset_metrics_daily m
           ON f.trading_pair = m.trading_pair AND f.date_key = m.date_key
    WHERE f.trading_pair = :pair
      AND f.date_key >= (SELECT max(date_key) FROM analytics.fct_ohlcv_daily) - :window
    ORDER BY f.date_key
    """,
    {"pair": pair, "window": int(window)},
)

figure = go.Figure()
figure.add_trace(
    go.Candlestick(
        x=history["date_key"],
        open=history["open_price"],
        high=history["high_price"],
        low=history["low_price"],
        close=history["close_price"],
        name=pair,
    )
)
for column, colour in (("sma_30d", "#f59e0b"), ("sma_200d", "#6366f1")):
    if history[column].notna().any():
        figure.add_trace(
            go.Scatter(
                x=history["date_key"],
                y=history[column],
                name=column.replace("_", " ").upper(),
                line={"width": 1.5, "color": colour},
            )
        )
figure.update_layout(
    height=480,
    xaxis_rangeslider_visible=False,
    margin={"l": 0, "r": 0, "t": 20, "b": 0},
    legend={"orientation": "h", "y": 1.08},
)
st.plotly_chart(figure, use_container_width=True)

# -------------------------------------------------------------- leaderboards
st.divider()
tab_movers, tab_risk, tab_quality = st.tabs(["Movers", "Risk", "Pipeline health"])

with tab_movers:
    movers = overview[
        [
            "trading_pair",
            "asset_name",
            "close_price",
            "daily_return",
            "return_7d",
            "return_30d",
            "volume_usd_24h",
        ]
    ].copy()
    # Returns are stored as fractions (0.05 = 5%). Streamlit's "%%" format is a
    # literal percent sign, not a scaling directive, so 0.05 would render as
    # "0.05%". Scale explicitly rather than relying on the format string.
    for column in ("daily_return", "return_7d", "return_30d"):
        movers[column] = movers[column] * 100
    st.dataframe(
        movers.sort_values("return_7d", ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "daily_return": st.column_config.NumberColumn("1d", format="%.2f%%"),
            "return_7d": st.column_config.NumberColumn("7d", format="%.2f%%"),
            "return_30d": st.column_config.NumberColumn("30d", format="%.2f%%"),
            "volume_usd_24h": st.column_config.NumberColumn("Volume 24h", format="$%.0f"),
        },
    )

with tab_risk:
    risk = overview[
        [
            "trading_pair",
            "annualised_volatility_30d",
            "drawdown_from_peak",
            "trend_regime",
            "up_day_ratio",
        ]
    ]
    st.dataframe(
        risk.sort_values("annualised_volatility_30d", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

with tab_quality:
    st.subheader("Data coverage")
    health = query(
        """
        SELECT trading_pair,
               count(*)                          AS rows_loaded,
               min(date_key)                     AS first_date,
               max(date_key)                     AS last_date,
               current_date - max(date_key)      AS staleness_days,
               sum(case when has_unknown_asset then 1 else 0 end) AS unmapped_rows
        FROM analytics.fct_ohlcv_daily
        GROUP BY 1 ORDER BY staleness_days DESC, 1
        """
    )
    st.dataframe(health, use_container_width=True, hide_index=True)

    stale = health[health["staleness_days"] > 2]
    if not stale.empty:
        st.error(f"{len(stale)} asset(s) have not updated in over two days.")
    else:
        st.success("All assets are fresh.")

    st.subheader("Task reliability")
    st.caption(
        "Read from analytics.mart_pipeline_health — operational telemetry "
        "modelled as data, not scraped from the Airflow UI."
    )
    try:
        sli = query(
            """
            SELECT dag_id, task_id, total_runs, success_rate_pct,
                   avg_duration_sec, p95_duration_sec, avg_row_count,
                   failures_last_7d, health_status
            FROM analytics.mart_pipeline_health
            ORDER BY CASE health_status
                       WHEN 'stale' THEN 0 WHEN 'degraded' THEN 1
                       WHEN 'unreliable' THEN 2 ELSE 3 END, task_id
            """
        )
        if sli.empty:
            st.info("No audit history yet. It fills in as DAG runs complete.")
        else:
            st.dataframe(
                sli,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "success_rate_pct": st.column_config.ProgressColumn(
                        "Success rate", min_value=0, max_value=100, format="%.1f%%"
                    ),
                },
            )
            unhealthy = sli[sli["health_status"] != "healthy"]
            if not unhealthy.empty:
                st.warning(f"{len(unhealthy)} task(s) are not healthy.")
    except Exception as exc:  # noqa: BLE001
        st.info(f"Pipeline health mart not built yet ({type(exc).__name__}).")
