"""
Page 4 — Trends
Line chart with month-over-month % change overlay
Months with > 10% increase highlighted in red
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from athena_conn import get_connection

st.set_page_config(page_title="Trends", layout="wide")
st.title("Waste Trends")
st.markdown("Month-over-month waste cost change. Months with >10% increase are highlighted.")


@st.cache_data(ttl=300)
def load_trends() -> pd.DataFrame:
    conn = get_connection()
    query = """
        WITH monthly AS (
            SELECT
                year,
                month,
                SUM(total_waste_cost) AS total_waste_cost
            FROM food_waste_db.fact_waste_summary
            WHERE year = 2025
              AND month BETWEEN 1 AND 12
            GROUP BY year, month
        )
        SELECT
            year,
            month,
            total_waste_cost,
            LAG(total_waste_cost) OVER (ORDER BY year, month) AS prev_month_cost,
            ROUND(
                (total_waste_cost - LAG(total_waste_cost) OVER (ORDER BY year, month))
                * 100.0
                / NULLIF(LAG(total_waste_cost) OVER (ORDER BY year, month), 0),
                2
            ) AS mom_pct_change
        FROM monthly
        ORDER BY year, month
    """
    return pd.read_sql(query, conn)


with st.spinner("Loading trend data..."):
    try:
        df = load_trends()

        if df.empty:
            st.warning("No trend data available.")
        else:
            df["period"] = df.apply(
                lambda r: pd.Timestamp(f"{int(r['year'])}-{int(r['month']):02d}-01"), axis=1
            )
            df["mom_pct_change"] = pd.to_numeric(df["mom_pct_change"], errors="coerce")

            fig = go.Figure()

            # Waste cost line
            fig.add_trace(go.Scatter(
                x=df["period"],
                y=df["total_waste_cost"],
                name="Total Waste Cost (₹)",
                mode="lines+markers",
                line={"color": "steelblue", "width": 2},
            ))

            # MoM % change bar (secondary y-axis)
            colors = [
                "red" if (v is not None and v > 10) else "lightgrey"
                for v in df["mom_pct_change"]
            ]
            fig.add_trace(go.Bar(
                x=df["period"],
                y=df["mom_pct_change"],
                name="MoM % Change",
                marker_color=colors,
                yaxis="y2",
                opacity=0.6,
            ))

            fig.update_layout(
                title="Monthly Waste Cost with MoM % Change",
                xaxis={"title": "Month"},
                yaxis={"title": "Total Waste Cost (₹)"},
                yaxis2={
                    "title": "MoM % Change",
                    "overlaying": "y",
                    "side": "right",
                    "zeroline": True,
                    "zerolinecolor": "grey",
                },
                legend={"x": 0.01, "y": 0.99},
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True)

            # Table of months with > 10% increase
            spikes = df[df["mom_pct_change"] > 10][["period", "total_waste_cost", "mom_pct_change"]]
            if not spikes.empty:
                st.subheader("Months with >10% Cost Increase")
                spikes_display = spikes.copy()
                spikes_display["period"] = spikes_display["period"].dt.strftime("%b %Y")
                spikes_display.columns = ["Month", "Waste Cost (₹)", "MoM % Change"]
                st.dataframe(spikes_display.reset_index(drop=True), use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load data: {e}")
