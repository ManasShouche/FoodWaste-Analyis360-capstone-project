"""
Page 2 — Location Analysis
Line chart: waste trend over time per location
Table: top 5 locations by total waste cost
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from athena_conn import get_connection

st.set_page_config(page_title="Location Analysis", layout="wide")
st.title("Location Analysis")
st.markdown("Monthly waste trend and top locations by cost.")


@st.cache_data(ttl=300)
def load_location_trend() -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT
            dl.location_name,
            fws.year,
            fws.month,
            SUM(fws.total_waste_cost)      AS total_waste_cost,
            SUM(fws.total_waste_quantity)  AS total_waste_quantity,
            AVG(fws.avg_waste_percentage)  AS avg_waste_pct
        FROM food_waste_db.fact_waste_summary fws
        JOIN food_waste_db.dim_location dl
            ON fws.location_sk = dl.location_sk
        WHERE fws.year = 2025
          AND fws.month BETWEEN 1 AND 12
        GROUP BY dl.location_name, fws.year, fws.month
        ORDER BY fws.year, fws.month
    """
    return pd.read_sql(query, conn)


with st.spinner("Loading location data..."):
    try:
        df = load_location_trend()

        if df.empty:
            st.warning("No location data available.")
        else:
            df["period"] = df.apply(
                lambda r: pd.Timestamp(f"{int(r['year'])}-{int(r['month']):02d}-01"), axis=1
            )

            # Line chart
            fig = px.line(
                df,
                x="period",
                y="total_waste_cost",
                color="location_name",
                labels={
                    "period": "Month",
                    "total_waste_cost": "Total Waste Cost (₹)",
                    "location_name": "Location",
                },
                title="Monthly Waste Cost by Location",
                markers=True,
            )
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")
            st.subheader("Top 5 Locations by Total Waste Cost")
            top5 = (
                df.groupby("location_name", as_index=False)["total_waste_cost"]
                .sum()
                .sort_values("total_waste_cost", ascending=False)
                .head(5)
                .rename(columns={
                    "location_name": "Location",
                    "total_waste_cost": "Total Waste Cost (₹)",
                })
            )
            top5["Total Waste Cost (₹)"] = top5["Total Waste Cost (₹)"].map("₹ {:,.2f}".format)
            st.table(top5.reset_index(drop=True))

    except Exception as e:
        st.error(f"Failed to load data: {e}")
