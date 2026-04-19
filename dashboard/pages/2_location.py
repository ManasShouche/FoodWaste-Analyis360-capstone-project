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
from chart_theme import apply_theme

st.set_page_config(page_title="Location Analysis", layout="wide")
st.title("Location Analysis")
st.markdown("Monthly waste trend and top locations by cost.")


@st.cache_data(ttl=300)
def load_available_years() -> list:
    conn = get_connection()
    df = pd.read_sql("SELECT DISTINCT year FROM food_waste_db.fact_waste_summary ORDER BY year", conn)
    return sorted(df["year"].astype(int).tolist())


@st.cache_data(ttl=300)
def load_location_trend(year: int) -> pd.DataFrame:
    conn = get_connection()
    query = f"""
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
        WHERE fws.year = {year}
        GROUP BY dl.location_name, fws.year, fws.month
        ORDER BY fws.year, fws.month
    """
    return pd.read_sql(query, conn)


years = load_available_years() or [2025]
selected_year = st.sidebar.selectbox("Year", options=years, index=max(0, len(years) - 1))

with st.spinner("Loading location data..."):
    try:
        df = load_location_trend(selected_year)

        if df.empty:
            st.warning("No location data available.")
        else:
            df["period"] = df.apply(
                lambda r: pd.Timestamp(f"{int(r['year'])}-{int(r['month']):02d}-01"), axis=1
            )

            # Top N slider
            top_n = st.slider("Show top N locations by total waste cost", min_value=3, max_value=20, value=4)
            top_locations = (
                df.groupby("location_name")["total_waste_cost"]
                .sum()
                .nlargest(top_n)
                .index.tolist()
            )
            filtered_top = df[df["location_name"].isin(top_locations)]

            fig = px.line(
                filtered_top.sort_values("period"),
                x="period",
                y="total_waste_cost",
                color="location_name",
                markers=True,
                labels={"total_waste_cost": "Total Waste Cost (₹)", "period": "Month", "location_name": "Location"},
                title=f"Monthly Waste Cost — Top {top_n} Locations",
            )
            apply_theme(fig)
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
