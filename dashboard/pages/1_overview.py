"""
Page 1 — Overview
KPI tiles: Total Waste Quantity, Total Waste Cost, Avg Waste Percentage
Bar chart: total waste cost by month
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from athena_conn import get_connection
from chart_theme import apply_theme

st.set_page_config(page_title="Overview", layout="wide")
st.title("Overview")
st.markdown("Platform-wide KPIs and monthly waste cost trend.")


@st.cache_data(ttl=300)
def load_available_years() -> list:
    conn = get_connection()
    df = pd.read_sql("SELECT DISTINCT year FROM food_waste_db.fact_waste_summary ORDER BY year", conn)
    return sorted(df["year"].astype(int).tolist())


@st.cache_data(ttl=300)
def load_kpis(year: int) -> pd.DataFrame:
    conn = get_connection()
    query = f"""
        SELECT
            year,
            month,
            SUM(total_waste_quantity)  AS total_waste_quantity,
            SUM(total_waste_cost)      AS total_waste_cost,
            AVG(avg_waste_percentage)  AS avg_waste_percentage
        FROM food_waste_db.fact_waste_summary
        WHERE year = {year}
        GROUP BY year, month
        ORDER BY year, month
    """
    return pd.read_sql(query, conn)


years = load_available_years() or [2025]
selected_year = st.sidebar.selectbox("Year", options=years, index=max(0, len(years) - 1))

with st.spinner("Loading overview data..."):
    try:
        df = load_kpis(selected_year)

        if df.empty:
            st.warning("No data found in FACT_WASTE_SUMMARY. Run the pipeline first.")
        else:
            # KPI tiles
            col1, col2, col3 = st.columns(3)
            col1.metric(
                "Total Waste Quantity",
                f"{df['total_waste_quantity'].sum():,.0f} portions",
            )
            col2.metric(
                "Total Waste Cost",
                f"₹ {df['total_waste_cost'].sum():,.2f}",
            )
            col3.metric(
                "Avg Waste Percentage",
                f"{df['avg_waste_percentage'].mean():.1f}%",
            )

            st.markdown("---")

            # Bar chart — monthly waste cost
            df["month_label"] = df["month"].apply(
                lambda m: pd.Timestamp(f"2025-{m:02d}-01").strftime("%b")
            )
            fig = px.bar(
                df,
                x="month_label",
                y="total_waste_cost",
                labels={"month_label": "Month", "total_waste_cost": "Total Waste Cost (₹)"},
                title=f"Monthly Total Waste Cost ({selected_year})",
                color="total_waste_cost",
                color_continuous_scale="Reds",
            )
            fig.update_layout(coloraxis_showscale=False)
            apply_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load data: {e}")
