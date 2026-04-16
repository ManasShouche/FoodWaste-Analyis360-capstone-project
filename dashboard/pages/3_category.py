"""
Page 3 — Category Analysis
Horizontal bar chart: top 10 menu items by waste cost
Dropdown filter: select category
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from athena_conn import get_connection

st.set_page_config(page_title="Category Analysis", layout="wide")
st.title("Category Analysis")
st.markdown("Top menu items by waste cost, filtered by category.")


@st.cache_data(ttl=300)
def load_available_years() -> list:
    conn = get_connection()
    df = pd.read_sql("SELECT DISTINCT year FROM food_waste_db.fact_waste_summary ORDER BY year", conn)
    return sorted(df["year"].astype(int).tolist())


@st.cache_data(ttl=300)
def load_menu_waste(year: int) -> pd.DataFrame:
    conn = get_connection()
    query = f"""
        SELECT
            dm.menu_item_name,
            dm.category,
            SUM(fw.waste_cost)         AS total_waste_cost,
            SUM(fw.quantity_wasted)    AS total_waste_quantity,
            AVG(fw.waste_percentage)   AS avg_waste_pct,
            DENSE_RANK() OVER (ORDER BY SUM(fw.waste_cost) DESC) AS waste_rank
        FROM food_waste_db.fact_waste fw
        JOIN food_waste_db.dim_menu dm
            ON fw.menu_sk = dm.menu_sk
        WHERE fw.year = {year}
        GROUP BY dm.menu_item_name, dm.category
        ORDER BY total_waste_cost DESC
    """
    return pd.read_sql(query, conn)


years = load_available_years()
selected_year = st.sidebar.selectbox("Year", options=years, index=len(years) - 1)

with st.spinner("Loading category data..."):
    try:
        df = load_menu_waste(selected_year)

        if df.empty:
            st.warning("No menu waste data available.")
        else:
            # Category filter
            categories = sorted(df["category"].dropna().unique().tolist())
            selected = st.selectbox("Filter by category", options=["All"] + categories)

            filtered = df if selected == "All" else df[df["category"] == selected]
            top10 = filtered.nlargest(10, "total_waste_cost")

            fig = px.bar(
                top10.sort_values("total_waste_cost"),
                x="total_waste_cost",
                y="menu_item_name",
                orientation="h",
                labels={
                    "total_waste_cost": "Total Waste Cost (₹)",
                    "menu_item_name": "Menu Item",
                },
                title=f"Top 10 Menu Items by Waste Cost — {selected}",
                color="total_waste_cost",
                color_continuous_scale="Oranges",
            )
            fig.update_layout(coloraxis_showscale=False, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load data: {e}")
