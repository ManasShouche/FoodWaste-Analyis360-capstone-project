"""
Page 5 — Root Cause Analysis
Table with colour-coded root causes and recommendations
"""

import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from athena_conn import get_connection

st.set_page_config(page_title="Root Cause", layout="wide")
st.title("Root Cause Analysis")
st.markdown("Waste classified by root cause with actionable recommendations.")

RECOMMENDATIONS = {
    "Overproduction":     "Cut prep volume by 25% — you're cooking more than guests eat.",
    "Storage / Spoilage": "Fix fridge timing or push supplier to deliver closer to service.",
    "Portion Mismatch":   "Shrink the default portion — most guests won't ask for more.",
    "Low Demand":         "Pull this item on slow days or swap it for something that moves.",
}

ROOT_CAUSE_COLORS = {
    "Overproduction":     "background-color: #ef9a9a; color: #7f0000",   # bold red
    "Storage / Spoilage": "background-color: #ffcc80; color: #7f3100",   # amber
    "Portion Mismatch":   "background-color: #fff176; color: #5f4c00",   # yellow
    "Low Demand":         "background-color: #b0bec5; color: #1a2b33",   # slate blue-grey
}


def _highlight_root_cause(row):
    color = ROOT_CAUSE_COLORS.get(row["Root Cause"], "")
    return [color] * len(row)


@st.cache_data(ttl=300)
def load_root_cause() -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT
            location_name,
            city,
            region,
            category_name,
            year,
            month,
            waste_percentage,
            total_waste_cost,
            total_waste_quantity,
            root_cause,
            recommendation
        FROM food_waste_db.waste_root_cause
        ORDER BY waste_percentage DESC
    """
    return pd.read_sql(query, conn)


with st.spinner("Loading root cause data..."):
    try:
        df = load_root_cause()

        if df.empty:
            st.warning("No root cause data found. Ensure the view has been created in Athena.")
        else:
            # Summary counts
            st.subheader("Root Cause Distribution")
            counts = df["root_cause"].value_counts().reset_index()
            counts.columns = ["Root Cause", "Count"]
            col1, col2, col3, col4 = st.columns(4)
            for col, (_, row) in zip([col1, col2, col3, col4], counts.iterrows()):
                col.metric(row["Root Cause"], row["Count"])

            st.markdown("---")

            # Filter controls
            locations = ["All"] + sorted(df["location_name"].unique().tolist())
            selected_loc = st.selectbox("Filter by location", locations)
            causes = ["All"] + sorted(df["root_cause"].unique().tolist())
            selected_cause = st.selectbox("Filter by root cause", causes)

            filtered = df.copy()
            if selected_loc != "All":
                filtered = filtered[filtered["location_name"] == selected_loc]
            if selected_cause != "All":
                filtered = filtered[filtered["root_cause"] == selected_cause]

            # Override recommendations with plain-English versions
            filtered = filtered.copy()
            filtered["recommendation"] = filtered["root_cause"].map(RECOMMENDATIONS).fillna(filtered["recommendation"])

            # Styled table
            display = filtered[[
                "location_name", "category_name", "waste_percentage",
                "total_waste_cost", "root_cause", "recommendation"
            ]].rename(columns={
                "location_name":    "Location",
                "category_name":    "Category",
                "waste_percentage": "Waste %",
                "total_waste_cost": "Waste Cost (₹)",
                "root_cause":       "Root Cause",
                "recommendation":   "Recommendation",
            })

            styled = display.style.apply(_highlight_root_cause, axis=1)
            st.dataframe(styled, use_container_width=True, height=600)

    except Exception as e:
        st.error(f"Failed to load data: {e}")
