

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import streamlit as st
import plotly.io as pio
import plotly.graph_objects as go

pio.templates["warm"] = go.layout.Template(
    layout=go.Layout(
        font=dict(color="#2C1A0E", size=13),
        title=dict(font=dict(color="#2C1A0E", size=16)),
        xaxis=dict(tickfont=dict(color="#2C1A0E"), title=dict(font=dict(color="#2C1A0E"))),
        yaxis=dict(tickfont=dict(color="#2C1A0E"), title=dict(font=dict(color="#2C1A0E"))),
        legend=dict(font=dict(color="#2C1A0E")),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
)
pio.templates.default = "plotly+warm"

st.set_page_config(
    page_title="Food Waste Optimization 360",
    page_icon="♻️",
    layout="wide",
)

st.title("Food Waste Optimization 360")
st.markdown("### Smart Kitchen Analytics Platform")

st.sidebar.title("Navigation")
st.sidebar.markdown(
    """
    Use the pages above to explore:
    - **Overview** — KPI tiles and monthly trend
    - **Location** — Waste by location over time
    - **Category** — Top menu items by waste cost
    - **Trends** — Month-over-month change
    - **Root Cause** — Classification and recommendations
    """
)

st.markdown(
    """
    ---
    Welcome to the Food Waste Optimization 360 dashboard.

    This platform analyses food production and waste data across all locations
    using a Medallion data architecture (Bronze → Silver → Gold) backed by
    AWS S3, Glue, and Athena.

    **Select a page from the sidebar to begin.**
    """
)
