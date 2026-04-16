"""
Page 6 — AI Chatbot
Architecture:
  User question
    → Bedrock Nova Micro (Call 1: SQL generation)
    → SQL validator (blocks writes + SELECT *)
    → Athena (execute query, return DataFrame)
    → Bedrock Nova Micro (Call 2: narrate result)
    → Streamlit chat UI (NL answer + table + SQL expander)
"""

import json
import os
import re

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from athena_conn import get_connection

st.set_page_config(page_title="AI Chatbot", layout="wide")
st.title("Ask Your Data")
st.markdown("Ask plain-English questions about food waste. Powered by AWS Bedrock Nova Micro.")

AWS_REGION   = os.environ.get("AWS_REGION", "ap-south-1")
ATHENA_DB    = os.environ.get("ATHENA_DATABASE", "food_waste_db")
MAX_ROWS     = 50   # cap rows sent to Bedrock for narration

# ---------------------------------------------------------------------------
# Schema context injected into every SQL-gen prompt
# ---------------------------------------------------------------------------
SCHEMA_CONTEXT = """
You are a SQL generator for a food waste analytics platform.
Database: food_waste_db (AWS Athena / Presto SQL dialect)

Tables and columns:
  fact_waste_summary(location_sk, menu_sk, waste_reason_sk, year INT, month INT,
                     total_waste_quantity DOUBLE, total_waste_cost DOUBLE, avg_waste_percentage DOUBLE)
  fact_waste(date_sk INT, location_sk, menu_sk, meal_period_sk, waste_reason_sk,
             year INT, month INT, quantity_wasted DOUBLE, waste_cost DOUBLE, waste_percentage DOUBLE)
  fact_production(date_sk INT, location_sk, menu_sk, meal_period_sk,
                  year INT, month INT, quantity_prepared DOUBLE, unit_cost DOUBLE)
  fact_consumption(date_sk INT, location_sk, menu_sk,
                   year INT, month INT, quantity_consumed DOUBLE, demand_gap DOUBLE)
  dim_location(location_sk, location_id, location_name, city, region, location_type,
               capacity INT, storage_rating DOUBLE)
  dim_menu(menu_sk, menu_item_id, menu_item_name, category, sub_category,
           veg_flag BOOLEAN, shelf_life_hours INT, prep_complexity)
  dim_date(date_sk INT, full_date, day_of_week INT, day_name, month INT,
           month_name, quarter INT, year INT, is_weekend BOOLEAN)
  dim_category(category_sk, category_name)
  dim_meal_period(meal_period_sk, meal_period_name)
  dim_waste_reason(waste_reason_sk, waste_reason_name)
  dim_supplier(supplier_sk, supplier_id, supplier_name, lead_time_days INT,
               quality_score DOUBLE, is_current BOOLEAN)

Rules — ALWAYS follow these:
- Return ONLY a valid Athena SQL query. Nothing else. No explanation. No markdown. No prose.
- Always prefix table names with the database: food_waste_db.table_name
- Only add year or month filters if the user explicitly mentions a specific year or month
- If no time period is mentioned, query all years and months (no WHERE on year/month)
- Never use SELECT *
- Use LIMIT 100 on all queries
- Prefer fact_waste_summary over fact_waste for aggregated questions
- Use LOWER() for string comparisons on category, location_name, region
- For city/location questions, JOIN fact_waste_summary with dim_location on location_sk
"""

NARRATION_SYSTEM = """
You are a concise food waste analyst assistant.
Given a user question and query results as JSON, write a 2-3 sentence plain-English answer.
Highlight the most important number or finding. Do not repeat the question.
Do not mention SQL. Be direct.
"""


# ---------------------------------------------------------------------------
# Bedrock client
# ---------------------------------------------------------------------------
@st.cache_resource
def get_bedrock_client():
    return boto3.client("bedrock-runtime", region_name=AWS_REGION)


def call_nova_micro(system_prompt: str, user_message: str) -> str:
    client = get_bedrock_client()
    body = {
        "messages": [{"role": "user", "content": [{"text": user_message}]}],
        "system": [{"text": system_prompt}],
        "inferenceConfig": {"maxTokens": 512, "temperature": 0},
    }
    response = client.invoke_model(
        modelId="apac.amazon.nova-micro-v1:0",
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )
    result = json.loads(response["body"].read())
    return result["output"]["message"]["content"][0]["text"].strip()


# ---------------------------------------------------------------------------
# SQL validator — hard block on writes and SELECT *
# ---------------------------------------------------------------------------
BLOCKED_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|MERGE)\b",
    re.IGNORECASE,
)


def validate_sql(sql: str) -> tuple[bool, str]:
    if BLOCKED_PATTERNS.search(sql):
        return False, "Query contains a write operation (INSERT/DROP/etc.) — blocked."
    if re.search(r"SELECT\s+\*", sql, re.IGNORECASE):
        return False, "SELECT * is not allowed — blocked."
    if not re.search(r"\bSELECT\b", sql, re.IGNORECASE):
        return False, "No SELECT statement found in generated SQL."
    return True, ""


# ---------------------------------------------------------------------------
# Athena execution
# ---------------------------------------------------------------------------
def run_athena_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn)


# ---------------------------------------------------------------------------
# Auto-visualization — no extra Bedrock call, pure heuristics
# ---------------------------------------------------------------------------
# Columns that identify a "category" axis vs a surrogate key to skip
_SK_COLS   = {"location_sk", "menu_sk", "date_sk", "meal_period_sk",
              "waste_reason_sk", "category_sk", "supplier_sk"}
_TIME_COLS = {"month", "year", "full_date", "period"}
_LABEL_COLS = {"location_name", "menu_item_name", "category", "category_name",
               "waste_reason_name", "meal_period_name", "city", "region",
               "month_name", "day_name"}


def auto_chart(df: pd.DataFrame) -> None:
    """Render the most appropriate Plotly chart for a DataFrame, or nothing."""
    if df is None or df.empty or len(df) < 2:
        return

    numeric_cols = [c for c in df.columns
                    if pd.api.types.is_numeric_dtype(df[c]) and c not in _SK_COLS]
    if not numeric_cols:
        return

    primary_y = numeric_cols[0]   # first numeric col is the main metric

    # --- time-series: has month + year (or just month) ---
    if "month" in df.columns and len(df) <= 36:
        if "year" in df.columns:
            df = df.copy()
            df["period"] = pd.to_datetime(
                df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
            )
            x_col = "period"
        else:
            x_col = "month"

        color_col = next((c for c in _LABEL_COLS if c in df.columns), None)
        fig = px.line(
            df.sort_values(x_col),
            x=x_col, y=primary_y, color=color_col,
            markers=True,
            labels={primary_y: primary_y.replace("_", " ").title(), x_col: "Period"},
            title=primary_y.replace("_", " ").title() + " Over Time",
        )
        st.plotly_chart(fig, use_container_width=True)
        return

    # --- categorical: has a label column ---
    label_col = next((c for c in _LABEL_COLS if c in df.columns), None)
    if label_col:
        top_n = df.nlargest(min(15, len(df)), primary_y)
        fig = px.bar(
            top_n.sort_values(primary_y),
            x=primary_y, y=label_col,
            orientation="h",
            color=primary_y,
            color_continuous_scale="Oranges",
            labels={primary_y: primary_y.replace("_", " ").title(), label_col: ""},
            title=primary_y.replace("_", " ").title() + " by " + label_col.replace("_", " ").title(),
        )
        fig.update_layout(coloraxis_showscale=False,
                          yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
        return

    # --- two numeric cols: scatter ---
    if len(numeric_cols) >= 2:
        fig = px.scatter(
            df, x=numeric_cols[0], y=numeric_cols[1],
            labels={numeric_cols[0]: numeric_cols[0].replace("_", " ").title(),
                    numeric_cols[1]: numeric_cols[1].replace("_", " ").title()},
            title=f"{numeric_cols[0].replace('_',' ').title()} vs "
                  f"{numeric_cols[1].replace('_',' ').title()}",
        )
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Main chat logic
# ---------------------------------------------------------------------------
def answer_question(question: str) -> dict:
    """
    Returns dict with keys: sql, df, narration, error
    """
    # Call 1 — SQL generation
    raw = call_nova_micro(
        system_prompt=SCHEMA_CONTEXT,
        user_message=question,
    )

    sql = re.sub(r"```(?:sql)?", "", raw, flags=re.IGNORECASE).replace("```", "").strip()

    ok, err = validate_sql(sql)
    if not ok:
        return {"sql": sql, "df": None, "narration": None, "error": err}

    try:
        df = run_athena_query(sql)
    except Exception as e:
        return {"sql": sql, "df": None, "narration": None, "error": f"Athena error: {e}"}

    if df.empty:
        return {"sql": sql, "df": df, "narration": "No data found for that question.", "error": None}

    # Call 2 — narrate result
    sample = df.head(MAX_ROWS).to_json(orient="records", date_format="iso")
    narration_prompt = f"Question: {question}\n\nQuery results (JSON):\n{sample}"
    narration = call_nova_micro(
        system_prompt=NARRATION_SYSTEM,
        user_message=narration_prompt,
    )

    return {"sql": sql, "df": df, "narration": narration, "error": None}


# ---------------------------------------------------------------------------
# Streamlit chat UI
# ---------------------------------------------------------------------------
if "messages" not in st.session_state:
    st.session_state.messages = []

# Render existing chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("df") is not None:
            auto_chart(msg["df"])
            st.dataframe(msg["df"], use_container_width=True)
        if msg.get("sql"):
            with st.expander("View generated SQL"):
                st.code(msg["sql"], language="sql")

# Suggested questions
if not st.session_state.messages:
    st.markdown("**Try asking:**")
    cols = st.columns(3)
    suggestions = [
        "Which location had the highest waste cost in 2026?",
        "What are the top 5 menu items by waste quantity?",
        "Show monthly waste trend for 2025",
        "Which waste reason is most common?",
        "Compare waste cost between 2025 and 2026",
        "Which category has the worst waste percentage?",
    ]
    for i, s in enumerate(suggestions):
        if cols[i % 3].button(s, key=f"suggest_{i}"):
            st.session_state.pending_question = s

# Handle suggested question click
question = st.chat_input("Ask about food waste data...")
if not question and "pending_question" in st.session_state:
    question = st.session_state.pop("pending_question")

if question:
    # Show user message
    with st.chat_message("user"):
        st.markdown(question)
    st.session_state.messages.append({"role": "user", "content": question})

    # Get answer
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            result = answer_question(question)

        if result["error"]:
            st.error(result["error"])
            if result.get("sql"):
                with st.expander("View generated SQL"):
                    st.code(result["sql"], language="sql")
            st.session_state.messages.append({
                "role": "assistant",
                "content": result["error"],
                "sql": result.get("sql"),
                "df": None,
            })
        else:
            st.markdown(result["narration"])
            if result["df"] is not None and not result["df"].empty:
                auto_chart(result["df"])
                st.dataframe(result["df"], use_container_width=True)
            with st.expander("View generated SQL"):
                st.code(result["sql"], language="sql")
            st.session_state.messages.append({
                "role": "assistant",
                "content": result["narration"],
                "sql": result["sql"],
                "df": result["df"],
            })
