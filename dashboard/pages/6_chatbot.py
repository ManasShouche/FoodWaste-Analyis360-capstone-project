"""
Page 6 — AI Chatbot
Architecture:
  User question
    → Athena information_schema (build live schema context)
    → Bedrock Claude Haiku (Call 1: SQL generation)
    → SQL validator (blocks writes + SELECT *)
    → Athena (execute query, return DataFrame)
    → Bedrock Claude Haiku (Call 2: narrate result)
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
from chart_theme import apply_theme

st.set_page_config(page_title="AI Chatbot", layout="wide")
st.title("Ask Your Data")
st.markdown("Ask plain-English questions about food waste. Powered by AWS Bedrock Claude Haiku.")

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
ATHENA_DB  = os.environ.get("ATHENA_DATABASE", "food_waste_db")
MAX_ROWS   = 50
MODEL_ID   = "apac.anthropic.claude-3-haiku-20240307-v1:0"

# ---------------------------------------------------------------------------
# Live schema pulled from Athena information_schema
# ---------------------------------------------------------------------------
SCHEMA_NOTES = """
Key constraints the model MUST respect:
- fact_waste_summary has NO menu_sk, NO waste_reason_sk, NO supplier_sk, NO date_sk.
  It is pre-aggregated by location + category. Filter category with: WHERE LOWER(fws.category) = 'snacks'
  NEVER join fact_waste_summary to dim_menu or dim_supplier.
- fact_production column is cost_per_unit (NOT unit_cost).
- storage_rating in dim_location is a VARCHAR string ('A', 'B', 'C') — never compare as a number.
- month_name only exists in dim_date. Fact tables only have month INT (1-12).
  For month names convert to int: January=1, February=2, etc.
- For supplier questions use fact_waste (has supplier_sk). Always add: WHERE ds.is_current = true
- fact_waste_summary has NO supplier_sk — never join it to dim_supplier.
"""

RULES = """
Rules — ALWAYS follow:
- Return ONLY a valid Athena SQL query. No explanation, no markdown, no prose.
- Always prefix table names with the database: food_waste_db.table_name
- Only filter year/month if the user explicitly mentions a time period
- Never use SELECT *
- Use LIMIT 100 on all queries
- Prefer fact_waste_summary for aggregated questions; use fact_waste for row-level detail
- Use LOWER() for string comparisons on category, location_name, region, city
"""


@st.cache_data(ttl=3600, show_spinner="Fetching live schema from Athena...")
def build_schema_context() -> str:
    """Query information_schema.columns to get real column names for all food_waste_db tables."""
    try:
        conn = get_connection()
        df = pd.read_sql(
            f"""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{ATHENA_DB}'
            ORDER BY table_name, ordinal_position
            """,
            conn,
        )
        if df.empty:
            raise ValueError("information_schema returned no rows")

        lines = [
            "You are a SQL generator for a food waste analytics platform.",
            f"Database: {ATHENA_DB} (AWS Athena / Presto SQL dialect)",
            "",
            "Tables and columns (pulled live from Athena information_schema):",
        ]
        for table, grp in df.groupby("table_name"):
            cols = ", ".join(
                f"{r['column_name']} {r['data_type'].upper()}"
                for _, r in grp.iterrows()
            )
            lines.append(f"  {table}({cols})")

        lines.append("")
        lines.append(SCHEMA_NOTES)
        lines.append(RULES)
        return "\n".join(lines)

    except Exception as e:
        # Fallback to hardcoded schema if information_schema fails
        st.warning(f"Could not fetch live schema ({e}) — using fallback schema.")
        return _fallback_schema()


def _fallback_schema() -> str:
    return f"""
You are a SQL generator for a food waste analytics platform.
Database: {ATHENA_DB} (AWS Athena / Presto SQL dialect)

Tables and columns:
  fact_waste_summary(location_sk VARCHAR, category_sk VARCHAR, category VARCHAR,
                     year INT, month INT, total_waste_quantity DOUBLE,
                     total_waste_cost DOUBLE, avg_waste_percentage DOUBLE, waste_event_count BIGINT)
  fact_waste(waste_sk VARCHAR, date_sk INT, location_sk VARCHAR, menu_sk VARCHAR,
             meal_period_sk VARCHAR, waste_reason_sk VARCHAR, supplier_sk VARCHAR,
             year INT, month INT, quantity_wasted DOUBLE, waste_cost DOUBLE, waste_percentage DOUBLE)
  fact_production(production_sk VARCHAR, date_sk INT, location_sk VARCHAR, menu_sk VARCHAR,
                  meal_period_sk VARCHAR, year INT, month INT,
                  quantity_prepared DOUBLE, cost_per_unit DOUBLE, batch_id VARCHAR)
  fact_consumption(consumption_sk VARCHAR, date_sk INT, location_sk VARCHAR, menu_sk VARCHAR,
                   year INT, month INT, quantity_prepared DOUBLE, quantity_wasted DOUBLE,
                   quantity_consumed DOUBLE, demand_gap DOUBLE)
  dim_location(location_sk VARCHAR, location_id VARCHAR, location_name VARCHAR,
               city VARCHAR, region VARCHAR, location_type VARCHAR,
               capacity INT, storage_rating VARCHAR)
  dim_menu(menu_sk VARCHAR, menu_item_id VARCHAR, menu_item_name VARCHAR,
           category VARCHAR, sub_category VARCHAR, veg_flag VARCHAR,
           shelf_life_hours INT, prep_complexity VARCHAR)
  dim_date(date_sk INT, full_date VARCHAR, day_of_week INT, day_name VARCHAR,
           month INT, month_name VARCHAR, quarter INT, year INT, is_weekend BOOLEAN)
  dim_category(category_sk VARCHAR, category_name VARCHAR)
  dim_meal_period(meal_period_sk VARCHAR, meal_period_name VARCHAR)
  dim_waste_reason(waste_reason_sk VARCHAR, waste_reason_name VARCHAR)
  dim_supplier(supplier_sk VARCHAR, supplier_id VARCHAR, supplier_name VARCHAR,
               menu_item_id VARCHAR, lead_time_days INT, quality_score DOUBLE, is_current BOOLEAN)

{SCHEMA_NOTES}
{RULES}
"""


ROUTER_SYSTEM = """
You are a food waste analytics assistant. Decide how to handle the user's message.

Reply with EXACTLY one of these two formats — no other text:

1. If the question could involve any data (locations, cities, categories, waste, cost, suppliers, trends, rankings, comparisons, months, years) — always choose this:
   QUERY

2. Only if the question is purely about definitions or concepts with zero data involved (e.g. "what is SCD2?", "what does waste percentage mean?", "how does the pipeline work?"):
   ANSWER: <your concise answer in 2-3 sentences>

When in doubt, choose QUERY.
"""

NARRATION_SYSTEM = """
You are a concise food waste analyst assistant.
Given a user question and query results as JSON, write 2-3 sentences in plain English.
Highlight the most important number or finding. Do not repeat the question. Do not mention SQL.
"""

# ---------------------------------------------------------------------------
# Bedrock — Claude Haiku
# ---------------------------------------------------------------------------
@st.cache_resource
def get_bedrock_client():
    return boto3.client("bedrock-runtime", region_name=AWS_REGION)


def call_claude(system_prompt: str, user_message: str) -> str:
    client = get_bedrock_client()
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "temperature": 0,
        "system": system_prompt,
        "messages": [{"role": "user", "content": [{"type": "text", "text": user_message}]}],
    }
    response = client.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )
    result = json.loads(response["body"].read())
    return result["content"][0]["text"].strip()


# ---------------------------------------------------------------------------
# SQL validator
# ---------------------------------------------------------------------------
BLOCKED_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|MERGE)\b",
    re.IGNORECASE,
)


def validate_sql(sql: str) -> tuple[bool, str]:
    if BLOCKED_PATTERNS.search(sql):
        return False, "Query contains a write operation — blocked."
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
# Auto-visualization
# ---------------------------------------------------------------------------
_SK_COLS    = {"location_sk", "menu_sk", "date_sk", "meal_period_sk",
               "waste_reason_sk", "category_sk", "supplier_sk"}
_LABEL_COLS = {"location_name", "menu_item_name", "category", "category_name",
               "waste_reason_name", "meal_period_name", "city", "region",
               "month_name", "day_name"}


def auto_chart(df: pd.DataFrame) -> None:
    if df is None or df.empty or len(df) < 2:
        return

    numeric_cols = [c for c in df.columns
                    if pd.api.types.is_numeric_dtype(df[c]) and c not in _SK_COLS]
    if not numeric_cols:
        return

    primary_y = numeric_cols[0]

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
        fig = px.line(df.sort_values(x_col), x=x_col, y=primary_y, color=color_col,
                      markers=True,
                      labels={primary_y: primary_y.replace("_", " ").title(), x_col: "Period"},
                      title=primary_y.replace("_", " ").title() + " Over Time")
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
        return

    label_col = next((c for c in _LABEL_COLS if c in df.columns), None)
    if label_col:
        top_n = df.nlargest(min(15, len(df)), primary_y)
        fig = px.bar(top_n.sort_values(primary_y), x=primary_y, y=label_col,
                     orientation="h", color=primary_y, color_continuous_scale="Oranges",
                     labels={primary_y: primary_y.replace("_", " ").title(), label_col: ""},
                     title=primary_y.replace("_", " ").title() + " by " + label_col.replace("_", " ").title())
        fig.update_layout(coloraxis_showscale=False, yaxis={"categoryorder": "total ascending"})
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
        return

    if len(numeric_cols) >= 2:
        fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1],
                         labels={numeric_cols[0]: numeric_cols[0].replace("_", " ").title(),
                                 numeric_cols[1]: numeric_cols[1].replace("_", " ").title()},
                         title=f"{numeric_cols[0].replace('_',' ').title()} vs "
                               f"{numeric_cols[1].replace('_',' ').title()}")
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Main chat logic
# ---------------------------------------------------------------------------
def answer_question(question: str, schema_context: str, max_retries: int = 2) -> dict:
    # Step 0 — route: does this need a DB query, a clarification, or a direct answer?
    route_raw = call_claude(system_prompt=ROUTER_SYSTEM, user_message=question).strip()

    if route_raw.startswith("ANSWER:"):
        direct_answer = route_raw[len("ANSWER:"):].strip()
        return {"sql": None, "df": None, "narration": direct_answer, "error": None, "type": "answer"}

    raw = call_claude(system_prompt=schema_context, user_message=question)
    sql = re.sub(r"```(?:sql)?", "", raw, flags=re.IGNORECASE).replace("```", "").strip()

    ok, err = validate_sql(sql)
    if not ok:
        return {"sql": sql, "df": None, "narration": None, "error": err}

    last_error = None
    for attempt in range(max_retries):
        try:
            df = run_athena_query(sql)
            last_error = None
            break
        except Exception as e:
            last_error = str(e)
            if attempt < max_retries - 1:
                fix_prompt = (
                    f"The following SQL query failed on Athena with this error:\n\n"
                    f"ERROR: {last_error}\n\n"
                    f"FAILING SQL:\n{sql}\n\n"
                    f"Original question: {question}\n\n"
                    f"Fix the SQL so it runs correctly. Return ONLY the fixed SQL query."
                )
                raw = call_claude(system_prompt=schema_context, user_message=fix_prompt)
                sql = re.sub(r"```(?:sql)?", "", raw, flags=re.IGNORECASE).replace("```", "").strip()

    if last_error:
        clarify = call_claude(
            system_prompt="You are a food waste analytics assistant.",
            user_message=(
                f"The question '{question}' failed to produce a working SQL query after {max_retries} attempts. "
                f"Last error: {last_error}. "
                f"Ask the user one short clarifying question to help you answer correctly."
            ),
        )
        return {"sql": sql, "df": None, "narration": clarify, "error": None, "type": "clarify"}

    if df.empty:
        return {"sql": sql, "df": df, "narration": "No data found for that question.", "error": None}

    sample = df.head(MAX_ROWS).to_json(orient="records", date_format="iso")
    narration = call_claude(
        system_prompt=NARRATION_SYSTEM,
        user_message=f"Question: {question}\n\nQuery results (JSON):\n{sample}",
    )
    return {"sql": sql, "df": df, "narration": narration, "error": None}


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------
if "schema_context" not in st.session_state:
    st.session_state.schema_context = build_schema_context()
schema_context = st.session_state.schema_context

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("df") is not None:
            auto_chart(msg["df"])
            st.dataframe(msg["df"], use_container_width=True)
        if msg.get("sql"):
            with st.expander("View generated SQL"):
                st.code(msg["sql"], language="sql")

if not st.session_state.messages:
    st.markdown("**Try asking:**")
    cols = st.columns(3)
    suggestions = [
        "Which location had the highest waste cost in 2025?",
        "What are the top 5 menu items by waste quantity?",
        "Show monthly waste trend for 2025",
        "Which waste reason is most common?",
        "Which supplier has the highest associated waste cost?",
        "Which category has the worst waste percentage?",
    ]
    for i, s in enumerate(suggestions):
        if cols[i % 3].button(s, key=f"suggest_{i}"):
            st.session_state.pending_question = s

question = st.chat_input("Ask about food waste data...")
if not question and "pending_question" in st.session_state:
    question = st.session_state.pop("pending_question")

if question:
    with st.chat_message("user"):
        st.markdown(question)
    st.session_state.messages.append({"role": "user", "content": question})

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            result = answer_question(question, schema_context)

        if result["error"]:
            st.error(result["error"])
            if result.get("sql"):
                with st.expander("View generated SQL"):
                    st.code(result["sql"], language="sql")
            st.session_state.messages.append({
                "role": "assistant", "content": result["error"],
                "sql": result.get("sql"), "df": None,
            })
        else:
            st.markdown(result["narration"])
            if result["df"] is not None and not result["df"].empty:
                auto_chart(result["df"])
                st.dataframe(result["df"], use_container_width=True)
            with st.expander("View generated SQL"):
                st.code(result["sql"], language="sql")
            st.session_state.messages.append({
                "role": "assistant", "content": result["narration"],
                "sql": result["sql"], "df": result["df"],
            })
