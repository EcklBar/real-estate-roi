"""
app.py -- Nadlanist Dashboard
Real estate analytics powered by Streamlit + Plotly.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ============================================
# DATABASE CONNECTION
# ============================================
WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5433")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "nadlanist")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD")

DATABASE_URL = (
    f"postgresql://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}"
    f"@{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"
)


@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL)


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(query, engine)

# ============================================
# PAGE CONFIG
# ============================================
st.set_page_config(
    page_title="Nadlanist",
    page_icon="🏠",
    layout="wide",
)

st.title("🏠 Nadlanist -- Real Estate Analytics")
st.markdown("Israeli real-estate transaction insights")

# ============================================
# KPI CARDS
# ============================================
try:
    kpi = run_query("""
        SELECT
            COUNT(*)              AS total_deals,
            ROUND(AVG(price))     AS avg_price,
            ROUND(MIN(price))     AS min_price,
            ROUND(MAX(price))     AS max_price
        FROM fact_transactions
    """)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Deals", f"{kpi['total_deals'][0]:,}")
    c2.metric("Avg Price", f"₪{kpi['avg_price'][0]:,.0f}")
    c3.metric("Min Price", f"₪{kpi['min_price'][0]:,.0f}")
    c4.metric("Max Price", f"₪{kpi['max_price'][0]:,.0f}")

except Exception as e:
    st.warning(f"Could not load KPIs: {e}")

# ============================================
# PRICE DISTRIBUTION CHART
# ============================================
st.subheader("Price Distribution")

try:
    prices = run_query("""
        SELECT price, transaction_type, raw_address
        FROM fact_transactions
        WHERE price IS NOT NULL
        ORDER BY price DESC
    """)

    if not prices.empty:
        fig = px.histogram(
            prices,
            x="price",
            nbins=20,
            title="Transaction Price Distribution",
            labels={"price": "Price (ILS)", "count": "Number of Deals"},
        )
        fig.update_layout(bargap=0.1)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No price data available.")

except Exception as e:
    st.warning(f"Could not load chart: {e}")

# ============================================
# TRANSACTIONS TABLE
# ============================================
st.subheader("Recent Transactions")

try:
    transactions = run_query("""
        SELECT
            t.raw_address,
            t.price,
            t.transaction_type,
            d.full_date AS deal_date
        FROM fact_transactions t
        LEFT JOIN dim_time d ON t.time_id = d.time_id
        ORDER BY t.price DESC
        LIMIT 50
    """)

    if not transactions.empty:
        st.dataframe(
            transactions,
            use_container_width=True,
            column_config={
                "price": st.column_config.NumberColumn("Price (ILS)", format="₪%d"),
                "raw_address": "Address",
                "transaction_type": "Type",
                "deal_date": "Date",
            },
        )
    else:
        st.info("No transactions to display.")

except Exception as e:
    st.warning(f"Could not load transactions: {e}")