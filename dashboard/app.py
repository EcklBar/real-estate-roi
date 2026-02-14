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
# TABS: Transactions | Listings
# ============================================
tab_transactions, tab_listings = st.tabs(["📊 Transactions (gov)", "📰 New Listings (Yad2 / Madlan)"])

# ============================================
# FILTERS (sidebar)
# ============================================
st.sidebar.header("Filters")

# Cities: from distinct first part of raw_address (city)
cities_df = run_query("""
    SELECT DISTINCT TRIM(SPLIT_PART(raw_address, ',', 1)) AS city
    FROM fact_transactions
    WHERE raw_address IS NOT NULL AND raw_address != ''
    ORDER BY 1
""")
city_options = ["All"] + cities_df["city"].tolist()
selected_city = st.sidebar.selectbox("City", city_options)

# Date range: from dim_time
dates_df = run_query("SELECT MIN(full_date) AS min_d, MAX(full_date) AS max_d FROM dim_time d JOIN fact_transactions t ON t.time_id = d.time_id")
if not dates_df.empty and dates_df["min_d"][0] and dates_df["max_d"][0]:
    d_min, d_max = dates_df["min_d"][0], dates_df["max_d"][0]
    date_range = st.sidebar.date_input("Date range", value=(d_min, d_max), min_value=d_min, max_value=d_max)
    if len(date_range) == 2:
        filter_start, filter_end = date_range[0], date_range[1]
    else:
        filter_start, filter_end = d_min, d_max
else:
    filter_start, filter_end = None, None

# Build filter clauses for queries (escape single quote in city for SQL safety)
city_safe = selected_city.replace("'", "''") if selected_city else ""
city_filter = "" if selected_city == "All" else f"AND TRIM(SPLIT_PART(t.raw_address, ',', 1)) = '{city_safe}'"
date_filter = ""
if filter_start and filter_end:
    date_filter = f"AND d.full_date BETWEEN '{filter_start}' AND '{filter_end}'"

# ============================================
# TAB: TRANSACTIONS
# ============================================
with tab_transactions:
    # KPI CARDS
    try:
        kpi = run_query(f"""
        SELECT
            COUNT(*)              AS total_deals,
            ROUND(AVG(t.price))   AS avg_price,
            ROUND(MIN(t.price))   AS min_price,
            ROUND(MAX(t.price))   AS max_price
        FROM fact_transactions t
        LEFT JOIN dim_time d ON t.time_id = d.time_id
        WHERE 1=1 {city_filter} {date_filter}
    """)

        c1, c2, c3, c4 = st.columns(4)
        total = kpi["total_deals"][0] or 0
        avg_p = kpi["avg_price"][0]
        min_p = kpi["min_price"][0]
        max_p = kpi["max_price"][0]
        c1.metric("Total Deals", f"{total:,}")
        c2.metric("Avg Price", f"₪{avg_p:,.0f}" if avg_p is not None else "N/A")
        c3.metric("Min Price", f"₪{min_p:,.0f}" if min_p is not None else "N/A")
        c4.metric("Max Price", f"₪{max_p:,.0f}" if max_p is not None else "N/A")

    except Exception as e:
        st.warning(f"Could not load KPIs: {e}")

    st.subheader("Price Distribution")
    try:
        prices = run_query(f"""
            SELECT t.price, t.transaction_type, t.raw_address
            FROM fact_transactions t
            LEFT JOIN dim_time d ON t.time_id = d.time_id
            WHERE t.price IS NOT NULL {city_filter} {date_filter}
            ORDER BY t.price DESC
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

    st.subheader("Average Price Over Time")
    try:
        trend = run_query(f"""
            SELECT
                d.year,
                d.month,
                d.month_name,
                d.full_date,
                AVG(t.price) AS avg_price,
                COUNT(*)    AS deal_count
            FROM fact_transactions t
            JOIN dim_time d ON t.time_id = d.time_id
            WHERE 1=1 {city_filter} {date_filter}
            GROUP BY d.year, d.month, d.month_name, d.full_date
            ORDER BY d.full_date
        """)
        if not trend.empty:
            fig_trend = px.line(
                trend,
                x="full_date",
                y="avg_price",
                title="Average transaction price by date",
                labels={"full_date": "Date", "avg_price": "Avg price (ILS)"},
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("No data for trend.")
    except Exception as e:
        st.warning(f"Could not load trend: {e}")

    st.subheader("Recent Transactions")
    try:
        transactions = run_query(f"""
            SELECT
                t.raw_address,
                t.price,
                t.transaction_type,
                d.full_date AS deal_date
            FROM fact_transactions t
            LEFT JOIN dim_time d ON t.time_id = d.time_id
            WHERE 1=1 {city_filter} {date_filter}
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

# ============================================
# TAB: LISTINGS (stream_listings)
# ============================================
with tab_listings:
    st.subheader("New Listings (Yad2 / Madlan)")
    st.caption("Listings consumed from Kafka and stored in stream_listings.")

    try:
        # Filters for listings: source, city (from stream_listings)
        list_sources_df = run_query("SELECT DISTINCT source FROM stream_listings ORDER BY 1")
        list_sources = ["All"] + (list_sources_df["source"].tolist() if not list_sources_df.empty else [])
        list_cities_df = run_query("SELECT DISTINCT city FROM stream_listings WHERE city IS NOT NULL AND city != '' ORDER BY 1")
        list_cities = ["All"] + (list_cities_df["city"].tolist() if not list_cities_df.empty else [])

        col_s, col_c = st.columns(2)
        with col_s:
            list_source = st.selectbox("Source", list_sources, key="list_source")
        with col_c:
            list_city = st.selectbox("City", list_cities, key="list_city")

        list_source_safe = list_source.replace("'", "''") if list_source else ""
        list_city_safe = list_city.replace("'", "''") if list_city else ""
        list_source_filter = "" if list_source == "All" else f"AND source = '{list_source_safe}'"
        list_city_filter = "" if list_city == "All" else f"AND city = '{list_city_safe}'"

        list_kpi = run_query(f"""
            SELECT COUNT(*) AS total, ROUND(AVG(price)) AS avg_price
            FROM stream_listings WHERE 1=1 {list_source_filter} {list_city_filter}
        """)
        if not list_kpi.empty and list_kpi["total"][0] and list_kpi["total"][0] > 0:
            st.metric("Listings", f"{list_kpi['total'][0]:,}", f"Avg ₪{list_kpi['avg_price'][0]:,.0f}" if list_kpi['avg_price'][0] else None)

        listings_df = run_query(f"""
            SELECT source, city, neighborhood, street, rooms, sqm, floor, price, price_per_sqm, property_type, url, created_at
            FROM stream_listings
            WHERE 1=1 {list_source_filter} {list_city_filter}
            ORDER BY created_at DESC
            LIMIT 200
        """)

        if not listings_df.empty:
            st.dataframe(
                listings_df,
                use_container_width=True,
                column_config={
                    "price": st.column_config.NumberColumn("Price (ILS)", format="₪%d"),
                    "price_per_sqm": st.column_config.NumberColumn("Price/sqm", format="₪%d"),
                    "url": st.column_config.LinkColumn("Link"),
                    "created_at": "Created",
                },
                hide_index=True,
            )
        else:
            st.info("No listings yet. Run the consumer and producers (Yad2/Madlan) to populate stream_listings.")

    except Exception as e:
        st.warning(f"Could not load listings: {e}")