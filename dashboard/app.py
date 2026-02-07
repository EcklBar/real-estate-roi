"""
Real Estate ROI Dashboard

Interactive Streamlit dashboard for exploring real estate
investment opportunities in Israel.
"""

import streamlit as st
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from components.charts import render_price_trends, render_roi_comparison, render_top_opportunities
from components.map_view import render_map

# ============================================
# Page Configuration
# ============================================

st.set_page_config(
    page_title="Real Estate ROI Predictor",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================
# Sidebar
# ============================================

st.sidebar.title("🏠 Real Estate ROI")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["Overview", "Price Trends", "ROI Analysis", "Map Explorer", "Top Opportunities"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Data Source**: [nadlan.gov.il](https://www.nadlan.gov.il/)"
)
st.sidebar.markdown("**Updated**: Daily at 06:00")

# ============================================
# Data Loading
# ============================================

@st.cache_data(ttl=3600)
def load_settlements_data():
    """Load settlement data from nadlan.gov.il CloudFront API."""
    from extract.gov_api import (
        get_settlement_data,
        extract_price_trends,
        SETTLEMENT_CODES,
    )
    from transform.feature_engineering import (
        calculate_roi_score,
        BENCHMARK_PRICE_PER_SQM,
        calculate_rental_yield,
    )

    all_data = {}
    all_trends = []

    for city_name, city_id in list(SETTLEMENT_CODES.items())[:10]:
        try:
            data = get_settlement_data(city_id)
            trends = extract_price_trends(data)

            for t in trends:
                t['settlementName'] = data.get('settlementName', city_name)
                t['settlementID'] = city_id

            all_data[city_name] = {
                'data': data,
                'trends': trends,
            }
            all_trends.extend(trends)
        except Exception:
            pass

    return all_data, all_trends


# ============================================
# Pages
# ============================================

if page == "Overview":
    st.title("🏠 Real Estate ROI Predictor")
    st.markdown("### Investment Opportunities in Israel")
    st.markdown("---")

    col1, col2, col3, col4 = st.columns(4)

    try:
        data, trends = load_settlements_data()
        col1.metric("Cities Tracked", len(data))
        col2.metric("Data Points", f"{len(trends):,}")

        # Get latest average price
        latest = [t for t in trends if t.get('settlementPrice') and t.get('numRooms') == 'all']
        if latest:
            latest_sorted = sorted(latest, key=lambda x: (x['year'], x['month']), reverse=True)
            avg_price = sum(t['settlementPrice'] for t in latest_sorted[:len(data)]) / len(data)
            col3.metric("Avg Price (all rooms)", f"₪{avg_price:,.0f}")

        col4.metric("Last Updated", "Live")

        st.markdown("---")
        st.markdown("### Quick Stats by City")

        city_stats = []
        for city_name, city_data in data.items():
            city_trends = [
                t for t in city_data['trends']
                if t.get('settlementPrice') and t.get('numRooms') == 'all'
            ]
            if city_trends:
                sorted_t = sorted(city_trends, key=lambda x: (x['year'], x['month']), reverse=True)
                latest_price = sorted_t[0]['settlementPrice']
                oldest_price = sorted_t[-1]['settlementPrice'] if len(sorted_t) > 1 else latest_price
                change = ((latest_price - oldest_price) / oldest_price * 100) if oldest_price else 0

                city_stats.append({
                    'City': city_name,
                    'Latest Avg Price': f"₪{latest_price:,.0f}",
                    'Change': f"{change:+.1f}%",
                    'Neighborhoods': len(city_data['data'].get('otherNeighborhoods', [])),
                })

        if city_stats:
            import pandas as pd
            st.dataframe(pd.DataFrame(city_stats), use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.info("Make sure you have an internet connection to fetch data from nadlan.gov.il")

elif page == "Price Trends":
    st.title("📈 Price Trends")
    try:
        data, trends = load_settlements_data()
        render_price_trends(data, trends)
    except Exception as e:
        st.error(f"Failed to load data: {e}")

elif page == "ROI Analysis":
    st.title("📊 ROI Analysis")
    try:
        data, trends = load_settlements_data()
        render_roi_comparison(data, trends)
    except Exception as e:
        st.error(f"Failed to load data: {e}")

elif page == "Map Explorer":
    st.title("🗺️ Map Explorer")
    try:
        data, trends = load_settlements_data()
        render_map(data, trends)
    except Exception as e:
        st.error(f"Failed to load data: {e}")

elif page == "Top Opportunities":
    st.title("🏆 Top Investment Opportunities")
    try:
        data, trends = load_settlements_data()
        render_top_opportunities(data, trends)
    except Exception as e:
        st.error(f"Failed to load data: {e}")
