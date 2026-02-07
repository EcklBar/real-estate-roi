"""
Folium Map Components for the Dashboard
"""

import streamlit as st
import folium
from typing import Dict, List

try:
    from streamlit_folium import st_folium
    HAS_ST_FOLIUM = True
except ImportError:
    HAS_ST_FOLIUM = False


# City coordinates for map centering
CITY_COORDS = {
    "תל אביב -יפו": (32.0853, 34.7818),
    "ירושלים": (31.7683, 35.2137),
    "חיפה": (32.7940, 34.9896),
    "באר שבע": (31.2530, 34.7915),
    "ראשון לציון": (31.9730, 34.7925),
    "פתח תקווה": (32.0841, 34.8878),
    "נתניה": (32.3215, 34.8532),
    "חולון": (32.0167, 34.7794),
    "רמת גן": (32.0700, 34.8243),
    "אשדוד": (31.8044, 34.6553),
    "הרצליה": (32.1629, 34.7914),
    "רעננה": (32.1849, 34.8706),
    "כפר סבא": (32.1780, 34.9066),
    "בת ים": (32.0236, 34.7503),
    "גבעתיים": (32.0719, 34.8124),
    "רמת השרון": (32.1461, 34.8394),
    "מודיעין-מכבים-רעות": (31.8969, 35.0104),
}


def get_roi_color(roi_score: float) -> str:
    """Get color based on ROI score."""
    if roi_score >= 75:
        return '#27ae60'  # Green
    elif roi_score >= 60:
        return '#2ecc71'  # Light green
    elif roi_score >= 50:
        return '#f39c12'  # Orange
    elif roi_score >= 35:
        return '#e67e22'  # Dark orange
    else:
        return '#e74c3c'  # Red


def render_map(data: Dict, trends: List[dict]):
    """Render interactive map with city markers colored by ROI."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
    from transform.feature_engineering import (
        calculate_roi_score,
        calculate_rental_yield,
        BENCHMARK_PRICE_PER_SQM,
    )

    st.markdown("### Israel Real Estate Map")
    st.markdown("Markers colored by ROI score: 🟢 High ROI | 🟡 Medium | 🔴 Low ROI")

    # Create map centered on Israel
    m = folium.Map(
        location=[31.8, 34.9],
        zoom_start=8,
        tiles='cartodbpositron',
    )

    # Add city markers
    for city_name, city_info in data.items():
        coords = CITY_COORDS.get(city_name) or CITY_COORDS.get(
            city_info['data'].get('settlementName', '')
        )
        if not coords:
            continue

        benchmark = BENCHMARK_PRICE_PER_SQM.get(city_name, 25000)
        city_trends = city_info['trends']

        roi = calculate_roi_score(
            price_per_sqm=benchmark,
            city=city_name,
            rental_yield=calculate_rental_yield(
                benchmark * 80, city_name, 80
            ),
            appreciation_trends=city_trends,
            deal_count=50,
        )

        color = get_roi_color(roi['roi_score'])

        # Get latest price for popup
        all_trends = [
            t for t in city_trends
            if t.get('settlementPrice') and t.get('numRooms') == 'all'
        ]
        latest_price = 'N/A'
        if all_trends:
            sorted_t = sorted(all_trends, key=lambda x: (x['year'], x['month']), reverse=True)
            latest_price = f"₪{sorted_t[0]['settlementPrice']:,.0f}"

        neighborhoods = len(city_info['data'].get('otherNeighborhoods', []))

        popup_html = f"""
        <div style="font-family: Arial; width: 200px;">
            <h4 style="margin: 0;">{city_name}</h4>
            <hr style="margin: 5px 0;">
            <b>ROI Score:</b> {roi['roi_score']}/100<br>
            <b>Avg Price:</b> {latest_price}<br>
            <b>Neighborhoods:</b> {neighborhoods}<br>
            <hr style="margin: 5px 0;">
            <small>
                Price: {roi['price_benchmark_score']:.0f} |
                Growth: {roi['appreciation_score']:.0f} |
                Yield: {roi['rental_yield_score']:.0f}
            </small>
        </div>
        """

        folium.CircleMarker(
            location=coords,
            radius=12,
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=f"{city_name} - ROI: {roi['roi_score']}",
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            weight=2,
        ).add_to(m)

    # Render map
    if HAS_ST_FOLIUM:
        st_folium(m, width=None, height=600, returned_objects=[])
    else:
        st.warning(
            "Install `streamlit-folium` for interactive maps: "
            "`pip install streamlit-folium`"
        )
        st.markdown("Map would render here with city ROI scores.")

    # Legend
    st.markdown("""
    | Color | ROI Score | Investment Rating |
    |-------|-----------|-------------------|
    | 🟢 | 75+ | Excellent |
    | 🟢 | 60-74 | Good |
    | 🟡 | 50-59 | Average |
    | 🟠 | 35-49 | Below Average |
    | 🔴 | <35 | Poor |
    """)
