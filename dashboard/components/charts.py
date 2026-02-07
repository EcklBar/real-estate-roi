"""
Plotly Chart Components for the Dashboard
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Dict, List


def render_price_trends(data: Dict, trends: List[dict]):
    """Render price trend charts."""

    # City selector
    cities = list(data.keys())
    selected_city = st.selectbox("Select City", cities, index=0)

    if selected_city not in data:
        st.warning("No data for selected city")
        return

    city_trends = data[selected_city]['trends']

    # Room type selector
    room_types = list(set(t.get('numRooms', 'all') for t in city_trends))
    room_types.sort(key=str)
    selected_rooms = st.multiselect(
        "Room Types",
        room_types,
        default=[r for r in room_types if r != 'all'][:3],
    )

    if not selected_rooms:
        st.info("Select at least one room type")
        return

    # Build dataframe
    df_rows = []
    for t in city_trends:
        if t.get('numRooms') in selected_rooms and t.get('settlementPrice'):
            df_rows.append({
                'Date': f"{t['year']}-{t['month']:02d}",
                'Rooms': str(t['numRooms']),
                'Settlement Price': t['settlementPrice'],
                'Country Avg': t.get('countryPrice'),
            })

    if not df_rows:
        st.info("No price data available for selection")
        return

    df = pd.DataFrame(df_rows)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')

    # Line chart
    fig = px.line(
        df,
        x='Date',
        y='Settlement Price',
        color='Rooms',
        title=f"Price Trends - {selected_city}",
        labels={'Settlement Price': 'Average Price (ILS)', 'Rooms': 'Room Count'},
    )

    fig.update_layout(
        yaxis_tickformat=',',
        hovermode='x unified',
        height=500,
    )

    st.plotly_chart(fig, use_container_width=True)

    # Comparison with country average
    st.markdown("### Settlement vs Country Average")

    all_rooms = [t for t in city_trends if t.get('numRooms') == 'all' and t.get('settlementPrice')]
    if all_rooms:
        comp_rows = []
        for t in all_rooms:
            comp_rows.append({
                'Date': f"{t['year']}-{t['month']:02d}",
                'Settlement': t['settlementPrice'],
                'Country': t.get('countryPrice'),
            })

        comp_df = pd.DataFrame(comp_rows)
        comp_df['Date'] = pd.to_datetime(comp_df['Date'])
        comp_df = comp_df.sort_values('Date')

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=comp_df['Date'], y=comp_df['Settlement'],
            name=selected_city, fill='tozeroy', opacity=0.3,
        ))
        fig2.add_trace(go.Scatter(
            x=comp_df['Date'], y=comp_df['Country'],
            name='Country Average', fill='tozeroy', opacity=0.3,
        ))
        fig2.update_layout(
            yaxis_tickformat=',',
            yaxis_title='Average Price (ILS)',
            height=400,
        )
        st.plotly_chart(fig2, use_container_width=True)


def render_roi_comparison(data: Dict, trends: List[dict]):
    """Render ROI comparison charts across cities."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
    from transform.feature_engineering import (
        calculate_roi_score,
        calculate_rental_yield,
        BENCHMARK_PRICE_PER_SQM,
        score_appreciation_trend,
    )

    # Calculate ROI for each city
    roi_data = []
    for city_name, city_info in data.items():
        city_trends = city_info['trends']
        appreciation = score_appreciation_trend(city_trends)
        benchmark = BENCHMARK_PRICE_PER_SQM.get(city_name)

        roi = calculate_roi_score(
            price_per_sqm=benchmark,
            city=city_name,
            rental_yield=calculate_rental_yield(
                benchmark * 80 if benchmark else 0, city_name, 80
            ),
            appreciation_trends=city_trends,
            deal_count=50,
        )

        roi_data.append({
            'City': city_name,
            'ROI Score': roi['roi_score'],
            'Price Score': roi['price_benchmark_score'],
            'Appreciation': roi['appreciation_score'],
            'Yield Score': roi['rental_yield_score'],
            'Transit': roi['transit_score'],
        })

    df = pd.DataFrame(roi_data).sort_values('ROI Score', ascending=True)

    # Horizontal bar chart
    fig = px.bar(
        df,
        x='ROI Score',
        y='City',
        orientation='h',
        title='ROI Score by City',
        color='ROI Score',
        color_continuous_scale='RdYlGn',
        range_color=[0, 100],
    )
    fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

    # Component breakdown
    st.markdown("### Score Breakdown")
    components = ['Price Score', 'Appreciation', 'Yield Score', 'Transit']

    fig2 = go.Figure()
    for component in components:
        fig2.add_trace(go.Bar(
            name=component,
            x=df['City'],
            y=df[component],
        ))

    fig2.update_layout(
        barmode='group',
        title='ROI Component Scores',
        yaxis_title='Score (0-100)',
        height=500,
    )
    st.plotly_chart(fig2, use_container_width=True)


def render_top_opportunities(data: Dict, trends: List[dict]):
    """Render top investment opportunities."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
    from transform.feature_engineering import (
        calculate_roi_score,
        calculate_rental_yield,
        estimate_monthly_rent,
        BENCHMARK_PRICE_PER_SQM,
        score_appreciation_trend,
    )

    st.markdown("### Simulated Property Analysis")
    st.info(
        "These are simulated properties based on real market data "
        "to demonstrate ROI scoring capabilities."
    )

    # Generate sample properties
    import random
    random.seed(42)

    properties = []
    for city_name, city_info in data.items():
        benchmark = BENCHMARK_PRICE_PER_SQM.get(city_name, 25000)
        city_trends = city_info['trends']

        for _ in range(3):
            rooms = random.choice([3, 3.5, 4, 4.5, 5])
            size = int(rooms * random.uniform(20, 28))
            variance = random.uniform(0.7, 1.3)
            price_sqm = int(benchmark * variance)
            price = price_sqm * size

            rental_yield = calculate_rental_yield(price, city_name, size)
            monthly_rent = estimate_monthly_rent(city_name, size)

            roi = calculate_roi_score(
                price_per_sqm=price_sqm,
                city=city_name,
                rental_yield=rental_yield,
                appreciation_trends=city_trends,
                deal_count=random.randint(10, 100),
            )

            properties.append({
                'City': city_name,
                'Rooms': rooms,
                'Size (sqm)': size,
                'Price': f"₪{price:,}",
                'Price/sqm': f"₪{price_sqm:,}",
                'Monthly Rent': f"₪{monthly_rent:,.0f}",
                'Yield %': f"{rental_yield}%",
                'ROI Score': roi['roi_score'],
                '_roi': roi['roi_score'],
            })

    df = pd.DataFrame(properties)
    df = df.sort_values('_roi', ascending=False)

    # Show top 15
    st.dataframe(
        df.drop(columns=['_roi']).head(15),
        use_container_width=True,
        hide_index=True,
    )

    # Distribution chart
    fig = px.histogram(
        df,
        x='_roi',
        nbins=20,
        title='ROI Score Distribution',
        labels={'_roi': 'ROI Score'},
        color_discrete_sequence=['#2ecc71'],
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
