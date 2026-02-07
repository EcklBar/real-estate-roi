"""
Nadlan.gov.il API Client

Fetches real estate data from the Israeli government's Nadlan website.
Data is served from CloudFront CDN as static JSON files.

Available data:
- Settlement (city) price trends by room count
- Neighborhood price trends by room count
- List of all neighborhoods (1700+)
- List of streets per city/neighborhood
"""

import requests
import warnings
import json

warnings.filterwarnings('ignore')

# Base URLs from nadlan.gov.il config
CONFIG_URL = "https://www.nadlan.gov.il/config.json"
S3_BASE = "https://d30nq1hiio0r3z.cloudfront.net/api"
S3_PAGES = f"{S3_BASE}/pages"


def get_config():
    """Fetch the live config from nadlan.gov.il (contains all base URLs)"""
    r = requests.get(CONFIG_URL, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def get_settlement_data(settlement_id):
    """
    Get settlement (city) data including price trends.

    Args:
        settlement_id: City code (e.g. 5000=Tel Aviv, 3000=Haifa, 7900=Jerusalem)

    Returns:
        dict with keys: settlementID, settlementName, otherNeighborhoods,
                        otherSettlmentStreets, trends
    """
    url = f"{S3_PAGES}/settlement/buy/{settlement_id}.json"
    r = requests.get(url, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def get_neighborhood_data(neighborhood_id):
    """
    Get neighborhood data including price trends.

    Args:
        neighborhood_id: Neighborhood ID (find via get_settlement_data)

    Returns:
        dict with keys: neighborhoodId, neighborhoodName, settlementID,
                        settlementName, trends, otherNeighborhoodStreets
    """
    url = f"{S3_PAGES}/neighborhood/buy/{neighborhood_id}.json"
    r = requests.get(url, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def get_neighborhoods_index():
    """Get index of all neighborhoods in Israel (1700+)"""
    url = f"{S3_BASE}/index/neigh.json"
    r = requests.get(url, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def get_settlement_info(settlement_id):
    """Get additional info about a settlement (demographics, etc.)"""
    url = f"{S3_BASE}/additional_info/settlements/{settlement_id}.json"
    r = requests.get(url, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def get_neighborhood_info(neighborhood_id):
    """Get additional info about a neighborhood"""
    url = f"{S3_BASE}/additional_info/neighborhoods/{neighborhood_id}.json"
    r = requests.get(url, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def extract_price_trends(data):
    """
    Extract price trends from settlement or neighborhood data.

    Returns list of dicts with: numRooms, year, month,
                                settlementPrice, countryPrice,
                                neighborhoodPrice (if neighborhood data)
    """
    trends = []
    for room_category in data.get('trends', {}).get('rooms', []):
        num_rooms = room_category.get('numRooms', 'all')
        summary = room_category.get('summary', {})

        for point in room_category.get('graphData', []):
            trend = {
                'numRooms': num_rooms,
                'year': point.get('year'),
                'month': point.get('month'),
                'settlementPrice': point.get('settlementPrice'),
                'countryPrice': point.get('countryPrice'),
                'neighborhoodPrice': point.get('neighborhoodPrice'),
            }
            trends.append(trend)

    return trends


if __name__ == "__main__":
    # --- Demo: Tel Aviv price trends ---
    print("=" * 60)
    print("  Nadlan.gov.il API - Real Estate Data")
    print("=" * 60)

    # 1. Get Tel Aviv data
    print("\n>>> Fetching Tel Aviv (5000) data...")
    tlv = get_settlement_data(5000)
    print(f"City: {tlv['settlementName']}")
    print(f"Neighborhoods: {len(tlv['otherNeighborhoods'])}")
    print(f"Streets: {len(tlv['otherSettlmentStreets'])}")

    # 2. Show price trends
    trends = extract_price_trends(tlv)
    print(f"\nPrice trends ({len(trends)} data points):")

    # Group by room count, show latest
    from itertools import groupby
    trends_with_price = [t for t in trends if t['settlementPrice']]
    for num_rooms, group in groupby(sorted(trends_with_price, key=lambda x: str(x['numRooms'])), key=lambda x: x['numRooms']):
        items = sorted(group, key=lambda x: (x['year'], x['month']), reverse=True)
        latest = items[0]
        oldest = items[-1]
        print(f"\n  {num_rooms} rooms:")
        print(f"    Latest ({latest['year']}/{latest['month']:02d}): {latest['settlementPrice']:>12,.0f} ILS (country avg: {latest['countryPrice']:>10,.0f})")
        print(f"    Oldest ({oldest['year']}/{oldest['month']:02d}): {oldest['settlementPrice']:>12,.0f} ILS")
        if oldest['settlementPrice'] and latest['settlementPrice']:
            change = ((latest['settlementPrice'] - oldest['settlementPrice']) / oldest['settlementPrice']) * 100
            print(f"    Change: {change:+.1f}%")

    # 3. Show some neighborhoods
    print(f"\n>>> First 10 neighborhoods in {tlv['settlementName']}:")
    for n in tlv['otherNeighborhoods'][:10]:
        print(f"  - {n['title']} (ID: {n['id']})")

    # 4. Get a neighborhood
    first_neigh = tlv['otherNeighborhoods'][0]
    print(f"\n>>> Fetching neighborhood: {first_neigh['title']}...")
    neigh = get_neighborhood_data(first_neigh['id'])
    neigh_trends = extract_price_trends(neigh)
    neigh_with_price = [t for t in neigh_trends if t.get('neighborhoodPrice')]
    if neigh_with_price:
        latest = sorted(neigh_with_price, key=lambda x: (x['year'], x['month']), reverse=True)[0]
        print(f"  Latest price ({latest['year']}/{latest['month']:02d}): {latest['neighborhoodPrice']:,.0f} ILS ({latest['numRooms']} rooms)")
