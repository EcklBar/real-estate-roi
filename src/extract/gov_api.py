"""
Gov.il Real Estate Transactions API Client

Dual-source data extraction:
1. CloudFront CDN API (nadlan.gov.il) -- settlement/neighborhood price trends (open access)
2. Selenium scraper (nadlan.gov.il) -- individual transaction records

Data source: https://www.nadlan.gov.il/
"""

import logging
import requests
import warnings
import json
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

# ============================================
# CloudFront CDN API (Open Access - Trends)
# ============================================

CONFIG_URL = "https://www.nadlan.gov.il/config.json"
S3_BASE = "https://d30nq1hiio0r3z.cloudfront.net/api"
S3_PAGES = f"{S3_BASE}/pages"

# Common settlement (city) codes
SETTLEMENT_CODES = {
    "תל אביב -יפו": 5000,
    "ירושלים": 3000,
    "חיפה": 4000,
    "באר שבע": 9000,
    "ראשון לציון": 8300,
    "פתח תקווה": 7900,
    "נתניה": 7400,
    "חולון": 6600,
    "רמת גן": 8600,
    "אשדוד": 70,
    "הרצליה": 6400,
    "רעננה": 8700,
    "כפר סבא": 6900,
    "בת ים": 6200,
    "מודיעין-מכבים-רעות": 1200,
    "גבעתיים": 6300,
    "רמת השרון": 2650,
}


def get_config() -> dict:
    """Fetch the live config from nadlan.gov.il (contains all base URLs)."""
    r = requests.get(CONFIG_URL, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def get_settlement_data(settlement_id: int) -> dict:
    """
    Get settlement (city) data including price trends.

    Args:
        settlement_id: City code (e.g. 5000=Tel Aviv, 3000=Haifa)

    Returns:
        dict with keys: settlementID, settlementName, otherNeighborhoods,
                        otherSettlmentStreets, trends
    """
    url = f"{S3_PAGES}/settlement/buy/{settlement_id}.json"
    r = requests.get(url, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def get_neighborhood_data(neighborhood_id: int) -> dict:
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


def get_neighborhoods_index() -> dict:
    """Get index of all neighborhoods in Israel (1700+)."""
    url = f"{S3_BASE}/index/neigh.json"
    r = requests.get(url, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def get_settlement_info(settlement_id: int) -> dict:
    """Get additional info about a settlement."""
    url = f"{S3_BASE}/additional_info/settlements/{settlement_id}.json"
    r = requests.get(url, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def get_neighborhood_info(neighborhood_id: int) -> dict:
    """Get additional info about a neighborhood."""
    url = f"{S3_BASE}/additional_info/neighborhoods/{neighborhood_id}.json"
    r = requests.get(url, verify=False, timeout=10)
    r.raise_for_status()
    return r.json()


def extract_price_trends(data: dict) -> List[dict]:
    """
    Extract price trends from settlement or neighborhood data.

    Returns list of dicts with: numRooms, year, month,
                                settlementPrice, countryPrice,
                                neighborhoodPrice (if neighborhood data)
    """
    trends = []
    for room_category in data.get('trends', {}).get('rooms', []):
        num_rooms = room_category.get('numRooms', 'all')

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


def fetch_all_settlements_trends(
    settlement_ids: Optional[List[int]] = None,
) -> List[dict]:
    """
    Fetch price trends for multiple settlements.

    Args:
        settlement_ids: List of settlement IDs. If None, uses all known codes.

    Returns:
        List of trend records with settlement info attached.
    """
    if settlement_ids is None:
        settlement_ids = list(SETTLEMENT_CODES.values())

    all_trends = []
    for sid in settlement_ids:
        try:
            data = get_settlement_data(sid)
            trends = extract_price_trends(data)
            for t in trends:
                t['settlementID'] = sid
                t['settlementName'] = data.get('settlementName', '')
            all_trends.extend(trends)
            logger.info(f"Fetched {len(trends)} trends for settlement {sid}")
            time.sleep(0.3)  # rate limiting
        except Exception as e:
            logger.warning(f"Failed to fetch settlement {sid}: {e}")

    return all_trends


# ============================================
# Selenium Scraper (Individual Deals)
# ============================================

class NadlanScraper:
    """
    Scrapes individual real estate transaction records from nadlan.gov.il.

    Uses Selenium to load the JavaScript-rendered page, then
    BeautifulSoup to parse the transaction table.
    """

    def __init__(self, headless: bool = True):
        """
        Initialize the scraper.

        Args:
            headless: Run browser in headless mode (no GUI).
        """
        self.headless = headless
        self._driver = None

    def _get_driver(self):
        """Create and return a Selenium WebDriver."""
        if self._driver is None:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options

            options = Options()
            if self.headless:
                options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument(
                "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )

            self._driver = webdriver.Chrome(options=options)
            self._driver.implicitly_wait(10)

        return self._driver

    def close(self):
        """Close the browser."""
        if self._driver:
            self._driver.quit()
            self._driver = None

    def scrape_deals(self, search_query: str, max_scroll: int = 10) -> List[dict]:
        """
        Scrape deals for a given search query.

        Args:
            search_query: Address, city, or neighborhood to search.
            max_scroll: Maximum number of scroll iterations to load more data.

        Returns:
            List of parsed transaction records.
        """
        from bs4 import BeautifulSoup

        driver = self._get_driver()
        url = f"https://www.nadlan.gov.il/?search={search_query}"

        logger.info(f"Scraping deals for: {search_query}")
        driver.get(url)
        time.sleep(3)  # Wait for initial load

        # Scroll to load all results
        last_height = driver.execute_script("return document.body.scrollHeight")
        for i in range(max_scroll):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        # Parse HTML
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        rows = soup.find_all("div", class_="tableRow")

        records = []
        for row in rows:
            cols = row.find_all("div", class_="tableCol")
            if len(cols) >= 7:
                record = self._parse_row(cols, search_query)
                if record:
                    records.append(record)

        logger.info(f"Scraped {len(records)} deals for '{search_query}'")
        return records

    def _parse_row(self, cols: list, search_query: str) -> Optional[dict]:
        """Parse a single table row into a deal record."""
        try:
            col_texts = [c.text.strip() for c in cols]

            # Parse price
            price_str = col_texts[7].replace(',', '').replace('₪', '').strip() if len(col_texts) > 7 else ''
            price = float(price_str) if price_str.isdigit() else None

            # Parse size
            size_str = col_texts[6].strip() if len(col_texts) > 6 else ''
            size_sqm = float(size_str) if size_str.replace('.', '').isdigit() else None

            # Parse rooms
            rooms_str = col_texts[4].strip() if len(col_texts) > 4 else ''
            rooms = float(rooms_str) if rooms_str.replace('.', '').isdigit() else None

            # Parse floor
            floor_str = col_texts[5].strip() if len(col_texts) > 5 else ''
            floor = int(float(floor_str)) if floor_str.replace('.', '').isdigit() else None

            # Parse date
            deal_date = None
            if col_texts[0]:
                try:
                    deal_date = datetime.strptime(col_texts[0], "%d/%m/%Y").date()
                except ValueError:
                    pass

            price_per_sqm = None
            if price and size_sqm and size_sqm > 0:
                price_per_sqm = round(price / size_sqm, 2)

            return {
                'transaction_date': deal_date,
                'address': col_texts[1] if len(col_texts) > 1 else '',
                'gush_helka': col_texts[2] if len(col_texts) > 2 else '',
                'property_type': col_texts[3] if len(col_texts) > 3 else '',
                'rooms': rooms,
                'floor': floor,
                'size_sqm': size_sqm,
                'price': price,
                'price_per_sqm': price_per_sqm,
                'change_percent': col_texts[8] if len(col_texts) > 8 else '',
                'search_query': search_query,
                'scraped_at': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f"Failed to parse row: {e}")
            return None

    def scrape_multiple_cities(
        self, cities: List[str], max_scroll: int = 10
    ) -> List[dict]:
        """
        Scrape deals for multiple cities.

        Args:
            cities: List of city names (Hebrew).
            max_scroll: Max scroll iterations per city.

        Returns:
            Combined list of all deal records.
        """
        all_records = []
        for city in cities:
            try:
                records = self.scrape_deals(city, max_scroll=max_scroll)
                all_records.extend(records)
                time.sleep(2)  # Rate limiting between cities
            except Exception as e:
                logger.error(f"Failed to scrape {city}: {e}")

        return all_records


# ============================================
# Unified Record Parser
# ============================================

def parse_transaction_record(record: dict) -> dict:
    """
    Normalize a scraped transaction record into a standard format.

    Field mapping:
    - price: Deal amount in ILS
    - transaction_date: Date of the deal
    - property_type: Type of property
    - rooms: Number of rooms
    - floor: Floor number
    - size_sqm: Size in square meters
    - price_per_sqm: Price per square meter

    Returns:
        Normalized dictionary with consistent field names.
    """
    def safe_float(value, default=None):
        try:
            return float(value) if value else default
        except (ValueError, TypeError):
            return default

    def safe_int(value, default=None):
        try:
            return int(float(value)) if value else default
        except (ValueError, TypeError):
            return default

    price = safe_float(record.get('price'))
    size_sqm = safe_float(record.get('size_sqm'))
    price_per_sqm = None
    if price and size_sqm and size_sqm > 0:
        price_per_sqm = round(price / size_sqm, 2)

    return {
        'city': record.get('city', '').strip(),
        'street': record.get('street', '').strip(),
        'house_number': str(record.get('house_number', '')).strip(),
        'address': record.get('address', '').strip(),
        'gush_helka': record.get('gush_helka', ''),
        'property_type': record.get('property_type', '').strip(),
        'rooms': safe_float(record.get('rooms')),
        'floor': safe_int(record.get('floor')),
        'total_floors': safe_int(record.get('total_floors')),
        'size_sqm': size_sqm,
        'year_built': safe_int(record.get('year_built')),
        'price': price,
        'price_per_sqm': price_per_sqm,
        'transaction_date': record.get('transaction_date'),
        'deal_nature': record.get('deal_nature', '').strip(),
        'is_new_project': record.get('is_new_project', False),
        'source_id': (
            f"{record.get('gush_helka', '')}_{record.get('transaction_date', '')}"
        ),
        'scraped_at': record.get('scraped_at'),
    }


# ============================================
# Example Usage
# ============================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Demo: CloudFront Trends API ---
    print("=" * 60)
    print("  Nadlan.gov.il API - Real Estate Data")
    print("=" * 60)

    print("\n>>> Fetching Tel Aviv (5000) data...")
    tlv = get_settlement_data(5000)
    print(f"City: {tlv['settlementName']}")
    print(f"Neighborhoods: {len(tlv['otherNeighborhoods'])}")
    print(f"Streets: {len(tlv['otherSettlmentStreets'])}")

    trends = extract_price_trends(tlv)
    print(f"\nPrice trends ({len(trends)} data points):")

    from itertools import groupby
    trends_with_price = [t for t in trends if t['settlementPrice']]
    for num_rooms, group in groupby(
        sorted(trends_with_price, key=lambda x: str(x['numRooms'])),
        key=lambda x: x['numRooms']
    ):
        items = sorted(group, key=lambda x: (x['year'], x['month']), reverse=True)
        latest = items[0]
        oldest = items[-1]
        print(f"\n  {num_rooms} rooms:")
        print(f"    Latest ({latest['year']}/{latest['month']:02d}): "
              f"{latest['settlementPrice']:>12,.0f} ILS "
              f"(country avg: {latest['countryPrice']:>10,.0f})")
        print(f"    Oldest ({oldest['year']}/{oldest['month']:02d}): "
              f"{oldest['settlementPrice']:>12,.0f} ILS")
        if oldest['settlementPrice'] and latest['settlementPrice']:
            change = (
                (latest['settlementPrice'] - oldest['settlementPrice'])
                / oldest['settlementPrice']
            ) * 100
            print(f"    Change: {change:+.1f}%")

    print(f"\n>>> First 10 neighborhoods in {tlv['settlementName']}:")
    for n in tlv['otherNeighborhoods'][:10]:
        print(f"  - {n['title']} (ID: {n['id']})")
