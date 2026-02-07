"""
Feature Engineering - ROI Score Calculation

Calculates a composite ROI score (1-100) for real estate properties
based on multiple factors:
1. Price vs market benchmark (is it a good deal?)
2. Price appreciation trend (is the area growing?)
3. Estimated rental yield
4. Proximity to transit/amenities
5. Market liquidity (how many deals in the area?)
"""

import logging
from typing import Optional, Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================
# Rental Yield Estimation
# ============================================

# Average monthly rent per sqm by city (approximate, ILS)
RENT_PER_SQM = {
    "תל אביב יפו": 75,
    "ירושלים": 55,
    "חיפה": 38,
    "באר שבע": 32,
    "ראשון לציון": 52,
    "פתח תקווה": 48,
    "נתניה": 45,
    "חולון": 50,
    "רמת גן": 55,
    "אשדוד": 38,
    "הרצליה": 65,
    "רעננה": 58,
    "כפר סבא": 50,
    "בת ים": 48,
    "גבעתיים": 58,
    "רמת השרון": 60,
    "מודיעין-מכבים-רעות": 48,
}

DEFAULT_RENT_PER_SQM = 45


def estimate_monthly_rent(city: str, size_sqm: float) -> float:
    """
    Estimate monthly rent for a property.

    Args:
        city: Normalized city name.
        size_sqm: Property size in square meters.

    Returns:
        Estimated monthly rent in ILS.
    """
    rent_per_sqm = RENT_PER_SQM.get(city, DEFAULT_RENT_PER_SQM)
    return rent_per_sqm * size_sqm


def calculate_rental_yield(
    price: float, city: str, size_sqm: float
) -> float:
    """
    Calculate annual rental yield percentage.

    Args:
        price: Property purchase price in ILS.
        city: Normalized city name.
        size_sqm: Property size in square meters.

    Returns:
        Annual rental yield as percentage (e.g. 3.5 = 3.5%).
    """
    if not price or price <= 0 or not size_sqm or size_sqm <= 0:
        return 0.0

    annual_rent = estimate_monthly_rent(city, size_sqm) * 12
    return round((annual_rent / price) * 100, 2)


# ============================================
# Price Benchmark Scoring
# ============================================

# Average price per sqm by city (approximate, from nadlan.gov.il trends)
BENCHMARK_PRICE_PER_SQM = {
    "תל אביב יפו": 45000,
    "ירושלים": 32000,
    "חיפה": 20000,
    "באר שבע": 14000,
    "ראשון לציון": 28000,
    "פתח תקווה": 26000,
    "נתניה": 24000,
    "חולון": 25000,
    "רמת גן": 33000,
    "אשדוד": 18000,
    "הרצליה": 42000,
    "רעננה": 35000,
    "כפר סבא": 28000,
    "בת ים": 24000,
    "גבעתיים": 35000,
    "רמת השרון": 38000,
    "מודיעין-מכבים-רעות": 24000,
}


def score_price_vs_benchmark(
    price_per_sqm: float, city: str
) -> float:
    """
    Score how the property price compares to market benchmark.

    Returns:
        Score 0-100. Higher = better deal (below market price).
    """
    benchmark = BENCHMARK_PRICE_PER_SQM.get(city)
    if not benchmark or not price_per_sqm:
        return 50.0  # neutral

    # Calculate percentage difference from benchmark
    diff_pct = ((benchmark - price_per_sqm) / benchmark) * 100

    # Map to 0-100 score
    # +20% below market = 100, at market = 50, +20% above = 0
    score = 50 + (diff_pct * 2.5)
    return max(0, min(100, round(score, 1)))


# ============================================
# Appreciation Trend Scoring
# ============================================

def score_appreciation_trend(trends: List[dict]) -> float:
    """
    Score the price appreciation trend for a location.

    Args:
        trends: List of price trend dicts with year, month, settlementPrice.

    Returns:
        Score 0-100. Higher = stronger appreciation.
    """
    prices = [
        t for t in trends
        if t.get('settlementPrice') is not None
    ]

    if len(prices) < 2:
        return 50.0  # neutral

    # Sort by date
    prices = sorted(prices, key=lambda x: (x['year'], x['month']))

    # Calculate year-over-year change
    oldest = prices[0]['settlementPrice']
    latest = prices[-1]['settlementPrice']

    if oldest <= 0:
        return 50.0

    total_change = ((latest - oldest) / oldest) * 100
    years = max(1, (prices[-1]['year'] - prices[0]['year']))
    annual_change = total_change / years

    # Map to 0-100
    # 10%+ annual = 100, 5% = 75, 0% = 50, -5% = 25, -10% = 0
    score = 50 + (annual_change * 5)
    return max(0, min(100, round(score, 1)))


# ============================================
# Transit Proximity Scoring
# ============================================

def score_transit_proximity(dist_to_train_km: Optional[float]) -> float:
    """
    Score proximity to public transit.

    Args:
        dist_to_train_km: Distance to nearest train station in km.

    Returns:
        Score 0-100. Higher = closer to transit.
    """
    if dist_to_train_km is None:
        return 50.0

    # Under 500m = 100, 1km = 80, 2km = 50, 5km = 10, 10km+ = 0
    if dist_to_train_km <= 0.5:
        return 100.0
    elif dist_to_train_km <= 1.0:
        return 80.0
    elif dist_to_train_km <= 2.0:
        return 60.0
    elif dist_to_train_km <= 5.0:
        return 30.0
    else:
        return max(0, 100 - (dist_to_train_km * 10))


# ============================================
# Amenity Scoring
# ============================================

def score_amenities(distances: Dict[str, Optional[float]]) -> float:
    """
    Score proximity to amenities (schools, parks, etc.).

    Args:
        distances: Dict with dist_to_X_km values.

    Returns:
        Score 0-100.
    """
    scores = []

    # Schools
    school_dist = distances.get('dist_to_schools_km')
    if school_dist is not None:
        if school_dist <= 0.5:
            scores.append(100)
        elif school_dist <= 1.0:
            scores.append(75)
        elif school_dist <= 2.0:
            scores.append(50)
        else:
            scores.append(max(0, 100 - school_dist * 20))
    else:
        scores.append(50)

    # Parks
    park_dist = distances.get('dist_to_parks_km')
    if park_dist is not None:
        if park_dist <= 0.3:
            scores.append(100)
        elif park_dist <= 0.7:
            scores.append(75)
        elif park_dist <= 1.5:
            scores.append(50)
        else:
            scores.append(max(0, 100 - park_dist * 25))
    else:
        scores.append(50)

    return round(sum(scores) / len(scores), 1) if scores else 50.0


# ============================================
# Market Liquidity Scoring
# ============================================

def score_liquidity(deal_count: int, months: int = 12) -> float:
    """
    Score market liquidity based on deal count.

    Args:
        deal_count: Number of deals in the area.
        months: Time period for the deal count.

    Returns:
        Score 0-100. Higher = more liquid market.
    """
    monthly_avg = deal_count / max(1, months)

    # 50+ deals/month = 100, 20 = 75, 5 = 40, 1 = 15, 0 = 0
    if monthly_avg >= 50:
        return 100.0
    elif monthly_avg >= 20:
        return 75.0
    elif monthly_avg >= 5:
        return 50.0
    elif monthly_avg >= 1:
        return 25.0
    else:
        return 5.0


# ============================================
# Composite ROI Score
# ============================================

# Weight configuration for ROI score
ROI_WEIGHTS = {
    "price_benchmark": 0.30,
    "appreciation": 0.25,
    "rental_yield": 0.20,
    "transit": 0.10,
    "amenities": 0.10,
    "liquidity": 0.05,
}


def calculate_roi_score(
    price_per_sqm: Optional[float] = None,
    city: str = "",
    rental_yield: Optional[float] = None,
    appreciation_trends: Optional[List[dict]] = None,
    dist_to_train_km: Optional[float] = None,
    amenity_distances: Optional[Dict] = None,
    deal_count: int = 0,
) -> Dict:
    """
    Calculate composite ROI score (1-100).

    Args:
        price_per_sqm: Property price per square meter.
        city: Normalized city name.
        rental_yield: Annual rental yield percentage.
        appreciation_trends: Price trend data for the area.
        dist_to_train_km: Distance to nearest train station.
        amenity_distances: Dict with distances to amenities.
        deal_count: Number of deals in the area (12 months).

    Returns:
        Dictionary with overall score and component scores.
    """
    # Calculate component scores
    price_score = score_price_vs_benchmark(price_per_sqm, city)

    appreciation_score = score_appreciation_trend(
        appreciation_trends or []
    )

    # Rental yield scoring (3%+ = 80, 2% = 50, 1% = 20)
    yield_score = 50.0
    if rental_yield is not None:
        yield_score = min(100, max(0, rental_yield * 25))

    transit_score = score_transit_proximity(dist_to_train_km)
    amenity_score = score_amenities(amenity_distances or {})
    liquidity_score = score_liquidity(deal_count)

    # Calculate weighted composite
    composite = (
        price_score * ROI_WEIGHTS["price_benchmark"]
        + appreciation_score * ROI_WEIGHTS["appreciation"]
        + yield_score * ROI_WEIGHTS["rental_yield"]
        + transit_score * ROI_WEIGHTS["transit"]
        + amenity_score * ROI_WEIGHTS["amenities"]
        + liquidity_score * ROI_WEIGHTS["liquidity"]
    )

    return {
        "roi_score": round(composite, 1),
        "price_benchmark_score": price_score,
        "appreciation_score": appreciation_score,
        "rental_yield_score": yield_score,
        "transit_score": transit_score,
        "amenity_score": amenity_score,
        "liquidity_score": liquidity_score,
        "rental_yield_pct": rental_yield,
        "calculated_at": datetime.now().isoformat(),
    }


def enrich_record_with_roi(
    record: dict,
    trends: Optional[List[dict]] = None,
    amenity_distances: Optional[Dict] = None,
    deal_count: int = 0,
) -> dict:
    """
    Enrich a transaction record with ROI metrics.

    Args:
        record: Parsed transaction record.
        trends: Price trends for the area.
        amenity_distances: Distances to amenities.
        deal_count: Number of deals in area.

    Returns:
        Record with ROI fields added.
    """
    result = record.copy()

    city = record.get('normalized_city') or record.get('city', '')
    price = record.get('price')
    size_sqm = record.get('size_sqm')
    price_per_sqm = record.get('price_per_sqm')

    # Calculate rental yield
    rental_yield = None
    monthly_rent = None
    if price and size_sqm:
        rental_yield = calculate_rental_yield(price, city, size_sqm)
        monthly_rent = estimate_monthly_rent(city, size_sqm)

    result['estimated_monthly_rent'] = monthly_rent
    result['rental_yield_pct'] = rental_yield

    # Calculate ROI score
    roi = calculate_roi_score(
        price_per_sqm=price_per_sqm,
        city=city,
        rental_yield=rental_yield,
        appreciation_trends=trends,
        dist_to_train_km=(
            amenity_distances.get('dist_to_train_stations_km')
            if amenity_distances else None
        ),
        amenity_distances=amenity_distances,
        deal_count=deal_count,
    )

    result.update(roi)
    return result


if __name__ == "__main__":
    # Demo
    print("=== ROI Score Calculation Demo ===\n")

    test_properties = [
        {
            "city": "תל אביב יפו",
            "price": 2_500_000,
            "size_sqm": 70,
            "price_per_sqm": 35714,
        },
        {
            "city": "באר שבע",
            "price": 800_000,
            "size_sqm": 80,
            "price_per_sqm": 10000,
        },
        {
            "city": "הרצליה",
            "price": 5_000_000,
            "size_sqm": 100,
            "price_per_sqm": 50000,
        },
    ]

    for prop in test_properties:
        rental_yield = calculate_rental_yield(
            prop['price'], prop['city'], prop['size_sqm']
        )
        monthly_rent = estimate_monthly_rent(prop['city'], prop['size_sqm'])

        roi = calculate_roi_score(
            price_per_sqm=prop['price_per_sqm'],
            city=prop['city'],
            rental_yield=rental_yield,
            dist_to_train_km=1.5,
            deal_count=30,
        )

        print(f"Property: {prop['city']} | {prop['size_sqm']}sqm | "
              f"{prop['price']:,} ILS")
        print(f"  Monthly rent estimate: {monthly_rent:,.0f} ILS")
        print(f"  Rental yield: {rental_yield}%")
        print(f"  ROI Score: {roi['roi_score']}/100")
        print(f"    Price benchmark: {roi['price_benchmark_score']}")
        print(f"    Transit:         {roi['transit_score']}")
        print(f"    Rental yield:    {roi['rental_yield_score']}")
        print()
