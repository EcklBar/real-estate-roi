"""
OpenStreetMap API Client

Uses Overpass API to fetch amenities (train stations, schools, parks)
for distance calculations and property enrichment.
"""

import logging
import requests
import time
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from math import radians, sin, cos, sqrt, atan2

logger = logging.getLogger(__name__)

OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"

# Israel bounding box (approximate)
ISRAEL_BBOX = {
    "south": 29.5,
    "west": 34.2,
    "north": 33.3,
    "east": 35.9,
}


@dataclass
class Amenity:
    """Represents a point of interest."""

    osm_id: int
    amenity_type: str
    name: Optional[str]
    lat: float
    lon: float
    tags: Dict

    def to_dict(self) -> Dict:
        return asdict(self)


class OpenStreetMapClient:
    """Client for fetching geographic data from OpenStreetMap."""

    def __init__(self, timeout: int = 60):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "RealEstateROI-DataPipeline/1.0"
        })
        self.timeout = timeout
        self.last_request_time = 0
        self.min_request_interval = 1.0

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    def _execute_query(self, query: str) -> Dict:
        """Execute Overpass API query."""
        self._rate_limit()
        try:
            response = self.session.post(
                OVERPASS_API_URL,
                data={"data": query},
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Overpass API request failed: {e}")
            raise

    def _parse_elements(self, data: Dict, amenity_type: str) -> List[Amenity]:
        """Parse Overpass API elements into Amenity objects."""
        amenities = []
        for element in data.get("elements", []):
            lat = element.get("lat") or element.get("center", {}).get("lat")
            lon = element.get("lon") or element.get("center", {}).get("lon")
            if lat and lon:
                amenities.append(Amenity(
                    osm_id=element["id"],
                    amenity_type=amenity_type,
                    name=element.get("tags", {}).get("name"),
                    lat=lat,
                    lon=lon,
                    tags=element.get("tags", {}),
                ))
        return amenities

    def get_train_stations(self, bbox: Dict = None) -> List[Amenity]:
        """Get train/metro/light rail stations in area."""
        bbox = bbox or ISRAEL_BBOX
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          node["railway"="station"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
          node["railway"="halt"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
          node["station"="light_rail"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
        );
        out body;
        """
        logger.info("Fetching train stations from OSM...")
        data = self._execute_query(query)
        stations = self._parse_elements(data, "train_station")
        logger.info(f"Found {len(stations)} train stations")
        return stations

    def get_schools(self, bbox: Dict = None) -> List[Amenity]:
        """Get schools in area."""
        bbox = bbox or ISRAEL_BBOX
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          node["amenity"="school"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
          way["amenity"="school"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
        );
        out center body;
        """
        logger.info("Fetching schools from OSM...")
        data = self._execute_query(query)
        schools = self._parse_elements(data, "school")
        logger.info(f"Found {len(schools)} schools")
        return schools

    def get_parks(self, bbox: Dict = None) -> List[Amenity]:
        """Get parks and green spaces in area."""
        bbox = bbox or ISRAEL_BBOX
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          node["leisure"="park"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
          way["leisure"="park"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
          node["leisure"="garden"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
        );
        out center body;
        """
        logger.info("Fetching parks from OSM...")
        data = self._execute_query(query)
        parks = self._parse_elements(data, "park")
        logger.info(f"Found {len(parks)} parks")
        return parks

    def get_all_amenities_for_city(self, city_bbox: Dict) -> Dict[str, List[Amenity]]:
        """Get all relevant amenities for a city."""
        return {
            "train_stations": self.get_train_stations(bbox=city_bbox),
            "schools": self.get_schools(bbox=city_bbox),
            "parks": self.get_parks(bbox=city_bbox),
        }


# ============================================
# Distance Calculations
# ============================================

def haversine_distance(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """
    Calculate distance between two points using Haversine formula.

    Returns:
        Distance in kilometers.
    """
    R = 6371  # Earth radius in km
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def find_nearest_amenity(
    lat: float, lon: float, amenities: List[Amenity]
) -> Tuple[Optional[Amenity], Optional[float]]:
    """Find the nearest amenity to a given point. Returns (amenity, distance_km)."""
    if not amenities or lat is None or lon is None:
        return None, None

    nearest = None
    min_distance = float('inf')
    for amenity in amenities:
        distance = haversine_distance(lat, lon, amenity.lat, amenity.lon)
        if distance < min_distance:
            min_distance = distance
            nearest = amenity

    return nearest, round(min_distance, 2)


def calculate_distances_for_property(
    lat: float, lon: float, amenities: Dict[str, List[Amenity]]
) -> Dict[str, Optional[float]]:
    """
    Calculate distances from a property to nearest amenities.

    Returns:
        Dictionary with distance to nearest of each type in km.
    """
    distances = {}
    for amenity_type, amenity_list in amenities.items():
        _, distance = find_nearest_amenity(lat, lon, amenity_list)
        distances[f"dist_to_{amenity_type}_km"] = distance
    return distances


# ============================================
# City Bounding Boxes
# ============================================

CITY_BBOXES = {
    "תל אביב יפו": {"south": 32.03, "west": 34.74, "north": 32.15, "east": 34.82},
    "ירושלים": {"south": 31.73, "west": 35.13, "north": 31.87, "east": 35.27},
    "חיפה": {"south": 32.77, "west": 34.95, "north": 32.87, "east": 35.07},
    "באר שבע": {"south": 31.21, "west": 34.75, "north": 31.30, "east": 34.85},
    "ראשון לציון": {"south": 31.93, "west": 34.75, "north": 32.00, "east": 34.82},
    "פתח תקווה": {"south": 32.07, "west": 34.85, "north": 32.13, "east": 34.93},
    "נתניה": {"south": 32.28, "west": 34.83, "north": 32.36, "east": 34.90},
    "חולון": {"south": 32.00, "west": 34.76, "north": 32.04, "east": 34.81},
    "רמת גן": {"south": 32.06, "west": 34.80, "north": 32.11, "east": 34.85},
    "אשדוד": {"south": 31.77, "west": 34.62, "north": 31.84, "east": 34.69},
    "הרצליה": {"south": 32.14, "west": 34.77, "north": 32.19, "east": 34.83},
    "רעננה": {"south": 32.17, "west": 34.85, "north": 32.21, "east": 34.89},
    "כפר סבא": {"south": 32.16, "west": 34.88, "north": 32.20, "east": 34.93},
}


def get_city_bbox(city: str) -> Optional[Dict]:
    """Get bounding box for a city."""
    return CITY_BBOXES.get(city)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    client = OpenStreetMapClient()

    print("=== Fetching Tel Aviv Amenities ===\n")
    bbox = CITY_BBOXES["תל אביב יפו"]
    stations = client.get_train_stations(bbox=bbox)

    print(f"Found {len(stations)} train stations:")
    for s in stations[:5]:
        print(f"  - {s.name}: ({s.lat}, {s.lon})")

    print("\n=== Distance Calculation Example ===")
    property_lat, property_lon = 32.0853, 34.7818  # Central Tel Aviv
    if stations:
        nearest, distance = find_nearest_amenity(property_lat, property_lon, stations)
        print(f"Property location: ({property_lat}, {property_lon})")
        print(f"Nearest station: {nearest.name} at {distance} km")
