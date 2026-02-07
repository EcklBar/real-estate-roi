"""
Hebrew Address Normalizer

Handles inconsistent Israeli address formats:
- City name variations (e.g. "ת"א" -> "תל אביב יפו")
- Street prefix removal (e.g. "רח' הרצל" -> "הרצל")
- Whitespace and punctuation normalization
"""

import re
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)

# ============================================
# City Name Normalization
# ============================================

CITY_ALIASES = {
    # Tel Aviv
    'ת"א': 'תל אביב יפו',
    "ת״א": "תל אביב יפו",
    "תל אביב": "תל אביב יפו",
    "תל-אביב": "תל אביב יפו",
    "תל אביב-יפו": "תל אביב יפו",
    "תל אביב -יפו": "תל אביב יפו",
    "תל-אביב-יפו": "תל אביב יפו",
    "יפו": "תל אביב יפו",
    # Jerusalem
    "י-ם": "ירושלים",
    "ירושלם": "ירושלים",
    # Haifa
    "חיפא": "חיפה",
    # Beer Sheva
    "ב"ש": "באר שבע",
    "ב״ש": "באר שבע",
    "באר-שבע": "באר שבע",
    "בארשבע": "באר שבע",
    "בר שבע": "באר שבע",
    "בא"ש": "באר שבע",
    # Rishon LeZion
    "ראשל"צ": "ראשון לציון",
    "ראשל״צ": "ראשון לציון",
    "ראשון": "ראשון לציון",
    "ראשון-לציון": "ראשון לציון",
    # Petah Tikva
    "פ"ת": "פתח תקווה",
    "פ״ת": "פתח תקווה",
    "פתח-תקוה": "פתח תקווה",
    "פתח תקוה": "פתח תקווה",
    "פ.ת.": "פתח תקווה",
    # Netanya
    "נתניא": "נתניה",
    # Ramat Gan
    "ר"ג": "רמת גן",
    "ר״ג": "רמת גן",
    "רמת-גן": "רמת גן",
    # Givatayim
    "גבעתים": "גבעתיים",
    "גבעת-יים": "גבעתיים",
    # Bat Yam
    "בת-ים": "בת ים",
    # Ramat HaSharon
    "רמה"ש": "רמת השרון",
    "רמת-השרון": "רמת השרון",
    # Modi'in
    "מודיעין": "מודיעין-מכבים-רעות",
    "מודיעין מכבים רעות": "מודיעין-מכבים-רעות",
    # Kfar Saba
    "כ"ס": "כפר סבא",
    "כ״ס": "כפר סבא",
    "כפר-סבא": "כפר סבא",
    # Herzliya
    "הרצלייה": "הרצליה",
    "הרצליא": "הרצליה",
}

# ============================================
# Street Normalization
# ============================================

# Prefixes to remove
STREET_PREFIXES = [
    "רח'",
    "רח׳",
    'רח"',
    "רח ",
    "רחוב ",
    "שד'",
    "שד׳",
    "שד ",
    "שדרות ",
    "שדרת ",
    "דרך ",
    "כיכר ",
    "סמטת ",
    "סמ' ",
    "סמ׳ ",
    "מעלות ",
]


def normalize_city(city: Optional[str]) -> Optional[str]:
    """
    Normalize a city name to standard form.

    Args:
        city: Raw city name (Hebrew).

    Returns:
        Normalized city name or original if no alias found.
    """
    if not city:
        return city

    city = city.strip()

    # Direct alias lookup
    if city in CITY_ALIASES:
        return CITY_ALIASES[city]

    # Case-insensitive / whitespace-normalized lookup
    normalized = re.sub(r'\s+', ' ', city).strip()
    if normalized in CITY_ALIASES:
        return CITY_ALIASES[normalized]

    return normalized


def normalize_street(street: Optional[str]) -> Optional[str]:
    """
    Normalize a street name by removing common prefixes.

    Args:
        street: Raw street name (Hebrew).

    Returns:
        Normalized street name without prefix.
    """
    if not street:
        return street

    street = street.strip()

    for prefix in STREET_PREFIXES:
        if street.startswith(prefix):
            street = street[len(prefix):].strip()
            break

    # Remove extra whitespace
    street = re.sub(r'\s+', ' ', street).strip()

    return street


def normalize_house_number(house_number: Optional[str]) -> Optional[str]:
    """
    Normalize a house number.

    Handles cases like "12א", "12-14", "12/3".
    """
    if not house_number:
        return house_number

    house_number = str(house_number).strip()

    # Remove trailing dots
    house_number = house_number.rstrip('.')

    return house_number if house_number else None


def normalize_address(record: dict) -> dict:
    """
    Normalize all address fields in a transaction record.

    Args:
        record: Transaction record dict with city, street, house_number fields.

    Returns:
        New dict with normalized address fields added.
    """
    result = record.copy()

    # Normalize city
    raw_city = record.get('city', '') or ''
    result['normalized_city'] = normalize_city(raw_city)

    # Normalize street
    raw_street = record.get('street', '') or ''
    result['normalized_street'] = normalize_street(raw_street)

    # Normalize house number
    raw_house = record.get('house_number', '') or ''
    result['normalized_house_number'] = normalize_house_number(raw_house)

    # Build full normalized address
    parts = []
    if result['normalized_street']:
        parts.append(result['normalized_street'])
    if result['normalized_house_number']:
        parts.append(result['normalized_house_number'])
    if result['normalized_city']:
        parts.append(result['normalized_city'])

    result['normalized_address'] = ', '.join(parts) if parts else None

    return result


def normalize_address_batch(records: List[dict]) -> List[dict]:
    """
    Normalize addresses for a batch of records.

    Args:
        records: List of transaction records.

    Returns:
        List of records with normalized address fields.
    """
    normalized = []
    for record in records:
        try:
            normalized.append(normalize_address(record))
        except Exception as e:
            logger.warning(f"Failed to normalize address: {e}")
            normalized.append(record)

    logger.info(f"Normalized {len(normalized)} addresses")
    return normalized


if __name__ == "__main__":
    # Demo
    test_cases = [
        {"city": 'ת"א', "street": "רח' הרצל", "house_number": "12"},
        {"city": "תל אביב -יפו", "street": "שדרות רוטשילד", "house_number": "45"},
        {"city": "ב\"ש", "street": "רחוב ויצמן", "house_number": "7א"},
        {"city": "ראשל\"צ", "street": "דרך הים", "house_number": "100"},
        {"city": "פ\"ת", "street": "סמ' האלון", "house_number": "3"},
        {"city": "ר\"ג", "street": "ז'בוטינסקי", "house_number": "10-12"},
        {"city": "ירושלם", "street": "כיכר ציון", "house_number": ""},
    ]

    print("=== Address Normalization Demo ===\n")
    for tc in test_cases:
        result = normalize_address(tc)
        print(f"  Input:  {tc['city']} | {tc['street']} {tc['house_number']}")
        print(f"  Output: {result['normalized_address']}")
        print()
