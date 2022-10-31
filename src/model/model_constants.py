from enum import Enum

class PropertyStatus(Enum):
    ACTIVE = 'active'
    SALE_AGREED = 'sale_agreed'
    SOLD = 'sold'
    UNKNOWN = 'unknown'