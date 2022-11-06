from enum import Enum

VENDOR_PREFIX = 'DAFT_'

class PropertyStatus(Enum):
    ACTIVE = 'active'
    SALE_AGREED = 'sale_agreed'
    SOLD = 'sold'
    UNKNOWN = 'unknown'