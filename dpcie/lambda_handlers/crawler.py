from dpcie.daft_client.enums import Ber
from dpcie.daft_client.daft import Daft
from dpcie.daft_client.enums import SearchType, Distance, PropertyType
from dpcie.daft_client.location import Location
from dpcie.service.listing_service import ListingService

def crawler_handler(event, context):
    daft = Daft()
    daft.set_location(Location.DUBLIN_16_DUBLIN, Distance.KM1)
    daft.set_search_type(SearchType.RESIDENTIAL_SALE)
    daft.set_property_type(PropertyType.HOUSE)
    daft.set_min_beds(3)
    daft.set_min_baths(2)
    daft.set_min_ber(Ber.A1)
    daft.set_max_ber(Ber.D1)
    daft.set_min_price(500000)
    daft.set_max_price(900000)

    listing_service = ListingService()

    listings = daft.search()
    for listing in listings:
        listing_service.saveNewListing(listing=listing)
