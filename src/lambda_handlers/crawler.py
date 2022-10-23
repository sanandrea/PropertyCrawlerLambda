from daft_client.listing import Listing
from daft_client.daft import Daft
from daft_client.enums import SearchType, Distance, PropertyType
from daft_client.location import Location
from service.listing_service import ListingService

listing_service = ListingService()

def lambda_handler(event, context):
    pass
    daft = Daft()
    daft.set_location(Location.BALLINTEER_DUBLIN, Distance.KM1)
    daft.set_search_type(SearchType.RESIDENTIAL_SALE)
    daft.set_property_type(PropertyType.HOUSE)
    daft.set_min_price(400000)
    daft.set_max_price(800000)

    #listings = daft.search()
    # for listing in listings:
    #     listing_service.saveNewListing(listing=listing)
    l = Listing({'listing':{}})
    l.shortcode = "113549630"
    l.price = "475000"
    listing_service.saveNewListing(l)