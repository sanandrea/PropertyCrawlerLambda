from daft_client.daft import Daft
from daft_client.enums import SearchType, Distance, PropertyType
from daft_client.location import Location

BALLINTEER_DUBLIN = {'id': '2050', 'displayName': 'Ballinteer, Dublin', 'displayValue': 'ballinteer-dublin'}


def lambda_handler(event, context):
    pass
    daft = Daft()
    daft.set_location(Location.BALLINTEER_DUBLIN, Distance.KM1)
    daft.set_search_type(SearchType.RESIDENTIAL_SALE)
    daft.set_property_type(PropertyType.HOUSE)
    daft.set_min_price(400000)
    daft.set_max_price(800000)

    listings = daft.search()
    for listing in listings:
        print(listing.title)
        print(listing.price)
