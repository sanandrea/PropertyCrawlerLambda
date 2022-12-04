from dpcie.daft_client.daft import Daft
from dpcie.daft_client.enums import Ber
from dpcie.daft_client.daft import Daft
from dpcie.daft_client.enums import SearchType, Distance, PropertyType
from dpcie.daft_client.location import Location
from unittest.mock import patch
from unittest.mock import MagicMock 


@patch('dpcie.daft_client.daft.requests')
def test_make_two_searches(requests_mock):
    result_mock = MagicMock()
    result_mock.json.return_value = {"listings":[],"paging": {"totalResults": 20} }
    requests_mock.post.return_value = result_mock
    daft = Daft()

    daft.set_search_type(SearchType.RESIDENTIAL_SALE)
    daft.set_location(Location.DUBLIN_16_DUBLIN, Distance.KM1)
    daft.set_property_type(PropertyType.HOUSE)
    daft.set_min_beds(3)
    daft.set_min_baths(2)
    daft.set_min_ber(Ber.A1)
    daft.set_max_ber(Ber.C3)
    daft.set_min_price(500000)
    daft.set_max_price(900000)
    assert daft._paging == {"from": "0", "pagesize": str(50)}

    daft.search()

    daft_2 = Daft()
    print(id(daft_2._paging))
    assert daft_2._paging == {"from": "0", "pagesize": str(50)}
