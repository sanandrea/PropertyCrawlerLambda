from dpcie.daft_client.listing import Listing

def a_listing():
    return Listing(result={
        "listing" : {
            "id": "123",
            "seoFriendlyPath": "my-home-in-dund-laoghaire.com",
            "title": "4 my home",
            "price": "€200,000",
            "numBathrooms": "2",
            "numBedrooms": "3",
            "daftShortcode": "1234321",
            "category": "house",
            "publishDate": 1667767445000,
            "ber": {
                "rating" : "A1"
            },
            "floorArea": {
                "unit": "METRES_SQUARED",
                "value": 123
            },
            "media": {
                "images": {}
            }
        }
    })