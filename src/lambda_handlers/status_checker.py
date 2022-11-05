from service.listing_service import ListingService

listing_service = ListingService()

def lambda_handler(event, context):
    stored_listings = listing_service.check_status_of_active_listings()