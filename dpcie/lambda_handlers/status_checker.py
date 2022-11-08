from dpcie.service.listing_service import ListingService

def status_checker_handler(event, context):
    listing_service = ListingService()
    stored_listings = listing_service.check_status_of_active_listings()