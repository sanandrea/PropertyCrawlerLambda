from daft_client.listing import Listing
from dao.listing_dao import ListingDao
from aws_lambda_powertools import Logger

logger = Logger()

class ListingService:
    def __init__(self, listingDao: ListingDao = None) -> None:
        if not listingDao:
            listingDao = ListingDao()
        self.listingDao = listingDao

    def saveNewListing(self, listing: Listing) -> None:
        latest_version = 0
        higher_version = 1
        response_latest_version = self.listingDao.getLatestItem(listing.shortcode)

        if 'Item' in response_latest_version:
            logger.info(f'Listing {listing.shortcode} exists already checking if price has changed ...')
            old_price = response_latest_version['Item']['price']['S']
            if old_price == listing.price:
                logger.info(f'Listing {listing.shortcode} price has not changed, skipping updates')
                return
            
            logger.info(f'Listing {listing.shortcode} price has not changed, skipping updates')
            latest_version = response_latest_version['Item']['latest']['N']
            higher_version = int(latest_version) + 1
            self.listingDao.update_existing_item(listing, latest_version, higher_version)
        else:
            self.listingDao.insert_new_item(listing)

        