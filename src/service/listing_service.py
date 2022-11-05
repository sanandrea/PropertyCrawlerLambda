import os
import boto3
import botocore
import requests
from http import HTTPStatus

from typing import List
from daft_client.listing import Listing
from dao.listing_dao import ListingDao
from aws_lambda_powertools import Logger
from model.model_constants import PropertyStatus, VENDOR_PREFIX
logger = Logger()

PAGE_HTML = 'page.html'

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

    def check_status_of_active_listings(self) -> List[Listing]:
        s3 = boto3.resource('s3')
        bucket_name = os.environ['PROPERTY_BUCKET_NAME']
        active_listings: List[Listing] = self.listingDao.get_all_by_status(PropertyStatus.ACTIVE)
        logger.info(f'Read {len(active_listings)}active from DynamoDB')

        for active_listing in active_listings:
            logger.info(f'Retrieving {active_listing.daft_link} document for {active_listing.shortcode}')
            r = requests.get(active_listing.daft_link, allow_redirects=False)
            logger.info(f'Request for document resulted in HTTP Status Code {r.status_code}')
            
            if r.status_code != HTTPStatus.OK:
                self.listingDao.update_property_status(active_listing, PropertyStatus.UNKNOWN)
                continue

            if check_if_property_is_saved_already(
                s3, 
                bucket_name=bucket_name, 
                property_key=listing_s3_key(active_listing)
                ):
                logger.info('Property is already saved in S3, skipping save')
                continue
            logger.info(f'Saving Property document in S3')
            save_listing_media_in_s3(s3, bucket_name, listing_s3_key(active_listing), r.text)


def listing_s3_key(listing: Listing) -> str:         
    return VENDOR_PREFIX + listing.shortcode + '/' + PAGE_HTML

def save_listing_media_in_s3(s3, bucket_name: str, property_key: str, body):
    try:
        object = s3.Object(bucket_name, property_key)
        object.put(Body=body)
    except Exception as e:
        logger.error(f'Could not save object in s3 {property_key} - {e}')
        raise e

def check_if_property_is_saved_already(s3, bucket_name: str, property_key: str) -> bool:
    try:
        s3.Object(bucket_name, property_key).load()
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Something else has gone wrong.
            raise
        