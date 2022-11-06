import os
from datetime import datetime
from typing import Dict, List
import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.client import Config
from aws_lambda_powertools import Logger

from dpcie.model.model_constants import VENDOR_PREFIX
from dpcie.model.model_constants import PropertyStatus
from dpcie.daft_client.listing import Listing

logger = Logger()


config = Config(
   retries = {
      'max_attempts': 3,
      'mode': 'standard'
   }
)
ddb_client = boto3.client('dynamodb', config=config)
serializer = TypeSerializer()

PK_NAME = 'pr_id'
SK_NAME = 'sk'

LATEST_VERSION_SK = 'v0'
FIRST_VERSION_SK = 'v1'
METADATA_SK = 'metadata'
PROPERTY_STATUS = 'propertyStatus'
ADDED_TIME_FIELD = 'addedTime'
INACTIVE_TIME_FIELD = 'inactiveTime'

class ListingDao:
    def __init__(self) -> None:
        self.tableName = os.environ['CRAWLER_TABLE_NAME']
        ddb_resource = boto3.resource('dynamodb')
        self.table = ddb_resource.Table(self.tableName)
    
    def getLatestItem(self, propertyId) -> Dict:
        return ddb_client.get_item(
            TableName = self.tableName,
            Key = {
                PK_NAME: {'S': VENDOR_PREFIX + propertyId},
                SK_NAME: {'S': LATEST_VERSION_SK}
            }
        )
    
    def insert_new_item(self, listing: Listing):
        current_time = datetime.utcnow().isoformat(timespec="seconds")
        metadata_dict = listing.as_dict_for_storage()
        low_level_copy = {k: serializer.serialize(v) for k,v in metadata_dict.items()}

        low_level_copy[PK_NAME] = {'S': VENDOR_PREFIX + listing.shortcode}
        low_level_copy[SK_NAME] = {'S': METADATA_SK}
        low_level_copy[ADDED_TIME_FIELD] = {'S': current_time}
        low_level_copy[PROPERTY_STATUS] = {'S': PropertyStatus.ACTIVE.value}

        ddb_client.transact_write_items(
            TransactItems = [
                {
                    'Put': {
                        'TableName': self.tableName,
                        'Item': low_level_copy
                    }
                },
                {
                    'Put': {
                        'TableName': self.tableName,
                        'Item': {
                            PK_NAME: {'S': VENDOR_PREFIX + listing.shortcode},
                            SK_NAME: {'S': LATEST_VERSION_SK},
                            'updateTime': {'S': current_time},
                            'price': {'S': listing.price},
                            'latest': {'N': '1'}
                        }
                    }
                },
                {
                    'Put': {
                        'TableName': self.tableName,
                        'Item': {
                            PK_NAME: {'S': VENDOR_PREFIX + listing.shortcode},
                            SK_NAME: {'S': FIRST_VERSION_SK},
                            'updateTime': {'S': current_time},
                            'price': {'S': listing.price}
                        }
                    }
                }
            ]
        )
    
    def update_existing_item(self, listing: Listing, 
    latest_version: int, 
    higher_version: int
) -> None:
        current_time = datetime.utcnow().isoformat(timespec="seconds")
        # See https://aws.amazon.com/blogs/database/implementing-version-control-using-amazon-dynamodb/
        ddb_client.transact_write_items(
            TransactItems = [
                {
                    'Update': {
                        'TableName': self.tableName,
                        'Key': {
                            PK_NAME: {'S': VENDOR_PREFIX + listing.shortcode},
                            SK_NAME: {'S': LATEST_VERSION_SK}
                        },
                        # Conditional write makes the update idempotent here 
                        # since the conditional check is on the same attribute 
                        # that is being updated.
                        'ConditionExpression': 
                            'attribute_not_exists(#latest) OR #latest = :latest',
                        'UpdateExpression': 'SET #latest = :higher_version, #time = :time, #price = :price',
                        'ExpressionAttributeNames': {
                            '#latest': 'latest',
                            '#time': 'updateTime',
                            '#price': 'price'
                        },
                        'ExpressionAttributeValues': {
                            ':latest': {'N': str(latest_version)},
                            ':higher_version': {'N': str(higher_version)},
                            ':time': {'S': current_time},
                            ':price': {'S': listing.price}
                        }
                    }
                },
                {
                    'Put': {
                        'TableName': self.tableName,
                        'Item': {
                            PK_NAME: {'S': VENDOR_PREFIX + listing.shortcode},
                            SK_NAME: {'S': 'v' + str(higher_version)},
                            'updateTime': {'S': current_time},
                            'price': {'S': listing.price}
                        }
                    }
                },
                {
                    'Update': {
                        'TableName': self.tableName,
                        'Key': {
                            PK_NAME: {'S': VENDOR_PREFIX + listing.shortcode},
                            SK_NAME: {'S': METADATA_SK}
                        },
                        'UpdateExpression': 'SET #price = :price',
                        'ExpressionAttributeNames': {
                            '#price': 'price'
                        },
                        'ExpressionAttributeValues': {
                            ':price': {'S': listing.price}
                        }
                    }
                }
            ]
        )

    def get_all_by_status(self, property_status: PropertyStatus) -> List[Listing]:
        response = self.table.scan(
            FilterExpression="#ps = :ps_v",
            ExpressionAttributeNames={
                '#ps' : PROPERTY_STATUS
            },
            ExpressionAttributeValues={
                ':ps_v' : property_status.value
            }
        )

        items = response['Items']

        while 'LastEvaluatedKey' in response:
            response = self.table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey'],
                FilterExpression="#ps = :ps_v",
                ExpressionAttributeNames={
                    '#ps' : PROPERTY_STATUS
                },
                ExpressionAttributeValues={
                    ':ps_v' : property_status.value
                }
            )
            items.extend(response['Items'])
        return [Listing({'listing': item}) for item in items]

    def update_property_status(self, listing: Listing, property_status: PropertyStatus):
        return self.table.update_item(
            Key={
                PK_NAME: VENDOR_PREFIX + listing.shortcode,
                SK_NAME: METADATA_SK 
            },
            UpdateExpression="SET #ps = :ps_v, #dt = :dt_v",
            ExpressionAttributeNames={
                '#ps': PROPERTY_STATUS,
                '#dt': INACTIVE_TIME_FIELD
            },
            ExpressionAttributeValues={
                ':ps_v': property_status.value,
                ':dt_v': datetime.utcnow().isoformat(timespec="seconds")
            },
        )
