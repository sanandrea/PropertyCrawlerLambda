import os
from time import sleep
import boto3
import uuid
import json
from dpcie.dao.listing_dao import ListingDao, config
from unittest.mock import patch
from unittest import TestCase
from conftest import a_listing 

ddb_client = boto3.client('dynamodb', config=config, endpoint_url='http://localhost:8000') 
ddb_resource = boto3.resource('dynamodb', config=config, endpoint_url='http://localhost:8000')


# @patch.dict(os.environ, {"CRAWLER_TABLE_NAME": "local-crawler-table"}, clear=True)
# @patch('dpcie.dao.listing_dao.ddb_client', ddb_client)
# def test_insert_new_listing(a_listing):
#     listing_dao = ListingDao(ddb_resource)
#     listing_dao.insert_new_item(a_listing)



f = open('dynamo-db-local-setup.json')
create_table_command = json.load(f)


class TestDao(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        return super().tearDownClass()

    def setUp(self) -> None:
        self.tableName = str(uuid.uuid4())
        self.patcher = patch.dict(os.environ, {"CRAWLER_TABLE_NAME": self.tableName})
        create_table_command['TableName'] = self.tableName
        ddb_client.create_table(**create_table_command)
        self.patcher.start()

    def tearDown(self) -> None:
        ddb_client.delete_table(TableName=self.tableName)

    def get_all_ddb_items(self):
        table = ddb_resource.Table(self.tableName)
        return table.scan(Limit=100)['Items']


    @patch('dpcie.dao.listing_dao.ddb_client', ddb_client)
    def test_save_new_listing(self):
        test_listing = a_listing()
        listing_dao = ListingDao(ddb_resource)
        listing_dao.insert_new_item(test_listing)
        ddb_items = self.get_all_ddb_items()
        pk = ListingDao.get_pk_for_listing(test_listing)

        assert len(ddb_items) == 3
        next(item for item in ddb_items if item['sk']=='v0' and item['pr_id'] == pk)
        next(item for item in ddb_items if item['sk']=='v1' and item['pr_id'] == pk)
        next(item for item in ddb_items if item['sk']=='metadata' and item['pr_id'] == pk)

    @patch('dpcie.dao.listing_dao.ddb_client', ddb_client)
    def test_update_listing(self):
        test_listing = a_listing()
        listing_dao = ListingDao(ddb_resource)
        listing_dao.insert_new_item(test_listing)
        test_listing._result['price'] = "€250,000"
        listing_dao.update_existing_item(test_listing, 1, 2)

        ddb_items = self.get_all_ddb_items()
        pk = ListingDao.get_pk_for_listing(test_listing)

        assert len(ddb_items) == 4
        next(item for item in ddb_items if item['sk']=='v0' and item['pr_id'] == pk)
        next(item for item in ddb_items if item['sk']=='v1' and item['pr_id'] == pk)
        next(item for item in ddb_items if item['sk']=='v2' and item['pr_id'] == pk)
        metadata = next(item for item in ddb_items if item['sk']=='metadata' and item['pr_id'] == pk)
        assert metadata['price'] == '250000'



