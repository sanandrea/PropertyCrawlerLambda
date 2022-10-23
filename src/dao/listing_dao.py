from datetime import datetime
import os
from typing import Dict
from daft_client.listing import Listing
import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.client import Config
from aws_lambda_powertools import Logger

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

VENDOR_PREFIX = 'DAFT_'

LATEST_VERSION_SK = 'v0'
METADATA_SK = 'metadata'

class ListingDao:
    def __init__(self) -> None:
        self.tableName = os.environ['CRAWLER_TABLE_NAME']
    
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
        # metadata_dict = listing.as_dict_for_storage()
        metadata_dict = {
        "bedrooms": "3 Bed",
        "bathrooms": "3 Bath",
        "ber": "C2",
        "daft_link": "http://www.daft.ie/for-sale/semi-detached-house-19-ticknock-dale-sandyford-dublin-18-sandyford-dublin-18/4091879",
        "surface": "120",
        "title": "19 Ticknock Dale Sandyford Dublin 18, Sandyford, Dublin 18",
        "images": [
            {
                "caption": "photo",
                "size720x480": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjcyMCwiaGVpZ2h0Ijo0ODB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjcvMS83MWJkNzc2ZmYxNGFmZDgyZDA5NjMxZDFkY2I5MmZkMC5qcGcifQ==?signature=b1b75b3dd81763b3ce592f57e5f98efe698f1922a45b9f6ecc43531342b0c79e",
                "size600x600": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6Imluc2lkZSIsIndpZHRoIjo2MDAsImhlaWdodCI6NjAwfX0sIm91dHB1dEZvcm1hdCI6ImpwZWciLCJrZXkiOiI3LzEvNzFiZDc3NmZmMTRhZmQ4MmQwOTYzMWQxZGNiOTJmZDAuanBnIn0=?signature=ce9420edd8887c4a3a214903fb44f950ef21870cf8345e1a63b1865b2d0a8167",
                "size400x300": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjQwMCwiaGVpZ2h0IjozMDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjcvMS83MWJkNzc2ZmYxNGFmZDgyZDA5NjMxZDFkY2I5MmZkMC5qcGcifQ==?signature=2720838f47c761c204d0793485fd920997dc7cdb10db70c3c414e692f1324c95",
                "size360x240": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjM2MCwiaGVpZ2h0IjoyNDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjcvMS83MWJkNzc2ZmYxNGFmZDgyZDA5NjMxZDFkY2I5MmZkMC5qcGcifQ==?signature=28ebdfbdc83f3edd51de4067fcabecc80c884d99abc0d50c83908f2f863af6c7",
                "size300x200": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjMwMCwiaGVpZ2h0IjoyMDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjcvMS83MWJkNzc2ZmYxNGFmZDgyZDA5NjMxZDFkY2I5MmZkMC5qcGcifQ==?signature=b4dcd4f8f84df2ed3c8f9af9777a5754e0809a706c59742b816b393ead72e887",
                "size320x280": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjMyMCwiaGVpZ2h0IjoyODB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjcvMS83MWJkNzc2ZmYxNGFmZDgyZDA5NjMxZDFkY2I5MmZkMC5qcGcifQ==?signature=d76de1aeb529203c502996ce8ae7c8d3f4a23740cca0f25c098ed65000f34592",
                "size72x52": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjcyLCJoZWlnaHQiOjUyfX0sIm91dHB1dEZvcm1hdCI6ImpwZWciLCJrZXkiOiI3LzEvNzFiZDc3NmZmMTRhZmQ4MmQwOTYzMWQxZGNiOTJmZDAuanBnIn0=?signature=bbf7e5523bc2b89a74a9e9294a407f16222367fe2a0b99ffd6a234b8df22ef66",
                "size680x392": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjY4MCwiaGVpZ2h0IjozOTJ9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjcvMS83MWJkNzc2ZmYxNGFmZDgyZDA5NjMxZDFkY2I5MmZkMC5qcGcifQ==?signature=45841889cef77ce7c3beebdd7f50eb3ca1f33774377dca30c9420fc07ce8bd19"
            },
            {
                "caption": "photo",
                "size720x480": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjcyMCwiaGVpZ2h0Ijo0ODB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjIvZi8yZjg1YWYzZjI5NzcyZmExOGIwYjk1NTlhODI0MmJhNS5qcGcifQ==?signature=e92a6a2efeac00d955fcf30acad9b9cce4d114ec252baf69c84417cce5132fc2",
                "size600x600": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6Imluc2lkZSIsIndpZHRoIjo2MDAsImhlaWdodCI6NjAwfX0sIm91dHB1dEZvcm1hdCI6ImpwZWciLCJrZXkiOiIyL2YvMmY4NWFmM2YyOTc3MmZhMThiMGI5NTU5YTgyNDJiYTUuanBnIn0=?signature=09ace594d35de210154d3868b5c2e604f422515b571449389ca5e7b7494ccc8b",
                "size400x300": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjQwMCwiaGVpZ2h0IjozMDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjIvZi8yZjg1YWYzZjI5NzcyZmExOGIwYjk1NTlhODI0MmJhNS5qcGcifQ==?signature=f96fbc71e39bde6c62393f09e3b92b26da64b72673d452d658d556c414fccbd4",
                "size360x240": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjM2MCwiaGVpZ2h0IjoyNDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjIvZi8yZjg1YWYzZjI5NzcyZmExOGIwYjk1NTlhODI0MmJhNS5qcGcifQ==?signature=a92ee557c28da5562c31cbda02a071c4de372a470d1de19a5faa577050a05896",
                "size300x200": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjMwMCwiaGVpZ2h0IjoyMDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjIvZi8yZjg1YWYzZjI5NzcyZmExOGIwYjk1NTlhODI0MmJhNS5qcGcifQ==?signature=9cbf2241f1ff6eebb53ed9e953fa15514638974b9dcfdb736fd7d9184597653d",
                "size320x280": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjMyMCwiaGVpZ2h0IjoyODB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjIvZi8yZjg1YWYzZjI5NzcyZmExOGIwYjk1NTlhODI0MmJhNS5qcGcifQ==?signature=9a06851b88f3d59c5cae4f7ea9940b9127b84ac0965a11a8abb58ec133939b2b",
                "size72x52": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjcyLCJoZWlnaHQiOjUyfX0sIm91dHB1dEZvcm1hdCI6ImpwZWciLCJrZXkiOiIyL2YvMmY4NWFmM2YyOTc3MmZhMThiMGI5NTU5YTgyNDJiYTUuanBnIn0=?signature=032cc908a8dba3941e92fd1e1049a1adb997191370c07d320bf298a5f51d6f11",
                "size680x392": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjY4MCwiaGVpZ2h0IjozOTJ9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjIvZi8yZjg1YWYzZjI5NzcyZmExOGIwYjk1NTlhODI0MmJhNS5qcGcifQ==?signature=27397fd8a7e541c458cf433fd85b6c98d4c9c5a870c6ee084c883ef00ef078d1"
            },
            {
                "caption": "photo",
                "size720x480": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjcyMCwiaGVpZ2h0Ijo0ODB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjMvNC8zNDA1ZDI3MDYzOTBjNWExMTJjMGZmODk5NDUyNGUxYy5qcGcifQ==?signature=dbeecd437c24ca471fd5f843970b8ca12aade72aa6fbf09d7fada866d161b4aa",
                "size600x600": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6Imluc2lkZSIsIndpZHRoIjo2MDAsImhlaWdodCI6NjAwfX0sIm91dHB1dEZvcm1hdCI6ImpwZWciLCJrZXkiOiIzLzQvMzQwNWQyNzA2MzkwYzVhMTEyYzBmZjg5OTQ1MjRlMWMuanBnIn0=?signature=704c55c644fd451bedbe7f0b8f2ed794430ef68b0fef7a33711ecc293e489e37",
                "size400x300": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjQwMCwiaGVpZ2h0IjozMDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjMvNC8zNDA1ZDI3MDYzOTBjNWExMTJjMGZmODk5NDUyNGUxYy5qcGcifQ==?signature=a09294c1ae455ee0d3f417fcf88961a3b22f7b1e7fb320fd4a170de76d9fcabc",
                "size360x240": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjM2MCwiaGVpZ2h0IjoyNDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjMvNC8zNDA1ZDI3MDYzOTBjNWExMTJjMGZmODk5NDUyNGUxYy5qcGcifQ==?signature=7feb1a79a710f07b95d511c5ce8ef75e40e558ff9279670b722f7b3ecef1b351",
                "size300x200": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjMwMCwiaGVpZ2h0IjoyMDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjMvNC8zNDA1ZDI3MDYzOTBjNWExMTJjMGZmODk5NDUyNGUxYy5qcGcifQ==?signature=5b04066829693e7f00d1d6543b9edbb6a100124cdc79d52d59ef276a3cfa0164",
                "size320x280": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjMyMCwiaGVpZ2h0IjoyODB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjMvNC8zNDA1ZDI3MDYzOTBjNWExMTJjMGZmODk5NDUyNGUxYy5qcGcifQ==?signature=20b5c2cf6e3763872745b0fafe0843cb7ee1c1625bfca88fcc24e1f27b8480db",
                "size72x52": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjcyLCJoZWlnaHQiOjUyfX0sIm91dHB1dEZvcm1hdCI6ImpwZWciLCJrZXkiOiIzLzQvMzQwNWQyNzA2MzkwYzVhMTEyYzBmZjg5OTQ1MjRlMWMuanBnIn0=?signature=b485d4d7229ec18011e0b8f942bf9cec1e366d99ca54e4d0faf66a5ef17ff4fc",
                "size680x392": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjY4MCwiaGVpZ2h0IjozOTJ9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjMvNC8zNDA1ZDI3MDYzOTBjNWExMTJjMGZmODk5NDUyNGUxYy5qcGcifQ==?signature=f87066e0f2feb5124d6a7568cf001cc4b1e9dbdbd4f9b408d6f7ae82cd8ad3fa"
            },
            {
                "caption": "photo",
                "size720x480": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjcyMCwiaGVpZ2h0Ijo0ODB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjYvMS82MTFhYWE1MGU0ODljYjUzY2E3YjJiZmE5MDczZWQ5Ni5qcGcifQ==?signature=936a5713395f367cc4c46fba5247e4e839dcc17c2a0a1ca073502974019dafbd",
                "size600x600": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6Imluc2lkZSIsIndpZHRoIjo2MDAsImhlaWdodCI6NjAwfX0sIm91dHB1dEZvcm1hdCI6ImpwZWciLCJrZXkiOiI2LzEvNjExYWFhNTBlNDg5Y2I1M2NhN2IyYmZhOTA3M2VkOTYuanBnIn0=?signature=939a6876e656a297c770f3232c7697511b21dfcd4917032f252b108d14515a3f",
                "size400x300": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjQwMCwiaGVpZ2h0IjozMDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjYvMS82MTFhYWE1MGU0ODljYjUzY2E3YjJiZmE5MDczZWQ5Ni5qcGcifQ==?signature=0855b20f9303c98b8ac7e5d706b18295769a3b3ee23a1662f00f59a3e1be4959",
                "size360x240": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjM2MCwiaGVpZ2h0IjoyNDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjYvMS82MTFhYWE1MGU0ODljYjUzY2E3YjJiZmE5MDczZWQ5Ni5qcGcifQ==?signature=6a77a721bc3067b9272dfd654b51ae16ac904d909d7468db3e2190b8c2269849",
                "size300x200": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjMwMCwiaGVpZ2h0IjoyMDB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjYvMS82MTFhYWE1MGU0ODljYjUzY2E3YjJiZmE5MDczZWQ5Ni5qcGcifQ==?signature=5c5b1d71747ce10058d9740b6fc17864660617cf0b73daffb62c7f19ab1608c9",
                "size320x280": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjMyMCwiaGVpZ2h0IjoyODB9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjYvMS82MTFhYWE1MGU0ODljYjUzY2E3YjJiZmE5MDczZWQ5Ni5qcGcifQ==?signature=b11eb9aa9bfc051d37b3c6cf68abb1a9c89adc17becf6f9508c0b89bf7b2c79b",
                "size72x52": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjcyLCJoZWlnaHQiOjUyfX0sIm91dHB1dEZvcm1hdCI6ImpwZWciLCJrZXkiOiI2LzEvNjExYWFhNTBlNDg5Y2I1M2NhN2IyYmZhOTA3M2VkOTYuanBnIn0=?signature=90bce7751c4332e8dab1824fe0e663b79563631a93fe9edc23698c11250e23a9",
                "size680x392": "https://media.daft.ie/eyJidWNrZXQiOiJtZWRpYW1hc3Rlci1zM2V1IiwiZWRpdHMiOnsicmVzaXplIjp7ImZpdCI6ImNvdmVyIiwid2lkdGgiOjY4MCwiaGVpZ2h0IjozOTJ9fSwib3V0cHV0Rm9ybWF0IjoianBlZyIsImtleSI6IjYvMS82MTFhYWE1MGU0ODljYjUzY2E3YjJiZmE5MDczZWQ5Ni5qcGcifQ==?signature=abff07feb8238620c7e7bdd192a812b0236123b03293956c09e7803db85f34ca"
            }
        ],
        "short_code": "113549630",
        "price": "475000",
        "category": "Buy",
        "publish_date": "2022-10-23 04:02:05"
    }
        low_level_copy = {k: serializer.serialize(v) for k,v in metadata_dict.items()}

        low_level_copy[PK_NAME] = {'S': VENDOR_PREFIX + listing.shortcode}
        low_level_copy[SK_NAME] = {'S': LATEST_VERSION_SK}

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
                            'UpdateTime': {'S': current_time},
                            'Price': {'S': listing.price},
                            'Latest': {'N': '0'}
                        }
                    }
                }
            ]
        )
    
    def update_existing_item(self, listing: Listing, latest_version: int, higher_version: int) -> None:
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
                            '#latest': 'Latest',
                            '#time': 'UpdateTime',
                            '#price': 'Price'
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
                            'UpdateTime': {'S': current_time},
                            'Price': {'S': listing.price}
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
                            '#price': 'Price'
                        },
                        'ExpressionAttributeValues': {
                            ':price': {'S': listing.price}
                        }
                    }
                }
            ]
        )


