import boto3
from dpcie.dao.listing_dao import PK_NAME, SK_NAME, METADATA_SK
from dpcie.model.model_constants import PropertyStatus

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('PropertiesList')

response = table.scan(
    FilterExpression="#s = :sk_f",
    ExpressionAttributeNames={
        '#s' : SK_NAME #sort key
    },
    ExpressionAttributeValues={
        ':sk_f' : METADATA_SK
    }
)

items = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(
        ExclusiveStartKey=response['LastEvaluatedKey'],
        FilterExpression="#s = :sk_f",
        ExpressionAttributeNames={
            '#s' : SK_NAME #sort key
        },
        ExpressionAttributeValues={
            ':sk_f' : METADATA_SK
        }
    )
    items.extend(response['Items'])

print(f'Collected {len(items)} items')

for item in items:
    new_added_time = item['publish_date'].replace(' ', 'T')
    response = table.update_item(
        Key={
            'pr_id':item[PK_NAME],
            'sk':item[SK_NAME]
        },
        UpdateExpression='SET #at = :at_v',
        ExpressionAttributeNames={
            '#at' : 'addedTime'
        },
        ExpressionAttributeValues={
            ':at_v': new_added_time
        }
    )
