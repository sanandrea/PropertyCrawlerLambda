import boto3
from dao.listing_dao import PK_NAME, SK_NAME, METADATA_SK
from model.model_constants import PropertyStatus

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('PropertiesList')

response = table.scan(
    ProjectionExpression='#k,#s',
    FilterExpression="#s <> :sk_f",
    ExpressionAttributeNames={
        '#k' : PK_NAME, #partition key
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
        ProjectionExpression='#k,#s',
        FilterExpression="#s = :sk_f",
        ExpressionAttributeNames={
            '#k' : PK_NAME, #partition key
            '#s' : SK_NAME #sort key
        },
        ExpressionAttributeValues={
            ':sk_f' : METADATA_SK
        }
    )
    items.extend(response['Items'])

print(f'Collected {len(items)} items')

for item in items:
    response = table.update_item(
        Key=item,
        UpdateExpression='SET #s = :status',
        ExpressionAttributeNames={
            '#s' : 'propertyStatus'
        },
        ExpressionAttributeValues={
            ':status' : PropertyStatus.ACTIVE.value
        }
    )
