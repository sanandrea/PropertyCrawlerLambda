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
print(response)

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
    print(item)
    current_dl = item['daft_link']
    tokens = current_dl.split('/')
    new_tokens = ['https:'] + tokens[1:]
    new_dl = '/'.join(new_tokens)
    response = table.update_item(
        Key={
            'pr_id':item[PK_NAME],
            'sk':item[SK_NAME]
        },
        UpdateExpression='SET #s = :status, #dl = :dl_v REMOVE #it',
        ExpressionAttributeNames={
            '#s' : 'propertyStatus',
            '#dl': 'daft_link',
            '#it': 'inactiveTime'
        },
        ExpressionAttributeValues={
            ':status' : PropertyStatus.ACTIVE.value,
            ':dl_v': new_dl
        }
    )
