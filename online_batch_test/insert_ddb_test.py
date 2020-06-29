import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('dev_dynamic_ads_similar_items')

response = table.put_item(
    Item={
        'item_id': 'test',
        'label': 'test',
        'add_cart_similar': {'123':Decimal('0.76')},
    }
)

response = table.query(
    KeyConditionExpression=Key('item_id').eq('test')
)
print(response['Items'])