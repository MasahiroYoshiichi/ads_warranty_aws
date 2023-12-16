import json
import boto3
from botocore.exceptions import ClientError

# DynamoDBクライアントの初期化
dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    print('jsonデータ:',json.dumps(event))
    # クエリパラメーターの取得
    params = event.get('queryStringParameters', {})
    transaction_id = params.get('transactionID')
    product_number = params.get('productNumber')

    if not transaction_id or not product_number:
        return {'statusCode': 400, 'body': json.dumps('TransactionIDとProductNumberの取得に失敗しました。')}

    # DynamoDBからデータを取得
    try:
        response = dynamodb.get_item(
            TableName='WarrantyTable',
            Key={
                'transactionID': {'S': transaction_id},
                'productNumber': {'S': product_number}
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        return {'statusCode': 500, 'body': json.dumps('DynamoDBからデータを取得できませんでした。')}

    # レスポンスの作成
    if 'Item' in response and 'objectUrl' in response['Item']:
        object_url = response['Item']['objectUrl']['S']
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',  # S3ホスト後に変更
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,OPTIONS'
            },
            'body': json.dumps({'objectUrl': object_url})
        }
    else:
        return {'statusCode': 404, 'body': json.dumps('オブジェクトURLが確認できませんでした。')}
