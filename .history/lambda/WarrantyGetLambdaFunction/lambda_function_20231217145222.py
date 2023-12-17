import json
import boto3
import os
from botocore.exceptions import ClientError
from urllib.parse import urlparse, unquote

# DynamoDBとS3クライアントの初期化
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

# 署名付きURLの生成
def generate_presigned_url(bucket_name, object_key):
    try:
        response = s3.generate_presigned_url('get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn = 3600,
            HttpMethod = 'GET'
        )
        return response
    except ClientError as e:
        print(e)
        return None


def lambda_handler(event, context):
    # イベント情報の出力
    print('jsonデータ:', json.dumps(event))
    
    
    # クエリパラメーターの取得
    params = event.get('queryStringParameters', {})
    transaction_id = params.get('transactionID')
    product_number = params.get('productNumber')
    
    # パラメーターの値を確認
    if not transaction_id or not product_number:
        return {'statusCode': 400, 'body': json.dumps('TransactionIDとProductNumberの取得に失敗しました。')}

    # DynamoDBから保証書データを取得
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

        # URLからオブジェクトキーの抽出
        object_key = object_url.split('/')[-1] if object_url else None
        
        
        if object_key:
            bucket_name = 'warranty-pdf-bucket'
            objectUrl = generate_presigned_url(bucket_name, object_key)
            
            
            # オブジェクトURLの有無
            if objectUrl:
                return {
                    'statusCode': 200,
                    'headers': {
                        'Access-Control-Allow-Origin': 'https://tht-marine.wats-ads.com', 
                        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                        'Access-Control-Allow-Methods': 'GET,OPTIONS',
                        'Cache-Control': 'no-cache, no-store, must-revalidate',
                        'Pragma': 'no-cache',  
                        'Expires': '0'
                    },
                    'body': json.dumps({'objectUrl': objectUrl})
                }
            else:
                return {
                    'statusCode': 500, 
                    'headers': {
                        'Access-Control-Allow-Origin': 'https://tht-marine.wats-ads.com',
                        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                        'Access-Control-Allow-Methods': 'GET,OPTIONS'
                    },
                    'body': json.dumps('署名付きURLの生成に失敗しました。')}
                    
        # 署名付きURL生成前のレスポンス       
        else:
            return {'statusCode': 200, 
                    'headers': {
                                'Access-Control-Allow-Origin': 'https://tht-marine.wats-ads.com',  
                                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                'Access-Control-Allow-Methods': 'GET,OPTIONS'
                            },
                    'body': json.dumps('オブジェクトキーが空または無効です。')}
                    
    # オブジェクトURLの生成確認
    else:
        return {'statusCode': 404, 
                'headers': {
                                'Access-Control-Allow-Origin': 'https://tht-marine.wats-ads.com', 
                                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                'Access-Control-Allow-Methods': 'GET,OPTIONS'
                            },
                'body': json.dumps('オブジェクトURLが確認できませんでした。')}
