import boto3
import json
import uuid

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('WarrantyTable')

def lambda_handler(event, context)
    # イベント情報出力
    body = json.loads(event['body'])
    
    # オブジェクト生成
    transaction_id = str(uuid.uuid4())
    product_number = f"{body['model']}-{body['serialNo']}"
    userFullName = f"{body['userLastName']} {body['userFirstName']}"
    
    # アクセス元情報を取得
    user_agent = event['headers'].get('User-Agent', '')
    ip_address = event['requestContext']['identity']['sourceIp']

    #DynamoDBへデータを格納
    try:
        table.put_item(
            Item={
                'transactionID': transaction_id,
                'productNumber': product_number,
                'model': body['model'],
                'serialNo': body['serialNo'],   
                'subStoreName': body['subStoreName'],
                'userLastName': body['userLastName'],
                'userFirstName': body['userFirstName'],
                'userFullName': userFullName,
                'postalCode': body['postalCode'],
                'prefecture': body['prefecture'],
                'address': body['address'],
                'phoneNumber': body['phoneNumber'],
                'email': body['email'],
                'purpose': body['purpose'],
                'objectUrl': "",
                'saleDate': body['saleDate'],
                'warrantyEndDate': body['warrantyEndDate'],
                'checklistItem1': body['checklistItem1'],
                'checklistItem2': body['checklistItem2'],
                'checklistItem3': body['checklistItem3'],
                'checklistItem4': body['checklistItem4'],
                'checklistItem5': body['checklistItem5'],
                'checklistItem6': body['checklistItem6'],
                'checklistItem7': body['checklistItem7'],
                'checklistItem8': body['checklistItem8'],
                'checklistItem9': body['checklistItem9'],
                'checklistItem10': body['checklistItem10'],
                'userAgent': user_agent,  
                'ipAddress': ip_address,  
            }
        )

        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': 'https://tht-marine.wats-ads.com/', 
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            },
            #　responceにパラメータを格納
            'body': json.dumps({'message': '登録完了しました', 'transactionID': transaction_id, 'productNumber': product_number})
        }
    except Exception as e:
        print('登録に失敗しました', e)
        return {
            'statusCode': 500,
            'body': json.dumps('DynamoDBへの登録が失敗しました。')
        }
