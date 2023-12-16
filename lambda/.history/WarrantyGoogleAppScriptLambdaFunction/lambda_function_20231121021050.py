import json
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import logging


def lambda_handler(event, context):

    logging.basicConfig(level=logging.INFO)

    try:
        # Google Sheets APIの認証設定
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = ServiceAccountCredentials.from_json_keyfile_name('service-account.json', scopes)
        client = gspread.authorize(creds)

        # スプレッドシートを開き、特定のワークシートを選択
        sheet = client.open('THT.Warranty.Requests').worksheet('Warranty')

        # DynamoDBイベントを処理
        for record in event['Records']:
            # レコードがデータ更新イベントの場合にのみ処理
            if record['eventName'] == 'MODIFY':
                new_data = record['dynamodb']['NewImage']
                data_to_write = [
                new_data.get('transactionID', {}).get('S', ''),
                new_data.get('productNumber', {}).get('S', ''),
                new_data.get('address', {}).get('S', ''),
                *[new_data.get(f'checklistItem{i}', {}).get('S', '') for i in range(1, 11)],
                new_data.get('email', {}).get('S', ''),
                new_data.get('model', {}).get('S', ''),
                new_data.get('objectUrl', {}).get('S', ''),
                new_data.get('phoneNumber', {}).get('S', ''),
                new_data.get('postalCode', {}).get('S', ''),
                new_data.get('prefecture', {}).get('S', ''),
                new_data.get('purpose', {}).get('S', ''),
                new_data.get('saleDate', {}).get('S', ''),
                new_data.get('serialNo', {}).get('S', ''),
                new_data.get('subStoreName', {}).get('S', ''),
                new_data.get('userFirstName', {}).get('S', ''),
                new_data.get('userFullName', {}).get('S', ''),
                new_data.get('userLastName', {}).get('S', ''),
                new_data.get('warrantyEndDate', {}).get('S', '')
            ]
            sheet.append_row(data_to_write)

        return {
            'statusCode': 200,
            'body': json.dumps('スプレッドシートへの書き込みが成功しました。')
        }
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('スプレッドシートへの書き込みが失敗しました。')
        }
