import json
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def lambda_handler(event, context):
    
    # Google Sheets APIの認証設定
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = ServiceAccountCredentials.from_json_keyfile_name('service-account.json', scopes)
    client = gspread.authorize(creds)
    sheet = client.open('Warranty').worksheet(Warranty)
    # DynamoDBイベントを処理
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_data = record['dynamodb']['NewImage']

            # スプレッドシートに書き込むデータを整形
            data_to_write = [new_data['column1']['S'],
                             new_data['column2']['S'], ...]

            # スプレッドシートにデータを追加
            sheet.append_row(data_to_write)

    return {
        'statusCode': 200,
        'body': json.dumps('Data written to Google Sheets successfully!')
    }
