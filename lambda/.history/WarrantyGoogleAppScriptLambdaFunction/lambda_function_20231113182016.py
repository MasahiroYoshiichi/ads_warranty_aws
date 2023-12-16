import json
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def lambda_handler(event, context):
    # 現在の日付からシート名を決定（例: "warranty2023-11"）
    current_month = datetime.now().strftime("warranty%Y-%m")
    
    # Google Sheets APIの認証設定
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = ServiceAccountCredentials.from_json_keyfile_name('service-account.json', scopes)
    client = gspread.authorize(creds)

    # スプレッドシートを開く（存在しない場合は新規作成）
    try:
        sheet = client.open('SpreadsheetName').worksheet(current_month)
    except gspread.exceptions.WorksheetNotFound:
        sheet = client.open('SpreadsheetName').add_worksheet(title=current_month, rows="100", cols="20")

    # DynamoDBイベントを処理
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_data = record['dynamodb']['NewImage']

            # スプレッドシートに書き込むデータを整形
            data_to_write = [new_data['column1']['S'], new_data['column2']['S'], ...]

            # スプレッドシートにデータを追加
            sheet.append_row(data_to_write)

    return {
        'statusCode': 200,
        'body': json.dumps('Data written to Google Sheets successfully!')
    }
