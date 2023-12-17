import json
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
import logging
from googleapiclient.discovery import build

def format_date(date_str):
    # ISO 8601 形式の日付を解析
    date_obj = datetime.fromisoformat(date_str.rstrip('Z'))
    
    # UTC時間を日本時間（JST, UTC+9）に変換
    jst = timezone(timedelta(hours=9))
    date_obj_jst = date_obj.replace(tzinfo=timezone.utc).astimezone(jst)
    
    # 日付を YYYY年MM月DD日 形式でフォーマット
    return date_obj_jst.strftime('%Y年%m月%d日')


def lambda_handler(event, context):

    logging.basicConfig(level=logging.INFO)

    try:
        # Google Sheets APIの認証設定
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('service-account.json', scopes)
        client = gspread.authorize(creds)
        service = build('sheets', 'v4', credentials=creds)
        

        # スプレッドシートを開き、特定のワークシートを選択
        spreadsheet_id = '10UU_zWC_NuFC2_MAz0XaS6YmPdHtTWp-RJpZ3AZYZp8'
        sheet_name = 'Warranty'
        sheet = client.open('THT.Warranty.Requests').worksheet(sheet_name)
        
        
        
        # 現在の日時を YYYY/MM/DD/SS フォーマットで取得
        current_time_jst = datetime.utcnow() + timedelta(hours=9)
        current_time = current_time_jst.strftime('%Y年%m月%d日%H時%M分')
        
        
        
        # DynamoDBイベントを処理
        for record in event['Records']:
            # レコードがデータ更新イベントの場合にのみ処理
            if record['eventName'] == 'MODIFY':
                new_data = record['dynamodb']['NewImage']
                
                formatted_sale_date = format_date(new_data['saleDate']['S'])
                formatted_warranty_end_date = format_date(new_data['warrantyEndDate']['S'])
                
                #オブジェクトURLのリンク化
                object_url = new_data.get('objectUrl', {}).get('S', '')
                formatted_object_url = f'=HYPERLINK("{object_url}", "{object_url}")' if object_url else ''
                
                # APIのURLを取得
                transaction_id = new_data.get('transactionID', {}).get('S', '')
                product_number = new_data.get('productNumber', {}).get('S', '')
                api_data = f'https://wrsfymaea8.execute-api.ap-northeast-1.amazonaws.com/main/get/spreadsheet?transactionID={transaction_id}&productNumber={product_number}'
                
                
                # スプレッドシートへの書き込み
                data_to_write = [
                current_time,
                new_data.get('transactionID', {}).get('S', ''),
                new_data.get('productNumber', {}).get('S', ''),
                new_data.get('model', {}).get('S', ''),
                new_data.get('serialNo', {}).get('S', ''),
                new_data.get('subStoreName', {}).get('S', ''),
                new_data.get('userLastName', {}).get('S', ''),
                new_data.get('userFirstName', {}).get('S', ''),
                new_data.get('postalCode', {}).get('S', ''),
                new_data.get('prefecture', {}).get('S', ''),
                new_data.get('address', {}).get('S', ''),
                new_data.get('phoneNumber', {}).get('S', ''),
                new_data.get('email', {}).get('S', ''),
                new_data.get('purpose', {}).get('S', ''),
                api_data,
                formatted_sale_date,
                formatted_warranty_end_date,
                '✔' if new_data.get('checklistItem1', {}).get('BOOL', False) else '',
                '✔' if new_data.get('checklistItem2', {}).get('BOOL', False) else '',
                '✔' if new_data.get('checklistItem3', {}).get('BOOL', False) else '',
                '✔' if new_data.get('checklistItem4', {}).get('BOOL', False) else '',
                '✔' if new_data.get('checklistItem5', {}).get('BOOL', False) else '',
                '✔' if new_data.get('checklistItem6', {}).get('BOOL', False) else '',
                '✔' if new_data.get('checklistItem7', {}).get('BOOL', False) else '',
                '✔' if new_data.get('checklistItem8', {}).get('BOOL', False) else '',
                '✔' if new_data.get('checklistItem9', {}).get('BOOL', False) else '',
                '✔' if new_data.get('checklistItem10', {}).get('BOOL', False) else '',
                new_data.get('userAgent', {}).get('S', ''),
                new_data.get('ipAddress', {}).get('S', '')
            ]
            response = sheet.append_row(data_to_write)
            row_number = response['updates']['updatedRange'].split('!')[1].split(':')[0][1:]
            
            # objectUrlをリンクとして設定
            object_url = new_data.get('objectUrl', {}).get('S', '')
            if object_url:
                range_ = f'{sheet_name}!O{row_number}'  # O列の対象行

                requests = {
                    "requests": [
                        {
                            "updateCells": {
                                "rows": [
                                    {
                                        "values": [
                                            {
                                                "userEnteredValue": {
                                                    "formulaValue": f'=HYPERLINK("{api_url}", "{api_url}")'
                                                }
                                            }
                                        ]
                                    }
                                ],
                                "fields": "userEnteredValue",
                                "start": {
                                    "sheetId": sheet.id,
                                    "rowIndex": int(row_number) - 1,
                                    "columnIndex": 14  # O列は15番目（インデックスは14）
                                }
                            }
                        }
                    ]
                }

                # リクエストを送信
                service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=requests).execute()

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
