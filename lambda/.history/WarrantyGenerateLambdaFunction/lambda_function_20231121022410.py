import boto3
import json
from PyPDF2 import PdfWriter, PdfReader, Transformation
import pytz
import fitz
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from io import BytesIO
import logging
import os


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')

def download_font_from_s3(bucket, key):
    try:
        font_response = s3_client.get_object(Bucket=bucket, Key=key)
        font_data = font_response['Body'].read()
        return BytesIO(font_data)
    except Exception as e:
        logger.error(f"フォントのダウンロードに失敗しました: {e}")
        raise
    
def format_date(date_str):
    # ISO 8601 形式の日付を解析
    date_obj = datetime.fromisoformat(date_str.rstrip('Z'))
    # 日付を YYYY年MM月DD日 形式でフォーマット
    return date_obj.strftime('%Y年%m月%d日')


def create_table(new_image):
    try:
        # 日付のフォーマット
        formatted_sale_date = format_date(new_image['saleDate']['S'])
        formatted_warranty_end_date = format_date(new_image['warrantyEndDate']['S'])
        
        data = [
            ["氏名", new_image['userFullName']['S']],
            ["型式", new_image['model']['S']],
            ["シリアルNo.", new_image['serialNo']['S']],
            ["販売日", formatted_sale_date],
            ["保証終了日", formatted_warranty_end_date]
        ]
        
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), leftMargin=250)

        # テーブルスタイルのカスタマイズ
        table_style = TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'JapaneseFont'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('SIZE', (0, 0), (-1, -1), 8), 
            ('MINIMUMHEIGHT', (0, 0), (-1, -1), 5),  
        ])
        
        table = Table(data, colWidths=[100, 200]) 
        table.setStyle(table_style)

        # テーブルの上部にスペースを作成
        spacer = Spacer(1, 20)  

        elements = [spacer, table] 
        doc.build(elements)

        buffer.seek(0)
        return buffer
    except Exception as e:
        logger.error(f"テーブルの作成に失敗しました: {e}")
        raise

def merge_pdf(original_pdf_stream, table_pdf_stream, owner_password):
    writer = PdfWriter()
    original_pdf_reader = PdfReader(original_pdf_stream)
    table_pdf_reader = PdfReader(table_pdf_stream)
    
    page = original_pdf_reader.pages[0]
    table_page = table_pdf_reader.pages[0]
    
    # PDFをマージ
    page.merge_page(table_page)
    writer.add_page(page)
    
    temp_pdf_stream = BytesIO()
    writer.write(temp_pdf_stream)
    temp_pdf_stream.seek(0)
    
    # PyMuPDFを使用してセキュリティ設定を適用
    output_file_path = "/tmp/output.pdf"
    doc = fitz.open("pdf", temp_pdf_stream.read())
    doc.save(
        output_file_path,
        encryption=fitz.PDF_ENCRYPT_AES_256,  # AES 256ビット暗号化を使用
        owner_pw=owner_password,              # 所有者パスワードの設定
        # user_pw=user_password,                # ユーザーパスワードの設定
        permissions=fitz.PDF_PERM_PRINT | fitz.PDF_PERM_COPY  # 印刷とコピーの許可
    )
    merged_pdf_stream = BytesIO(open(output_file_path, "rb").read())
    merged_pdf_stream.seek(0)
    return merged_pdf_stream

def generate_s3_object_url(bucket, key, region):
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
    
def store_url_in_dynamodb(table_name, url, transactionID, productNumber):
    try:
        dynamodb_client.update_item(
            TableName=table_name,
            Key={
                'transactionID': {'S': transactionID},
                'productNumber': {'S': productNumber}
            },
            UpdateExpression="set objectUrl = :u",
            ExpressionAttributeValues={
                ':u': {'S': url}
            }
        )
        logger.info(f"URLがDynamoDBで更新されました: {transactionID}")
    except Exception as e:
        logger.error(f"DynamoDBでのURL更新に失敗しました: {e}")
        raise
    
def lambda_handler(event, context):
    # S3バケットの設定
    pdf_warranty_bucket = 'warranty-pdf-bucket'
    pdf_table_bucket = 'warranty-table-pdf-bucket'
    pdf_format_bucket = 'warranty-format-bucket'
    pdf_format_bucket_key = 'format.pdf'
    font_bucket_name = 'warranty-font-bucket'
    font_file_key = 'NotoSansJP-Light.ttf'
    owner_password = os.environ.get('OWNER_PASSWORD')
    
    logger.info(f'新規データ: {json.dumps(event)}')

    
    # 生成PDFの日本語フォントを取得
    try:
        font_stream = download_font_from_s3(font_bucket_name, font_file_key)
        font_stream.seek(0)
        pdfmetrics.registerFont(TTFont('JapaneseFont', font_stream))
    except Exception as e:
        logger.error(f"日本語フォントの取得に失敗しました: {e}")
        raise
    
    # ベースとなるPDFファイルの取得
    try:
        original_pdf_response = s3_client.get_object(Bucket=pdf_format_bucket, Key=pdf_format_bucket_key)
        original_pdf = original_pdf_response['Body'].read()
    except Exception as e:
        logger.error(f"フォーマットPDFの取得に失敗しました: {e}")
        raise
    
    # DynamoDBから取得した各レコードに対して処理
    try:
        for record in event['Records']:
            if record['eventName'] == 'INSERT':
                new_image = record['dynamodb']['NewImage']
                logger.info(f'新規データ: {json.dumps(record)}')
                transactionID = new_image.get('transactionID', {}).get('S', 'unknown')
                product_number = new_image.get('productNumber', {}).get('S', 'unknown')

                
                # 新規データからテーブルを含むPDFを生成
                table_pdf_buffer = create_table(new_image)
                
                # タイムスタンプと製品番号の設定
                timestamp_jst = datetime.now(tz=pytz.timezone('Asia/Tokyo')).strftime('%Y年%m月%d日%H時%M分%S秒')
                product_number = new_image.get('productNumber', {}).get('S', 'unknown')

                # PDFの結合と変換
                merged_pdf_stream = merge_pdf(BytesIO(original_pdf), table_pdf_buffer, owner_password)
                
                # 最終PDFファイルのS3キー設定
                timestamp_jst = datetime.now(tz=pytz.timezone('Asia/Tokyo')).strftime('%Y年%m月%d日%H時%M分%S秒')
                pdf_warranty_bucket_key = f"{product_number}_{timestamp_jst}.pdf"
                
                # 最終PDFファイルをS3に保存
                s3_client.put_object(Bucket=pdf_warranty_bucket, Key=pdf_warranty_bucket_key, Body=merged_pdf_stream.getvalue())
                
                # 生成されたPDFのURLを生成
                region = 'ap-northeast-1'
                s3_url = generate_s3_object_url(pdf_warranty_bucket, pdf_warranty_bucket_key, region)
                
                # DynamoDBにURLを保存
                dynamodb_table_name = 'WarrantyTable'
                store_url_in_dynamodb(dynamodb_table_name, s3_url, transactionID, product_number)

                logger.info('PDFが変更され、アップロードされました。')
    except Exception as e:
        logger.error(f"PDFの処理に失敗しました: {e}")
        raise

