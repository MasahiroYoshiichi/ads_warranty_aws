import boto3
import json
from PyPDF2 import PdfWriter, PdfReader, Transformation
import pytz
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from io import BytesIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
lambda_client = boto3.client('lambda')

font_stream = download_font_from_s3(font_bucket_name, font_file_key)
font_stream.seek(0)
font_bucket_name = 'warranty-font-bucket'
font_file_key = 'NotoSansJP-VariableFont_wght.ttf'
pdfmetrics.registerFont(TTFont('JapaneseFont', font_stream))

def generate_s3_object_url(bucket, key, region):
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
    
def store_url_in_dynamodb(table_name, url, yourPrimaryKeyValue):
    dynamodb_client.put_item(
        TableName=table_name,
        Item={
            'transactionID': {'S': yourPrimaryKeyValue}, # Replace with your primary key and its value
            'url': {'S': url}
        }
    )

def download_font_from_s3(bucket, key):
    try:
        font_response = s3_client.get_object(Bucket=bucket, Key=key)
        font_data = font_response['Body'].read()
        return BytesIO(font_data)
    except Exception as e:
        logger.error(f"フォントのダウンロードに失敗しました: {e}")
        raise
    
def invoke_lambda(lambda_function_name, payload):
    lambda_client.invoke(
        FunctionName=lambda_function_name,
        InvocationType='Event', # Use 'RequestResponse' for synchronous execution
        Payload=json.dumps(payload)
    )

def create_table(new_image):
    try:
        data = [
            ["氏名", "型式", "シリアルNo.", "販売日", "保証終了日"],
            [new_image['userFullName']['S'], new_image['model']['S'], new_image['serialNo']['S'], new_image['saleDate']['S'], new_image['warrantyEndDate']['S']]
        ]
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=18)
        table_style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, -1), 'JapaneseFont'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ])
        
        table = Table(data)
        table.setStyle(table_style)
        
        elements = [table]
        doc.build(elements)
        
        buffer.seek(0)
        return buffer
    except Exception as e:
        logger.error(f"テーブルの作成に失敗しました: {e}")
        raise

def lambda_handler(event, context):
    pdf_warranty_bucket = 'warranty-pdf-bucket'
    pdf_table_bucket = 'warranty-table-pdf-bucket'
    pdf_format_bucket = 'warranty-format-bucket'
    pdf_format_bucket_key = 'format.pdf'
    
    try:
        original_pdf_response = s3_client.get_object(Bucket=pdf_format_bucket, Key=pdf_format_bucket_key)
        original_pdf = original_pdf_response['Body'].read()
    except Exception as e:
        logger.error(f"フォーマットPDFの取得に失敗しました: {e}")
        raise
    
    try:
        for record in event['Records']:
            new_image = record['dynamodb']['NewImage']
            logger.info(f'新規データ: {json.dumps(new_image)}')
            
            table_pdf_buffer = create_table(new_image)
            
            timestamp_jst = datetime.now(tz=pytz.timezone('Asia/Tokyo')).strftime('%Y年%m月%d日%H時%M分%S秒')
            product_number = new_image.get('productNumber', {}).get('S', 'unknown')
            table_pdf_key = f"table_{product_number}_{timestamp_jst}.pdf"
            s3_client.put_object(Bucket=pdf_table_bucket, Key=table_pdf_key, Body=table_pdf_buffer.getvalue())
            logger.info('テーブルのみを含むPDFがS3に保存されました。')
                
            writer = PdfWriter()
            original_pdf_reader = PdfReader(BytesIO(original_pdf))
            table_pdf_reader = PdfReader(table_pdf_buffer)
            
            x_position = 100  # X座標
            y_position = 150  # Y座標
            page = original_pdf_reader.pages[0]
            
            transformation = Transformation().translate(tx=x_position, ty=y_position)
            table_pdf_page = table_pdf_reader.pages[0]
            table_pdf_page.add_transformation(transformation)
            
            writer.add_page(page)
            
            output_pdf_stream = BytesIO()
            writer.write(output_pdf_stream)
            output_pdf_stream.seek(0)
            
            timestamp_jst = datetime.now(tz=pytz.timezone('Asia/Tokyo')).strftime('%Y年%m月%d日%H時%M分%S秒')
            product_number = new_image.get('productNumber', {}).get('S', 'unknown')
            pdf_warranty_bucket_key = f"{product_number}_{timestamp_jst}.pdf"
            
            s3_client.put_object(Bucket=pdf_warranty_bucket, Key=pdf_warranty_bucket_key, Body=output_pdf_stream.getvalue())
            
            bucket_name = pdf_warranty_bucket
            key = pdf_warranty_bucket_key
            region = 'ap-northeast-1'
            s3_url = generate_s3_object_url(bucket_name, key, region)
            
            dynamodb_table_name = 'WarrantyTable',
            yourPrimaryKeyValue = new_image.get('transactionID', {}).get('S', 'unknown')
            store_url_in_dynamodb(dynamodb_table_name, s3_url, yourPrimaryKeyValue)
            
            lambda_function_name = 'WarrantySESLambdaFunction'
            payload = {'url': s3_url}
            invoke_lambda(lambda_function_name, payload)

            
            logger.info('PDFが変更され、アップロードされました。')
    except Exception as e:
        logger.error(f"PDFの処理に失敗しました: {e}")
        raise
