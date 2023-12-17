import boto3
import json
from PyPDF2 import PdfWriter, PdfReader, Transformation
import pytz
import fitz
from datetime import datetime, timezone, timedelta
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from io import BytesIO
import logging
import os

# ロギング設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# クラインアント設定
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')

# 日本語フォントの取得
def download_font_from_s3(bucket, key):
    try:
        font_response = s3_client.get_object(Bucket=bucket, Key=key)
        font_data = font_response['Body'].read()
        return BytesIO(font_data)
    except Exception as e:
        logger.error(f"フォントのダウンロードに失敗しました: {e}")
        raise
    
# ISO8601形式を変換
def format_date(date_str):
    # ISO 8601 形式の日付を解析
    date_obj = datetime.fromisoformat(date_str.rstrip('Z'))
    
    # UTC時間を日本時間（JST, UTC+9）に変換
    jst = timezone(timedelta(hours=9))
    date_obj_jst = date_obj.replace(tzinfo=timezone.utc).astimezone(jst)
    
    # 日付を YYYY年MM月DD日 形式でフォーマット
    return date_obj_jst.strftime('%Y年%m月%d日')


# 保証書テーブル作成
def create_table(new_image):
    try:
        # DynamoDBから取得した日付データを日本時間にフォーマット
        formatted_sale_date = format_date(new_image['saleDate']['S'])
        formatted_warranty_end_date = format_date(new_image['warrantyEndDate']['S'])
        
        # PDFテーブルに表示するデータを構成
        data = [
            ["氏名", new_image['userFullName']['S']],
            ["型式", new_image['model']['S']],
            ["シリアルNo.", new_image['serialNo']['S']],
            ["販売日", formatted_sale_date],
            ["保証終了日", formatted_warranty_end_date]
        ]

        # バイトストリームオブジェクトを作成
        buffer = BytesIO()
        
        # PDF文書の設定
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
        
        # テーブルスタイル作成
        table = Table(data, colWidths=[100, 200]) 
        table.setStyle(table_style)
        spacer = Spacer(1, 20)  
        elements = [spacer, table] 
        doc.build(elements)

        # バッファを先頭に戻す
        buffer.seek(0)
        # 作成したPDF文書のバイトストリームを返す
        return buffer
    except Exception as e:
        logger.error(f"テーブルの作成に失敗しました: {e}")
        raise


def merge_pdf(original_pdf_stream, table_pdf_stream, owner_password):
    # PDFの書き込み用オブジェクトの初期化
    writer = PdfWriter()
    
    # フォーマットPDFとテーブルPDFを読み込む
    original_pdf_reader = PdfReader(original_pdf_stream)
    table_pdf_reader = PdfReader(table_pdf_stream)
    
    # PDFから最初のページを取得
    page = original_pdf_reader.pages[0]
    table_page = table_pdf_reader.pages[0]
    
    # テーブルページを元のページにマージ
    page.merge_page(table_page)
    writer.add_page(page)
    
    # 一時的なPDFストリームを作成、マージされた内容書き込み
    temp_pdf_stream = BytesIO()
    writer.write(temp_pdf_stream)
    temp_pdf_stream.seek(0)
    
    # PyMuPDFを使用してセキュリティ設定を適用
    output_file_path = "/tmp/output.pdf"
    doc = fitz.open("pdf", temp_pdf_stream.read())

    # PDFを暗号化して保存
    doc.save(
        output_file_path,
        encryption=fitz.PDF_ENCRYPT_AES_256,  # AES 256ビット暗号化を使用
        owner_pw=owner_password,              # 所有者パスワードの設定
        # user_pw=user_password,              # ユーザーパスワードの設定（今回は設定無し）
        permissions=fitz.PDF_PERM_PRINT | fitz.PDF_PERM_COPY  # 印刷とコピーの許可
    )

    # 暗号化されたPDFをストリームを返却
    merged_pdf_stream = BytesIO(open(output_file_path, "rb").read())
    merged_pdf_stream.seek(0)
    return merged_pdf_stream


# S3オブジェクトURLを生成
def generate_s3_object_url(bucket, key, region):
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
    
# WarrantyテーブルにS3オブジェクトURLを格納
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
    # 連携データ出力
    logger.info(f'新規データ: {json.dumps(event)}')
     
     
    # S3バケットの設定
    pdf_warranty_bucket = 'warranty-pdf-bucket'
    pdf_table_bucket = 'warranty-table-pdf-bucket'
    assets_bucket = 'warranty-assets-bucket'
    pdf_format_bucket_key = 'format/format.pdf'
    font_file_key = 'font/NotoSansJP-Light.ttf'
    
    # PDFオーナーパスワード
    owner_password = os.environ.get('OWNER_PASSWORD')
    
    # 生成PDFの日本語フォントを取得
    try:
        font_stream = download_font_from_s3(assets_bucket, font_file_key)
        font_stream.seek(0)
        pdfmetrics.registerFont(TTFont('JapaneseFont', font_stream))
    except Exception as e:
        logger.error(f"日本語フォントの取得に失敗しました: {e}")
        raise
    
    # フォーマットPDFファイルの取得
    try:
        original_pdf_response = s3_client.get_object(Bucket=assets_bucket, Key=pdf_format_bucket_key)
        original_pdf = original_pdf_response['Body'].read()
    except Exception as e:
        logger.error(f"フォーマットPDFの取得に失敗しました: {e}")
        raise
    
    # PDF保証書の生成と保管
    try:
        for record in event['Records']:
            if record['eventName'] == 'INSERT':
                
                # 新規データを取得
                new_image = record['dynamodb']['NewImage']
                
                # PKとSKを取得
                transactionID = new_image.get('transactionID', {}).get('S', 'unknown')
                product_number = new_image.get('productNumber', {}).get('S', 'unknown')

                # 新規データからテーブルを含むPDFを生成
                table_pdf_buffer = create_table(new_image)
                
                # タイムスタンプ生成
                timestamp_jst = datetime.now(tz=pytz.timezone('Asia/Tokyo')).strftime('%Y年%m月%d日%H時%M分%S秒')

                # PDFの結合と変換
                merged_pdf_stream = merge_pdf(BytesIO(original_pdf), table_pdf_buffer, owner_password)
                
                # 保証書PDFファイルのS3キー設定
                timestamp_jst = datetime.now(tz=pytz.timezone('Asia/Tokyo')).strftime('%Y年%m月%d日%H時%M分%S秒')
                pdf_warranty_bucket_key = f"{product_number}_{timestamp_jst}.pdf"
                
                # 保諸所PDFファイルをS3に保存
                s3_client.put_object(Bucket=pdf_warranty_bucket, Key=pdf_warranty_bucket_key, Body=merged_pdf_stream.getvalue())
                
                # 保証書PDFのURLを生成
                region = 'ap-northeast-1'
                s3_url = generate_s3_object_url(pdf_warranty_bucket, pdf_warranty_bucket_key, region)
                
                # DynamoDBにURLを保存
                dynamodb_table_name = 'WarrantyTable'
                store_url_in_dynamodb(dynamodb_table_name, s3_url, transactionID, product_number)

                logger.info('PDFが生成され、アップロードされました。')
    except Exception as e:
        logger.error(f"PDFの処理に失敗しました: {e}")
        raise

