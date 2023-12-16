import boto3
from botocore.exceptions import ClientError

# Initialize SES client
ses_client = boto3.client('ses', region_name='your-region')  # Replace with your AWS region

def send_email(source_email, destination_emails, subject, body, url):
    try:
        response = ses_client.send_email(
            Source=source_email,
            Destination={
                'ToAddresses': destination_emails
            },
            Message={
                'Subject': {
                    'Data': subject
                },
                'Body': {
                    'Text': {
                        'Data': body + "\n\nURL: " + url
                    }
                }
            }
        )
        return response
    except ClientError as e:
        print(e.response['Error']['Message'])
        raise

def lambda_handler(event, context):
    s3_url = event.get('url', '')

    source_email = 'ads.tht.warranty@gmail.com'
    destination_emails = ['tohatsu-service@tohatsu.co.jp', 'tohatsu-pservice@tohatsu.co.jp'] 
    subject = '保証証生成のお知らせ'
    body =  body = """
お客様へ

この度は当社の製品をご購入いただき、誠にありがとうございます。
保証書が生成されましたので、以下のリンクからダウンロードしてください。

{url}

今後とも当社製品をどうぞよろしくお願いいたします。
    """.format(url=s3_url)

    response = send_email(source_email, destination_emails, subject, body, s3_url)
    print(response)

    return {
        'statusCode': 200,
        'body': json.dumps('Email送信に成功しました。')
    }
