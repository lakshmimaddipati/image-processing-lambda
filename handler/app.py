import base64
import json
import boto3
from io import BytesIO
# from PIL import Image, ImageOps ##python imaging library for providing pyhton with image editing properties
import os
import uuid
from datetime import datetime

##"arn": "arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-pillow:1"  Layer

s3 = boto3.client('s3')
#size = int(os.getenv('THUMBNAIL_SIZE'))
size = 128
dbtable = os.getenv('DYNAMODB_TABLE')
dynamodb = boto3.resource(
    'dynamodb', region_name=str(os.getenv('REGION_NAME')))  ##we need a handle to perform crud ops


def decode_event(event):
    # decoded_body = base64.b64decode(str(event['Records'][0]['body'])[2:-1]).decode('utf-8')
    # event['Records'][0]['body'] = decoded_body
    event = json.load(event)
    print('Input Event ------')
    print(event)
    print('------------------')

    return event


def s3_thumbnail_generator(event, context):
    # event = decode_event(event)   
    print("Event::", event)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    img_size = event['Records'][0]['s3']['object']['size']

    ##create image only if it is not in thumbnail

    if (not key.endswith("_thumbnail.png")):
        image = get_s3_img(bucket, key)

        thumbnail = image_to_thumbnail(image) ##resizing the image

        thumbnail_key = new_filename(key)   ##new thumbnail key

        url = upload_to_s3(bucket, thumbnail_key, thumbnail, img_size)  ##passing url to upload to s3
        print(url, "url")
        return url

def get_s3_img(bucket, key):
    print(bucket, "bucket")
    print(key, "key")
    res = s3.get_object(Bucket = bucket, Key = key)
    #res = s3.Object(bucket, key + '_DONE').put(Body="")
    img_content = res['Body'].read() ##parse that reponse

    file = BytesIO(img_content) ##create byte (actual file) from that image
    # img = Image.open(file)  ##create the image
    return file 

def image_to_thumbnail(img):
    # return ImageOps.fit(img, (size,size), Image.ANTIALIAS)  ##gives actual reduced image
    return img


def new_filename(key):
    key_split = key.rsplit('.', 1)
    return key_split[0] + "_thumbnail.png"  ##helper func that appends this text to each reduced img


def upload_to_s3(bucket, key, img, img_size):
    output_thumbnail = BytesIO() ##saving img to BytesIO object to avoid writing to disk

    img.save(output_thumbnail, format='PNG')

    output_thumbnail.seek(0) ##sets the current position of file to 0

    response = s3.put_object(
    ACL='public-read',
    Body=output_thumbnail,
    Bucket=bucket,
    ContentType='image/png',
    Key=key
)
    print(response)

    url = '{}/{}/{}'.format(s3.meta.endpoint_url, bucket, key)

    # save image url to db:
    s3_save_thumbnail_url_to_dynamodb(url_path=url, img_size=img_size)

    return url


def s3_save_thumbnail_url_to_dynamodb(url_path, img_size):
    toint = float(img_size*0.53)/1000  ##rough estimate
    table = dynamodb.Table(dbtable)
    response = table.put_item(
        Item={
            'id': str(uuid.uuid4()),
            'url': str(url_path),
            'approxReducedSize': str(toint) + str(' KB'),
            'createdAt': str(datetime.now()),
            'updatedAt': str(datetime.now())
        }
    )

# get all image urls from the bucked and show in a json format
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(response)
    }


def s3_get_thumbnail_urls(event, context):
    # get all image urls from the db and showing in a json format
    table = dynamodb.Table(dbtable)
    response = table.scan()
    data = response['Items']
    # paginate through the results in a loop
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(data)
    }

def s3_get_item(event, context):
    # event = decode_event(event)

    table = dynamodb.Table(dbtable)
    response = table.get_item(Key={
        'id': event['pathParameters']['id']
    })

    item = response['Item']

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(item),
        'isBase64Encoded': False,
    }


def s3_delete_item(event, context):
    # event = decode_event(event)
    item_id = event['pathParameters']['id']

    # Set the default error response
    response = {
        "statusCode": 500,
        "body": f"An error occured while deleting post {item_id}"
    }
    table = dynamodb.Table(dbtable)
    response = table.delete_item(Key={
        'id': item_id
    })
    all_good_response = {
        "deleted": True,
        "itemDeletedId": item_id
    }

   # If deletion is successful for post
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        response = {
            "statusCode": 200,
            'headers': {'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'},
            'body': json.dumps(all_good_response),
        }
    return response
    