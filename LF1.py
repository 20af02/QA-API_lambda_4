import json
from variables import *
import boto3
from requests_aws4auth import AWS4Auth
import requests
from boto3.dynamodb.conditions import Key
from collections import OrderedDict
import uuid


def parse_elastic_response(response):
    tmp = list()
    for hit in response['hits']['hits']:
        tmp.append(hit['_source']['id'])
    return tmp


def lambda_handler(event, context):
    host = 'https://search-post1-kcl6ksh2qggcjszkz3m7rmqilq.us-east-1.es.amazonaws.com/'
    path = 'posts/_search'
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.session.Session(aws_access_key_id=ACCESS_KEY,
                                        aws_secret_access_key=SECRET_KEY, region_name=region).get_credentials()
    awsauth = AWS4Auth(ACCESS_KEY, SECRET_KEY, region,
                       service, credentials.token)

    # client = boto3.client('dynamodb', region_name='us-east-1')
    sns_client = boto3.client('sns', region_name='us-east-1',
                              aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    lex_tags_client = boto3.client('lexv2-runtime', region_name='us-east-1',
                                   aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    # find if plane text vs regular tags
    tags = event["queryStringParameters"]["q"]

    # if planetext
    sessionId = str(uuid.uuid4())
    if tags:
        lex_tags_data_get_intent = lex_tags_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId='en_US',
            sessionId=sessionId,
            text="show me posts"
        )
        lex_tags_data_get_data = lex_tags_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId='en_US',
            sessionId=sessionId,
            text=tags
        )
        tags = list()
        interps = lex_tags_data_get_data['interpretations']
        interp = [
            x for x in interps if x['intent']['name'] == 'SearchPostsIntent'][0]['intent']
        tags_ = [x['value']['resolvedValues']
                 for x in interp['slots']['tagOne']['values']]
        for tmp in tags_:
            for x in tmp:
                tags.append(x)

    # query lex

    # otherwise keep indeex search same as before (other values from lex-query should overwrite these)

    url = host+path
    headers = {'Content-Type': "application/json",
               'Accept': "application/json"}
    AWS_ACCESS_KEY_ID = ACCESS_KEY
    AWS_SECRET_ACCESS_KEY = SECRET_KEY
    print(event)
    try:
        # Modify payload to only get 3 tags
        payload = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "tags": x
                            }
                        } for x in tags
                    ]
                }
            }
        }

        r = requests.get(url, auth=(USER, PASS), headers=headers, json=payload)
        print(r.content)
        print("Request Generated Successfully")
    except Exception as e:
        print("Error generating Request:")
        print(e)
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': e

        }

    # DynamoDB for text
    results = json.loads(r.content)
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
                                  aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        table = dynamodb.Table('posts')
        ids = parse_elastic_response(results)
        if ids == []:
            return {
                'statusCode': 404,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': "no answers found for this category"

            }
        responses = list()
        ES_URL = ''
        for id in ids:
            resp = table.query(KeyConditionExpression=Key('id').eq(str(id)), )
            responses.append(resp)
        dict = OrderedDict()
        for data in responses:
            dict[data['Items'][0]['id']] = [data['Items']
                                            [0]['date'], data['Items'][0]['posts']]
        print(dict)
        # send to sns
        sns_client.publish(
            TopicArn=TOPIC_ARN,
            Message=json.dumps(dict),
            Subject="Posts for tags: " + ",".join(tags)
        )
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps(dict)
        }
    except Exception as e:
        print("Error generating Request:")
        print(e)
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': e

        }
