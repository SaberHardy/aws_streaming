import json
import urllib.request
import boto3


def lambda_handler(event, context):
    api_url = "https://jsonplaceholder.typicode.com/users"
    kinesis_stream_name = "api_stream_kinesis_26_10"

    try:
        with urllib.request.urlopen(api_url) as response:
            data = json.loads(response.read().decode())

        user_json = json.dumps(data)

        kinesis_client = boto3.client('kinesis')
        kinesis_client.put_record(
            StreamName=kinesis_stream_name,
            Data=user_json,
            PartitionKey="123")

        return {
            'statusCode': 200,
            'body': 'Data sent to Kinesis successfully'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
