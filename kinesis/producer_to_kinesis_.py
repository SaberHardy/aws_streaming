import boto3
import json

def lambda_handler(event, context):
    # Replace 'apiStreamData25_10' with your Kinesis Data Stream name
    stream_name = 'apiStreamData25_10'

    # Initialize the Kinesis client
    kinesis_client = boto3.client('kinesis')

    # Read the JSON file from S3 or any other source
    s3_bucket = 'bdus829'
    filename = 'source.json'
    partition_key = "123"
    s3_client = boto3.client('s3')

    # Read and serialize the JSON data
    with open(filename, 'r') as f:
        data = json.load(f)

    # Serialize the data to bytes
    data_bytes = json.dumps(data).encode('utf-8')

    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=data_bytes,
        PartitionKey=partition_key
    )

    print(response)

    return {
        'statusCode': 200,
        'body': f'Data put into Kinesis stream with sequence number: {response["SequenceNumber"]}'
    }
