import base64
import json
import boto3

# Initialize the DynamoDB client
dynamodb = boto3.client('dynamodb')

# Define the DynamoDB table
table_name = 'sourceJsonTable'


def lambda_handler(event, context):
    for record in event['Records']:
        kinesis_record = record['kinesis']
        kinesis_payload = base64.b64decode(kinesis_record['data']).decode('utf-8')
        print(f"Received data: {kinesis_payload}")

        # Transform the JSON data for DynamoDB
        data = json.loads(kinesis_payload)
        item = {
            "id": {"S": data['id']},
            "name": {"S": data["name"]},
            "school": {"S": data["School"]}  # Note: Case-sensitive field names
        }

        try:
            response = dynamodb.put_item(
                TableName=table_name,
                Item=item
            )
            print(f"Data added to DynamoDB: {response}")
        except Exception as e:
            print(f"Error adding data to DynamoDB: {str(e)}")

    # Return a response if needed
    return {
        'statusCode': 200,
        'body': 'Data processed and added to DynamoDB'
    }
