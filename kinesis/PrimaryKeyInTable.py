import boto3

# Initialize the DynamoDB client
dynamodb = boto3.client('dynamodb')

# Replace 'YourTableName' with your table name
table_name = 'sourceJsonTable'

response = dynamodb.describe_table(TableName=table_name)
key_schema = response['Table']['KeySchema']

for key in key_schema:
    if key['KeyType'] == 'HASH':
        primary_key_name = key['AttributeName']
        print(f"Primary Key Attribute Name: {primary_key_name}")
