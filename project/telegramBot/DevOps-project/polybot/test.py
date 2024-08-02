import os 
import json
import boto3

secrets_manager_client = boto3.client('secretsmanager', region_name='us-east-1')
secret_name = 'HasanSM'
response = secrets_manager_client.get_secret_value(SecretId=secret_name)
secret_string = response['SecretString']
secret_dict = json.loads(secret_string)
TELEGRAM_TOKEN = secret_dict['TELEGRAM_TOKEN']
print(TELEGRAM_TOKEN)
