import flask
from flask import request
import os
import boto3
import json
from bot import ObjectDetectionBot


app = flask.Flask(__name__)

secrets_manager_client = boto3.client('secretsmanager', region_name='us-east-1')
secret_name = 'HasanSM'
response = secrets_manager_client.get_secret_value(SecretId=secret_name)
secret_string = response['SecretString']
secret_dict = json.loads(secret_string)
TELEGRAM_TOKEN = secret_dict['TELEGRAM_TOKEN']


#TELEGRAM_APP_URL = os.environ['TELEGRAM_APP_URL']
TELEGRAM_APP_URL = 'hasandm.atech-bot.click'

@app.route('/', methods=['GET'])
def index():
    return 'Ok'

@app.route(f'/{TELEGRAM_TOKEN}/', methods=['POST'])
def webhook():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'

@app.route(f'/results/', methods=['GET', 'POST'])
def results():
    prediction_id = request.args.get('predictionId')

    # Retrieve results from DynamoDB and send to the end-user
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table_name = 'HasanDB'
    table = dynamodb.Table(table_name)
    response = table.get_item(
        Key={
            'prediction_id': prediction_id
        }
    )
    item = response.get('Item')

    chat_id = item.get('chat_id')
    text_results = item.get('labels')

    bot.send_text(chat_id, text_results)
    return 'Ok'

@app.route(f'/loadTest/', methods=['POST'])
def load_test():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'

def run_flask():
    app.run(host='0.0.0.0')

if __name__ == "__main__":
    bot = ObjectDetectionBot(TELEGRAM_TOKEN, TELEGRAM_APP_URL)
    app.run(host='0.0.0.0', port=8443, debug=True)