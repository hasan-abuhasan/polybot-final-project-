import telebot
from loguru import logger
import os
import time
from telebot.types import InputFile
import boto3
import requests
import json

s3 = boto3.client('s3')
bucket = 'hasan-abu-hasan-bucket'
queue_url = 'HasanQueue'
sqs_client = boto3.client('sqs', region_name='us-east-1')
CERTIFICATE_FILE_NAME = "YOURPUBLIC.pem"
#telegram_chat_url = os.environ['telegram_chat_url']
telegram_chat_url= 'hasandm.atech-bot.click'
telegram_chat_url = f'https://{telegram_chat_url}'
secrets_manager_client = boto3.client('secretsmanager', region_name='us-east-1')
secret_name = 'HasanSM'
response = secrets_manager_client.get_secret_value(SecretId=secret_name)
secret_string = response['SecretString']
secret_dict = json.loads(secret_string)
TELEGRAM_TOKEN = secret_dict['TELEGRAM_TOKEN']

class Bot:
    def __init__(self, token, telegram_chat_url):
        # create a new instance of the TeleBot class.
        # all communication with Telegram servers are done using self.telegram_bot_client
        self.telegram_bot_client = telebot.TeleBot(token)
        # remove any existing webhooks configured in Telegram servers
        self.telegram_bot_client.remove_webhook()
        time.sleep(0.5)
        # set the webhook URL
        self.telegram_bot_client.set_webhook(url=f'{telegram_chat_url}/{token}/', certificate = open(CERTIFICATE_FILE_NAME, 'r'),  timeout=60)
        logger.info(f'Telegram Bot information\n\n{self.telegram_bot_client.get_me()}')
    def send_text(self, chat_id, text):
        self.telegram_bot_client.send_message(chat_id, text)
    def send_text_with_quote(self, chat_id, text, quoted_msg_id):
        self.telegram_bot_client.send_message(chat_id, text, reply_to_message_id=quoted_msg_id)
    def is_current_msg_photo(self, msg):
        return 'photo' in msg
    def download_user_photo(self, msg):
        """
        Downloads the photos that sent to the Bot to photos directory (should be existed)
        :return:
        """
        if not self.is_current_msg_photo(msg):
            raise RuntimeError(f'Message content of type \'photo\' expected')
        file_info = self.telegram_bot_client.get_file(msg['photo'][-1]['file_id'])
        data = self.telegram_bot_client.download_file(file_info.file_path)
        folder_name = file_info.file_path.split('/')[0]
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        with open(file_info.file_path, 'wb') as photo:
            photo.write(data)
        return file_info.file_path
    def send_photo(self, chat_id, img_path):
        if not os.path.exists(img_path):
            raise RuntimeError("Image path doesn't exist")
        self.telegram_bot_client.send_photo(
            chat_id,
            InputFile(img_path)
        )
    def handle_message(self, msg):
        """Bot Main message handler"""
        logger.info(f'Incoming message: {msg}')
        self.send_text(msg['chat']['id'], f'Your original message: {msg["text"]}')
class ObjectDetectionBot(Bot):
    def handle_message(self, msg):
        logger.info(f'Incoming message: {msg}')
        if self.is_current_msg_photo(msg):
            photo_path = self.download_user_photo(msg)
            # TODO upload the photo to S3
            s3.upload_file(photo_path, bucket, os.path.basename(photo_path))
            # TODO send a job to the SQS queue
            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody = f'{{"photo_key": "{os.path.basename(photo_path)}", "chat_id": {msg["chat"]["id"]}}}'
            )
            # TODO send message to the Telegram end-user (e.g. Your image is being processed. Please wait...)
            self.send_text(msg['chat']['id'], 'Your image is being processed. Please wait...')