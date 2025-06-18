import pika
import json
from io import BytesIO
from PIL import Image
from telegram import Bot
import asyncio
import logging
from dotenv import load_dotenv
import os

logging.getLogger('pika').setLevel(logging.WARNING)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_IDS = os.getenv('TELEGRAM_CHAT_IDS')

CHAT_IDS = [chat_id.strip() for chat_id in TELEGRAM_CHAT_IDS.split(';')]

async def send_to_telegram(image_data, caption="Садка"):
    try:
        bot = Bot(token=TELEGRAM_TOKEN)
        image = Image.open(BytesIO(image_data))
        output = BytesIO()
        image.save(output, format='JPEG')
        for chat_id in CHAT_IDS:
            try:
                output.seek(0)
                await bot.send_photo(chat_id=chat_id, photo=output, caption=caption)
                logger.info("the image was successfully sent to tg with chat_id: %s", chat_id)
            except Exception as e:
                logger.error(f"error sending to tg with chat_id: {chat_id}: {e}")
                continue
        output.close()
    except Exception as e:
        logger.error(f"error sending to tg: {e}")

def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        image_data = message.get('image')
        metadata = message.get('metadata', {})
        logger.info(f"message received: {metadata}")

        if isinstance(image_data, str):
            import base64
            image_data = base64.b64decode(image_data)

        asyncio.run(send_to_telegram(image_data))
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag)

def main():
    try:
        with pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST)) as connection:
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
            logger.info("waiting for messages")
            channel.start_consuming()
    except Exception as e:
        logger.error(f"еrror connection to rabbit: {e}")

if __name__ == "__main__":
    main()