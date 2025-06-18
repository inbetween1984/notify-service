import os
import json
import asyncio
import base64
from io import BytesIO
from PIL import Image
from aiogram import Bot
from aio_pika import connect_robust, IncomingMessage
from dotenv import load_dotenv
import logging
from aiogram.types import BufferedInputFile


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_IDS = [chat_id.strip() for chat_id in os.getenv('TELEGRAM_CHAT_IDS').split(';')]

bot = Bot(token=TELEGRAM_TOKEN)

async def send_to_telegram(image_data: bytes, caption: str = "Садка"):
    try:
        output = BytesIO()
        image = Image.open(BytesIO(image_data))
        image.save(output, format='JPEG')
        output.seek(0)

        for chat_id in TELEGRAM_CHAT_IDS:
            try:
                buffered_file = BufferedInputFile(output.getvalue(), filename="image.jpg")
                await bot.send_photo(chat_id=chat_id, photo=buffered_file, caption=caption)
                logger.info(f"image sent to chat_id {chat_id}")
                output.seek(0)
            except Exception as e:
                logger.error(f"error sending image to {chat_id}: {e}")
        output.close()

    except Exception as e:
        logger.error(f"error in send_to_telegram: {e}")

async def callback(message: IncomingMessage):
        try:
            data = json.loads(message.body)
            image_data = data.get("image")
            metadata = data.get("metadata", {})
            #caption = metadata.get("caption") текст сообщения
            logger.info(f"received message with metadata: {metadata}")

            if isinstance(image_data, str):
                image_data = base64.b64decode(image_data)

            await send_to_telegram(image_data)
            await message.ack()

        except Exception as e:
            logger.error(f"error processing message: {e}")
            await message.nack(requeue=True)

async def main():
    try:
        connection = await connect_robust(RABBITMQ_HOST)
        async with connection:
            channel = await connection.channel()
            async with channel:
                await channel.set_qos(prefetch_count=1)

                queue = await channel.declare_queue(RABBITMQ_QUEUE, durable=True)
                logger.info("waiting for messages...")

                await queue.consume(callback=callback)

                await asyncio.Future()

    except Exception as e:
        logger.error(f"error during RabbitMQ processing: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("bot stopped")
