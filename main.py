import os
import json
import asyncio
import base64
from io import BytesIO

import aiohttp
from PIL import Image
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aio_pika import connect_robust, IncomingMessage
from aiogram.filters import Command
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
dp = Dispatcher()


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

@dp.message(Command("export"))
async def handle_export_command(message: Message):
    export_url = "http://localhost:8000/actions/export"
    request_payload = {
        "entity_ids": ["1", "2"],
        "actions": ["eat", "sleep", "drink"],
        "start_time": "2025-06-23",
        "end_time": "2025-06-25"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(export_url, json=request_payload) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    excel_file = BufferedInputFile(content, filename="export.xlsx")
                    await message.answer_document(excel_file, caption="Вот ваш экспортированный файл Excel 📊")
                    logger.info(f"Excel файл отправлен пользователю {message.from_user.id}")
                else:
                    error_text = f"Не удалось получить данные. Статус: {resp.status}"
                    await message.answer(error_text)
                    logger.error(error_text)
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        await message.answer("Произошла ошибка при обращении к API.")

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

async def start():
    asyncio.create_task(main())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(start())
    except (KeyboardInterrupt, SystemExit):
        logger.info("bot stopped")
