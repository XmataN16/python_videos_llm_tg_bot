import os
import logging
from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка токена из .env
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("❌ Не найден TELEGRAM_BOT_TOKEN в .env файле!")

# Инициализация
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "✅ Бот работает!\n"
        "Напишите любой текст — я повторю его (эхо-режим).\n"
        "Это тестовая версия перед подключением аналитики."
    )
    logger.info(f"User {message.from_user.id} started bot")

@router.message()
async def echo(message: Message):
    await message.answer(f"Вы написали: {message.text}")
    logger.info(f"Echo: {message.text}")

async def main():
    dp.include_router(router)
    logger.info("🚀 Бот запущен и ожидает сообщений...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())