import os
import logging
from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

from .database import Database
from .schemas import QueryParams

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация
bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
dp = Dispatcher(storage=MemoryStorage())
router = Router()

# Глобальные объекты
db = Database()

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "Бот аналитики видео запущен!\n\n"
        "Примеры запросов:\n"
        "• Сколько всего видео есть в системе?\n"
        "• Сколько видео у креатора aca1061a9d324ecf8c3fa2bb32d7be63 вышло с 1 по 5 ноября 2025?\n"
        "• Сколько видео набрало больше 1000 просмотров?\n"
        "• На сколько просмотров в сумме выросли все видео 28 ноября 2025?\n"
        "• Сколько разных видео получали новые просмотры 27 ноября 2025?"
    )


async def main():
    await db.connect()
    logger.info("✅ Подключено к базе данных PostgreSQL")
    
    dp.include_router(router)
    logger.info("🚀 Бот запущен и готов к работе")
    
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())