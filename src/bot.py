import os
import logging
from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

from .parser import RussianQueryParser
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
parser = RussianQueryParser()

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

@router.message()
async def handle_query(message: types.Message):
    if not message.text:
        await message.answer("❌ Пожалуйста, отправьте текстовый запрос")
        return
    
    await bot.send_chat_action(chat_id=message.chat.id, action="typing")
    
    try:
        # Шаг 1: Парсинг запроса
        query_params = parser.parse(message.text)
        
        if not query_params:
            await message.answer(
                "❓ Не удалось распознать запрос. Попробуйте сформулировать его по примерам из /start"
            )
            return
        
        logger.info(f"Parsed: {query_params.query_type} | Params: {query_params.parameters}")
        
        # Шаг 2: Выполнение запроса к БД
        result = await db.execute_query(query_params)
        
        # Шаг 3: Отправка результата (ТОЛЬКО число!)
        await message.answer(str(result))
        
    except ValueError as e:
        await message.answer(f"⚠️ Ошибка в параметрах запроса: {str(e)}")
    except Exception as e:
        logger.error(f"Query error: {e}", exc_info=True)
        await message.answer("❌ Внутренняя ошибка сервера")


async def main():
    await db.connect()
    logger.info("✅ Подключено к базе данных PostgreSQL")
    
    dp.include_router(router)
    logger.info("🚀 Бот запущен и готов к работе")
    
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())