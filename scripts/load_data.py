#!/usr/bin/env python3
"""
Скрипт для загрузки данных из videos.json в PostgreSQL
"""

import json
import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime
import re

import asyncpg
from dotenv import load_dotenv

# Добавляем корневую директорию в sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

load_dotenv()

# Настройки подключения к БД
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "videos_analytics"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}


def clean_key(key: str) -> str:
    """Очистка ключа от пробелов по краям и нормализация"""
    return key.strip().replace('\u00a0', ' ')  # удаляем неразрывные пробелы


async def clean_json_data(data):
    """
    Рекурсивная очистка ключей и конвертация строк дат в datetime объекты
    """
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            clean_k = clean_key(k)
            
            # Конвертация строковых дат в datetime
            if clean_k.endswith('_at') and isinstance(v, str):
                try:
                    # Парсим ISO 8601 формат с часовым поясом
                    v = datetime.fromisoformat(v.replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    pass  # Оставляем как есть, если не удалось распарсить
            
            cleaned[clean_k] = await clean_json_data(v)
        return cleaned
    
    elif isinstance(data, list):
        return [await clean_json_data(item) for item in data]
    
    else:
        return data


async def load_videos_data():
    """
    Загрузка данных из JSON файла в базу данных
    """
    # Подключение к базе данных
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        print("🔌 Подключение к базе данных...")
        
        # Проверка подключения
        version = await conn.fetchval("SELECT version();")
        pg_version = version.split()[1]
        print(f"✅ Подключено к PostgreSQL: {pg_version}")
        
        # Чтение JSON файла
        json_path = Path(__file__).parent.parent / "data" / "videos.json"
        
        if not json_path.exists():
            print(f"❌ Файл не найден: {json_path}")
            return
        
        print(f"📖 Чтение файла: {json_path}")
        
        with open(json_path, "r", encoding="utf-8") as f:
            raw_content = f.read()
            
            # Предварительная очистка "грязных" ключей в JSON (пробелы после кавычек)
            # Исправляем шаблон: "ключ " -> "ключ"
            raw_content = re.sub(r'"\s*([^"]+?)\s*"\s*:', r'"\1":', raw_content)
            
            # Парсим JSON
            raw_data = json.loads(raw_content)
        
        # Очистка данных и конвертация дат
        print("🧹 Очистка ключей и конвертация дат...")
        data = await clean_json_data(raw_data)
        
        videos = data.get("videos", [])
        print(f"📊 Найдено {len(videos)} видео для загрузки")
        
        # Счетчики
        video_count = 0
        snapshot_count = 0
        error_count = 0
        
        # Загрузка видео и снапшотов
        for idx, video in enumerate(videos, 1):
            try:
                # Валидация обязательных полей
                required_fields = ['id', 'video_created_at', 'views_count', 'likes_count', 
                                 'reports_count', 'comments_count', 'creator_id', 'created_at', 'updated_at']
                for field in required_fields:
                    if field not in video:
                        raise ValueError(f"Отсутствует обязательное поле: {field}")
                
                # Вставка видео
                await conn.execute(
                    """
                    INSERT INTO videos (
                        id, video_created_at, views_count, likes_count,
                        reports_count, comments_count, creator_id, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    video["id"],
                    video["video_created_at"],  # Теперь это datetime объект
                    video["views_count"],
                    video["likes_count"],
                    video["reports_count"],
                    video["comments_count"],
                    video["creator_id"],
                    video["created_at"],
                    video["updated_at"]
                )
                video_count += 1
                
                # Вставка снапшотов
                snapshots = video.get("snapshots", [])
                for snapshot in snapshots:
                    try:
                        # Валидация обязательных полей снапшота
                        snap_required = ['id', 'video_id', 'views_count', 'likes_count',
                                       'reports_count', 'comments_count', 'delta_views_count',
                                       'delta_likes_count', 'delta_reports_count', 'delta_comments_count',
                                       'created_at', 'updated_at']
                        for field in snap_required:
                            if field not in snapshot:
                                raise ValueError(f"Отсутствует поле в снапшоте: {field}")
                        
                        await conn.execute(
                            """
                            INSERT INTO video_snapshots (
                                id, video_id, views_count, likes_count, reports_count, comments_count,
                                delta_views_count, delta_likes_count, delta_reports_count, delta_comments_count,
                                created_at, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                            ON CONFLICT (id) DO NOTHING
                            """,
                            snapshot["id"],
                            snapshot["video_id"],
                            snapshot["views_count"],
                            snapshot["likes_count"],
                            snapshot["reports_count"],
                            snapshot["comments_count"],
                            snapshot["delta_views_count"],
                            snapshot["delta_likes_count"],
                            snapshot["delta_reports_count"],
                            snapshot["delta_comments_count"],
                            snapshot["created_at"],
                            snapshot["updated_at"]
                        )
                        snapshot_count += 1
                    except Exception as e:
                        error_count += 1
                        if error_count <= 5:  # Логируем только первые 5 ошибок
                            print(f"⚠️ Ошибка при вставке снапшота {snapshot.get('id', 'N/A')}: {e}")
                        continue
                
            except Exception as e:
                error_count += 1
                if error_count <= 5:  # Логируем только первые 5 ошибок
                    print(f"⚠️ Ошибка при вставке видео {video.get('id', 'N/A')} (#{idx}): {e}")
                    print(f"   Данные: {video.keys() if isinstance(video, dict) else 'не словарь'}")
                continue
        
        # Вывод статистики
        print("\n" + "="*60)
        print(f"✅ Загрузка завершена!")
        print(f"🎥 Успешно загружено видео: {video_count}")
        print(f"📸 Успешно загружено снапшотов: {snapshot_count}")
        if error_count > 0:
            print(f"❌ Ошибок: {error_count}")
        print("="*60)
        
        # Проверка загруженных данных
        total_videos = await conn.fetchval("SELECT COUNT(*) FROM videos;")
        total_snapshots = await conn.fetchval("SELECT COUNT(*) FROM video_snapshots;")
        
        print(f"\n📊 Проверка в базе данных:")
        print(f"   Видео в БД: {total_videos}")
        print(f"   Снапшотов в БД: {total_snapshots}")
        
        # Дополнительная проверка: примеры данных
        if total_videos > 0:
            sample = await conn.fetch("SELECT id, creator_id, views_count, video_created_at FROM videos LIMIT 3;")
            print(f"\n🔍 Примеры видео (первые 3):")
            for row in sample:
                # Преобразуем UUID в строку для корректного вывода
                video_id = str(row['id'])[:8]
                creator_id = str(row['creator_id'])[:8]
                print(f"   • ID: {video_id}..., Creator: {creator_id}..., Views: {row['views_count']}, Created: {row['video_created_at']}")
        
        if total_snapshots > 0:
            sample = await conn.fetch("""
                SELECT vs.id, vs.video_id, vs.delta_views_count, vs.created_at 
                FROM video_snapshots vs
                ORDER BY vs.created_at DESC
                LIMIT 3;
            """)
            print(f"\n🔍 Последние снапшоты (3 шт):")
            for row in sample:
                snapshot_id = str(row['id'])[:8]
                video_id = str(row['video_id'])[:8]
                print(f"   • ID: {snapshot_id}..., Video: {video_id}..., Delta Views: {row['delta_views_count']}, Time: {row['created_at']}")
        
    except Exception as e:
        print(f"❌ Критическая ошибка при загрузке данных: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await conn.close()
        print("\n🔌 Соединение с базой данных закрыто")


async def main():
    """
    Основная функция
    """
    print("\n🚀 Запуск загрузки данных из videos.json в PostgreSQL")
    print("="*60)
    
    # Загрузка данных
    await load_videos_data()
    
    print("\n✅ Готово!")


if __name__ == "__main__":
    asyncio.run(main())