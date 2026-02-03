import os
import asyncpg
from typing import Optional
from datetime import datetime, timezone
from .schemas import QueryParams


class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        self.pool = await asyncpg.create_pool(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            database=os.getenv("DB_NAME", "videos_analytics"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
            min_size=1,
            max_size=10
        )
    
    async def close(self):
        if self.pool:
            await self.pool.close()
    
    async def execute_query(self, query_params: QueryParams) -> int:
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        query_type = query_params.query_type
        params = query_params.parameters
        
        if query_type == "total_videos_count":
            return await self.pool.fetchval("SELECT COUNT(*) FROM videos")
        
        elif query_type == "creator_videos_count":
            creator_id = params.get("creator_id")
            if not creator_id:
                raise ValueError("creator_id required")
            
            query = "SELECT COUNT(*) FROM videos WHERE creator_id = $1"
            args = [creator_id]
            arg_num = 2
            
            if "start_date" in params:
                # Конвертируем строку в datetime с UTC временной зоной
                start_dt = datetime.strptime(f"{params['start_date']} 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                query += f" AND video_created_at >= ${arg_num}"
                args.append(start_dt)
                arg_num += 1
            
            if "end_date" in params:
                end_dt = datetime.strptime(f"{params['end_date']} 23:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                query += f" AND video_created_at <= ${arg_num}"
                args.append(end_dt)
            
            return await self.pool.fetchval(query, *args)
        
        elif query_type == "videos_with_min_views":
            min_views = params.get("min_views")
            if min_views is None:
                raise ValueError("min_views required")
            return await self.pool.fetchval(
                "SELECT COUNT(*) FROM videos WHERE views_count > $1", 
                int(min_views)
            )
        
        elif query_type == "total_views_growth":
            date = params.get("date")
            if not date:
                raise ValueError("date required")
            # Конвертируем дату в datetime объекты для начала и конца дня (UTC)
            start_dt = datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return await self.pool.fetchval("""
                SELECT COALESCE(SUM(delta_views_count), 0)
                FROM video_snapshots
                WHERE created_at >= $1 AND created_at < $2
            """, start_dt, end_dt)
        
        elif query_type == "videos_with_new_views":
            date = params.get("date")
            if not date:
                raise ValueError("date required")
            start_dt = datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return await self.pool.fetchval("""
                SELECT COUNT(DISTINCT video_id)
                FROM video_snapshots
                WHERE created_at >= $1 
                  AND created_at < $2 
                  AND delta_views_count > 0
            """, start_dt, end_dt)
        
        else:
            raise ValueError(f"Unknown query type: {query_type}")