# src/schemas.py
from pydantic import BaseModel
from typing import Optional, Dict
from datetime import date


class QueryParams(BaseModel):
    """Параметры распаршенного запроса"""
    query_type: str  # total_videos_count | creator_videos_count | videos_with_min_views | total_views_growth | videos_with_new_views
    parameters: Dict[str, str] = {}
    raw_query: str  # исходный запрос для логирования