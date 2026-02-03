import re
from typing import Optional
from .schemas import QueryParams


class RussianQueryParser:
    """Парсер запросов на русском языке без использования LLM"""
    
    def __init__(self):
        self.month_map = {
            'января': '01', 'январь': '01',
            'февраля': '02', 'февраль': '02',
            'марта': '03', 'март': '03',
            'апреля': '04', 'апрель': '04',
            'мая': '05',
            'июня': '06', 'июнь': '06',
            'июля': '07', 'июль': '07',
            'августа': '08', 'август': '08',
            'сентября': '09', 'сентябрь': '09',
            'октября': '10', 'октябрь': '10',
            'ноября': '11', 'ноябрь': '11',
            'декабря': '12', 'декабрь': '12'
        }
    
    def parse(self, query: str) -> Optional[QueryParams]:
        query_lower = re.sub(r'\s+', ' ', query.lower().strip())
        
        # Тип 1: "Сколько всего видео?"
        if self._matches_total_videos(query_lower):
            return QueryParams(
                query_type="total_videos_count",
                parameters={},
                raw_query=query
            )
        
        # Тип 2: "Сколько видео у креатора ... с 1 по 5 ноября 2025?"
        creator_match = self._parse_creator_query(query_lower)
        if creator_match:
            return QueryParams(
                query_type="creator_videos_count",
                parameters=creator_match,
                raw_query=query
            )
        
        # Тип 3: "Сколько видео набрало больше 1000 просмотров?"
        min_views_match = self._parse_min_views(query_lower)
        if min_views_match:
            return QueryParams(
                query_type="videos_with_min_views",
                parameters={"min_views": str(min_views_match)},
                raw_query=query
            )
        
        # Тип 5: "Сколько РАЗНЫХ видео получали НОВЫЕ просмотры 27 ноября 2025?"
        # Проверяем ДО типа 4, потому что "просмотры" есть в обоих запросах
        new_views_match = self._parse_new_views(query_lower)
        if new_views_match:
            return QueryParams(
                query_type="videos_with_new_views",
                parameters={"date": new_views_match},
                raw_query=query
            )
        
        # Тип 4: "На сколько просмотров в сумме ВЫРОСЛИ все видео 28 ноября 2025?"
        growth_match = self._parse_views_growth(query_lower)
        if growth_match:
            return QueryParams(
                query_type="total_views_growth",
                parameters={"date": growth_match},
                raw_query=query
            )
        
        return None
    
    def _matches_total_videos(self, query: str) -> bool:
        patterns = [
            r'сколько всего видео',
            r'общее количество видео',
            r'всего видео',
            r'сколько видео в системе'
        ]
        return any(re.search(p, query) for p in patterns)
    
    def _parse_creator_query(self, query: str) -> Optional[dict]:
        uuid_match = re.search(r'([a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', query)
        if not uuid_match:
            return None
        
        creator_id = uuid_match.group(1)
        if '-' not in creator_id:
            creator_id = f"{creator_id[:8]}-{creator_id[8:12]}-{creator_id[12:16]}-{creator_id[16:20]}-{creator_id[20:]}"
        
        # Диапазон дат "с 1 по 5 ноября 2025"
        date_range = re.search(
            r'с\s+(\d{1,2})\s+(?:по|до)\s+(\d{1,2})\s+(январ[ья]|феврал[ья]|марта?|апрел[ья]|мая|июн[ья]|июл[ья]|августа?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s+(\d{4})',
            query
        )
        
        if date_range:
            day_start = int(date_range.group(1))
            day_end = int(date_range.group(2))
            month_ru = date_range.group(3).rstrip('ь').rstrip('я') + 'я'
            year = int(date_range.group(4))
            month = self.month_map.get(month_ru, '01')
            
            return {
                "creator_id": creator_id,
                "start_date": f"{year}-{month}-{day_start:02d}",
                "end_date": f"{year}-{month}-{day_end:02d}"
            }
        
        # Одна дата "28 ноября 2025"
        single_date = re.search(
            r'(\d{1,2})\s+(январ[ья]|феврал[ья]|марта?|апрел[ья]|мая|июн[ья]|июл[ья]|августа?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s+(\d{4})',
            query
        )
        
        if single_date:
            day = int(single_date.group(1))
            month_ru = single_date.group(2).rstrip('ь').rstrip('я') + 'я'
            year = int(single_date.group(3))
            month = self.month_map.get(month_ru, '01')
            
            date_str = f"{year}-{month}-{day:02d}"
            return {
                "creator_id": creator_id,
                "start_date": date_str,
                "end_date": date_str
            }
        
        return {"creator_id": creator_id}
    
    def _parse_min_views(self, query: str) -> Optional[int]:
        # Игнорируем "новые просмотры"
        if re.search(r'новы[её]\s+просмотр', query):
            return None
        
        match = re.search(r'больше\s+([\d\s]+)\s+просмотр', query)
        if match:
            num_str = match.group(1).replace(' ', '').replace('\xa0', '')
            try:
                return int(num_str)
            except ValueError:
                return None
        return None
    
    def _parse_views_growth(self, query: str) -> Optional[str]:
        # Ключевые слова для РОСТА: выросли, прирост, в сумме
        if not re.search(r'(выросл[иао]|прирост|в сумм[еу])', query):
            return None
        return self._extract_date(query)
    
    def _parse_new_views(self, query: str) -> Optional[str]:
        # Ключевые слова для УНИКАЛЬНЫХ видео: разных, уникальных, получали новые
        if not re.search(r'(разн[ыо]х видео|уникальн[ыо]х видео|получали новы[её] просмотры)', query):
            return None
        return self._extract_date(query)
    
    def _extract_date(self, query: str) -> Optional[str]:
        match = re.search(
            r'(\d{1,2})\s+(январ[ья]|феврал[ья]|марта?|апрел[ья]|мая|июн[ья]|июл[ья]|августа?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s+(\d{4})',
            query
        )
        
        if match:
            day = int(match.group(1))
            month_ru = match.group(2).rstrip('ь').rstrip('я') + 'я'
            year = int(match.group(3))
            month = self.month_map.get(month_ru, '01')
            return f"{year}-{month}-{day:02d}"
        
        return None