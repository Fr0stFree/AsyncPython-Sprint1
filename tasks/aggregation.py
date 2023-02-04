import os
import logging
import json

from .calculation import City

logger = logging.getLogger(__name__)


class DataAggregationTask:
    STORAGE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'storage.json')
    
    __slots__ = '_city_name', '_date', '_avg_temp', '_good_weather_hours'
    
    def __init__(self, city: City) -> None:
        self._city_name: str = city.name
        self._date: str = city.date
        self._avg_temp: float = city.avg_temperature
        self._good_weather_hours: int = city.good_weather_hours
    
    def run(self) -> None:
        """Метод для агрегации данных. Сохраняет в хранилище полученные данные."""
        self._update_storage()
    
    def _update_storage(self):
        logger.info('Aggregating data for %s. Date: %s.', self._city_name, self._date)
        storage = self._get_or_create_storage()
        city_data = storage.setdefault(self._city_name, {})
        date_data = city_data.setdefault(self._date, {})
        date_data['avg_temp'] = self._avg_temp
        date_data['good_weather_hours'] = self._good_weather_hours
        logger.info('Data for %s aggregated successfully. Date: %s.', self._city_name, self._date)
        self._save_storage(storage)
    
    @classmethod
    def _get_or_create_storage(cls) -> dict:
        if not os.path.exists(cls.STORAGE_PATH):
            return {}
        with open(cls.STORAGE_PATH, 'r') as f:
            return json.load(f)
    
    @classmethod
    def _save_storage(cls, data: dict) -> None:
        with open(cls.STORAGE_PATH, 'w') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    
    @classmethod
    def get_data(cls) -> dict:
        """Метод возвращает данные из файла, в котором хранятся данные о погоде."""
        return cls._get_or_create_storage()