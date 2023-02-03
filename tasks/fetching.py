import logging
from typing import Optional

from api_client import YandexWeatherAPI


logger = logging.getLogger(__name__)


class DataFetchingTask:
    ywAPI = YandexWeatherAPI()
    
    __slots__ = ('_city_name', '_data')
    
    def __init__(self, city_name: str) -> None:
        self._city_name = city_name
        self._data = None
    
    def run(self) -> None:
        """
        Метод выполняет запрос к API Яндекс.Погоды и сохраняет данные в атрибуте _data.
        В случае ошибки во время запроса, метод выбрасывает исключение.
        """
        logger.info('Fetching data for %s.', self._city_name)
        try:
            resp = self.ywAPI.get_forecasting(self._city_name)
        except Exception as ex:
            error_msg = 'Error during fetching data for %s: %s.', self._city_name, ex
            logger.error(error_msg)
            raise Exception(error_msg)
        self._data = resp
        logger.info('Data for %s fetched successfully.', self._city_name)
    
    @property
    def result(self) -> tuple[str, Optional[dict]]:
        """
        Метод возвращает название города и данные, полученные в результате запроса.
        :return: Кортеж из названия города и словаря с данными о погоде.
        """
        return self._city_name, self._data


