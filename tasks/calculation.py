import logging
from typing import NamedTuple
from multiprocessing.queues import Queue

from exceptions import IncompleteDataError


logger = logging.getLogger(__name__)


class City(NamedTuple):
    """Вспомогательный класс для хранения данных о городе"""
    name: str
    date: str
    avg_temperature: float
    avg_good_weather_hours: float
    
    def __str__(self):
        return f'{self.name} at {self.date}: {self.avg_temperature} °C, ' \
               f'{self.avg_good_weather_hours} благоприятных часов'


class DataCalculationTask:
    MIN_HOUR = 9
    MAX_HOUR = 19
    ACCEPTABLE_WEATHER_CONDITIONS = ('clear', 'partly-cloudy', 'cloudy', 'overcast')
    
    __slots__ = '_day_data', '_city_name', '_date', '_average_temp', '_good_weather_hours', '_queue'
    
    def __init__(self, city_name: str, data: dict, queue: Queue) -> None:
        self._day_data: dict = data['hours']
        self._city_name: str = city_name
        self._date: str = data['date']
        self._average_temp: float | None = None
        self._good_weather_hours: int = 0
        self._queue = queue
    
    def run(self) -> None:
        """
        Метод рассчитывает среднюю температуру и количество благоприятных часов за указанный день.
        В случае успеха, результат помещается в очередь.
        """
        logger.info('Calculating data for %s. Date: %s.', self._city_name, self._date)
        try:
            self._validate_data()
        except IncompleteDataError:
            return
        self._calculate_avg_temp()
        self._calculate_good_weather_hours()
        result = City(self._city_name, self._date, self._average_temp, self._good_weather_hours)
        self._queue.put(result)
        logger.info('Data for %s calculated successfully. Date: %s. '
                    'Avg temp: %s. Good weather hours: %s.',
                    self._city_name, self._date, self._average_temp, self._good_weather_hours)
    
    def _calculate_avg_temp(self) -> None:
        temp = [int(hour['temp']) for hour in self._day_data if
                self._is_time_acceptable(hour['hour'])]
        self._average_temp = round(sum(temp) / len(temp), 1)
    
    def _calculate_good_weather_hours(self):
        """Метод для расчёта количества часов с хорошей погодой."""
        for hour in self._day_data:
            if self._is_time_acceptable(hour['hour']) and self._is_weather_good(hour['condition']):
                self._good_weather_hours += 1
    
    def _validate_data(self) -> None:
        """Метод получает данные о погоде и возвращает словарь с данными для расчета."""
        if len(self._day_data) != 24:
            logger.error('Incorrect data for %s in %s. Expected 24 hours, got %s.',
                         self._city_name, self._date, len(self._day_data))
            raise IncompleteDataError('Incorrect data for %s in %s. Expected 24 hours, got %s.',
                                      self._city_name, self._date, len(self._day_data))
    
    @classmethod
    def _is_weather_good(cls, condition: str) -> bool:
        return condition in cls.ACCEPTABLE_WEATHER_CONDITIONS
    
    @classmethod
    def _is_time_acceptable(cls, hour: str) -> bool:
        return cls.MIN_HOUR <= int(hour) <= cls.MAX_HOUR
