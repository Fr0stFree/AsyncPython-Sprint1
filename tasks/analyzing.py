import multiprocessing
import os
import logging
from typing import Union, NamedTuple
from multiprocessing.queues import Queue


logger = logging.getLogger(__name__)


class City(NamedTuple):
    """Вспомогательный класс для хранения данных о городе"""
    name: str
    avg_temp: float
    avg_good_weather_hours: float
    
    def __str__(self):
        return f'{self.name}: {self.avg_temp} °C, {self.avg_good_weather_hours} благоприятных часов'


class DataAnalyzingTask:
    
    __slots__ = ('_data', '_results', '_queue')

    def __init__(self, data: dict) -> None:
        self._data = data
        self._queue = Queue(ctx=multiprocessing.get_context('spawn'))
        self._results = []
        
    def run(self) -> None:
        """
        Метод производит анализ данных о погоде для каждого из городов. Расчётт производится в
        отдельном процессе. Результаты доступны через очередь.
        """
        for city_name, city_data in self._data.items():
            process = multiprocessing.Process(target=self._analyze_city,
                                              args=(city_name, city_data, self._queue))
            process.start()
            process.join()
        while not self._queue.empty():
            self._results.append(self._queue.get())
        self._sort_results()
        
    def _analyze_city(self, name: str, data: dict, queue: Queue) -> None:
        logger.info('Analyzing data for %s. Process id: %s.', name, os.getpid())
        temperatures = [data['avg_temp'] for data in data.values()]
        good_weather_hours = [data['good_weather_hours'] for data in data.values()]
        result = City(name=name,
                      avg_temp=self._calculate_average(temperatures),
                      avg_good_weather_hours=self._calculate_average(good_weather_hours))
        logger.info('Data for %s analyzed successfully. Process id: %s.', name, os.getpid())
        queue.put(result)
        
    @staticmethod
    def _calculate_average(indications: list[Union[int, float]]) -> float:
        return round(sum(indications) / len(indications), 1)
    
    def _sort_results(self) -> None:
        self._results.sort(key=lambda city: (city.avg_temp, city.avg_good_weather_hours),
                           reverse=True)
    
    @property
    def result(self) -> list[City]:
        return self._results
        
