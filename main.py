import logging.config
import threading
import multiprocessing
from multiprocessing.queues import Queue


from tasks import DataFetchingTask, DataCalculationTask, DataAggregationTask, DataAnalyzingTask
from utils import CITIES
from settings import LOGGING


logging.config.dictConfig(LOGGING)


if __name__ == "__main__":
    cities = CITIES.keys()
    
    # Fetching
    fetching_tasks = []
    for city in cities:
        task = DataFetchingTask(city)
        fetching_tasks.append(task)
        thread = threading.Thread(target=task.run)
        thread.start()
        thread.join()
    results = [task.result for task in fetching_tasks]

    # Calculation
    queue = Queue(ctx=multiprocessing.get_context('spawn'))
    for city_name, city_data in results:
        for day_city_data in city_data['forecasts']:
            task = DataCalculationTask(city_name, day_city_data, queue)
            process = multiprocessing.Process(target=task.run)
            process.start()
            process.join()
    results = [queue.get() for _ in range(queue.qsize())]

    # Aggregation
    for city in results:
        task = DataAggregationTask(city)
        thread = threading.Thread(target=task.run)
        thread.start()
        thread.join()
    data_from_storage = DataAggregationTask.get_data()
    
    # # Analyzing
    task = DataAnalyzingTask(data_from_storage)
    task.run()
    
    result = task.result[0]
    print(f'Самый благоприятный город для отдыха: {result.name} '
          f'с средней температурой {result.avg_temp} °C '
          f'и средним количеством благоприятных часов {result.avg_good_weather_hours}')
