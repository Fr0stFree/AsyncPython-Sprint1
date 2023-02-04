import json
import multiprocessing
from multiprocessing.queues import Queue

import pytest
from _pytest.monkeypatch import MonkeyPatch

from tasks.calculation import DataCalculationTask, City


@pytest.fixture(scope='module')
def monkeypatch():
    monkeypatch = MonkeyPatch()
    yield monkeypatch
    monkeypatch.undo()


@pytest.fixture(scope='module')
def forecast_day_data():
    path = './examples/response.json'
    with open(path, 'r') as f:
        data = json.load(f)
    return data['forecasts'][0]


def test_calculation_task_without_queue(forecast_day_data):
    task = DataCalculationTask('Moscow', forecast_day_data)
    task.run()
    assert isinstance(task.result, City)
    assert isinstance(task.result.avg_temperature, float)
    assert -40 < task.result.avg_temperature < 40
    assert isinstance(task.result.good_weather_hours, int)
    assert 0 <= task.result.good_weather_hours <= 24
    

def test_calculation_task_with_queue(forecast_day_data):
    queue = Queue(ctx=multiprocessing.get_context('spawn'))
    task = DataCalculationTask('Moscow', forecast_day_data, queue)
    task.run()
    process = multiprocessing.Process(target=task.run)
    process.start()
    process.join()
    result_from_queue = queue.get()
    assert result_from_queue == task.result
    assert isinstance(result_from_queue, City)
    assert isinstance(result_from_queue.avg_temperature, float)
    assert -40 < result_from_queue.avg_temperature < 40
    assert isinstance(result_from_queue.good_weather_hours, int)
    assert 0 <= result_from_queue.good_weather_hours <= 24
    

def test_calculation_task_with_incomplete_data(monkeypatch, forecast_day_data):
    monkeypatch.setitem(forecast_day_data, 'hours', [])
    task = DataCalculationTask('Moscow', forecast_day_data)
    task.run()
    assert task.result is None
