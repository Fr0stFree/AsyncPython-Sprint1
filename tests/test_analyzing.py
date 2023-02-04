import json
import multiprocessing
from multiprocessing.queues import Queue

import pytest
from _pytest.monkeypatch import MonkeyPatch

from tasks.aggregation import DataAggregationTask
from tasks.analyzing import DataAnalyzingTask, City
from utils import CITIES


@pytest.fixture(scope='module')
def get_data():
    all_data: dict = DataAggregationTask.get_data()
    return all_data

def test_analyzing_all_cities(get_data):
    task = DataAnalyzingTask(get_data)
    task.run()
    assert isinstance(task.result, list)
    assert len(task.result) == len(CITIES)
    best_city = task.result[0]
    for city in task.result[1:]:
        assert best_city.avg_temp >= city.avg_temp
        assert best_city.avg_good_weather_hours >= city.avg_good_weather_hours
    

def test_analyze_one_city(get_data):
    city = 'Moscow'.upper()
    data = {city: get_data[city]}
    task = DataAnalyzingTask(data)
    task.run()
    assert isinstance(task.result, list)
    assert len(task.result) == 1
    assert isinstance(task.result[0], City)
    assert task.result[0].name == city


def test_analyze_with_very_good_fake_city(get_data):
    test_city = {
        'test_city': {
            "2022-05-26": {
                "avg_temp": 40.0,
                "good_weather_hours": 10
            },
            "2022-05-27": {
                "avg_temp": 45.0,
                "good_weather_hours": 11
            },
            "2022-05-28": {
                "avg_temp": 50.0,
                "good_weather_hours": 12
            },
        }
    }
    data = {**get_data, **test_city}
    task = DataAnalyzingTask(data)
    task.run()
    winner = task.result[0]
    assert winner.name == 'test_city'
    assert winner.avg_good_weather_hours == 11
    assert winner.avg_temp == 45.0
    

def test_analyze_with_bad_fake_city(get_data):
    test_city = {
        'test_city': {
            "2022-05-26": {
                "avg_temp": -40.0,
                "good_weather_hours": 0
            },
            "2022-05-27": {
                "avg_temp": -45.0,
                "good_weather_hours": 1
            },
            "2022-05-28": {
                "avg_temp": -50.0,
                "good_weather_hours": 2
            },
        }
    }
    data = {**get_data, **test_city}
    task = DataAnalyzingTask(data)
    task.run()
    looser = task.result[-1]
    assert looser.name == 'test_city'
    assert looser.avg_good_weather_hours == 1
    assert looser.avg_temp == -45.0