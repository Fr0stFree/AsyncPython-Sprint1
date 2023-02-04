import pytest
from _pytest.monkeypatch import MonkeyPatch

from tasks.fetching import DataFetchingTask
from utils import CITIES
from exceptions import InvalidRequestDataError, BadAPIResponseError
from api_client import YandexWeatherAPI


@pytest.fixture(scope='module')
def monkeypatch():
    monkeypatch = MonkeyPatch()
    yield monkeypatch
    monkeypatch.undo()
    

@pytest.mark.parametrize('city', CITIES.keys())
def test_valid_cities_fetching(city):
    task = DataFetchingTask(city)
    task.run()
    assert task.result is not None
    assert task.result[0] == city
    assert isinstance(task.result[1], dict)
    

def test_unable_to_fetch_data_for_invalid_city():
    city = 'something_really_wierd'
    task = DataFetchingTask(city)
    with pytest.raises(InvalidRequestDataError):
        task.run()
    assert task.result is None
        

def test_getting_bad_from_api(monkeypatch):
    city = 'Moscow'.upper()
    def _do_req(*args, **kwargs):
        raise BadAPIResponseError
    monkeypatch.setattr(YandexWeatherAPI, '_do_req', _do_req)
    task = DataFetchingTask(city)
    with pytest.raises(BadAPIResponseError):
        task.run()
    assert task.result is None
    
    

    
