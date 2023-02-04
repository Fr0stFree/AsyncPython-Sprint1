import pytest

from tasks.aggregation import DataAggregationTask, City


    
def test_able_to_save_and_retrieve_data():
    city = City('test_city', '2020-01-01', 10.0, 10)
    task = DataAggregationTask(city)
    data = DataAggregationTask.get_data()
    with pytest.raises(KeyError):
        data[city.name] == {city.date: {'avg_temp': city.avg_temperature,
                                        'good_weather_hours': city.good_weather_hours}}
    task.run()
    data = DataAggregationTask.get_data()
    assert data[city.name] == {city.date: {'avg_temp': city.avg_temperature,
                                           'good_weather_hours': city.good_weather_hours}}
    
def test_able_to_save_data_with_existing_data():
    city_1 = City('test_city_1', '2020-01-01', 10.0, 10)
    task = DataAggregationTask(city_1)
    task.run()
    city_2 = City('test_city_2', '2020-01-02', 15.0, 15)
    task = DataAggregationTask(city_2)
    task.run()
    data = DataAggregationTask.get_data()
    assert data[city_1.name] == {city_1.date: {'avg_temp': city_1.avg_temperature,
                                               'good_weather_hours': city_1.good_weather_hours}}
    assert data[city_2.name] == {city_2.date: {'avg_temp': city_2.avg_temperature,
                                               'good_weather_hours': city_2.good_weather_hours}}
    assert len(data) == 2
    

def test_able_add_data_to_existing_data():
    city_at_day_1 = City('test_city', '2020-01-01', 10.0, 10)
    task = DataAggregationTask(city_at_day_1)
    task.run()
    city_at_day_2 = City('test_city', '2020-01-02', 15.0, 15)
    task = DataAggregationTask(city_at_day_2)
    task.run()
    data = DataAggregationTask.get_data()
    assert len(data) == 1
    assert len(data[city_at_day_1.name]) == 2
    assert data[city_at_day_1.name][city_at_day_1.date] == {
        'avg_temp': city_at_day_1.avg_temperature,
        'good_weather_hours': city_at_day_1.good_weather_hours
    }
    assert data[city_at_day_1.name][city_at_day_2.date] == {
        'avg_temp': city_at_day_2.avg_temperature,
        'good_weather_hours': city_at_day_2.good_weather_hours
    }
