import logging
import json
from urllib.request import urlopen
from http import HTTPStatus

from utils import CITIES, ERR_MESSAGE_TEMPLATE
from exceptions import BadAPIResponseError, InvalidRequestDataError


logger = logging.getLogger()


class YandexWeatherAPI:
    """
    Base class for requests
    """
    @staticmethod
    def _do_req(url):
        """Base request method"""
        with urlopen(url) as req:
            resp = req.read().decode("utf-8")
            resp = json.loads(resp)
        if req.status != HTTPStatus.OK:
            raise BadAPIResponseError(
                'Error during request to Yandex Weather APP. Status code: %s' % req.status
            )
        return resp

    @staticmethod
    def _get_url_by_city_name(city_name: str) -> str:
        try:
            return CITIES[city_name]
        except KeyError:
            raise InvalidRequestDataError("Please check that city %s exists", city_name)

    def get_forecasting(self, city_name: str):
        """
        :param city_name: key as str
        :return: response data as json
        """
        city_url = self._get_url_by_city_name(city_name)
        return self._do_req(city_url)
