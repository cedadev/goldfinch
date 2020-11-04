import os
import pytest

from pywps import Service
from pywps.tests import client_for, assert_response_success

from .common import get_output
from goldfinch.processes.wps_get_weather_stations import GetWeatherStations


def _run_with_inputs(process, datainputs):
    client = client_for(Service(processes=[process()]))

    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier={process.__name__}"
        f"&datainputs={datainputs}")

    return resp


def _check_for_file(resp, filename):
    output_url = resp.xml.findall('.//{http://www.opengis.net/wps/1.0.0}Reference')[0].get('href')
    assert os.path.basename(output_url) == filename


def test_wps_get_weather_stations_no_params_fail():
    datainputs = ""
    resp = _run_with_inputs(GetWeatherStations, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')


def test_wps_get_weather_stations_counties_success():
    datainputs = "counties=cornwall,devon"
    resp = _run_with_inputs(GetWeatherStations, datainputs)

    assert_response_success(resp)
    _check_for_file(resp, 'weather_stations.txt')


def test_wps_get_weather_stations_counties_fail():
    datainputs = "counties=NONSENSE_COUNT"
    resp = _run_with_inputs(GetWeatherStations, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')


def test_wps_get_weather_stations_bbox():
    datainputs = "bbox=-1,52,-0.5,53"
    resp = _run_with_inputs(GetWeatherStations, datainputs)

    assert_response_success(resp)
    _check_for_file(resp, 'weather_stations.txt')


def test_wps_get_weather_stations_start_end_success():
    datainputs = "start=2017-10-10T00:00:00;end=2018-02-03T12:00:00;counties=cornwall"
    resp = _run_with_inputs(GetWeatherStations, datainputs)

    assert_response_success(resp)
    _check_for_file(resp, 'weather_stations.txt')


def test_wps_get_weather_stations_start_end_fail():
    datainputs = "start=2018-02-03T12:00:00;end=2017-10-10T00:00:00;counties=cornwall"
    resp = _run_with_inputs(GetWeatherStations, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')