import os
import pytest

from pywps.tests import assert_response_success

from .common import get_output, run_with_inputs, check_for_output_file
from goldfinch.processes.wps_get_weather_stations import GetWeatherStations


def test_wps_get_weather_stations_no_params_fail(midas_metadata):
    datainputs = ""
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')


def test_wps_get_weather_stations_counties_success(midas_metadata):
    datainputs = "counties=cornwall,devon"
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert_response_success(resp)
    check_for_output_file(resp, 'weather_stations.txt')


def test_wps_get_weather_stations_counties_fail(midas_metadata):
    datainputs = "counties=NONSENSE_COUNT"
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')


def test_wps_get_weather_stations_bbox(midas_metadata):
    datainputs = "bbox=-1,52,-0.5,53"
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert_response_success(resp)
    check_for_output_file(resp, 'weather_stations.txt')


def test_wps_get_weather_stations_start_end_success(midas_metadata):
    datainputs = "start=2017-10-10T00:00:00;end=2018-02-03T12:00:00;counties=cornwall"
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert_response_success(resp)
    check_for_output_file(resp, 'weather_stations.txt')


def test_wps_get_weather_stations_start_end_fail(midas_metadata):
    datainputs = "start=2018-02-03T12:00:00;end=2017-10-10T00:00:00;counties=cornwall"
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')

