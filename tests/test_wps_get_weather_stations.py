import pytest

from pywps.tests import assert_response_success

from .common import run_with_inputs, check_for_output_file
from goldfinch.processes.wps_get_weather_stations import GetWeatherStations


def test_wps_get_weather_stations_no_params_fail(midas_metadata):
    datainputs = ""
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')


def test_wps_get_weather_stations_counties_success(midas_metadata):
    datainputs = "counties=cornwall;counties=devon"
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert_response_success(resp)
    check_for_output_file(resp, 'weather_stations.txt')


@pytest.mark.xfail
def test_wps_get_weather_stations_counties_split_by_bar_success(midas_metadata):
    datainputs = "counties=cornwall|devon"
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


def test_wps_get_weather_stations_DateRange_success(midas_metadata):
    datainputs = "DateRange=2017-10-10/2018-02-03;counties=cornwall"
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert_response_success(resp)
    check_for_output_file(resp, 'weather_stations.txt')


def test_wps_get_weather_stations_DateRange_fail(midas_metadata):
    datainputs = "DateRange=2018-02-03/2017-10-10;counties=cornwall"
    resp = run_with_inputs(GetWeatherStations, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')
