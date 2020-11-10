import os
import pytest

from pywps.tests import assert_response_success

from .common import get_output, run_with_inputs, check_for_output_file
from goldfinch.processes.wps_extract_uk_station_data import ExtractUKStationData


def test_wps_extract_uk_station_data_no_params_fail():
    datainputs = ""
    resp = run_with_inputs(ExtractUKStationData, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')

# TODO: work out how to raise an exception in the code that gets put in the 
#       Exception Report returned by the server
def test_wps_extract_uk_station_data_no_table_fail():
    datainputs = "counties=devon;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00"
    resp = run_with_inputs(ExtractUKStationData, datainputs)

    assert "please provide: ['obs_table']" in resp.response[0].decode('utf-8')


def test_wps_extract_uk_station_data_one_county_success():
    datainputs = "obs_table=RD;counties=devon;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00"
    resp = run_with_inputs(ExtractUKStationData, datainputs)

    assert_response_success(resp)
    output = get_output(resp.xml)

    assert 'output' in output
    assert 'stations' in output
