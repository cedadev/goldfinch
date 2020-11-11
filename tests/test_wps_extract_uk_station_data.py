import os
import pytest

from pywps.tests import assert_response_success

from .common import get_output, run_with_inputs, check_for_output_file
from goldfinch.processes.wps_extract_uk_station_data import ExtractUKStationData

data_inputs = ['obs_table=TD;counties=devon;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00',
               'obs_table=TD;counties=surrey;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00',
               'obs_table=TD;counties=surrey;start=2017-11-07T00:00:00;end=2019-03-15T00:00:00',
               'obs_table=TD;counties=devon,kent,surrey;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00',
               'obs_table=TD;counties=isle of wight;start=2017-11-07T00:00:00;end=2019-03-15T00:00:00',
               'obs_table=TD;counties=devon;start=2017-11-06T00:12:00;end=2019-01-15T00:12:00',
               'obs_table=TD;counties=powys (north),powys (south);start=2017-11-06T00:12:00;end=2019-01-15T00:12:00',
               'obs_table=TD;counties=devon;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00;chunk_rule=month',
               'obs_table=TD;counties=surrey;start=2017-11-07T00:00:00;end=2019-03-15T00:00:00;chunk_rule=year',
               'obs_table=TD;bbox=0,0,10,10;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00',
               'obs_table=TD;delimiter=tab;bbox=-5,-23,41,64;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00']

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


@pytest.mark.parametrize('ex_input', data_inputs)
def test_wps_extract_uk_station_data_success(ex_input):
    resp = run_with_inputs(ExtractUKStationData, ex_input)

    assert_response_success(resp)
    output = get_output(resp.xml)

    assert 'output' in output
    assert 'stations' in output
