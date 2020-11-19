import dateutil.parser as dp
import os
import pandas
import pytest
import re

from pywps.tests import assert_response_success

from .common import get_output, run_with_inputs
from goldfinch.processes.wps_extract_uk_station_data import ExtractUKStationData

data_inputs = ['obs_table=TD;delimiter=tab;counties=devon;start=2017-01-01T00:00:00;end=2019-01-31T00:00:00',
               'obs_table=TD;counties=surrey;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00',
               'obs_table=TD;counties=surrey;start=2017-11-07T00:00:00;end=2019-03-15T00:00:00',
               'obs_table=TD;counties=devon,kent,surrey;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00',
               'obs_table=TD;counties=isle of wight;start=2017-11-07T00:00:00;end=2019-03-15T00:00:00',
               'obs_table=TD;counties=devon;start=2017-11-06T00:12:00;end=2019-01-15T00:12:00',
               'obs_table=TD;counties=powys (north),powys (south);start=2017-11-06T00:12:00;end=2019-01-15T00:12:00',
               'obs_table=TD;counties=surrey;start=2017-11-07T00:00:00;end=2019-03-15T00:00:00',
               'obs_table=TD;bbox=0,4,50,55,urn:ogc:def:crs:EPSG:6.6:4326,2;start=2017-10-01T00:00:00;'
               'end=2018-01-31T00:00:00',
               'obs_table=TD;delimiter=tab;bbox=-5,-23,41,64;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00']

station_inputs = ['56810', '17101,1007', '1039,57199,1144']


def _extract_filepath(url):
    parts = url.split('/')
    path = '/tmp/' + '/'.join(parts[-2:])
    return path


def test_wps_extract_uk_station_data_no_params_fail(midas_metadata, midas_data):
    datainputs = ""
    resp = run_with_inputs(ExtractUKStationData, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')


# TODO: work out how to raise an exception in the code that gets put in the
#       Exception Report returned by the server
@pytest.mark.skip
def test_wps_extract_uk_station_data_no_table_fail(midas_metadata, midas_data):
    datainputs = "counties=devon;start=2017-10-01T00:00:00;end=2018-01-31T00:00:00"
    resp = run_with_inputs(ExtractUKStationData, datainputs)

    assert "please provide: ['obs_table']" in resp.response[0].decode('utf-8')


def test_wps_extract_uk_station_data_empty_bbox_fail(midas_metadata, midas_data):
    datainputs = "obs_table=TD;bbox=0,0,10,10,urn:ogc:def:crs:EPSG:6.6:4326,2;start=2017-10-01T00:00:00;" \
                 "end=2018-01-31T00:00:00"
    resp = run_with_inputs(ExtractUKStationData, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')


@pytest.mark.parametrize('ex_input', data_inputs)
def test_wps_extract_uk_station_data_success(midas_metadata, midas_data, ex_input):
    resp = run_with_inputs(ExtractUKStationData, ex_input)

    assert_response_success(resp)
    output = get_output(resp.xml)
    input_list = ex_input.split(';')

    # added delimeter finder
    delimiter_dict = {
        'comma': ',',
        ',': ',',
        'tab': '\t'
    }
    del_pattern = re.compile('^delimiter=.*$')
    given_del_list = [inp for inp in input_list if del_pattern.match(inp)]

    if given_del_list:
        given_del = given_del_list[0][10:]
    else:
        given_del = 'comma'

    output_file = _extract_filepath(output['output'])
    df = pandas.read_csv(output_file, skipinitialspace=True, sep=delimiter_dict[given_del])

    start_pattern = re.compile('^start=.*$')
    start = dp.isoparse([inp for inp in input_list if start_pattern.match(inp)][0][6:])

    end_pattern = re.compile('^end=.*$')
    end = dp.isoparse([inp for inp in input_list if end_pattern.match(inp)][0][4:])

    dates = list(map(dp.isoparse, df['ob_end_time'].tolist()))
    assert all(date >= start and date <= end for date in dates)

    assert 'output' in output
    assert 'stations' in output


@pytest.mark.parametrize('station_ids', station_inputs)
def test_wps_extract_uk_station_id_match_csv(station_ids):
    datainputs = f"obs_table=TD;station_ids={station_ids};start=2017-10-01T00:00:00;end=2017-10-02T00:00:00"
    resp = run_with_inputs(ExtractUKStationData, datainputs)

    assert_response_success(resp)
    output = get_output(resp.xml)

    output_file = _extract_filepath(output['output'])
    df = pandas.read_csv(output_file, skipinitialspace=True)

    expected_ids = set(map(int, station_ids.split(',')))
    found_ids = set(df['src_id'].to_list())

    assert found_ids == expected_ids

    assert 'output' in output
    assert 'stations' in output
    assert 'doc_links_file' in output
