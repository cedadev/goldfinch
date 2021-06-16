import dateutil.parser as dp
import pandas
import pytest
import re

from pywps.tests import assert_response_success

from .common import get_output, run_with_inputs
from .test_wps_extract_uk_station_data import station_inputs, _extract_filepath
from goldfinch.processes.wps_extract_uk_station_data_with_date_input import ExtractUKStationDataWithDateInput


data_inputs = ['obs_table=TD;delimiter=tab;counties=DEVON;TemporalRange=2017-01-01/2019-01-31',
               'obs_table=TD;counties=SURREY;TemporalRange=2017-10-01/2018-01-31',
               'obs_table=TD;counties=SURREY;TemporalRange=2017-11-07/2019-03-15',
               'obs_table=TD;counties=DEVON;counties=KENT;counties=SURREY;TemporalRange=2017-10-01/2018-01-31',
               'obs_table=TD;counties=ISLE OF WIGHT;TemporalRange=2017-11-07/2019-03-15',
               'obs_table=TD;counties=DEVON;TemporalRange=2017-11-06/2019-01-15',
               'obs_table=TD;counties=POWYS (NORTH);counties=POWYS (SOUTH);TemporalRange=2017-11-06/2019-01-15',
               'obs_table=TD;counties=SURREY;TemporalRange=2017-11-07/2019-03-15',
               'obs_table=TD;bbox=0,4,50,55,urn:ogc:def:crs:EPSG:6.6:4326,2;TemporalRange=2017-10-01'
                   '/2018-01-31',
               'obs_table=TD;delimiter=tab;bbox=-5,-23,41,64;TemporalRange=2017-10-01/2018-01-31']
#data_inputs = data_inputs[:1]
large_inputs = ['obs_table=TD;delimiter=tab;counties=DEVON;TemporalRange=2017-01-01/2019-01-31',
                'obs_table=TD;counties=DEVON;TemporalRange=2017-11-06/2019-01-15',
                'obs_table=TD;bbox=0,4,50,55,urn:ogc:def:crs:EPSG:6.6:4326,2;TemporalRange=2017-10-01'
                   '/2019-01-31']



def test_wps_extract_uk_station_data_with_date_input_no_params_fail(load_test_data):
    datainputs = ""
    resp = run_with_inputs(ExtractUKStationDataWithDateInput, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')


# TODO: work out how to raise an exception in the code that gets put in the
#       Exception Report returned by the server
@pytest.mark.skip
def test_wps_extract_uk_station_data_with_date_input_no_table_fail(load_test_data):
    datainputs = "counties=DEVON;TemporalRange=2017-10-01/2018-01-31"
    resp = run_with_inputs(ExtractUKStationDataWithDateInput, datainputs)

    assert "please provide: ['obs_table']" in resp.response[0].decode('utf-8')


def test_wps_extract_uk_station_data_with_date_input_empty_bbox_fail(load_test_data):
    datainputs = "obs_table=TD;bbox=0,0,10,10,urn:ogc:def:crs:EPSG:6.6:4326,2;TemporalRange=2017-10-01/" \
                 "2018-01-31"
    resp = run_with_inputs(ExtractUKStationDataWithDateInput, datainputs)

    assert "ExceptionReport" in resp.response[0].decode('utf-8')


@pytest.mark.parametrize('ex_input', data_inputs)
def test_wps_extract_uk_station_data_with_date_input_success(load_test_data, ex_input):
    resp = run_with_inputs(ExtractUKStationDataWithDateInput, ex_input)

    assert_response_success(resp)
    output = get_output(resp.xml)
    input_list = ex_input.split(';')


    # added delimeter finder
    delimiter_dict = {
        'comma': ',',
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

    # start_pattern = re.compile('^start=.*$')
    # start = dp.isoparse([inp for inp in input_list if start_pattern.match(inp)][0][6:])

    # end_pattern = re.compile('^end=.*$')
    # end = dp.isoparse([inp for inp in input_list if end_pattern.match(inp)][0][4:])

    date_range = [inp for inp in input_list if inp.startswith('TemporalRange=')][0].split('=')[1]
    start, end = [dp.isoparse(dt) for dt in date_range.split('/')]

    dates = list(map(dp.isoparse, df['ob_end_time'].tolist()))
    assert all(date >= start and date <= end for date in dates)

    assert 'output' in output
    assert 'stations' in output


@pytest.mark.xfail(reason='Jobs fail because the request is larger than the limits')
@pytest.mark.parametrize('ex_input', large_inputs)
def test_wps_extract_uk_station_data_with_date_input_failure(load_test_data, size_limits, ex_input):
    resp = run_with_inputs(ExtractUKStationDataWithDateInput, ex_input)

    assert_response_success(resp)


@pytest.mark.parametrize('station_ids', station_inputs)
def test_wps_extract_uk_station_id_match_csv(load_test_data, station_ids):
    datainputs = f"obs_table=TD;station_ids={station_ids};TemporalRange=2017-01-01/2019-10-02"
    resp = run_with_inputs(ExtractUKStationDataWithDateInput, datainputs)

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


