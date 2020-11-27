import itertools
import os
import pathlib
import pytest

from pywps.tests import assert_response_success

from .common import get_output, run_with_inputs, check_for_output_file
from goldfinch.processes.wps_get_weather_stations import GetWeatherStations

times = ['DateRange=2010-01-01/2011-01-01;', 'DateRange=2014-01-01/2015-01-01;',
         'DateRange=2018-01-01/2019-01-01;']
counties = ['counties=OXFORDSHIRE','counties=DEVON','counties=BERKSHIRE;counties=DORSET',
            'counties=YORKSHIRE;counties=SURREY;counties=HAMPSHIRE']

file_names = [name[0] + name[1] for name in itertools.product(['1','2','3'],['a','b','c','d'])]
inputs = [inp[0] + inp[1] for inp in itertools.product(times, counties)]

params = list(zip(file_names, inputs))


def _extract_filepath(url):
    parts = url.split('/')
    path = '/tmp/' + '/'.join(parts[-2:])
    return path

@pytest.mark.skip(reason="comparison on wps already made")
@pytest.mark.parametrize("file_name,param", params)
def test_compare_weather_station_output_counties(file_name, param):
    resp = run_with_inputs(GetWeatherStations, param)

    assert_response_success(resp)
    output = get_output(resp.xml)
    output_file = _extract_filepath(output['output'])

    file_content = open(output_file).read()
    current_dir = pathlib.Path(__file__).parent.absolute()
    wps_file_content = open(os.path.join(current_dir, 'ceda-wps-example-data', 'stations', f'{file_name}.txt')).read()

    assert file_content == wps_file_content
    