import pytest
import itertools

from pywps.tests import assert_response_success

from .common import get_output, run_with_inputs, check_for_output_file
from goldfinch.processes.wps_get_weather_stations import GetWeatherStations

times = ['DateRange=2010-01-01/2010-12-31;', 'DateRange=2014-01-01/2014-12-31;',
         'DateRange=2018-01-01/2018-12-31;']
counties = ['counties=OXFORDSHIRE','counties=DEVON','counties=BERKSHIRE;counties=DORSET',
            'counties=YORKSHIRE;counties=SURREY;counties=HAMPSHIRE']

file_names = [name[0] + name[1] for name in itertools.product(['1','2','3'],['a','b','c','d'])]
inputs = [inp[0] + inp[1] for inp in itertools.product(times, counties)]

params = list(zip(file_names, inputs))

@pytest.mark.parametrize("file_name,param", params)
def test_compare_weather_station_output(file_name, param):
    import pdb; pdb.set_trace()

    resp = run_with_inputs(GetWeatherStations, param)

    

    assert_response_success(resp)
    output = get_output(resp.xml)

    