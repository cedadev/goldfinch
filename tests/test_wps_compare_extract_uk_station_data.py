import itertools
import os
import pandas
import pathlib
import pytest

from pywps.tests import assert_response_success

from .common import get_output, run_with_inputs
from goldfinch.processes.wps_extract_uk_station_data import ExtractUKStationData

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
def test_compare_extract_station_data_td(file_name, param):
    data_inputs = f'obs_table=TD;{param}'
    resp = run_with_inputs(ExtractUKStationData, data_inputs)

    assert_response_success(resp)
    output = get_output(resp.xml)
    output_file = _extract_filepath(output['output'])

    file_content = pandas.read_csv(output_file)
    current_dir = pathlib.Path(__file__).parent.absolute()
    wps_file_content = pandas.read_csv(os.path.join(current_dir, 'ceda-wps-example-data',
                                                    'extract_data', f'{file_name}TD.csv'))

    assert file_content.equals(wps_file_content)


@pytest.mark.skip(reason="comparison on wps already made")
@pytest.mark.parametrize("file_name,param", params)
def test_compare_extract_station_data_wd(file_name, param):
    data_inputs = f'obs_table=WD;{param}'
    resp = run_with_inputs(ExtractUKStationData, data_inputs)

    assert_response_success(resp)
    output = get_output(resp.xml)
    output_file = _extract_filepath(output['output'])

    import pdb; pdb.set_trace()

    file_content = pandas.read_csv(output_file)
    current_dir = pathlib.Path(__file__).parent.absolute()
    wps_file_content = pandas.read_csv(os.path.join(current_dir, 'ceda-wps-example-data',
                                                    'extract_data', f'{file_name}WD.csv'))

    assert file_content.equals(wps_file_content)


@pytest.mark.skip(reason="comparison on wps already made")
def test_compare_extract_station_data_bbox_td():
    data_inputs = f'obs_table=TD;DateRange=2015-01-01/2015-01-03;bbox=-3,52,1.25,54.5'
    resp = run_with_inputs(ExtractUKStationData, data_inputs)

    assert_response_success(resp)
    output = get_output(resp.xml)
    output_file = _extract_filepath(output['output'])

    file_content = pandas.read_csv(output_file)
    current_dir = pathlib.Path(__file__).parent.absolute()
    wps_file_content = pandas.read_csv(os.path.join(current_dir, 'bboxTD.csv'))

    assert file_content.equals(wps_file_content)


@pytest.mark.skip(reason="comparison on wps already made")
def test_compare_extract_station_data_one_station_td():
    data_inputs = f'obs_table=TD;DateRange=2014-01-01/2015-01-01;station_ids=605'
    resp = run_with_inputs(ExtractUKStationData, data_inputs)

    assert_response_success(resp)
    output = get_output(resp.xml)
    output_file = _extract_filepath(output['output'])

    file_content = pandas.read_csv(output_file)
    current_dir = pathlib.Path(__file__).parent.absolute()
    wps_file_content = pandas.read_csv(os.path.join(current_dir, '605TD.csv'))

    assert file_content.equals(wps_file_content)

