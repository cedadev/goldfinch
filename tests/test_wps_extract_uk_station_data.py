import pytest

from pywps import Service
from pywps.tests import client_for, assert_response_success

from .common import get_output
from goldfinch.processes.wps_extract_uk_station_data import ExtractUKStationData


def test_wps_extract_uk_station_data():
    client = client_for(Service(processes=[ExtractUKStationData()]))
    datainputs = "counties=devon;StartDateTime=2017-10-01%2000:00:00;EndDateTime=2018-01-31%2000:00:00"
    resp = client.get(
        "?service=WPS&request=Execute&version=1.0.0&identifier=ExtractUKStationData&datainputs={}".format(
            datainputs))

    try:
        assert_response_success(resp)
    except Exception:
        raise AssertionError(f"Failed response: {resp.response}")

    assert 'output' in get_output(resp.xml)
