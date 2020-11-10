from pywps import Service

from .common import wps_test_client_for
from goldfinch.processes import processes


def test_wps_caps():
    client = wps_test_client_for(Service(processes=processes))
    resp = client.get(service='wps', request='getcapabilities', version='1.0.0')
    names = resp.xpath_text('/wps:Capabilities'
                            '/wps:ProcessOfferings'
                            '/wps:Process'
                            '/ows:Identifier')
    assert sorted(names.split()) == [
        'ExtractUKStationData',
        'GetWeatherStations',
    ]
