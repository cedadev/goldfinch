import pytest

from pywps.tests import assert_response_success

from .common import run_with_inputs, check_for_output_file
from goldfinch.processes.wps_get_weather_stations import GetWeatherStations

