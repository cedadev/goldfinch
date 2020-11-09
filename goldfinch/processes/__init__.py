from .wps_get_weather_stations import GetWeatherStations
from .wps_extract_uk_station_data import ExtractUKStationData

processes = [
    GetWeatherStations(),
    ExtractUKStationData(),
]
