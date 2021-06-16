from .wps_get_weather_stations import GetWeatherStations
from .wps_extract_uk_station_data import ExtractUKStationData
from .wps_extract_uk_station_data_with_date_input import ExtractUKStationDataWithDateInput

processes = [
    GetWeatherStations(),
    ExtractUKStationData(),
    ExtractUKStationDataWithDateInput(),
]
