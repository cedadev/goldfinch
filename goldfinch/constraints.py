""" Module of methods for determining
validity of a request """

from datetime import timedelta
import dateutil.parser as dp
import glob
import os

def check_request_size(station_list, inputs):
    year_sizes = get_year_sizes(inputs['obs_table'])

    n_stations = len(station_list)
    dates = inputs['DateRange']
    start_date = dp.isoparse(dates[0])
    end_date = dp.isoparse(dates[1])

    # Something like this
    # Sum of year sizes for date ranges (if half a year then half the size)
    # Multiply by (selected stations / all stations)
    # assert is smaller than a set ceiling.


def get_year_sizes(table_name):
    year_sizes = {}

    pattern = f"/badc/ukmo-midas/data/{table_name}/yearly_files/*"
    year_files = glob.glob(pattern)

    for year_path in year_files:
        year = os.path.basename(year_path)[-10:-6]
        year_sizes[year] = os.stat(year_path).st_size


def get_year_proportions(start, end):
    year_proportions = {}

    start_year = start.year
    end_year = end.year
    year_diff = end_year - start_year

    if year_diff != 0:
        start_prop_delta = start - dp.isoparse(str((start + timedelta(days=360)).year))
        start_prop = (-1 * start_prop_delta.days) / 365
        year_proportions[start_year] = start_prop

        end_prop_delta = dp.isoparse(str(end.year)) - end
        end_prop = (-1 * end_prop_delta.days) / 365
        year_proportions[end_year] = end_prop
        
    else:
        prop_delta = end - start
        prop = (-1 * prop_delta.days) / 365
        year_proportions[start_year] = prop

