""" This module is attempting to estimate the size of a data request.
Using the sizes of the yearly files for each table, I have created some linear model
which take a date from 1957 onwards (normalised to 0 or more) and return an estimated size
of the data for that year. To estimate over a date range, we can integrate the model.

In most cases tables have data before 1957 however, it is usually of insignificant size.
Cutting the data here also makes the linear model more accurate. """

from datetime import timedelta
import dateutil.parser as dp
import glob
import os
import scipy.integrate as integrate

TABLE_MODELS = {
    'TD': lambda x: 330695*x + 17338156,
    'WD': lambda x: 31789*x + 37823234,
    'RH': lambda x: 3512674*x,
    'WM': lambda x: 4961883*x + 21431612,
    'RO': lambda x: 1754260*x,
    'RD': lambda x: 756422*x + 101165112,
    'WH': lambda x: 18281684*x,
    'ST': lambda x: 2303563*x
}

def check_request_size(station_list, inputs):
    SIZE_LIMIT = int(os.environ.get('MIDAS_TEST_REQUEST_SIZE_LIMIT', '500000000')) #5*10^8, 500mb
    TOTAL_STATION_ESTIMATE = int(os.environ.get('MIDAS_TEST_TOTAL_STATIONS', '1000'))

    table = inputs['obs_table']

    if dp.isoparse(inputs['end']) < dp.isoparse('1957') or table == 'RS':
        return

    file_size_model = TABLE_MODELS[table]
    start_converted = _convert_date(dp.isoparse(inputs['start']))
    end_converted = _convert_date(dp.isoparse(inputs['end']))

    adjusted_2020 = 2020 - 1957
    if start_converted < 0:
        """ If the date range begins before 1957, ignore the data before 1957.
        In many cases, the Y intercept of the model is 0 therefore, integrating in the
        negative X will add a negative value to result. """
        start_converted = 0;

    if end_converted > adjusted_2020:
        """ These models were created on data from 1957 - 2020.
        In the interest of future proofing, files beyond 2020 will be modeled to 
        have the same size as 2020 was"""
        linear_size_estimate = integrate.quad(file_size_model, start_converted, adjusted_2020)[0]
        static_y = file_size_model(adjusted_2020)
        remainder = end_converted - adjusted_2020
        static_size_estimate = static_y * remainder # Equivalent to integrating y = <static_y>
        size_estimate = linear_size_estimate + static_size_estimate
    else:
        size_estimate = integrate.quad(file_size_model, start_converted, end_converted)[0]

    n_stations = len(station_list) 

    if (n_stations / TOTAL_STATION_ESTIMATE) * size_estimate > SIZE_LIMIT:
        raise Exception('The estimated amount of data you have selected is too large. '
                        'Please select less stations or a smaller time range.')


def _convert_date(date):
    base = date.year - 1957
    day_remainder_delta = date - dp.isoparse(str(date.year))
    fractional = day_remainder_delta.days / 360
    return base + fractional
