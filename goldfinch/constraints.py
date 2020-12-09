""" Module of methods for determining
validity of a request """

from datetime import timedelta
import dateutil.parser as dp
import glob
import numpy as np
import os
import scipy.integrate as integrate

SIZE_LIMIT = 5 * 10^8

TABLE_MODELS = {
    'TD': lambda x: 330695*x + 17338156,
    'WD': lambda x: 31789*x + 37823234,
    'RH': lambda x: -1776*(x**3) + 184528*(x**2) - 1427575*x + 19656292,
    'WM': lambda x: 4961883*x + 21431612,
    'RO': lambda x: 4551794*np.exp(0.0558*x),
    'RD': lambda x: 756422*x + 101165112,
    'WH': lambda x: 22011601*(x**0.91),
    'ST': lambda x: 3006720*np.exp(0.068*x)
}

def check_request_size(station_list, inputs):
    table = inputs['obs_table']

    if dp.isoparse(inputs['end']) < dp.isoparse('1957') or table == 'RS':
        return True

    file_size_model = TABLE_MODELS[table]
    start_converted = _convert_date(inputs['start'])
    end_converted = _convert_date(inputs['end'])

    size_estimate = integrate.quad(file_size_model, start_converted, end_converted)[0]

    n_stations = len(station_list)
    total_station_estimate = 1000 

    return (n_stations / total_station_estimate) * size_estimate < SIZE_LIMIT


def _convert_date(date):
    base = date.year - 1957
    day_remainder_delta = date - dp.isoparse(str(date.year))
    fractional = day_remainder_delta.days / 360
    return base + fractional
