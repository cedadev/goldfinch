import copy

from pywps.app.exceptions import ProcessError

from midas_extract.stations import StationIDGetter


UK_COUNTIES = [
    'cornwall',
    'devon',
    'wiltshire'
]

ALLOWED_DATA_TYPES = ['CLBD', 'CLBN', 'CLBR', 'CLBW', 'DCNN', 'FIXD',
 'ICAO', 'LPMS', 'RAIN', 'SHIP', 'WIND', 'WMO']


def translate_bbox(wps_bbox):
    """
    Converts bbox definition in WPS to bbox definition in MIDAS code:

      * Input =  (w, s, e, n)
      * Output = (n, w, s, e)

    """
    (w, s, e, n) = wps_bbox
    return (n, w, s, e)


def get_station_list(counties, bbox, data_types, start_time, end_time, output_file):
    """
    Wrapper to call of midas station getter code.
    """
    # Translate bbox if it is used
    if bbox is not None:
        bbox = translate_bbox(bbox)

    station_getter = StationIDGetter(
        counties,
        bbox=bbox,
        data_types=data_types,
        start_time=revert_datetime_to_long_string(start_time),
        end_time=revert_datetime_to_long_string(end_time),
        output_file=output_file,
        quiet=True)

    return station_getter.st_list


def revert_datetime_to_long_string(dt):
    """
    Turns a date/time into a long string as needed by midas code.
    """
    return str(dt).replace('-', '').replace(' ', '').replace('T', '').replace(':', '')


def validate_inputs(inputs, defaults=None):
    """
    Receive inputs dictionary, process it, perform validations and 
    return a new dictionary.

    Also sets defaults for values based on dictionary of `defaults`.
    """
    if not defaults: defaults = {}

    resp = copy.deepcopy(inputs)

    if 'counties' in inputs:
        resp['counties'] = [_ for _ in inputs['counties'][0].data.split(',')]
        
        if not set(UK_COUNTIES).issuperset(set(resp['counties'])):
            raise ProcessError(f'Counties must be valid UK counties, not: {resp["counties"]}.')

    else:
        resp['counties'] = []

    if 'bbox' in inputs:
        resp['bbox'] = [float(_) for _ in inputs['bbox'][0].data.split(',')]
    else:
        resp['bbox'] = None

    if 'datatypes' in inputs:
        resp['datatypes'] = [data_type.data for data_type in inputs['datatypes']]
    else:
        resp['datatypes'] = []

    # Fix datetimes
    for dt in ('start', 'end'):
        resp[dt] = revert_datetime_to_long_string(inputs[dt][0].data)

    # Add default values for those inputs not set
    for key, value in defaults.items():
        if not resp.get(key, None):
            resp[key] = value

    if resp['counties'] == [] and resp['bbox'] == None and \
        resp.get('station_ids', []) == [] and \
        not resp.get('input_job_id'):
        raise Exception('Invalid arguments provided. Must provide one of (i) a geographical bounding box, (ii) a list of counties,'
                        ' (iii) a set of station IDs or (iv) an input job ID from which a file containing a set of selected station'
                        ' IDs can be extracted.')

    if resp['start'] > resp['end']:
        raise Exception('Invalid arguments provided. Start cannot be after end date/time.')

    return resp


def locate_process_dir(job_id):
    raise NotImplementedError()
