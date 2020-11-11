import copy

from pywps.app.exceptions import ProcessError

from goldfinch.time_split import DurationSplitter

from midas_extract.stations import StationIDGetter
from midas_extract.subsetter import MIDASSubsetter
from midas_extract.vocabs import UK_COUNTIES, DATA_TYPES, TABLE_NAMES


WEATHER_STATIONS_FILE_NAME = 'weather_stations.txt'


def translate_bbox(wps_bbox):
    """
    Converts bbox definition in WPS to bbox definition in MIDAS code:

      * Input =  (w, s, e, n)
      * Output = (n, w, s, e)

    """
    (w, s, e, n) = wps_bbox
    return (n, w, s, e)


def get_station_list(counties, bbox, start, end, output_file, data_type=None):
    """
    Wrapper to call of midas station getter code.
    """
    # Translate bbox if it is used
    if bbox is not None:
        bbox = translate_bbox(bbox)

    station_getter = StationIDGetter(
        counties,
        bbox=bbox,
        data_type=data_type,
        start_time=revert_datetime_to_long_string(start),
        end_time=revert_datetime_to_long_string(end),
        output_file=output_file,
        quiet=True)

    return station_getter.st_list


def filter_observations(table_name, output_path, start=None, end=None, columns="all", 
                        conditions=None, src_ids=None, region=None, delimiter="default", 
                        tmp_dir=None, verbose=False):
    """
    Wrapper to call to observation subsetter class.

    Args:
        table_name ([type]): [description]
        output_path ([type]): [description]
        start_time ([type], optional): [description]. Defaults to None.
        end_time ([type], optional): [description]. Defaults to None.
        columns (str, optional): [description]. Defaults to "all".
        conditions ([type], optional): [description]. Defaults to None.
        src_ids ([type], optional): [description]. Defaults to None.
        region ([type], optional): [description]. Defaults to None.
        delimiter (str, optional): [description]. Defaults to "default".
        tmp_dir ([type], optional): [description]. Defaults to None.
        verbose (int, optional): [description]. Defaults to 1.
    """

    return MIDASSubsetter(table_name, output_path, startTime=revert_datetime_to_long_string(start), 
                          endTime=revert_datetime_to_long_string(end), columns=columns, 
                          conditions=conditions, src_ids=src_ids, region=region, delimiter=delimiter, 
                          tmp_dir=tmp_dir, verbose=verbose)


def filter_obs_by_time_chunk(table_name, output_path, start=None, end=None, columns="all", 
                        conditions=None, src_ids=None, region=None, delimiter="default", 
                        chunk_rule='year', tmp_dir=None, verbose=False):
    """
    Loops through time chunks extracting data to files in required time chunks.
    We pass the context object through here so that we can report progress.
    Returns a list of output file paths produced.
    """
    start_hr_min = start[8:12]
    end_hr_min = end[8:12]

    # Split the time chunks appropriately
    ds = DurationSplitter()
    time_splits = ds.splitDuration(start[:8], end[:8], chunk_rule)
    first_date = True

    # Set the extension
    if delimiter == "comma":
        ext = "csv"
    else:
        ext = "txt"

    # Create list to return
    output_file_paths = []

    progress = 5 # % of way to completion

    for count, (start_date, end_date) in enumerate(time_splits):

        # Add appropriate hours and minutes to date strings to make 12 character times
        if first_date == True:
            start = "%s%s" % (start_date.date, start_hr_min)
            first_date = False
        else:
            start = "%s0000" % (start_date.date)

        if end_date == time_splits[-1][-1]:
            end = "%s%s" % (end_date.date, end_hr_min)
        else:
            end = "%s2359" % (end_date.date)

        # Decide the output file path
        output_file_path = "%s-%s-%s.%s" % (output_path, start, end, ext)
        output_file_paths.append(output_file_path)

        # Call subsetter to extract and write the data
        filter_observations(table_name, output_file_path, start=start, end=end, columns=columns,
                            conditions=conditions, src_ids=src_ids, region=region, delimiter=delimiter, 
                            tmp_dir=tmp_dir, verbose=verbose)

        # Report on progress
        progress = int(float(count) / len(time_splits) * 100)

    return output_file_paths


def revert_datetime_to_long_string(dt):
    """
    Turns a date/time into a long string as needed by midas code.
    """
    return str(dt).replace('-', '').replace(' ', '').replace('T', '').replace(':', '')


def validate_inputs(inputs, defaults=None, required=None):
    """
    Receive inputs dictionary, process it, perform validations and 
    return a new dictionary.

    Also sets defaults for values based on dictionary of `defaults`.
    
    You can set `required` as a sequence of keys that must exist.
    """
    if not defaults: defaults = {}
    if not required: required = []

    req_not_present = []
    
    for req_input in required:
        if req_input not in inputs:
            req_not_present.append(req_input)

    # Fail if required parameters are not present
    if req_not_present:
        raise ProcessError(f'Missing input parameters, please provide: {req_not_present}.')

    resp = copy.deepcopy(inputs)

    if 'counties' in inputs:
        resp['counties'] = [_ for _ in inputs['counties'][0].data.split(',')]
        
        if not set(UK_COUNTIES).issuperset(set(resp['counties'])):
            raise ProcessError(f'Counties must be valid UK counties, not: {resp["counties"]}.')
    else:
        resp['counties'] = []

    if 'obs_table' in inputs:
        resp['obs_table'] = inputs['obs_table'][0].data

    if 'station_ids' in inputs:
        resp['station_ids'] = [s_id for s_id in inputs['station_ids'][0].data.split(',')]
    else:
        resp['station_ids'] = []

    if 'bbox' in inputs:
        resp['bbox'] = [float(_) for _ in inputs['bbox'][0].data.split(',')]
    else:
        resp['bbox'] = None

    if 'datatypes' in inputs:
        resp['datatypes'] = [data_type.data for data_type in inputs['datatypes']]
    else:
        resp['datatypes'] = None

    if 'delimiter' in inputs:
        resp['delimiter'] = inputs['delimiter'][0].data

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


def read_from_file(fpath, converter=str):
    with open(fpath) as reader:
        return [dtype(_) for _ in reader.read().strip().split()]