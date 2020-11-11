import os.path

from pywps import Process, LiteralInput, ComplexOutput, BoundingBoxInput, Format
from pywps import FORMATS
from pywps.app.Common import Metadata

from goldfinch.util import (get_station_list, validate_inputs, locate_process_dir,
    filter_obs_by_time_chunk, read_from_file, TABLE_NAMES, WEATHER_STATIONS_FILE_NAME)


import logging
LOGGER = logging.getLogger("PYWPS")


class ExtractUKStationData(Process):
    """A process extracting UK station data."""
    def __init__(self):
        inputs = [
            LiteralInput('start', 'Start Date Time',
                         abstract='The first date/time for which to search for operating weather stations.',
                         data_type='dateTime',
                         default='2017-10-01T12:00:00Z'),
            LiteralInput('end', 'End Date Time',
                         abstract='The last date/time for which to search for operating weather stations.',
                         data_type='dateTime',
                         default='2018-02-25T12:00:00Z'),
            LiteralInput('chunk_rule', 'Chunk Rule for Outputs',
                         abstract='The period of time spanned by each output file.',
                         data_type='string',
                         allowed_values=['decade', 'year', 'month'],
                         default='year',
                         min_occurs=0),
            # BoundingBoxInput('BBox', 'Bounding Box',
            #                  abstract='The spatial bounding box within which to search for weather stations.'
            #                           ' This input will be ignored if counties are provided.',
            #                  crss=['epsg:4326', 'epsg:3035'],
            #                  min_occurs=0),
            LiteralInput('bbox', 'Bounding Box',
                         abstract='The spatial bounding box within which to search for weather stations.'
                         ' This input will be ignored if counties are provided.'
                         ' Provide the bounding box as: "W,S,E,N".',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1),
            LiteralInput('counties', 'Counties',
                         abstract='A list of counties within which to search for weather stations.',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1),
            LiteralInput('station_ids', 'Station Source IDs',
                         abstract='A list of weather stations source IDs.'
                                  ' This input will be ignored if an input job ID is provided.',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1),
            LiteralInput('input_job_id', 'Input Job Id',
                         abstract='The Id of a separate WPS Job used to select a set of weather stations.',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1),
            LiteralInput('obs_table', 'Obervation Table Name',
                         abstract='The name of the database table used in the MIDAS database to identify'
                                  ' a particular selection of weather observations.',
                         data_type='string',
                         allowed_values=TABLE_NAMES,
                         # UK Daily Temperature, UK Daily Weather, UK Daily Rain,
                         # UK Hourly Rain, UK Sub-hourly Rain (to April 2005),
                         # UK Soil Temperature, UK Hourly Weather,
                         # UK Mean Wind, Global Radiation Observations
                         min_occurs=0,
                         max_occurs=1),
            LiteralInput('delimiter', 'Delimiter',
                         abstract='The delimiter to be used in the output files.',
                         data_type='string',
                         allowed_values=['comma', 'tab'],
                         default='comma',
                         min_occurs=0,
                         max_occurs=1),
        ]
        outputs = [
            ComplexOutput('output', 'Output',
                          abstract='Observations file (CSV or tab-delimited).',
                          as_reference=True,
                          supported_formats=[FORMATS.TEXT]),
            ComplexOutput('stations', 'Station list output',
                          abstract='Station list.',
                          as_reference=True,
                          supported_formats=[FORMATS.TEXT]),
            ComplexOutput('doc_links_file', 'Documentation links file',
                          abstract='File containing links to metadata and documentation.',
                          as_reference=True,
                          supported_formats=[FORMATS.TEXT]),
            ]

        super(ExtractUKStationData, self).__init__(
            self._handler,
            identifier='ExtractUKStationData',
            title='Extract UK Station Data',
            abstract='The Extract UK Station Data process provides tools to'
                     ' access surface station weather observations for a range'
                     ' of variables throughout the UK.'
                     ' These include temperature, rainfall and wind measurements.'
                     ' These records are available from 1859 to this year.'
                     ' You can select which stations you require using'
                     ' either a bounding box, a list of UK counties,'
                     ' a list of station IDs or an uploaded file containing station IDs.'
                     ' Data is returned in CSV or tab-delimited text files.'
                     ' Please see the disclaimer.',
            keywords=['stations', 'uk', 'extract', 'observations', 'data'],
            metadata=[
                Metadata('User Guide', 'http://badc.nerc.ac.uk/data/ukmo-midas/WPS.html'),
                Metadata('CEDA WPS', '****TO BE ADDED*****'),
                Metadata('Disclaimer' 'https://help.ceda.ac.uk/article/4642-disclaimer')
            ],
            version='2.0.0',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True
        )


    def _handler(self, request, response):
        # TODO: dry-run
        LOGGER.info("Extracting UK station data")

        # Set self.response so it can be modified in other methods
        self.response = response
        # Now set status to started
        self.response.update_status('Job is now running', 0)

        # Define defaults for arguments that might not be set
        input_defaults = {'station_ids': [], 'input_job_id': None, 
                          'chunk_rule': 'year', 'delimiter': 'comma'}

        inputs = validate_inputs(request.inputs, defaults=input_defaults,
                                 required=['obs_table', 'start', 'end'])

        # Resolve the list of stations
        stations_file = WEATHER_STATIONS_FILE_NAME
        stations_file_path = os.path.join(self.workdir, stations_file)

        station_list = self._resolve_station_list(inputs, stations_file_path)

        # Write stations file
        self._write_stations_file(stations_file_path, station_list)

        # Estimate we are 5% of the way through
        self.response.update_status('Extracted station ID list.', 5)

        # Check request size against limits
        self._check_request_size(station_list, inputs)

        # Define data file base
        prefix = 'station_data'
        if inputs['delimiter'] == 'comma':
            ext = 'csv'
        else:
            ext = 'txt'

        output_file_base = os.path.join(self.workdir, prefix)

        # Need temp dir for big file extractions
        proc_tmp_dir = os.path.join(self.workdir, 'tmp')
        
        if not os.path.isdir(proc_tmp_dir):
            os.makedirs(proc_tmp_dir)

        # Get Obs Table Names to extract from
        obs_table = inputs['obs_table']

        # Extract the observations by filtering the full dataset
        output_paths = filter_obs_by_time_chunk(obs_table, output_file_base, 
                                           start=inputs['start'], end=inputs['end'], 
                                           src_ids=station_list, delimiter=inputs['delimiter'],
                                           tmp_dir=proc_tmp_dir) 

        # Write docs links to output file
        doc_links_file = os.path.join(self.workdir, 'doc_links.txt')
        self._write_doc_links_file(doc_links_file, obs_table)

        # We can log information at any time to the main log file
        for output_path in output_paths:
            LOGGER.info('Written output file: {}'.format(output_path))

        self.response.outputs['stations'].file = stations_file_path
        return self.response

    def _resolve_station_list(self, inputs, stations_file_path):
        """
        Works out whether we need to generate a station list or use those
        sent as inputs.
        """
        station_list = None

        # Use input station list if provided
        if inputs['station_ids'] != []:
            station_list = inputs['station_ids']

        # Use input job ID to extract text file of stations
        elif inputs['input_job_id']:
            input_process_dir = locate_process_dir(inputs['input_job_id'])
            input_weather_stations_file = os.path.join(input_process_dir, WEATHER_STATIONS_FILE_NAME)
            station_list = read_from_file(input_weather_stations_file)

        # If not dry run
        if not station_list:
            # Call code to get Weather Stations
            counties_list = self._get_counties(inputs)
            station_list = get_station_list(counties_list, inputs['bbox'],  
                                            inputs['start'], inputs['end'], 
                                            stations_file_path)

        # Write the file one per station id per line
        station_list.sort()

        return station_list

    def _write_stations_file(self, stations_file_path, station_list):
        "Writes stations file (that were used in the extraction)."
        with open(stations_file_path, "w") as fout:
            fout.write("\r\n".join([str(station) for station in station_list]))

        self.response.outputs['stations'].file = stations_file_path

    def _check_request_size(self, station_list, inputs):
        """Checks the size of the request against allowed limits.

        Args:
            inputs ([type]): [description]

        Raises:
            Exception: [description]
            Exception: [description]
            Exception: [description]
        """
        n_stations = len(station_list)
        STATION_LIMIT = 100

        n_years = int(inputs['start'][:4]) - int(inputs['end'][:4])

        if n_stations > STATION_LIMIT and inputs['chunk_rule'] == 'decadal':

            inputs['chunk_rule'] = 'year'
            raise Exception('The number of selected station IDs has been calculated to be '
                            'greater than {}. Please select a chunk size other than "decadal" '
                            'for such a large volume of data.'.format(STATION_LIMIT))

        if n_years > 1 and n_stations > STATION_LIMIT:
            raise Exception('The number of selected station IDs has been calculated to be '
                            'greater than {}. Please select a time window no longer than 1 '
                            'year.'.format(STATION_LIMIT))

        if n_stations == 0:
            raise Exception('No weather stations have been found for this request. Please '
                            'modify your request and try again.')


    def _write_doc_links_file(self, doc_links_file, obs_table):
        "Write documentation links file."

        midas_table_to_moles_dict = {
            "WM":   "http://catalogue.ceda.ac.uk/uuid/a1f65a362c26c9fa667d98c431a1ad38",
            "RH":   "http://catalogue.ceda.ac.uk/uuid/bbd6916225e7475514e17fdbf11141c1",
            "CURS": "http://catalogue.ceda.ac.uk/uuid/7f76ab4a47ee107778e0a7e8a701ee77",
            "ST":   "http://catalogue.ceda.ac.uk/uuid/8dc05f6ecc6065a5d10fc7b8829589ec",
            "GL":   "http://catalogue.ceda.ac.uk/uuid/0ec59f09b3158829a059fe70b17de951",
            "CUNS": "http://catalogue.ceda.ac.uk/uuid/bef3d059255a0feaa14eb78c77d7bc48",
            "TMSL": "http://catalogue.ceda.ac.uk/uuid/33ca1887e5f116057340e404b2c752f2",
            "RO":   "http://catalogue.ceda.ac.uk/uuid/b4c028814a666a651f52f2b37a97c7c7",
            "MO":   "http://catalogue.ceda.ac.uk/uuid/77910bcec71c820d4c92f40d3ed3f249",
            "RS":   "http://catalogue.ceda.ac.uk/uuid/455f0dd48613dada7bfb0ccfcb7a7d41",
            "RD":   "http://catalogue.ceda.ac.uk/uuid/c732716511d3442f05cdeccbe99b8f90",
            "CUNL": "http://catalogue.ceda.ac.uk/uuid/ec1d8e1e511838b9303921986a0137de",
            "TD":   "http://catalogue.ceda.ac.uk/uuid/1bb479d3b1e38c339adb9c82c15579d8",
            "SCLE": "http://catalogue.ceda.ac.uk/uuid/1d9aa0abc4e93fca1f91c8a187d46567",
            "WD":   "http://catalogue.ceda.ac.uk/uuid/954d743d1c07d1dd034c131935db54e0",
            "WH":   "http://catalogue.ceda.ac.uk/uuid/916ac4bbc46f7685ae9a5e10451bae7c",
            "CURL": "http://catalogue.ceda.ac.uk/uuid/fe9a02b85b50d3ee1d0b7366355bb9d8"
            }

        if obs_table not in midas_table_to_moles_dict:
            return

        with open(doc_links_file, "w") as fout:
            fout.write("Link to documentation: {}#tab_linked_docs\n".format(midas_table_to_moles_dict[obs_table]))

        self.response.outputs['doc_links_file'].file = doc_links_file

    def _get_counties(self, inputs):
        "Returns a list of UK counties as specified in args dictionary."
        return inputs["counties"]

