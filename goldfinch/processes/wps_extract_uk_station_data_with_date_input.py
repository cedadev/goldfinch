import os
import os.path

from pywps import Process, LiteralInput, ComplexOutput, BoundingBoxInput, FORMATS
from pywps.app.Common import Metadata

from midas_extract.vocabs import TABLE_NAMES, MIDAS_CATALOGUE_DICT, UK_COUNTIES

from goldfinch.util import (get_station_list, validate_inputs, locate_process_dir,
                            filter_obs_by_time_chunk, read_from_file,
                            WEATHER_STATIONS_FILE_NAME, get_valid_date_range)

from goldfinch.constraints import check_request_size

import logging
LOGGER = logging.getLogger("PYWPS")


class ExtractUKStationDataWithDateInput(Process):
    """A process extracting UK station data."""
    def __init__(self):
        inputs = [
            LiteralInput('obs_table', 'Obervation Table Name',
                         abstract='The name of the database table used in the MIDAS database to identify'
                                  ' a particular selection of weather observations.',
                         data_type='string',
                         allowed_values=TABLE_NAMES,
                         min_occurs=1,
                         max_occurs=1),
            LiteralInput('TemporalRange', 'Date Range',
                         abstract='The date range to search for station data, formatted as: '
                                  'YYYY-MM-DD/YYYY-MM-DD',
                         data_type='string',
                         default=get_valid_date_range(),
                         min_occurs=0,
                         max_occurs=1),
            BoundingBoxInput('bbox', 'Bounding Box',
                         abstract='The spatial bounding box within which to search for station data.'
                                  ' This input will be ignored if counties are provided.',
                         crss=['-12.0, 49.0, 3.0, 61.0,epsg:4326'],
                         min_occurs=0,
                         max_occurs=1),
            LiteralInput('counties', 'Counties',
                         abstract='A list of counties within which to search for weather stations.',
                         data_type='string',
                         allowed_values=UK_COUNTIES,
                         min_occurs=0,
                         max_occurs=len(UK_COUNTIES)),
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

        super(ExtractUKStationDataWithDateInput, self).__init__(
            self._handler,
            identifier='ExtractUKStationDataWithDateInput',
            title='Extract UK Station Data With Date Input',
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
                Metadata('CEDA WPS UI', 'https://ceda-wps-ui.ceda.ac.uk'),
                Metadata('CEDA WPS', 'https://ceda-wps.ceda.ac.uk'),
                Metadata('Disclaimer', 'https://help.ceda.ac.uk/article/4642-disclaimer')
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
                          'chunk_rule': None, 'delimiter': 'comma'}

        inputs = validate_inputs(request.inputs, defaults=input_defaults,
                                 required=['obs_table', 'TemporalRange'])

        # Resolve the list of stations
        stations_file = WEATHER_STATIONS_FILE_NAME
        stations_file_path = os.path.join(self.workdir, stations_file)

        station_list = self._resolve_station_list(inputs, stations_file_path)

        # Write stations file
        self._write_stations_file(stations_file_path, station_list)

        # Estimate we are 5% of the way through
        self.response.update_status('Extracted station ID list.', 5)

        # Check request size against limits
        check_request_size(station_list, inputs)

        # Define data file base
        prefix = 'station_data'
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

        # Register output file(s)
# TODO: register more than one if multiple
        output_path = output_paths[0]
        LOGGER.info('Written output file: {}'.format(output_path))
        self.response.outputs['output'].file = output_path

        # Write docs links to output file
        doc_links_file = os.path.join(self.workdir, 'doc_links.txt')
        self._write_doc_links_file(doc_links_file, obs_table)

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

    def _write_doc_links_file(self, doc_links_file, obs_table):
        "Write documentation links file."

        if obs_table not in MIDAS_CATALOGUE_DICT:
            return

        with open(doc_links_file, "w") as fout:
            fout.write("Link to documentation: {}#tab_linked_docs\n".format(MIDAS_CATALOGUE_DICT[obs_table]))

        self.response.outputs['doc_links_file'].file = doc_links_file

    def _get_counties(self, inputs):
        "Returns a list of UK counties as specified in args dictionary."
        return inputs["counties"]
