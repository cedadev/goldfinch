import os
import pytest
from pathlib import Path

from midas_extract.testing import get_file_path

opj = os.path.join
home = Path.home()

mini_ceda_archive = opj(home.as_posix(), '.mini_ceda_archive', 'archive')
midas_data_dir = opj('badc', 'ukmo-midas', 'data')
midas_metadata_dir = opj('badc', 'ukmo-midas', 'metadata')

metadata_files = [
    opj('GEAR', 'GEAR.DATA'),
    opj('SRCC', 'SRCC.DATA'),
    opj('SRCE', 'SRCE.DATA.COMMAS_REMOVED'),
    opj('table_structures', 'GEOGRAPHIC_AREA.txt'),
    opj('table_structures', 'SCTB.txt'),
    opj('table_structures', 'SRTB.txt'),
    opj('table_structures', 'TDTB.txt')
]


@pytest.fixture
def midas_data():
    resp = []

    dr = opj(midas_data_dir, 'TD', 'yearly_files')
    years = [2017, 2018, 2019]

    for year in years:
        fpath = opj(dr, f'midas_tmpdrnl_{year}01-{year}12.txt')
        resp.append(get_file_path(fpath))

    os.environ['MIDAS_DATA_DIR'] = opj(mini_ceda_archive,
                                       midas_data_dir)
    return resp


@pytest.fixture
def midas_metadata():
    resp = []

    for mfile in metadata_files:
        mpath = opj(midas_metadata_dir, mfile)
        resp.append(get_file_path(mpath))

    os.environ['MIDAS_METADATA_DIR'] = opj(mini_ceda_archive,
                                           midas_metadata_dir)
    return resp


@pytest.fixture
def size_limits():
    os.environ['MIDAS_TEST_REQUEST_SIZE_LIMIT'] = '5000000' #5mb
    os.environ['MIDAS_TEST_TOTAL_STATIONS'] = '100'
