import os
opj = os.path.join
import pytest
from pathlib import Path

from git import Repo

home = Path.home()

TEST_DATA_REPO_URL = "https://github.com/cedadev/mini-ceda-archive"
TEST_DATA_REPO_BRANCH = "main"

MINI_CEDA_ARCHIVE_DIR = opj(home.as_posix(), '.mini-ceda-archive')
mini_ceda_archive = opj (MINI_CEDA_ARCHIVE_DIR, TEST_DATA_REPO_BRANCH, 'archive')

midas_data_dir = opj('badc', 'ukmo-midas', 'data')
midas_metadata_dir = opj('badc', 'ukmo-midas', 'metadata')


@pytest.fixture
def load_test_data():
    """
    This fixture ensures that the required test data repository
    has been cloned to the cache directory within the home directory.
    """
    branch = TEST_DATA_REPO_BRANCH
    target = os.path.join(MINI_CEDA_ARCHIVE_DIR, branch)

    if not os.path.isdir(MINI_CEDA_ARCHIVE_DIR):
        os.makedirs(MINI_CEDA_ARCHIVE_DIR)

    if not os.path.isdir(target):
        repo = Repo.clone_from(TEST_DATA_REPO_URL, target)
        repo.git.checkout(branch)

    if not os.environ.get("MIDAS_AUTO_UPDATE_TEST_DATA", True) == "FALSE":
        repo = Repo(target)
        repo.git.checkout(branch)
        repo.remotes[0].pull()
 
    # Set the environment variables so the correct test data paths are used
    os.environ['MIDAS_DATA_DIR'] = opj(mini_ceda_archive,
                                           midas_data_dir)
    os.environ['MIDAS_METADATA_DIR'] = opj(mini_ceda_archive,
                                               midas_metadata_dir)


@pytest.fixture
def size_limits():
    os.environ['MIDAS_TEST_REQUEST_SIZE_LIMIT'] = '5000000' #5mb
    os.environ['MIDAS_TEST_TOTAL_STATIONS'] = '100'

