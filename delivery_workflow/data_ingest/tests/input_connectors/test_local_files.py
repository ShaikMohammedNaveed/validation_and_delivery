import os
import pickle
from unittest.mock import patch

import pytest

from common.constants import DATA_DIR
from data_ingest.src.input_connectors.base import InputBatch, InputItemStatus
from data_ingest.src.input_connectors.local_files import LocalFilesConnector


@pytest.fixture
def local_files_connector():
    relative_save_path = "26-01-2024/raw_input_batch"
    return LocalFilesConnector(file_list=[], relative_save_path=relative_save_path)


@pytest.fixture
def local_files_with_notebooks():
    notebook_files = [
        f
        for f in os.listdir(os.path.join(DATA_DIR, "tests/raw/"))
        if f.endswith(".ipynb")
    ]
    file_list = [
        {"file_path": os.path.join(DATA_DIR, "tests/raw/", filename)}
        for filename in notebook_files
    ]
    return LocalFilesConnector(
        file_list=file_list, relative_save_path="test_notebooks_backup"
    )


def test_init_connector_with_non_existing_file():
    file_list = [{"file_path": "non_existing_file.ipynb"}]
    local_files_connector = LocalFilesConnector(file_list=file_list)
    local_files_connector.load_data()
    input_batch = local_files_connector.get_data()
    assert (
        input_batch.items[0].metadata.status == InputItemStatus.ERROR
    ), "The status of the first item should be ERROR"
    assert (
        input_batch.items[0].content is None
    ), "The content of the first item should be None"


def test_load_data_full(local_files_with_notebooks):
    local_files_with_notebooks.load_data()
    local_files_with_notebooks.save_data()
    input_batch = local_files_with_notebooks.get_data()
    assert (
        input_batch.items[0].metadata.status == InputItemStatus.OK
    ), "The status of the first item should be OK"
    assert (
        "file_path" in input_batch.items[0].metadata.data
    ), "The metadata should contain the 'file_path' key"


def test_common_metadata_initialization():
    common_metadata = {"source": "notebooks"}
    file_list = [{"file_path": "dummy_path"}]
    local_files_connector = LocalFilesConnector(
        file_list=file_list, common_metadata=common_metadata
    )
    assert (
        local_files_connector._input_batch.metadata == common_metadata
    ), "The common metadata should be set correctly during initialization"


def test_load_data(local_files_connector):
    local_files_connector.load_data()
    assert local_files_connector.get_data() == InputBatch(items=[], metadata=None)


def test_save_data(local_files_connector):
    local_files_connector.load_data()
    print(local_files_connector.save_data())
    with open(
        os.path.join(
            DATA_DIR, local_files_connector.params["relative_save_path"] + ".pkl"
        ),
        "rb",
    ) as file:
        data = pickle.load(file)
    assert data == local_files_connector.get_data()
