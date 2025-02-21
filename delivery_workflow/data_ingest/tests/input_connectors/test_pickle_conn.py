import os
import pickle
from unittest.mock import patch

import pytest

from common.constants import DATA_DIR
from data_ingest.src.input_connectors.base import InputBatch
from data_ingest.src.input_connectors.pickle_conn import PickleConnector


@pytest.fixture
def pickle_connector():
    relative_file_path = "test_notebooks_backup.pkl"
    return PickleConnector(relative_file_path=relative_file_path)


def test_load_data_success(pickle_connector):
    pickle_connector.load_data()
    input_batch = pickle_connector.get_data()
    assert isinstance(
        input_batch, InputBatch
    ), "Loaded data should be an instance of InputBatch"
    assert (
        input_batch.items is not None
    ), "InputBatch items should not be None after loading data"


def test_load_data_file_not_found():
    with pytest.raises(FileNotFoundError):
        connector = PickleConnector(relative_file_path="non_existent_file.pkl")
        connector.load_data()


def test_save_data(pickle_connector):
    # Load data to ensure there is data to save
    pickle_connector.load_data()
    save_path = pickle_connector.save_data(
        relative_save_path="26-01-2024/test_save_output"
    )
    assert os.path.isfile(save_path), "The data should be saved to a file"

    # Load the saved data and compare
    with open(save_path, "rb") as file:
        saved_data = pickle.load(file)
    loaded_data = pickle_connector.get_data()
    assert (
        saved_data == loaded_data
    ), "The saved data should be the same as the loaded data"
