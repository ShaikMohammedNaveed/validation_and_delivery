from datetime import datetime, timezone

import pytest

from data_ingest.src.input_connectors.base import (
    InputBatch,
    InputItem,
    InputItemMetadata,
    InputItemStatus,
)
from data_ingest.src.input_connectors.gdrive import GDriveConnector
from data_ingest.src.input_connectors.retrievers.gdrive_retriever import (
    DownloadStatus,
    GDriveFile,
    RevisionInstructionByRevId,
    RevisionInstructionByTS,
    RevisionSelectionByRevId,
    RevisionSelectionByTS,
)


def test_gdrive_files_uris():
    gdrive_conn = GDriveConnector(["id1", "id2"])
    assert gdrive_conn._gdrive_files_uris == ["id1", "id2"]
    assert gdrive_conn._per_item_metadata is None


def test_gdrive_connector_file_item_field_key_error():
    with pytest.raises(KeyError):
        GDriveConnector([{"f": "id1"}, {"f": "id2"}])


def test_gdrive_file_items():
    # I know that testing _ fields is not good. At least something.
    gdrive_conn = GDriveConnector(
        [{"file_uri": "id1"}, {"file_uri": "id2"}],
    )
    assert gdrive_conn._gdrive_files_uris == ["id1", "id2"]
    assert gdrive_conn._per_item_metadata == {"id1": {}, "id2": {}}


def test_gdrive_file_items_with_prev_revisions():
    # I know that testing _ fields is not good. At least something.
    gdrive_conn = GDriveConnector(
        [
            {
                "file_uri": "https://colab.research.google.com/drive/id1#revisionId=rev_id1",
                "meta_field": 11,
            },
            {
                "file_uri": "https://colab.research.google.com/drive/id2#revisionId=rev_id2",
                "meta_field": 22,
            },
            {
                "file_uri": "https://colab.research.google.com/drive/id3",
                "meta_field": 33,
            },
        ],
    )
    assert gdrive_conn._gdrive_files_uris == [
        "https://colab.research.google.com/drive/id1#revisionId=rev_id1",
        "https://colab.research.google.com/drive/id2#revisionId=rev_id2",
        "https://colab.research.google.com/drive/id3",
    ]
    assert gdrive_conn._per_item_metadata == {
        "https://colab.research.google.com/drive/id1#revisionId=rev_id1": {
            "meta_field": 11
        },
        "https://colab.research.google.com/drive/id2#revisionId=rev_id2": {
            "meta_field": 22
        },
        "https://colab.research.google.com/drive/id3": {"meta_field": 33},
    }


def test_gdrive_gfiles_to_input_items():
    # I know that testing _ fields is not good. At least something.
    gdrive_conn = GDriveConnector(
        [
            {
                "file_uri": "https://colab.research.google.com/drive/id1#revisionId=rev_id1",
                "meta_field": 11,
            },
            {
                "file_uri": "https://colab.research.google.com/drive/id2#revisionId=rev_id2",
                "meta_field": 22,
            },
            {
                "file_uri": "https://colab.research.google.com/drive/id3",
                "meta_field": 33,
            },
        ],
        common_metadata={"some": "info"},
    )
    gdrive_files = [
        GDriveFile(
            file_id="id1",
            content="1",
            original_file_uri="https://colab.research.google.com/drive/id1#revisionId=rev_id1",
            revision_id="new_rev1",
            revision_timestamp="some_time1",
            status=DownloadStatus.OK,
        ),
        GDriveFile(
            file_id="id2",
            content="2",
            original_file_uri="https://colab.research.google.com/drive/id2#revisionId=rev_id2",
            revision_id="new_rev2",
            revision_timestamp="some_time2",
            status=DownloadStatus.SKIPPED,
        ),
        GDriveFile(
            file_id="id3",
            content="3",
            original_file_uri="https://colab.research.google.com/drive/id3",
            revision_id="new_rev3",
            revision_timestamp="some_time3",
            status=DownloadStatus.ERROR,
        ),
    ]
    items = gdrive_conn._convert_gdrive_files_to_items(gdrive_files)

    expected_items = [
        InputItem(
            content="1",
            metadata=InputItemMetadata(
                status=InputItemStatus.OK,
                data={
                    "original_uri": "https://colab.research.google.com/drive/id1#revisionId=rev_id1",
                    "file_id": "id1",
                    "requested_revision_id": "new_rev1",
                    "requested_revision_ts": "some_time1",
                    "meta_field": 11,
                },
            ),
        ),
        InputItem(
            content="2",
            metadata=InputItemMetadata(
                status=InputItemStatus.SKIPPED,
                data={
                    "original_uri": "https://colab.research.google.com/drive/id2#revisionId=rev_id2",
                    "file_id": "id2",
                    "requested_revision_id": "new_rev2",
                    "requested_revision_ts": "some_time2",
                    "meta_field": 22,
                },
            ),
        ),
        InputItem(
            content="3",
            metadata=InputItemMetadata(
                status=InputItemStatus.ERROR,
                data={
                    "original_uri": "https://colab.research.google.com/drive/id3",
                    "file_id": "id3",
                    "requested_revision_id": "new_rev3",
                    "requested_revision_ts": "some_time3",
                    "meta_field": 33,
                },
            ),
        ),
    ]
    assert items == expected_items


def test_gdrive_connector_actual_download_no_prev_rev_no_instruction():
    conn = GDriveConnector(
        [
            {
                "file_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                "file1_metadata_field": "file_metadata",
            }
        ],
        {
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
        revision_instructions_map=None,
    )

    conn.load_data()
    data = conn.get_data()
    expected_data = InputBatch(
        items=[
            InputItem(
                content='{"nbformat":4,"nbformat_minor":0,"metadata":{"colab":{"provenance":[],"authorship_tag":"ABX9TyPSttmLyJmfOewfLn0yPUFA"},"kernelspec":{"name":"python3","display_name":"Python 3"},"language_info":{"name":"python"}},"cells":[{"cell_type":"code","execution_count":null,"metadata":{"id":"ET0t25E0Vj6N"},"outputs":[],"source":["test change"]},{"cell_type":"code","source":["test change with revision"],"metadata":{"id":"nMENaac-Vlov"},"execution_count":null,"outputs":[]},{"cell_type":"code","source":["test change without pin"],"metadata":{"id":"JxK_wODyVnso"},"execution_count":null,"outputs":[]}]}',
                metadata=InputItemMetadata(
                    status=InputItemStatus.OK,
                    data={
                        "original_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "file_id": "1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "requested_revision_id": "0BzvC7kiIr38UQzNZWFl5SFZBeG1LRWxtSTRqZVhTSkswNm9zPQ",
                        "requested_revision_ts": "2024-01-26T10:36:00.124Z",
                        "file1_metadata_field": "file_metadata",
                    },
                ),
            )
        ],
        metadata={
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
    )
    assert data == expected_data, f"Expected data to be {expected_data}, but got {data}"


def test_gdrive_connector_actual_download_prev_rev_no_instruction():
    conn = GDriveConnector(
        [
            {
                "file_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#revisionId=0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                "file1_metadata_field": "file_metadata",
            }
        ],
        {
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
        revision_instructions_map=None,
    )
    conn.load_data()
    data = conn.get_data()
    expected_data = InputBatch(
        items=[
            InputItem(
                content='{"nbformat":4,"nbformat_minor":0,"metadata":{"colab":{"provenance":[],"authorship_tag":"ABX9TyPSttmLyJmfOewfLn0yPUFA"},"kernelspec":{"name":"python3","display_name":"Python 3"},"language_info":{"name":"python"}},"cells":[{"cell_type":"code","execution_count":null,"metadata":{"id":"ET0t25E0Vj6N"},"outputs":[],"source":["test change"]},{"cell_type":"code","source":["test change with revision"],"metadata":{"id":"nMENaac-Vlov"},"execution_count":null,"outputs":[]},{"cell_type":"code","source":["test change without pin"],"metadata":{"id":"JxK_wODyVnso"},"execution_count":null,"outputs":[]}]}',
                metadata=InputItemMetadata(
                    status=InputItemStatus.OK,
                    data={
                        "original_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#revisionId=0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                        "file_id": "1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "requested_revision_id": "0BzvC7kiIr38UQzNZWFl5SFZBeG1LRWxtSTRqZVhTSkswNm9zPQ",
                        "requested_revision_ts": "2024-01-26T10:36:00.124Z",
                        "file1_metadata_field": "file_metadata",
                    },
                ),
            )
        ],
        metadata={
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
    )
    assert data == expected_data, f"Expected data to be {expected_data}, but got {data}"


def test_gdrive_connector_actual_download_prev_rev_instruction_latest():
    conn = GDriveConnector(
        [
            {
                "file_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#revisionId=0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                "file1_metadata_field": "file_metadata",
            }
        ],
        {
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
        revision_instructions_map=RevisionInstructionByTS(
            how=RevisionSelectionByTS.LATEST
        ),
    )
    conn.load_data()
    data = conn.get_data()
    expected_data = InputBatch(
        items=[
            InputItem(
                content='{"nbformat":4,"nbformat_minor":0,"metadata":{"colab":{"provenance":[],"authorship_tag":"ABX9TyPSttmLyJmfOewfLn0yPUFA"},"kernelspec":{"name":"python3","display_name":"Python 3"},"language_info":{"name":"python"}},"cells":[{"cell_type":"code","execution_count":null,"metadata":{"id":"ET0t25E0Vj6N"},"outputs":[],"source":["test change"]},{"cell_type":"code","source":["test change with revision"],"metadata":{"id":"nMENaac-Vlov"},"execution_count":null,"outputs":[]},{"cell_type":"code","source":["test change without pin"],"metadata":{"id":"JxK_wODyVnso"},"execution_count":null,"outputs":[]}]}',
                metadata=InputItemMetadata(
                    status=InputItemStatus.OK,
                    data={
                        "original_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#revisionId=0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                        "file_id": "1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "requested_revision_id": "0BzvC7kiIr38UQzNZWFl5SFZBeG1LRWxtSTRqZVhTSkswNm9zPQ",
                        "requested_revision_ts": "2024-01-26T10:36:00.124Z",
                        "file1_metadata_field": "file_metadata",
                    },
                ),
            )
        ],
        metadata={
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
    )
    assert data == expected_data, f"Expected data to be {expected_data}, but got {data}"


def test_gdrive_connector_actual_download_prev_rev_instruction_latest_nq_exists():
    conn = GDriveConnector(
        [
            {
                "file_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#revisionId=0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                "file1_metadata_field": "file_metadata",
            }
        ],
        {
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
        revision_instructions_map=RevisionInstructionByRevId(
            how=RevisionSelectionByRevId.LATEST_NOT_EQ,
            revision_id="0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
        ),
    )
    conn.load_data()
    data = conn.get_data()
    expected_data = InputBatch(
        items=[
            InputItem(
                content='{"nbformat":4,"nbformat_minor":0,"metadata":{"colab":{"provenance":[],"authorship_tag":"ABX9TyPSttmLyJmfOewfLn0yPUFA"},"kernelspec":{"name":"python3","display_name":"Python 3"},"language_info":{"name":"python"}},"cells":[{"cell_type":"code","execution_count":null,"metadata":{"id":"ET0t25E0Vj6N"},"outputs":[],"source":["test change"]},{"cell_type":"code","source":["test change with revision"],"metadata":{"id":"nMENaac-Vlov"},"execution_count":null,"outputs":[]},{"cell_type":"code","source":["test change without pin"],"metadata":{"id":"JxK_wODyVnso"},"execution_count":null,"outputs":[]}]}',
                metadata=InputItemMetadata(
                    status=InputItemStatus.OK,
                    data={
                        "original_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#revisionId=0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                        "file_id": "1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "requested_revision_id": "0BzvC7kiIr38UQzNZWFl5SFZBeG1LRWxtSTRqZVhTSkswNm9zPQ",
                        "requested_revision_ts": "2024-01-26T10:36:00.124Z",
                        "file1_metadata_field": "file_metadata",
                    },
                ),
            )
        ],
        metadata={
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
    )
    assert data == expected_data, f"Expected data to be {expected_data}, but got {data}"


def test_gdrive_connector_actual_download_prev_rev_instruction_latest_nq_not_exists():
    conn = GDriveConnector(
        [
            {
                "file_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#revisionId=0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                "file1_metadata_field": "file_metadata",
            }
        ],
        {
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
        revision_instructions_map=RevisionInstructionByRevId(
            how=RevisionSelectionByRevId.LATEST_NOT_EQ,
            revision_id="0BzvC7kiIr38UQzNZWFl5SFZBeG1LRWxtSTRqZVhTSkswNm9zPQ",
        ),
    )
    conn.load_data()
    data = conn.get_data()
    expected_data = InputBatch(
        items=[
            InputItem(
                content=None,
                metadata=InputItemMetadata(
                    status=InputItemStatus.SKIPPED,
                    data={
                        "original_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#revisionId=0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                        "file_id": "1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "requested_revision_id": "0BzvC7kiIr38UQzNZWFl5SFZBeG1LRWxtSTRqZVhTSkswNm9zPQ",
                        "requested_revision_ts": "2024-01-26T10:36:00.124Z",
                        "file1_metadata_field": "file_metadata",
                    },
                ),
            )
        ],
        metadata={
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
    )
    assert data == expected_data, f"Expected data to be {expected_data}, but got {data}"


def test_gdrive_connector_actual_download_prev_rev_instruction_eq_exists():
    conn = GDriveConnector(
        [
            {
                "file_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                "file1_metadata_field": "file_metadata",
            }
        ],
        {
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
        revision_instructions_map=RevisionInstructionByRevId(
            how=RevisionSelectionByRevId.EQUAL,
            revision_id="0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
        ),
    )
    conn.load_data()
    data = conn.get_data()
    expected_data = InputBatch(
        items=[
            InputItem(
                content='{"nbformat":4,"nbformat_minor":0,"metadata":{"colab":{"provenance":[],"authorship_tag":"ABX9TyNjaTJtVeMvPWXEsSw5xnJq"},"kernelspec":{"name":"python3","display_name":"Python 3"},"language_info":{"name":"python"}},"cells":[{"cell_type":"code","execution_count":null,"metadata":{"id":"ET0t25E0Vj6N"},"outputs":[],"source":["test change"]},{"cell_type":"code","source":["test change with revision"],"metadata":{"id":"nMENaac-Vlov"},"execution_count":null,"outputs":[]}]}',
                metadata=InputItemMetadata(
                    status=InputItemStatus.OK,
                    data={
                        "original_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "file_id": "1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "requested_revision_id": "0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                        "requested_revision_ts": "2024-01-26T10:34:42.808Z",
                        "file1_metadata_field": "file_metadata",
                    },
                ),
            )
        ],
        metadata={
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
    )
    assert data == expected_data, f"Expected data to be {expected_data}, but got {data}"


def test_gdrive_connector_actual_download_prev_rev_instruction_before_ts():
    conn = GDriveConnector(
        [
            {
                "file_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                "file1_metadata_field": "file_metadata",
            }
        ],
        {
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
        revision_instructions_map=RevisionInstructionByTS(
            how=RevisionSelectionByTS.BEFORE_OR_EQ,
            utc_timestamp=datetime.strptime(
                "2024-01-26T10:34:43.808Z", "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=timezone.utc),
        ),
    )
    conn.load_data()
    data = conn.get_data()
    expected_data = InputBatch(
        items=[
            InputItem(
                content='{"nbformat":4,"nbformat_minor":0,"metadata":{"colab":{"provenance":[],"authorship_tag":"ABX9TyNjaTJtVeMvPWXEsSw5xnJq"},"kernelspec":{"name":"python3","display_name":"Python 3"},"language_info":{"name":"python"}},"cells":[{"cell_type":"code","execution_count":null,"metadata":{"id":"ET0t25E0Vj6N"},"outputs":[],"source":["test change"]},{"cell_type":"code","source":["test change with revision"],"metadata":{"id":"nMENaac-Vlov"},"execution_count":null,"outputs":[]}]}',
                metadata=InputItemMetadata(
                    status=InputItemStatus.OK,
                    data={
                        "original_uri": "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "file_id": "1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
                        "requested_revision_id": "0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
                        "requested_revision_ts": "2024-01-26T10:34:42.808Z",
                        "file1_metadata_field": "file_metadata",
                    },
                ),
            )
        ],
        metadata={
            "common_metadata_field1": "batch metadata data1",
            "common_metadata_field2": "batch metadata data2",
        },
    )
    assert data == expected_data, f"Expected data to be {expected_data}, but got {data}"
