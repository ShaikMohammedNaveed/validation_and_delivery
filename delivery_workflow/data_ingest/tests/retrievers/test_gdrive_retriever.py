from datetime import datetime

import pytest
from pytz import timezone

from data_ingest.src.gdrive_utils.auth import build_services
from data_ingest.src.input_connectors.retrievers.gdrive_retriever import (
    DownloadStatus,
    GDriveFile,
    GDriveRetriever,
    RevisionInstructionByRevId,
    RevisionInstructionByTS,
    RevisionSelectionByRevId,
    RevisionSelectionByTS,
)


@pytest.fixture
def setup_retriever():
    return GDriveRetriever(["id1", "id2", "id3"])


def test_parse_uri_to_ids_simple(setup_retriever):
    expected_output = {"id1": "id1", "id2": "id2", "id3": "id3"}
    assert (
        setup_retriever.parse_uri_to_ids(list(expected_output.keys()))
        == expected_output
    )


def test_parse_uri_to_ids_complex(setup_retriever):
    expected_output = {
        "https://id1": None,
        "https://colab.research.google.com/drive/1QUYpxUDB8Zx9whWEgeSsozus_H_X1CL5": "1QUYpxUDB8Zx9whWEgeSsozus_H_X1CL5",
        "https://colab.research.google.com/drive/1a0XS1q3IKk1GGWvMoItNinJ1O_RuOOOa#revisionId=0BzvC7kiIr38UdDNZRnlqcHFiVW9raUNnbEtER010RnBTS0MwPQ": "1a0XS1q3IKk1GGWvMoItNinJ1O_RuOOOa",
        "https://drive.google.com/file/d/1-0d0VwDcd4tArqiXmhx3CeXPwW_B/view?usp=drive_link": None,
        "https://colab.research.google.com/drive/1a0XS1q3IKk1GGWvMoItNinJ1O_RuOOOa#scrollTo=GHbwDLfROGjg": "1a0XS1q3IKk1GGWvMoItNinJ1O_RuOOOa",
    }
    assert (
        setup_retriever.parse_uri_to_ids(list(expected_output.keys()))
        == expected_output
    )


def test_parse_uri_to_ids_mixed(setup_retriever):
    expected_output = {
        "id1": "id1",
        "id2": "id2",
        "id3": "id3",
        "https://colab.research.google.com/drive/1QUYpxUDB8Zx9whWEgeSsozus_H_X1CL5": "1QUYpxUDB8Zx9whWEgeSsozus_H_X1CL5",
        "https://id1": None,
    }
    assert (
        setup_retriever.parse_uri_to_ids(list(expected_output.keys()))
        == expected_output
    )


@pytest.fixture
def gdrive_real_retriever():
    return GDriveRetriever(
        [
            "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#scrollTo=JxK_wODyVnso",
            "https://colab.research.google.com/drive/1ScxD543LMipK62SEBIqsJNIqvv5mvi76#revisionId=0BzvC7kiIr38UV1lUQXFMa1pGVHdUYnZiR21HUmtNQkdHUmZrPQ",
        ]
    )


def test_ensure_gdrive_files_initialized(gdrive_real_retriever):
    expected_files = [
        GDriveFile(
            file_id="1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
            content=None,
            original_file_uri="https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#scrollTo=JxK_wODyVnso",
            revision_id=None,
            revision_timestamp=None,
            status=DownloadStatus.OK,
        ),
        GDriveFile(
            file_id="1ScxD543LMipK62SEBIqsJNIqvv5mvi76",
            content=None,
            original_file_uri="https://colab.research.google.com/drive/1ScxD543LMipK62SEBIqsJNIqvv5mvi76#revisionId=0BzvC7kiIr38UV1lUQXFMa1pGVHdUYnZiR21HUmtNQkdHUmZrPQ",
            revision_id=None,
            revision_timestamp=None,
            status=DownloadStatus.OK,
        ),
    ]
    assert gdrive_real_retriever.gdrive_files == expected_files


def test_retriever_retrieve_not_none():
    uris = [
        "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#scrollTo=JxK_wODyVnso",
        "https://colab.research.google.com/drive/1ScxD543LMipK62SEBIqsJNIqvv5mvi76#scrollTo=iy5D2llaVz9Z",
        "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#scrollTo=JxK_wODyVnso",
        "https://colab.research.google.com/drive/1ScxD543LMipK62SEBIqsJNIqvv5mvi76#scrollTo=iy5D2llaVz9Z",
        "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2#scrollTo=JxK_wODyVnso",
        "https://colab.research.google.com/drive/1ScxD543LMipK62SEBIqsJNIqvv5mvi76#scrollTo=iy5D2llaVz9Z",
    ]
    retriever = GDriveRetriever(uris)
    gdrive_files = retriever.retrieve()
    for gf in gdrive_files:
        assert gf.content is not None


@pytest.fixture
def dummy_revisions():
    # made disordered manually
    revisions_dummy = [
        {
            "id": "0BzvC7kiIr38UcmkxNVBheG5aSFI5WHBNdWFLNlovdVJJc0FFPQ",
            "modifiedTime": "2024-01-26T10:35:13.750Z",
        },
        {
            "id": "0BzvC7kiIr38UV1lUQXFMa1pGVHdUYnZiR21HUmtNQkdHUmZrPQ",
            "modifiedTime": "2024-01-26T10:35:40.684Z",
        },
        {
            "id": "0BzvC7kiIr38UMGVwRHA5cXpuL2lTazgySDBzRU44cERKSTBzPQ",
            "modifiedTime": "2024-01-26T10:35:33.670Z",
        },
    ]
    return revisions_dummy


def test_select_revision_equal(gdrive_real_retriever, dummy_revisions):
    instruction = RevisionInstructionByRevId(
        how=RevisionSelectionByRevId.EQUAL,
        revision_id="0BzvC7kiIr38UMGVwRHA5cXpuL2lTazgySDBzRU44cERKSTBzPQ",
    )
    expected_revision = {
        "id": "0BzvC7kiIr38UMGVwRHA5cXpuL2lTazgySDBzRU44cERKSTBzPQ",
        "modifiedTime": "2024-01-26T10:35:33.670Z",
    }
    assert instruction.select_revision(dummy_revisions) == expected_revision


def test_select_revision_not_found_returns_latest(
    gdrive_real_retriever, dummy_revisions
):
    instruction = RevisionInstructionByRevId(
        how=RevisionSelectionByRevId.EQUAL,
        revision_id="NOT_FOUND",
    )
    expected_revision = {
        "id": "0BzvC7kiIr38UV1lUQXFMa1pGVHdUYnZiR21HUmtNQkdHUmZrPQ",
        "modifiedTime": "2024-01-26T10:35:40.684Z",
        "failed_to_satisfy": True,
    }
    assert instruction.select_revision(dummy_revisions) == expected_revision


def test_select_revision_latest_not_eq(gdrive_real_retriever, dummy_revisions):
    instruction = RevisionInstructionByRevId(
        how=RevisionSelectionByRevId.LATEST_NOT_EQ,
        revision_id="0BzvC7kiIr38UMGVwRHA5cXpuL2lTazgySDBzRU44cERKSTBzPQ",
    )
    expected_revision = {
        "id": "0BzvC7kiIr38UV1lUQXFMa1pGVHdUYnZiR21HUmtNQkdHUmZrPQ",
        "modifiedTime": "2024-01-26T10:35:40.684Z",
    }
    assert instruction.select_revision(dummy_revisions) == expected_revision


def test_select_revision_latest(gdrive_real_retriever, dummy_revisions):
    instruction = RevisionInstructionByTS(
        how=RevisionSelectionByTS.LATEST,
    )

    instruction.utc_timestamp = None

    assert instruction.select_revision(dummy_revisions) == {
        "id": "0BzvC7kiIr38UV1lUQXFMa1pGVHdUYnZiR21HUmtNQkdHUmZrPQ",
        "modifiedTime": "2024-01-26T10:35:40.684Z",
    }


def test_select_revision_before(gdrive_real_retriever, dummy_revisions):
    instruction = RevisionInstructionByTS(
        how=RevisionSelectionByTS.BEFORE_OR_EQ,
        utc_timestamp=datetime.utcnow().replace(tzinfo=timezone("UTC")),
    )

    instruction.utc_timestamp = datetime.strptime(
        "2024-01-26T10:35:40.684Z", "%Y-%m-%dT%H:%M:%S.%fZ"
    ).replace(tzinfo=timezone("UTC"))
    assert instruction.select_revision(dummy_revisions) == {
        "id": "0BzvC7kiIr38UV1lUQXFMa1pGVHdUYnZiR21HUmtNQkdHUmZrPQ",
        "modifiedTime": "2024-01-26T10:35:40.684Z",
    }
    instruction.utc_timestamp = datetime.strptime(
        "2024-01-26T10:35:39.684Z", "%Y-%m-%dT%H:%M:%S.%fZ"
    ).replace(tzinfo=timezone("UTC"))
    assert instruction.select_revision(dummy_revisions) == {
        "id": "0BzvC7kiIr38UMGVwRHA5cXpuL2lTazgySDBzRU44cERKSTBzPQ",
        "modifiedTime": "2024-01-26T10:35:33.670Z",
    }


def test_select_revision_after(gdrive_real_retriever, dummy_revisions):
    instruction = RevisionInstructionByTS(
        how=RevisionSelectionByTS.AFTER_OR_EQ,
        utc_timestamp=datetime.utcnow().replace(tzinfo=timezone("UTC")),
    )

    assert instruction.select_revision(dummy_revisions) == {
        "id": "0BzvC7kiIr38UV1lUQXFMa1pGVHdUYnZiR21HUmtNQkdHUmZrPQ",
        "modifiedTime": "2024-01-26T10:35:40.684Z",
        "failed_to_satisfy": True,
    }
    instruction.utc_timestamp = datetime.strptime(
        "2024-01-26T10:35:40.684Z", "%Y-%m-%dT%H:%M:%S.%fZ"
    ).replace(tzinfo=timezone("UTC"))
    assert instruction.select_revision(dummy_revisions) == {
        "id": "0BzvC7kiIr38UV1lUQXFMa1pGVHdUYnZiR21HUmtNQkdHUmZrPQ",
        "modifiedTime": "2024-01-26T10:35:40.684Z",
    }
    instruction.utc_timestamp = datetime.strptime(
        "2024-01-26T10:35:00.684Z", "%Y-%m-%dT%H:%M:%S.%fZ"
    ).replace(tzinfo=timezone("UTC"))
    assert instruction.select_revision(dummy_revisions) == {
        "id": "0BzvC7kiIr38UcmkxNVBheG5aSFI5WHBNdWFLNlovdVJJc0FFPQ",
        "modifiedTime": "2024-01-26T10:35:13.750Z",
    }


def test_retrieve_latest_revision():
    instruction = RevisionInstructionByTS(how=RevisionSelectionByTS.LATEST)
    gdrive_real_retriever = GDriveRetriever(
        ["https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2"],
        revision_instructions_map={
            "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2": instruction
        },
    )
    result = gdrive_real_retriever.retrieve()
    expected_output = [
        GDriveFile(
            file_id="1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
            content='{"nbformat":4,"nbformat_minor":0,"metadata":{"colab":{"provenance":[],"authorship_tag":"ABX9TyPSttmLyJmfOewfLn0yPUFA"},"kernelspec":{"name":"python3","display_name":"Python 3"},"language_info":{"name":"python"}},"cells":[{"cell_type":"code","execution_count":null,"metadata":{"id":"ET0t25E0Vj6N"},"outputs":[],"source":["test change"]},{"cell_type":"code","source":["test change with revision"],"metadata":{"id":"nMENaac-Vlov"},"execution_count":null,"outputs":[]},{"cell_type":"code","source":["test change without pin"],"metadata":{"id":"JxK_wODyVnso"},"execution_count":null,"outputs":[]}]}',
            original_file_uri="https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
            revision_id="0BzvC7kiIr38UQzNZWFl5SFZBeG1LRWxtSTRqZVhTSkswNm9zPQ",
            revision_timestamp="2024-01-26T10:36:00.124Z",
            status=DownloadStatus.OK,
        )
    ]
    assert result == expected_output, f"Expected {expected_output}, but got {result}"


def test_retrieve_revision_before_or_equal_to_timestamp():
    instruction = RevisionInstructionByTS(how=RevisionSelectionByTS.BEFORE_OR_EQ)
    instruction.utc_timestamp = datetime.strptime(
        "2024-01-26T10:34:50.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"
    ).replace(tzinfo=timezone("UTC"))
    gdrive_real_retriever = GDriveRetriever(
        ["https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2"],
        revision_instructions_map={
            "https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2": instruction
        },
    )
    result = gdrive_real_retriever.retrieve()
    expected_output = [
        GDriveFile(
            file_id="1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
            content='{"nbformat":4,"nbformat_minor":0,"metadata":{"colab":{"provenance":[],"authorship_tag":"ABX9TyNjaTJtVeMvPWXEsSw5xnJq"},"kernelspec":{"name":"python3","display_name":"Python 3"},"language_info":{"name":"python"}},"cells":[{"cell_type":"code","execution_count":null,"metadata":{"id":"ET0t25E0Vj6N"},"outputs":[],"source":["test change"]},{"cell_type":"code","source":["test change with revision"],"metadata":{"id":"nMENaac-Vlov"},"execution_count":null,"outputs":[]}]}',
            original_file_uri="https://colab.research.google.com/drive/1Q0h-09ZZDuntxBcIAhgNE-u9jb-UjkV2",
            revision_id="0BzvC7kiIr38UQ2Q2OXFzQk1VVk9BYnU1TVh0eWJuSThBYVhFPQ",
            revision_timestamp="2024-01-26T10:34:42.808Z",
            status=DownloadStatus.OK,
        )
    ]
    assert result == expected_output, f"Expected {expected_output}, but got {result}"
