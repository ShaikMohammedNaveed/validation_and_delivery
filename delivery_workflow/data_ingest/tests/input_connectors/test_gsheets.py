from datetime import timedelta

from data_ingest.src.input_connectors.gsheets import GSheetsConnector


def no_revision_instructions_or_filtering_run():
    sheet_id = "1qBU7Kvuuij2fxbqPxebReKMxWgIBmOIE5Gi4ZuX0j_4"
    sheet_names = ["Conversations_Batch_1"]
    conn = GSheetsConnector(
        sheet_id=sheet_id,
        sheet_names=sheet_names,
        gdrive_file_link_column_name="task_link",
        common_metadata={"gsheets_md_field": "main run data"},
        relative_save_path="convos1_test",
    )
    conn.load_data()
    conn.save_data()


def no_revision_instructions_with_column_filtering_run():
    sheet_id = "1qBU7Kvuuij2fxbqPxebReKMxWgIBmOIE5Gi4ZuX0j_4"
    sheet_names = ["Conversations_Batch_6"]
    conn = GSheetsConnector(
        sheet_id=sheet_id,
        sheet_names=sheet_names,
        gdrive_file_link_column_name="task_link",
        common_metadata={"gsheets_md_field": "main run data"},
        relative_save_path="convos6_test_with_done",
        column_filter_map={"completion_status": "Done"},
    )
    conn.load_data()
    conn.save_data()


def timestamp_revision_instructions_with_column_filtering_run():
    sheet_id = "1qBU7Kvuuij2fxbqPxebReKMxWgIBmOIE5Gi4ZuX0j_4"
    sheet_names = ["Reviews"]
    conn = GSheetsConnector(
        sheet_id=sheet_id,
        sheet_names=sheet_names,
        gdrive_file_link_column_name="Task Link [Google Colab]",
        common_metadata={"gsheets_md_field": "main run data"},
        relative_save_path="reviews_with_ts_and_code_2",
        column_filter_map={"Code Quality": "2"},
        find_revision_by_timestamp_column_name="Timestamp",
        timestamp_column_timezone_delta=timedelta(hours=2),
    )
    conn.load_data()
    conn.save_data()


timestamp_revision_instructions_with_column_filtering_run()
