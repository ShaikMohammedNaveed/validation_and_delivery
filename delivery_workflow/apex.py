from collections import defaultdict
import datetime
from delivery_workflow.data_ingest.src.input_connectors import GSheetsConnector
from delivery_workflow.parsers.src.apex_parser import process_notebook_batch_concurrently
import json
from delivery_workflow.config import settings
from delivery_workflow.parsers.src.utils import split_jsonl_to_json, zip_folder_with_timestamp, update_google_sheet, split_jsonl_to_sheet
from delivery_workflow.data_ingest.src.gdrive_utils import upload_folder, create_or_get_drive_folder
from delivery_workflow.validation.apex_validation import validate_notebooks_in_input_batch
from delivery_workflow.sheet_util import get_colab_links_from_folder, write_links_to_sheet, get_json_files_from_folder, write_files_to_sheet, copy_google_sheet, update_google_sheet_from_json, copy_specific_tabs_google_sheet, copy_google_sheet_to_drive
from delivery_workflow.notify import send_email_notification, send_lwc_issue_email_notification, send_email_notification_apex, send_email_notification_json_only
import os
from dotenv import load_dotenv
from delivery_workflow.move import create_google_drive_folder, move_files_from_sheet
from google.oauth2 import service_account
from googleapiclient.discovery import build
import re
import tempfile

# Load environment variables from .env file
load_dotenv()

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")

# Configuration constants (dynamic via form, fallbacks from settings)
INPUT_SHEET_ID = settings.APEX_INPUT_SHEET_ID
INPUT_SHEET_NAME = settings.APEX_INPUT_SHEET_NAME
TASK_LINK_COLUMN = settings.APEX_TASK_LINK_COLUMN
input_file_path = f'{settings.APEX_OUTPUT_DIR}/parsed_input_batch.jsonl'
output_file_path = f'{settings.APEX_OUTPUT_DIR}/client_parsed_batch.jsonl'
json_output_directory = settings.APEX_JSON_OUTPUT_DIR
FOLDER_ID = settings.APEX_GDRIVE_DIR_FOLDER_ID_COLLABS
JSON_FOLDER_ID = settings.APEX_GOOGLE_DRIVE_JSON_FOLDER_ID
email_list = settings.apex_email_list
tab_names = ['delivery']

COMMON_METADATA = {"batch": "1"}
COLUMN_FILTER_MAP = None

def get_drive_service():
    """Helper function to get Google Drive service using environment credentials."""
    if not GOOGLE_CREDENTIALS:
        raise ValueError("Google credentials not found in environment variables")
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDENTIALS),
        scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
    )
    return build('drive', 'v3', credentials=creds)

def validate_notebook_link(link: str) -> bool:
    """Validate if the link is a valid Google Drive or Colab link."""
    pattern = r'^(https?:\/\/(colab\.research\.google\.com|drive\.google\.com)\/)'
    return bool(re.search(pattern, link))

def extract_header_issues(parsed_input_batch):
    """
    Filter notebooks with issues and log their details.
    """
    removed_uris = {}  # Track notebooks with issues
    filtered_items = []  # Successfully processed items

    for parsed_input in parsed_input_batch:
        if parsed_input['status'] == 'FAILED':
            print(parsed_input['error_msg'])  # Log error
            uri = parsed_input['uri']
            removed_uris[uri] = uri
        else:
            filtered_items.append(parsed_input['parsed_data'])

    # Log removed URIs
    print(f"Found issues with {len(removed_uris.keys())} files")
    with open(f'{settings.APEX_OUTPUT_DIR}/header_issues.txt', 'w') as file:
        for uri in removed_uris.values():
            file.write(f"{uri}\n")

    return {"items": filtered_items}

def is_valid_json(input_data):
    """
    Check if input data is a valid JSON string.
    """
    try:
        json.loads(input_data)
        return True
    except ValueError:
        return False

def process_jsonl(input_file_path, output_file_path):
    """
    Clean and save data from input JSONL to output JSONL.
    """
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
        for line in input_file:
            item = json.loads(line)  # Load JSON object
            output_file.write(json.dumps(item) + '\n')

    print(f"Processed lines from {input_file_path} and saved to {output_file_path}.")

def run_apex_google_drive(folder_link: str, delivery_type: str, emails: list[str], validate: bool, **config):
    """
    Process APEX notebooks from a Google Drive folder link, deliver them, and notify via email.
    """
    if not validate_notebook_link(folder_link):
        raise ValueError("Invalid Drive folder link. Must be a Google Drive link.")

    drive_service = get_drive_service()
    folder_id = folder_link.split("folders/")[1].split("?")[0] if folder_link.startswith("https") else folder_link
    if not folder_id:
        raise ValueError("Invalid Drive folder link format. Must be a Google Drive folder link.")

    colab_links = get_colab_links_from_folder(drive_service, folder_id)

    if colab_links:
        write_links_to_sheet(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), config.get('input_sheet_name', INPUT_SHEET_NAME), colab_links)
        print("Colab links have been successfully copied to the Google Sheet.")
    else:
        raise ValueError("No Colab links found in the specified folder.")

    # Connect to Google Sheets
    conn = GSheetsConnector(
        sheet_id=config.get('input_sheet_id', INPUT_SHEET_ID),
        sheet_names=[config.get('input_sheet_name', INPUT_SHEET_NAME)],
        gdrive_file_link_column_name=config.get('task_link_column', TASK_LINK_COLUMN),
        common_metadata=COMMON_METADATA,
        column_filter_map=COLUMN_FILTER_MAP,
        max_workers=24,
    )
    
    # Load data from Google Sheets
    conn.load_data()
    input_batch = conn.get_data(as_json=True)
    
    if validate:
        validate_notebooks_in_input_batch(input_batch, drive_service, 'issues', config.get('input_sheet_id', INPUT_SHEET_ID))

    # Process notebooks concurrently
    parsed_input_batch = process_notebook_batch_concurrently(input_batch, max_workers=20)

    # Filter out problematic notebooks
    cleaned_batch = extract_header_issues(parsed_input_batch)

    # Validate and save cleaned batch
    parsed_input_batch_json_str = json.dumps(cleaned_batch)
    if is_valid_json(parsed_input_batch_json_str):
        with open(f'{config.get("output_dir", settings.APEX_OUTPUT_DIR)}/parsed_input_batch.jsonl', 'w') as outfile:
            for item in cleaned_batch['items']:
                json.dump(item, outfile)
                outfile.write('\n')
        print("Exported parsed_input_batch as a JSONL file.")
    else:
        raise ValueError("The parsed_input_batch is not a valid JSON.")

    # Process and upload JSONL files
    process_jsonl(input_file_path, output_file_path)
    split_jsonl_to_json(output_file_path, config.get("json_output_dir", json_output_directory))
    destination_folder = create_or_get_drive_folder(drive_service, config.get("google_drive_dir", settings.APEX_GOOGLE_DRIVE_DIR))
    upload_folder(drive_service, config.get("json_output_dir", json_output_directory), destination_folder, force_replace=True)
    
    if delivery_type in ['normal', 'rework']:
        collab_destination_folder = create_google_drive_folder(f"Delivery-Batch-Apex-Colab-{datetime.now().strftime('%Y-%m-%d')}", config.get("gdrive_dir_folder_id_collabs", FOLDER_ID), drive_service)
        move_files_from_sheet(config.get('input_sheet_name', INPUT_SHEET_NAME), f"https://docs.google.com/spreadsheets/d/{config.get('input_sheet_id', INPUT_SHEET_ID)}", collab_destination_folder)

    JSON_FOLDER_ID = destination_folder.split('/')[-1]
    json_files = get_json_files_from_folder(drive_service, JSON_FOLDER_ID)
    if json_files:
        write_files_to_sheet(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), 'processed', json_files)
        print("JSON file names and links have been successfully copied to the Google Sheet.")
    else:
        print("No JSON files found in the specified folder.")
    
    zip_folder_with_timestamp(config.get("json_output_dir", json_output_directory))
    new_sheet_info = copy_google_sheet(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), emails)

    if delivery_type in ['normal', 'rework']:
        send_email_notification_apex(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            collab_folder_url=f'https://drive.google.com/drive/folders/{collab_destination_folder}',
            sheet_url=new_sheet_info["new_sheet_url"],
            batch=config.get('input_sheet_name', INPUT_SHEET_NAME),
            project="Apex"
        )
    else:  # snapshot
        send_email_notification_json_only(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            batch=config.get('input_sheet_name', INPUT_SHEET_NAME),
            project="Apex"
        )
    return 'done'

def run_apex_sheet(sheet_link: str, delivery_type: str, emails: list[str], validate: bool, **config):
    """
    Process APEX notebooks from a Google Sheets link, deliver them, and notify via email.
    """
    sheet_id = sheet_link.split("spreadsheets/d/")[1].split("/")[0] if sheet_link.startswith("https") else sheet_link
    if not sheet_id:
        raise ValueError("Invalid Sheets link. Must be a Google Sheets link.")

    drive_service = get_drive_service()
    conn = GSheetsConnector(
        sheet_id=sheet_id,
        sheet_names=[config.get('input_sheet_name', INPUT_SHEET_NAME)],
        gdrive_file_link_column_name=config.get('task_link_column', TASK_LINK_COLUMN),
        common_metadata=COMMON_METADATA,
        column_filter_map=COLUMN_FILTER_MAP,
        max_workers=24,
    )
    
    conn.load_data()
    input_batch = conn.get_data(as_json=True)
    
    if validate:
        validate_notebooks_in_input_batch(input_batch, drive_service, 'issues', sheet_id)

    # Process notebooks concurrently
    parsed_input_batch = process_notebook_batch_concurrently(input_batch, max_workers=20)

    # Filter out problematic notebooks
    cleaned_batch = extract_header_issues(parsed_input_batch)

    # Validate and save cleaned batch
    parsed_input_batch_json_str = json.dumps(cleaned_batch)
    if is_valid_json(parsed_input_batch_json_str):
        with open(f'{config.get("output_dir", settings.APEX_OUTPUT_DIR)}/parsed_input_batch.jsonl', 'w') as outfile:
            for item in cleaned_batch['items']:
                json.dump(item, outfile)
                outfile.write('\n')
        print("Exported parsed_input_batch as a JSONL file.")
    else:
        raise ValueError("The parsed_input_batch is not a valid JSON.")

    # Process and upload JSONL files
    process_jsonl(input_file_path, output_file_path)
    split_jsonl_to_json(output_file_path, config.get("json_output_dir", json_output_directory))
    split_jsonl_to_sheet(output_file_path, sheet_id, 'preprocess', drive_service)

    if delivery_type == 'normal':
        json_folder_name_prefix = "Delivery-Batch-Apex-Json"
        colab_folder_name_prefix = "Delivery-Batch-Apex-Colab"
        sheet_name_prefix = "Delivery-Batch-Apex-Sheet"
    elif delivery_type == 'rework':
        json_folder_name_prefix = "Rework-Delivery-Batch-Apex-Json"
        colab_folder_name_prefix = "Rework-Delivery-Batch-Apex-Colab"
        sheet_name_prefix = "Rework-Delivery-Batch-Apex-Sheet"
    elif delivery_type == 'snapshot':
        json_folder_name_prefix = "Snapshot-Delivery-Batch-Apex-Json"
        colab_folder_name_prefix = "Snapshot-Delivery-Batch-Apex-Colab"
        sheet_name_prefix = "Snapshot-Delivery-Batch-Apex-Sheet"

    print(f"✅ Uploading Jsons to Drive")
    destination_folder = create_or_get_drive_folder(drive_service, config.get("google_drive_dir", settings.APEX_GOOGLE_DRIVE_DIR), folder_prefix=json_folder_name_prefix)
    upload_folder(drive_service, config.get("json_output_dir", json_output_directory), destination_folder, force_replace=True)

    if delivery_type in ['normal', 'rework']:
        print(f"✅ Creating Google Drive For Collabs")
        collab_destination_folder = create_google_drive_folder(colab_folder_name_prefix, config.get("gdrive_dir_folder_id_collabs", FOLDER_ID), drive_service)
        print(f"✅ Moving Collabs to Drive For Collabs")
        move_files_from_sheet(config.get('input_sheet_name', INPUT_SHEET_NAME), f"https://docs.google.com/spreadsheets/d/{sheet_id}", collab_destination_folder)

    JSON_FOLDER_ID = destination_folder.split('/')[-1]
    json_files = get_json_files_from_folder(drive_service, JSON_FOLDER_ID)
    if json_files:
        write_files_to_sheet(drive_service, sheet_id, 'delivery', json_files)
        print("JSON file names and links have been successfully copied to the Google Sheet.")
    else:
        print("No JSON files found in the specified folder.")

    zip_folder_with_timestamp(config.get("json_output_dir", json_output_directory))
    new_sheet_info = copy_specific_tabs_google_sheet(drive_service, sheet_id, emails, tab_names)
    copy_google_sheet_to_drive(drive_service, new_sheet_info["new_sheet_id"], sheet_name_prefix, config.get("google_drive_json_folder_id", JSON_FOLDER_ID))

    if delivery_type in ['normal', 'rework']:
        send_email_notification_apex(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            collab_folder_url=f'https://drive.google.com/drive/folders/{collab_destination_folder}',
            sheet_url=new_sheet_info["new_sheet_url"],
            batch=config.get('input_sheet_name', INPUT_SHEET_NAME),
            project="Apex"
        )
        return 'done'
    else:  # snapshot
        send_email_notification_json_only(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            batch=config.get('input_sheet_name', INPUT_SHEET_NAME),
            project="Apex"
        )
    return 'done'

def run_apex_json_file(json_data: dict, delivery_type: str, emails: list[str], validate: bool, **config):
    """
    Process APEX notebooks from a JSON object, deliver them, and notify via email.
    """
    if not isinstance(json_data, dict):
        raise ValueError("Invalid JSON data. Must be a dictionary.")

    # Save JSON temporarily for processing
    with tempfile.NamedTemporaryFile(delete=False, suffix='.json', dir=config.get('output_dir', settings.APEX_OUTPUT_DIR)) as tmp:
        json.dump(json_data, tmp)
        tmp_path = tmp.name

    drive_service = get_drive_service()
    update_google_sheet_from_json(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), config.get('input_sheet_name', INPUT_SHEET_NAME), tmp_path)

    # Initialize Google Sheets connection
    conn = GSheetsConnector(
        sheet_id=config.get('input_sheet_id', INPUT_SHEET_ID),
        sheet_names=[config.get('input_sheet_name', INPUT_SHEET_NAME)],
        gdrive_file_link_column_name=config.get('task_link_column', TASK_LINK_COLUMN),
        common_metadata=COMMON_METADATA,
        column_filter_map=COLUMN_FILTER_MAP,
        max_workers=24,
    )
    
    conn.load_data()
    input_batch = conn.get_data(as_json=True)
    total_number_collab_links = len(input_batch['items'])
    print(f'✅ Processing {total_number_collab_links} Unique Collab links')

    if validate:
        validation_results = validate_notebooks_in_input_batch(input_batch, drive_service, 'issues', config.get('input_sheet_id', INPUT_SHEET_ID))
        if validation_results['status'] == "failed":
            print(f"❌ Internal Validator failed {validation_results['count_of_collabs_with_issues']} Collab Notebooks out of the {total_number_collab_links}")
            new_sheet_info = copy_specific_tabs_google_sheet(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), emails, ['issues'])
            print(f'Sheet link to Internal Validator Errors: {new_sheet_info["new_sheet_url"]}')
            send_lwc_issue_email_notification(
                sender_email=SENDER_EMAIL,
                sender_password=SENDER_PASSWORD,
                recipient_emails=emails,
                sheet_url=new_sheet_info["new_sheet_url"],
                batch=config.get('input_sheet_name', INPUT_SHEET_NAME),
                project="Apex",
                file_type="Collabs"
            )
            return "Sent Email with Issues"

        # Parse notebooks
        input_batch = validation_results['data']
    
    print(f"✅ Parsing {total_number_collab_links} Collab Notebooks into Json")
    # Process notebooks concurrently
    parsed_input_batch = process_notebook_batch_concurrently(input_batch, max_workers=20)

    # Filter out problematic notebooks
    cleaned_batch = extract_header_issues(parsed_input_batch)

    # Validate and save cleaned batch
    parsed_input_batch_json_str = json.dumps(cleaned_batch)
    if is_valid_json(parsed_input_batch_json_str):
        with open(f'{config.get("output_dir", settings.APEX_OUTPUT_DIR)}/parsed_input_batch.jsonl', 'w') as outfile:
            for item in cleaned_batch['items']:
                json.dump(item, outfile)
                outfile.write('\n')
        print("Exported parsed_input_batch as a JSONL file.")
    else:
        raise ValueError("The parsed_input_batch is not a valid JSON.")

    # Process and upload JSONL files
    process_jsonl(input_file_path, output_file_path)
    split_jsonl_to_json(output_file_path, config.get("json_output_dir", json_output_directory))
    split_jsonl_to_sheet(output_file_path, config.get('input_sheet_id', INPUT_SHEET_ID), 'preprocess', drive_service)

    if delivery_type == 'normal':
        json_folder_name_prefix = "Delivery-Batch-Apex-Json"
        colab_folder_name_prefix = "Delivery-Batch-Apex-Colab"
        sheet_name_prefix = "Delivery-Batch-Apex-Sheet"
    elif delivery_type == 'rework':
        json_folder_name_prefix = "Rework-Delivery-Batch-Apex-Json"
        colab_folder_name_prefix = "Rework-Delivery-Batch-Apex-Colab"
        sheet_name_prefix = "Rework-Delivery-Batch-Apex-Sheet"
    elif delivery_type == 'snapshot':
        json_folder_name_prefix = "Snapshot-Delivery-Batch-Apex-Json"
        colab_folder_name_prefix = "Snapshot-Delivery-Batch-Apex-Colab"
        sheet_name_prefix = "Snapshot-Delivery-Batch-Apex-Sheet"

    print(f"✅ Uploading Jsons to Drive")
    destination_folder = create_or_get_drive_folder(drive_service, config.get("google_drive_dir", settings.APEX_GOOGLE_DRIVE_DIR), folder_prefix=json_folder_name_prefix)
    upload_folder(drive_service, config.get("json_output_dir", json_output_directory), destination_folder, force_replace=True)

    if delivery_type in ['normal', 'rework']:
        print(f"✅ Creating Google Drive For Collabs")
        collab_destination_folder = create_google_drive_folder(colab_folder_name_prefix, config.get("gdrive_dir_folder_id_collabs", FOLDER_ID), drive_service)
        print(f"✅ Moving Collabs to Drive For Collabs")
        move_files_from_sheet(config.get('input_sheet_name', INPUT_SHEET_NAME), f"https://docs.google.com/spreadsheets/d/{config.get('input_sheet_id', INPUT_SHEET_ID)}", collab_destination_folder)

    JSON_FOLDER_ID = destination_folder.split('/')[-1]
    json_files = get_json_files_from_folder(drive_service, JSON_FOLDER_ID)
    if json_files:
        write_files_to_sheet(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), 'delivery', json_files)
        print("JSON file names and links have been successfully copied to the Google Sheet.")
    else:
        print("No JSON files found in the specified folder.")

    zip_folder_with_timestamp(config.get("json_output_dir", json_output_directory))
    new_sheet_info = copy_specific_tabs_google_sheet(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), emails, tab_names)
    copy_google_sheet_to_drive(drive_service, new_sheet_info["new_sheet_id"], sheet_name_prefix, config.get("google_drive_json_folder_id", JSON_FOLDER_ID))

    if delivery_type in ['normal', 'rework']:
        send_email_notification_apex(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            collab_folder_url=f'https://drive.google.com/drive/folders/{collab_destination_folder}',
            sheet_url=new_sheet_info["new_sheet_url"],
            batch=config.get('input_sheet_name', INPUT_SHEET_NAME),
            project="Apex"
        )
        return 'done'
    else:  # snapshot
        send_email_notification_json_only(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            batch=config.get('input_sheet_name', INPUT_SHEET_NAME),
            project="Apex"
        )
    return 'done'

def run_apex_json_file_test(json_data: dict, batch_name: str, **config):
    """
    Test function to process APEX input data from a JSON object.
    """
    if not isinstance(json_data, dict):
        raise ValueError("Invalid JSON data. Must be a dictionary.")

    # Save JSON temporarily for processing
    with tempfile.NamedTemporaryFile(delete=False, suffix='.json', dir=config.get('output_dir', settings.APEX_OUTPUT_DIR)) as tmp:
        json.dump(json_data, tmp)
        tmp_path = tmp.name

    drive_service = get_drive_service()
    update_google_sheet_from_json(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), config.get('input_sheet_name', INPUT_SHEET_NAME), tmp_path)

    # Initialize Google Sheets connection
    conn = GSheetsConnector(
        sheet_id=config.get('input_sheet_id', INPUT_SHEET_ID),
        sheet_names=[config.get('input_sheet_name', INPUT_SHEET_NAME)],
        gdrive_file_link_column_name=config.get('task_link_column', TASK_LINK_COLUMN),
        common_metadata=COMMON_METADATA,
        column_filter_map=COLUMN_FILTER_MAP,
        max_workers=24,
    )
    
    conn.load_data()
    input_batch = conn.get_data(as_json=True)
    validate_notebooks_in_input_batch(input_batch, drive_service, 'issues', config.get('input_sheet_id', INPUT_SHEET_ID))

    # Process notebooks concurrently
    parsed_input_batch = process_notebook_batch_concurrently(input_batch, max_workers=20)

    # Filter out problematic notebooks
    cleaned_batch = extract_header_issues(parsed_input_batch)

    # Validate and save cleaned batch
    parsed_input_batch_json_str = json.dumps(cleaned_batch)
    if is_valid_json(parsed_input_batch_json_str):
        with open(f'{config.get("output_dir", settings.APEX_OUTPUT_DIR)}/parsed_input_batch.jsonl', 'w') as outfile:
            for item in cleaned_batch['items']:
                json.dump(item, outfile)
                outfile.write('\n')
        print("Exported parsed_input_batch as a JSONL file.")
    else:
        raise ValueError("The parsed_input_batch is not a valid JSON.")

    # Process and upload JSONL files
    process_jsonl(input_file_path, output_file_path)
    split_jsonl_to_json(output_file_path, config.get("json_output_dir", json_output_directory))
    split_jsonl_to_sheet(output_file_path, config.get('input_sheet_id', INPUT_SHEET_ID), 'preprocess', drive_service)

    destination_folder = create_or_get_drive_folder(drive_service, config.get("google_drive_dir", settings.APEX_GOOGLE_DRIVE_DIR))
    upload_folder(drive_service, config.get("json_output_dir", json_output_directory), destination_folder, force_replace=True)

    JSON_FOLDER_ID = destination_folder.split('/')[-1]
    json_files = get_json_files_from_folder(drive_service, JSON_FOLDER_ID)
    if json_files:
        write_files_to_sheet(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), 'delivery', json_files)
        print("JSON file names and links have been successfully copied to the Google Sheet.")
    else:
        print("No JSON files found in the specified folder.")

    zip_folder_with_timestamp(config.get("json_output_dir", json_output_directory))
    new_sheet_info = copy_specific_tabs_google_sheet(drive_service, config.get('input_sheet_id', INPUT_SHEET_ID), email_list, tab_names)
    copy_google_sheet_to_drive(drive_service, new_sheet_info["new_sheet_id"], f"Delivery-Batch-Apex-Sheet-{datetime.now().strftime('%Y-%m-%d')}", config.get("google_drive_json_folder_id", JSON_FOLDER_ID))

    send_email_notification_apex(
        sender_email=SENDER_EMAIL,
        sender_password=SENDER_PASSWORD,
        recipient_emails=email_list,
        json_folder_url=destination_folder,
        collab_folder_url='https://drive.google.com/drive/folders/1jqgG7dfdx5_-gCkbCdhBn87aGuGFkTCm',  # Example, update as needed
        sheet_url=new_sheet_info["new_sheet_url"],
        batch=config.get('input_sheet_name', INPUT_SHEET_NAME),
        project="Apex"
    )
    return 'done'