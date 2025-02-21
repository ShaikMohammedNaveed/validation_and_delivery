from datetime import timedelta
import datetime
from delivery_workflow.data_ingest.src.input_connectors import GSheetsConnector
from delivery_workflow.parsers.src.parser import Parser
import json
from delivery_workflow.config import settings
from delivery_workflow.parsers.src.utils import split_jsonl_to_json, zip_folder_with_timestamp, update_google_sheet
from delivery_workflow.data_ingest.src.gdrive_utils import upload_folder, create_or_get_drive_folder
from delivery_workflow.sheet_util import get_colab_links_from_folder, write_links_to_sheet, get_json_files_from_folder, write_files_to_sheet, copy_google_sheet, update_google_sheet_from_json, copy_specific_tabs_google_sheet, copy_google_sheet_to_drive
from delivery_workflow.notify import send_email_notification_with_zip_folder, send_email_notification, send_lwc_issue_email_notification, send_email_notification_apex, send_email_notification_json_only
import os
from dotenv import load_dotenv
from delivery_workflow.validation.lwc_validator_reviewer import validate_notebook
from delivery_workflow.validation.client_lwc_json_validator import main_validator
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
input_sheet_id = settings.LWC_INPUT_SHEET_ID
input_sheet_name = settings.LWC_INPUT_SHEET_NAME
task_link_column = settings.LWC_TASK_LINK_COLUMN
input_file_path = f'{settings.LWC_OUTPUT_DIR}/parsed_input_batch.jsonl'
output_file_path = f'{settings.LWC_OUTPUT_DIR}/client_parsed_batch.jsonl'
json_output_directory = settings.LWC_JSON_OUTPUT_DIR
FOLDER_ID = settings.LWC_GDRIVE_DIR_FOLDER_ID_COLLABS
JSON_FOLDER_ID = settings.LWC_GOOGLE_DRIVE_JSON_FOLDER_ID
email_list = settings.lwc_email_list
tab_names = ['delivery']

# Common configurations
input_sheet_names = [input_sheet_name]
gdrive_file_link_column_name = task_link_column
common_metadata = {"batch": "2"}
column_filter_map = None  # Optional column filters
keys_to_remove = ['Metadata', 'Score', 'Comments', 'Suggested Conversation']

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
    Identify and log header issues in parsed input batch.
    """
    items = parsed_input_batch['items']
    removed_uris = {}
    turn_issues = {}
    failed_uris = []
    filtered_items = []

    for item in items:
        if item['parsed']['status'] == 'ERROR':
            print(item['parsed']['messages'])
            uri = item['metadata']['data']['original_uri']
            removed_uris[uri] = uri
        elif item['parsed']['status'] == 'FAILED':
            print("failed")
            uri = item['metadata']['data']['original_uri']
            print(uri)
            turn_issues[uri] = uri
            failed_uris.append(uri)
        else:
            filtered_items.append(item)

    parsed_input_batch['items'] = filtered_items
    print(f"Found issues with {len(removed_uris.keys())} files")
    with open(f'{settings.LWC_OUTPUT_DIR}/header_issues.txt', 'w') as file:
        for uri in removed_uris.values():
            file.write(f"{uri}\n")

    print(f"Found turn issues with {len(turn_issues.keys())} files")
    with open(f'{settings.LWC_OUTPUT_DIR}/turn_issues.txt', 'w') as file:
        for uri in turn_issues.values():
            file.write(f"{uri}\n")

    print(f"Found failed issues with {len(failed_uris)} files")
    with open(f'{settings.LWC_OUTPUT_DIR}/failed_issues.txt', 'w') as file:
        for uri in failed_uris:
            file.write(f"{uri}\n")

    return parsed_input_batch

def is_valid_json(input_data):
    """
    Validate if the input data is a valid JSON string.
    """
    try:
        json.loads(input_data)
        return True
    except ValueError:
        return False

def clean_item(item):
    """
    Clean an individual item by removing unnecessary fields.
    """
    # Remove the 'content' field if it exists
    if 'content' in item:
        del item['content']

    # Rename the 'parsed' key to 'parsed_details'
    if 'parsed' in item:
        item['data'] = item.pop('parsed')
    
    return item

def process_jsonl(input_file_path, output_file_path):
    """
    Process the input JSONL file and save cleaned data to an output JSONL file.
    """
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
        for line in input_file:
            item = json.loads(line)  # Load JSON object
            cleaned_item = clean_item(item)  # Clean the item
            output_file.write(json.dumps(cleaned_item) + '\n')  # Write the cleaned item

    print(f"Processed lines from {input_file_path} and saved to {output_file_path}.")

def run_lwc_google_drive(folder_link: str, delivery_type: str, emails: list[str], validate: bool, **config):
    """
    Process LWC notebooks from a Google Drive folder link, deliver them, and notify via email.
    """
    if not validate_notebook_link(folder_link):
        raise ValueError("Invalid Drive folder link. Must be a Google Drive link.")

    drive_service = get_drive_service()
    folder_id = folder_link.split("folders/")[1].split("?")[0] if folder_link.startswith("https") else folder_link
    if not folder_id:
        raise ValueError("Invalid Drive folder link format. Must be a Google Drive folder link.")

    colab_links = get_colab_links_from_folder(drive_service, folder_id)

    if colab_links:
        write_links_to_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), config.get('input_sheet_name', input_sheet_name), colab_links)
        print("Colab links have been successfully copied to the Google Sheet.")
    else:
        raise ValueError("No Colab links found in the specified folder.")

    # Initialize Google Sheets connection
    conn = GSheetsConnector(
        sheet_id=config.get('input_sheet_id', input_sheet_id),
        sheet_names=[config.get('input_sheet_name', input_sheet_name)],
        gdrive_file_link_column_name=config.get('task_link_column', task_link_column),
        common_metadata=common_metadata,
        column_filter_map=column_filter_map,
        max_workers=24,
    )

    conn.load_data()
    input_batch = conn.get_data(as_json=True)
    
    if validate:
        validate_notebook(input_batch, drive_service, 'issues', config.get('input_sheet_id', input_sheet_id))

    # Parse notebooks
    parser = Parser()
    parsed_input_batch = parser.parse_notebooks(input_batch)

    # Remove unnecessary keys from metadata
    for item in parsed_input_batch['items']:
        for key in keys_to_remove:
            if key in item['metadata']['data']:
                del item['metadata']['data'][key]

    # Filter problematic notebooks
    cleaned_batch = extract_header_issues(parsed_input_batch)

    # Validate and export parsed batch
    parsed_input_batch_json_str = json.dumps(cleaned_batch)
    if is_valid_json(parsed_input_batch_json_str):
        with open(f'{config.get("output_dir", settings.LWC_OUTPUT_DIR)}/parsed_input_batch.jsonl', 'w') as outfile:
            for item in parsed_input_batch['items']:
                if item['metadata']['status'] == 'ERROR':
                    continue
                json.dump(item, outfile)
                outfile.write('\n')
        print("Exported parsed_input_batch as a JSONL file.")
    else:
        raise ValueError("The parsed_input_batch is not a valid JSON.")

    # Process and upload JSONL files
    process_jsonl(input_file_path, output_file_path)
    split_jsonl_to_json(output_file_path, config.get("json_output_dir", json_output_directory))
    update_google_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), 'delivery', output_file_path)
    destination_folder = create_or_get_drive_folder(drive_service, config.get("google_drive_dir", settings.LWC_GOOGLE_DRIVE_DIR), "lwc")
    upload_folder(drive_service, config.get("json_output_dir", json_output_directory), destination_folder, force_replace=True)

    JSON_FOLDER_ID = destination_folder.split('/')[-1]
    json_files = get_json_files_from_folder(drive_service, JSON_FOLDER_ID)
    if json_files:
        write_files_to_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), 'processed', json_files)
        print("JSON file names and links have been successfully copied to the Google Sheet.")
    else:
        print("No JSON files found in the specified folder.")

    zip_folder_with_timestamp(config.get("json_output_dir", json_output_directory))
    new_sheet_info = copy_google_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), emails)

    if delivery_type in ['normal', 'rework']:
        collab_destination_folder = create_google_drive_folder(f"Delivery-Batch-Lwc-Colab-{datetime.now().strftime('%Y-%m-%d')}", config.get("gdrive_dir_folder_id_collabs", FOLDER_ID), drive_service)
        move_files_from_sheet(config.get('input_sheet_name', input_sheet_name), f"https://docs.google.com/spreadsheets/d/{config.get('input_sheet_id', input_sheet_id)}", collab_destination_folder)

        send_email_notification_apex(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            collab_folder_url=f'https://drive.google.com/drive/folders/{collab_destination_folder}',
            sheet_url=new_sheet_info["new_sheet_url"],
            batch=config.get('input_sheet_name', input_sheet_name),
            project="LWC"
        )
    else:  # snapshot
        send_email_notification_json_only(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            batch=config.get('input_sheet_name', input_sheet_name),
            project="LWC"
        )
    return 'done'

def run_lwc_sheet(sheet_link: str, delivery_type: str, emails: list[str], validate: bool, **config):
    """
    Process LWC notebooks from a Google Sheets link, deliver them, and notify via email.
    """
    try:
        sheet_id = sheet_link.split("spreadsheets/d/")[1].split("/")[0] if sheet_link.startswith("https") else sheet_link
        if not sheet_id or not re.match(r'^[a-zA-Z0-9-_]+$', sheet_id):
            raise ValueError("Invalid Sheets link. Must be a valid Google Sheets link.")

        drive_service = get_drive_service()
        conn = GSheetsConnector(
            sheet_id=sheet_id,
            sheet_names=[config.get('input_sheet_name', settings.LWC_INPUT_SHEET_NAME)],
            gdrive_file_link_column_name=config.get('task_link_column', settings.LWC_TASK_LINK_COLUMN),
            common_metadata={"batch": "2"},
            column_filter_map=None,
            max_workers=24,
        )
        
        conn.load_data()
        input_batch = conn.get_data(as_json=True)
        
        if validate:
            validation_results = validate_notebook(input_batch, drive_service, 'issues', sheet_id)
            if validation_results['status'] == "failed":
                print(f"❌ Internal Validator failed {validation_results['count_of_collabs_with_issues']} Collab Notebooks")
                new_sheet_info = copy_specific_tabs_google_sheet(drive_service, sheet_id, emails, ['issues'])
                print(f'Sheet link to Internal Validator Errors: {new_sheet_info["new_sheet_url"]}')
                send_lwc_issue_email_notification(
                    sender_email=SENDER_EMAIL,
                    sender_password=SENDER_PASSWORD,
                    recipient_emails=emails,
                    sheet_url=new_sheet_info["new_sheet_url"],
                    batch=config.get('input_sheet_name', settings.LWC_INPUT_SHEET_NAME),
                    project="LWC",
                    file_type="Collab"
                )
                return "Sent Email with Issues"

            input_batch = validation_results['data']

        # Parse notebooks
        parser = Parser()
        parsed_input_batch = parser.parse_notebooks(input_batch)

        # Remove unnecessary keys from metadata
        for item in parsed_input_batch['items']:
            for key in ['Metadata', 'Score', 'Comments', 'Suggested Conversation']:
                if key in item['metadata']['data']:
                    del item['metadata']['data'][key]

        # Filter problematic notebooks
        cleaned_batch = extract_header_issues(parsed_input_batch)

        # Validate and export parsed batch
        parsed_input_batch_json_str = json.dumps(cleaned_batch)
        if not is_valid_json(parsed_input_batch_json_str):
            raise ValueError("The parsed_input_batch is not a valid JSON.")

        with open(f'{config.get("output_dir", settings.LWC_OUTPUT_DIR)}/parsed_input_batch.jsonl', 'w') as outfile:
            for item in parsed_input_batch['items']:
                if item['metadata']['status'] == 'ERROR':
                    continue
                json.dump(item, outfile)
                outfile.write('\n')
        print("Exported parsed_input_batch as a JSONL file.")

        # Process and upload JSONL files
        process_jsonl(settings.LWC_OUTPUT_DIR + '/parsed_input_batch.jsonl', settings.LWC_OUTPUT_DIR + '/client_parsed_batch.jsonl')
        split_jsonl_to_json(settings.LWC_OUTPUT_DIR + '/client_parsed_batch.jsonl', config.get("json_output_dir", settings.LWC_JSON_OUTPUT_DIR))
        clean_json_output_directory = f'{config.get("output_dir", settings.LWC_OUTPUT_DIR)}/{config.get("input_sheet_name", settings.LWC_INPUT_SHEET_NAME)}'
        os.makedirs(clean_json_output_directory, exist_ok=True)
        update_google_sheet(drive_service, sheet_id, 'delivery', settings.LWC_OUTPUT_DIR + '/client_parsed_batch.jsonl')
        validator_results = main_validator(config.get("json_output_dir", settings.LWC_JSON_OUTPUT_DIR), emails, clean_json_output_directory)

        if validator_results['status'] == 'failed':
            print(f"❌ Client Validator failed {validator_results['data']['total_files_failed']} of the {validator_results['data']['total_files']} Jsons")
            print(f'Sheet link to Client Validator Errors: {validator_results["sheet_url"]}')
            send_lwc_issue_email_notification(
                sender_email=SENDER_EMAIL,
                sender_password=SENDER_PASSWORD,
                recipient_emails=emails,
                sheet_url=validator_results["sheet_url"],
                batch=config.get('input_sheet_name', settings.LWC_INPUT_SHEET_NAME),
                project="LWC",
                file_type="Json"
            )
            zip_folder_with_timestamp(config.get("json_output_dir", settings.LWC_JSON_OUTPUT_DIR))
            zip_folder_with_timestamp(clean_json_output_directory)
            return "Sent Email with Issues"

        if delivery_type == 'normal':
            json_folder_name_prefix = "Delivery-Batch-Lwc-Json"
            colab_folder_name_prefix = "Delivery-Batch-Lwc-Colab"
            sheet_name_prefix = "Delivery-Batch-Lwc-Sheet"
        elif delivery_type == 'rework':
            json_folder_name_prefix = "Rework-Delivery-Batch-Lwc-Json"
            colab_folder_name_prefix = "Rework-Delivery-Batch-Lwc-Colab"
            sheet_name_prefix = "Rework-Delivery-Batch-Lwc-Sheet"
        elif delivery_type == 'snapshot':
            json_folder_name_prefix = "Snapshot-Delivery-Batch-Lwc-Json"
            colab_folder_name_prefix = "Snapshot-Delivery-Batch-Lwc-Colab"
            sheet_name_prefix = "Snapshot-Delivery-Batch-Lwc-Sheet"

        print(f"✅ Uploading Jsons to Drive")
        destination_folder = create_or_get_drive_folder(drive_service, config.get("google_drive_dir", settings.LWC_GOOGLE_DRIVE_DIR), folder_prefix=json_folder_name_prefix)
        upload_folder(drive_service, clean_json_output_directory, destination_folder, force_replace=True)

        if delivery_type in ['normal', 'rework']:
            print(f"✅ Creating Google Drive For Collabs")
            collab_destination_folder = create_google_drive_folder(colab_folder_name_prefix, config.get("gdrive_dir_folder_id_collabs", settings.LWC_GDRIVE_DIR_FOLDER_ID_COLLABS), drive_service)
            print(f"✅ Moving Collabs to Drive For Collabs")
            move_files_from_sheet(config.get('input_sheet_name', settings.LWC_INPUT_SHEET_NAME), f"https://docs.google.com/spreadsheets/d/{sheet_id}", collab_destination_folder)

        new_sheet_info = copy_specific_tabs_google_sheet(drive_service, sheet_id, emails, ['delivery'])
        copy_google_sheet_to_drive(drive_service, new_sheet_info["new_sheet_id"], sheet_name_prefix, config.get("google_drive_json_folder_id", settings.LWC_GOOGLE_DRIVE_JSON_FOLDER_ID))

        if delivery_type in ['normal', 'rework']:
            send_email_notification_apex(
                sender_email=SENDER_EMAIL,
                sender_password=SENDER_PASSWORD,
                recipient_emails=emails,
                json_folder_url=destination_folder,
                collab_folder_url=f'https://drive.google.com/drive/folders/{collab_destination_folder}',
                sheet_url=new_sheet_info["new_sheet_url"],
                batch=config.get('input_sheet_name', settings.LWC_INPUT_SHEET_NAME),
                project="LWC"
            )
            return 'done'
        else:  # snapshot
            send_email_notification_json_only(
                sender_email=SENDER_EMAIL,
                sender_password=SENDER_PASSWORD,
                recipient_emails=emails,
                json_folder_url=destination_folder,
                batch=config.get('input_sheet_name', settings.LWC_INPUT_SHEET_NAME),
                project="LWC"
            )
        return 'done'

    except ValueError as ve:
        raise ValueError(f"Validation error in LWC sheet delivery: {str(ve)}")
    except Exception as e:
        raise Exception(f"Failed to process LWC sheet: {str(e)}")
    
def run_lwc_json_file(json_data: dict, delivery_type: str, emails: list[str], validate: bool, **config):
    """
    Process LWC notebooks from a JSON object, deliver them, and notify via email.
    """
    try:
        if not isinstance(json_data, (dict, list)):
            raise ValueError("Invalid JSON data. Must be a dictionary or list.")
        if isinstance(json_data, list):
            json_data = {"items": json_data}  # Normalize to expected structure

        # Save JSON temporarily for processing
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json', dir=config.get('output_dir', settings.LWC_OUTPUT_DIR)) as tmp:
            json.dump(json_data, tmp)
            tmp_path = tmp.name
    except ValueError as ve:
        raise ValueError(f"Validation error in JSON data: {str(ve)}")
    except Exception as e:
        raise Exception(f"Failed to process JSON file: {str(e)}")
    drive_service = get_drive_service()
    update_google_sheet_from_json(drive_service, config.get('input_sheet_id', input_sheet_id), config.get('input_sheet_name', input_sheet_name), tmp_path)

    # Initialize Google Sheets connection
    conn = GSheetsConnector(
        sheet_id=config.get('input_sheet_id', input_sheet_id),
        sheet_names=[config.get('input_sheet_name', input_sheet_name)],
        gdrive_file_link_column_name=config.get('task_link_column', task_link_column),
        common_metadata=common_metadata,
        column_filter_map=column_filter_map,
        max_workers=24,
    )

    conn.load_data()
    input_batch = conn.get_data(as_json=True)
    total_number_collab_links = len(input_batch['items'])
    print(f'✅ Processing {total_number_collab_links} Unique Collab links')

    if validate:
        validation_results = validate_notebook(input_batch, drive_service, 'issues', config.get('input_sheet_id', input_sheet_id))
        if validation_results['status'] == "failed":
            print(f"❌ Internal Validator failed {validation_results['count_of_collabs_with_issues']} Collab Notebooks out of the {total_number_collab_links}")
            new_sheet_info = copy_specific_tabs_google_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), emails, ['issues'])
            print(f'Sheet link to Internal Validator Errors: {new_sheet_info["new_sheet_url"]}')
            send_lwc_issue_email_notification(
                sender_email=SENDER_EMAIL,
                sender_password=SENDER_PASSWORD,
                recipient_emails=emails,
                sheet_url=new_sheet_info["new_sheet_url"],
                batch=config.get('input_sheet_name', input_sheet_name),
                project="LWC",
                file_type="Collab"
            )
            return "Sent Email with Issues"

        input_batch = validation_results['data']

    print(f"✅ Parsing {total_number_collab_links} Collab Notebooks into Json")
    parser = Parser()
    parsed_input_batch = parser.parse_notebooks(input_batch)

    # Remove unnecessary keys from metadata
    for item in parsed_input_batch['items']:
        for key in keys_to_remove:
            if key in item['metadata']['data']:
                del item['metadata']['data'][key]

    # Filter problematic notebooks
    cleaned_batch = extract_header_issues(parsed_input_batch)

    # Validate and export parsed batch
    parsed_input_batch_json_str = json.dumps(cleaned_batch)
    if is_valid_json(parsed_input_batch_json_str):
        with open(f'{config.get("output_dir", settings.LWC_OUTPUT_DIR)}/parsed_input_batch.jsonl', 'w') as outfile:
            for item in parsed_input_batch['items']:
                if item['metadata']['status'] == 'ERROR':
                    continue
                json.dump(item, outfile)
                outfile.write('\n')
        print("Exported parsed_input_batch as a JSONL file.")
    else:
        raise ValueError("The parsed_input_batch is not a valid JSON.")

    # Process and upload JSONL files
    process_jsonl(input_file_path, output_file_path)
    split_jsonl_to_json(output_file_path, config.get("json_output_dir", json_output_directory))
    clean_json_output_directory = f'{config.get("output_dir", settings.LWC_OUTPUT_DIR)}/{config.get("input_sheet_name", input_sheet_name)}'
    os.makedirs(clean_json_output_directory, exist_ok=True)
    update_google_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), 'delivery', output_file_path)
    validator_results = main_validator(config.get("json_output_dir", json_output_directory), emails, clean_json_output_directory)

    if validator_results['status'] == 'failed':
        print(f"❌ Client Validator failed {validator_results['data']['total_files_failed']} of the {validator_results['data']['total_files']} Jsons")
        print(f'Sheet link to Client Validator Errors: {validator_results["sheet_url"]}')
        send_lwc_issue_email_notification(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            sheet_url=validator_results["sheet_url"],
            batch=config.get('input_sheet_name', input_sheet_name),
            project="LWC",
            file_type="Json"
        )
        zip_folder_with_timestamp(config.get("json_output_dir", json_output_directory))
        zip_folder_with_timestamp(clean_json_output_directory)
        return "Sent Email with Issues"

    if delivery_type == 'normal':
        json_folder_name_prefix = "Delivery-Batch-Lwc-Json"
        colab_folder_name_prefix = "Delivery-Batch-Lwc-Colab"
        sheet_name_prefix = "Delivery-Batch-Lwc-Sheet"
    elif delivery_type == 'rework':
        json_folder_name_prefix = "Rework-Delivery-Batch-Lwc-Json"
        colab_folder_name_prefix = "Rework-Delivery-Batch-Lwc-Colab"
        sheet_name_prefix = "Rework-Delivery-Batch-Lwc-Sheet"
    elif delivery_type == 'snapshot':
        json_folder_name_prefix = "Snapshot-Delivery-Batch-Lwc-Json"
        colab_folder_name_prefix = "Snapshot-Delivery-Batch-Lwc-Colab"
        sheet_name_prefix = "Snapshot-Delivery-Batch-Lwc-Sheet"

    print(f"✅ Uploading Jsons to Drive")
    destination_folder = create_or_get_drive_folder(drive_service, config.get("google_drive_dir", settings.LWC_GOOGLE_DRIVE_DIR), folder_prefix=json_folder_name_prefix)
    upload_folder(drive_service, clean_json_output_directory, destination_folder, force_replace=True)

    if delivery_type in ['normal', 'rework']:
        print(f"✅ Creating Google Drive For Collabs")
        collab_destination_folder = create_google_drive_folder(colab_folder_name_prefix, config.get("gdrive_dir_folder_id_collabs", FOLDER_ID), drive_service)
        print(f"✅ Moving Collabs to Drive For Collabs")
        move_files_from_sheet(config.get('input_sheet_name', input_sheet_name), f"https://docs.google.com/spreadsheets/d/{config.get('input_sheet_id', input_sheet_id)}", collab_destination_folder)

    new_sheet_info = copy_specific_tabs_google_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), emails, tab_names)
    copy_google_sheet_to_drive(drive_service, new_sheet_info["new_sheet_id"], sheet_name_prefix, config.get("google_drive_json_folder_id", JSON_FOLDER_ID))

    if delivery_type in ['normal', 'rework']:
        send_email_notification_apex(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            collab_folder_url=f'https://drive.google.com/drive/folders/{collab_destination_folder}',
            sheet_url=new_sheet_info["new_sheet_url"],
            batch=config.get('input_sheet_name', input_sheet_name),
            project="LWC"
        )
        return 'done'
    else:  # snapshot
        send_email_notification_json_only(
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            recipient_emails=emails,
            json_folder_url=destination_folder,
            batch=config.get('input_sheet_name', input_sheet_name),
            project="LWC"
        )
    return 'done'

def process_lwc_json_file_only(json_data: dict, batch_name: str, **config):
    """
    Main function to process LWC input data from a JSON object.
    """
    if not isinstance(json_data, dict):
        raise ValueError("Invalid JSON data. Must be a dictionary.")

    # Save JSON temporarily for processing
    with tempfile.NamedTemporaryFile(delete=False, suffix='.json', dir=config.get('output_dir', settings.LWC_OUTPUT_DIR)) as tmp:
        json.dump(json_data, tmp)
        tmp_path = tmp.name

    drive_service = get_drive_service()
    update_google_sheet_from_json(drive_service, config.get('input_sheet_id', input_sheet_id), config.get('input_sheet_name', input_sheet_name), tmp_path)

    # Initialize Google Sheets connection
    conn = GSheetsConnector(
        sheet_id=config.get('input_sheet_id', input_sheet_id),
        sheet_names=[config.get('input_sheet_name', input_sheet_name)],
        gdrive_file_link_column_name=config.get('task_link_column', task_link_column),
        common_metadata=common_metadata,
        column_filter_map=column_filter_map,
        max_workers=24,
    )

    conn.load_data()
    input_batch = conn.get_data(as_json=True)
    total_number_collab_links = len(input_batch['items'])
    validation_results = validate_notebook(input_batch, drive_service, 'issues', config.get('input_sheet_id', input_sheet_id))
    if validation_results['status'] == "failed":
        print('❌ Validator failed while running checks on Collabs')
        new_sheet_info = copy_specific_tabs_google_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), email_list, ['issues'])
        data = {
            "stage": "internal_validator",
            "status": validation_results['status'],
            "total_number_collab_links": total_number_collab_links,
            "issue_sheet_link": new_sheet_info,
            "count_of_collabs_with_issues": validation_results['count_of_collabs_with_issues']
        }
        print(f'Error Sheet link: {new_sheet_info["new_sheet_url"]}')
        return data

    # Parse notebooks
    input_batch = validation_results['data']
    parser = Parser()
    parsed_input_batch = parser.parse_notebooks(input_batch)

    # Remove unnecessary keys from metadata
    for item in parsed_input_batch['items']:
        for key in keys_to_remove:
            if key in item['metadata']['data']:
                del item['metadata']['data'][key]

    # Filter problematic notebooks
    cleaned_batch = extract_header_issues(parsed_input_batch)

    # Validate and export parsed batch
    parsed_input_batch_json_str = json.dumps(cleaned_batch)
    if is_valid_json(parsed_input_batch_json_str):
        with open(f'{config.get("output_dir", settings.LWC_OUTPUT_DIR)}/parsed_input_batch.jsonl', 'w') as outfile:
            for item in parsed_input_batch['items']:
                if item['metadata']['status'] == 'ERROR':
                    continue
                json.dump(item, outfile)
                outfile.write('\n')
        print("Exported parsed_input_batch as a JSONL file.")
    else:
        raise ValueError("The parsed_input_batch is not a valid JSON.")

    # Process and upload JSONL files
    process_jsonl(input_file_path, output_file_path)
    split_jsonl_to_json(output_file_path, config.get("json_output_dir", json_output_directory))
    clean_json_output_directory = f'{config.get("output_dir", settings.LWC_OUTPUT_DIR)}/{config.get("input_sheet_name", input_sheet_name)}'
    os.makedirs(clean_json_output_directory, exist_ok=True)
    update_google_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), 'delivery', output_file_path)
    validator_results = main_validator(config.get("json_output_dir", json_output_directory), email_list, clean_json_output_directory)

    if validator_results['status'] == 'failed':
        print("❌ Client Validator failed")
        print(f'Error Sheet link: {validator_results["sheet_url"]}')
        zip_folder_with_timestamp(config.get("json_output_dir", json_output_directory))
        zip_folder_with_timestamp(clean_json_output_directory)
        data = {
            "stage": "client_validator",
            "status": validation_results['status'],
            "total_number_collab_links": total_number_collab_links,
            "issue_sheet_link": validator_results["sheet_url"],
            "count_of_collabs_with_issues": validation_results['count_of_collabs_with_issues']
        }
        return data

    zip_folder_with_timestamp(config.get("json_output_dir", json_output_directory))
    lwc_zip = zip_folder_with_timestamp(clean_json_output_directory)
    new_sheet_info = copy_specific_tabs_google_sheet(drive_service, config.get('input_sheet_id', input_sheet_id), email_list, tab_names)
    send_email_notification_with_zip_folder(
        sender_email=SENDER_EMAIL,
        sender_password=SENDER_PASSWORD,
        recipient_emails=email_list,
        sheet_url=new_sheet_info["new_sheet_url"],
        batch=config.get('input_sheet_name', input_sheet_name),
        project="LWC",
        zip_file_path=lwc_zip
    )
    return 'done'