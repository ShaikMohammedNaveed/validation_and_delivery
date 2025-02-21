import os
from dotenv import load_dotenv
from delivery_workflow.apex import run_apex_google_drive, run_apex_json_file, run_apex_sheet
from delivery_workflow.lwc import run_lwc_google_drive, run_lwc_json_file, run_lwc_sheet
from delivery_workflow.notify import send_email_notification_apex, send_email_notification_json_only, send_lwc_issue_email_notification
from flask import request, Response, jsonify, request
import json
import re
from googleapiclient.discovery import build
from google.oauth2 import service_account
from werkzeug.utils import secure_filename

# Load environment variables from .env file
load_dotenv()

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")

def validate_sheet_link(link: str) -> bool:
    """Validate if the link is a valid Google Sheets link."""
    pattern = r'^(https?:\/\/docs\.google\.com\/spreadsheets\/d\/[a-zA-Z0-9-_]+)'
    return bool(re.search(pattern, link))

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

def validate_sheet_link(link: str) -> bool:
    """Validate if the link is a valid Google Sheets link."""
    pattern = r'^(https?:\/\/docs\.google\.com\/spreadsheets\/d\/[a-zA-Z0-9-_]+)'
    return bool(re.search(pattern, link))

def deliver_notebook(module: str, input_data: str, delivery_type: str, emails: list[str], process_type: str, batch_name: str, validate: bool, json_file=None):
    if not emails or not isinstance(emails, list) or not all(isinstance(email, str) for email in emails):
        return Response("❌ Invalid or missing email recipients. Provide a comma-separated list of emails.", status=400)

    from delivery_workflow.config import settings
    import re

    # Helper functions for validation
    def is_valid_google_id(id_str: str) -> bool:
        if not id_str or not isinstance(id_str, str):
            return False
        return bool(re.match(r'^[a-zA-Z0-9-_]+$', id_str))

    def is_valid_dir_path(path: str) -> bool:
        if not path or not isinstance(path, str):
            return False
        return bool(re.match(r'^[a-zA-Z0-9_/\\-]+$', path))

    apex_config = {
        "input_sheet_id": request.form.get("apex_input_sheet_id", settings.APEX_INPUT_SHEET_ID),
        "input_sheet_name": request.form.get("apex_input_sheet_name", settings.APEX_INPUT_SHEET_NAME),
        "task_link_column": request.form.get("apex_task_link_column", settings.APEX_TASK_LINK_COLUMN),
        "output_dir": request.form.get("apex_output_dir", settings.APEX_OUTPUT_DIR),
        "json_output_dir": request.form.get("apex_json_output_dir", settings.APEX_JSON_OUTPUT_DIR),
        "gdrive_dir_folder_id_collabs": request.form.get("apex_gdrive_dir_folder_id_collabs", settings.APEX_GDRIVE_DIR_FOLDER_ID_COLLABS),
        "google_drive_json_folder_id": request.form.get("apex_google_drive_json_folder_id", settings.APEX_GOOGLE_DRIVE_JSON_FOLDER_ID)
    }

    lwc_config = {
        "input_sheet_id": request.form.get("lwc_input_sheet_id", settings.LWC_INPUT_SHEET_ID),
        "input_sheet_name": request.form.get("lwc_input_sheet_name", settings.LWC_INPUT_SHEET_NAME),
        "task_link_column": request.form.get("lwc_task_link_column", settings.LWC_TASK_LINK_COLUMN),
        "output_dir": request.form.get("lwc_output_dir", settings.LWC_OUTPUT_DIR),
        "json_output_dir": request.form.get("lwc_json_output_dir", settings.LWC_JSON_OUTPUT_DIR),
        "gdrive_dir_folder_id_collabs": request.form.get("lwc_gdrive_dir_folder_id_collabs", settings.LWC_GDRIVE_DIR_FOLDER_ID_COLLABS),
        "google_drive_json_folder_id": request.form.get("lwc_google_drive_json_folder_id", settings.LWC_GOOGLE_DRIVE_JSON_FOLDER_ID)
    }

    try:
        config = apex_config if module == "apex" else lwc_config

        if module == "lwc":
            if not is_valid_google_id(config["input_sheet_id"]):
                return Response("❌ Invalid LWC Input Sheet ID. Must be a valid Google Sheet ID (e.g., 1XEs_8KeOAkMp5Nk83v1RzncoXdA7NYLWE8op35vJ41A).", status=400)
            if not config["input_sheet_name"] or not isinstance(config["input_sheet_name"], str):
                return Response("❌ Invalid LWC Sheet Name. Must be a non-empty string (e.g., lwc_batch).", status=400)
            if not config["task_link_column"] or not isinstance(config["task_link_column"], str):
                return Response("❌ Invalid LWC Task Link Column. Must be a non-empty string (e.g., colab_task_link).", status=400)
            if not is_valid_dir_path(config["output_dir"]):
                return Response("❌ Invalid LWC Output Dir. Must be a valid directory path (e.g., output/lwc).", status=400)
            if not is_valid_dir_path(config["json_output_dir"]):
                return Response("❌ Invalid LWC JSON Output Dir. Must be a valid directory path (e.g., output/lwc/json_files).", status=400)
            if not is_valid_google_id(config["gdrive_dir_folder_id_collabs"]):
                return Response("❌ Invalid LWC Collab Folder ID. Must be a valid Google Drive Folder ID (e.g., 1vKgRppT7Lq9J5QK_Z7VOFWxKLQygTVfn).", status=400)
            if not is_valid_google_id(config["google_drive_json_folder_id"]):
                return Response("❌ Invalid LWC JSON Folder ID. Must be a valid Google Drive Folder ID (e.g., 1vKgRppT7Lq9J5QK_Z7VOFWxKLQygTVfn).", status=400)

            # Ensure directories exist and are writable
            try:
                os.makedirs(config["output_dir"], exist_ok=True)
                os.makedirs(config["json_output_dir"], exist_ok=True)
            except Exception as e:
                return Response(f"❌ Cannot create or access LWC directories: {str(e)}", status=400)

        elif module == "apex":
            # Similar validation for Apex config (optional, for consistency)
            if not is_valid_google_id(config["input_sheet_id"]):
                return Response("❌ Invalid Apex Input Sheet ID. Must be a valid Google Sheet ID (e.g., 1mFUh8Yhhqd3mtT3fJr8qu8gPuGVX8kGJ-UbM_PY3AJQ).", status=400)
            # ... (similar validations for other Apex fields)

        if process_type == "json":
            if not json_file or json_file.filename == '':
                return Response("❌ No JSON file uploaded or filename is empty.", status=400)
            filename = secure_filename(json_file.filename)
            if not filename.endswith('.json'):
                return Response("❌ Uploaded file must be a .json file.", status=400)

            try:
                # Read file as bytes and ensure it's bytes-like
                file_bytes = json_file.read()
                if not isinstance(file_bytes, (bytes, bytearray)):
                    return Response("❌ Invalid file content: Expected bytes-like object, got string.", status=400)

                # Try to decode with UTF-8 first, then fall back to other encodings
                json_content = None
                encodings = ['utf-8', 'utf-16', 'latin-1']
                for encoding in encodings:
                    try:
                        json_content = file_bytes.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                if json_content is None:
                    return Response("❌ Failed to decode JSON file with supported encodings.", status=400)

                # Parse JSON with error handling for structure
                try:
                    json_data = json.loads(json_content)
                    if not isinstance(json_data, (dict, list)):
                        return Response("❌ JSON data must be an object or array.", status=400)
                except json.JSONDecodeError as je:
                    return Response(f"❌ Invalid JSON file format: {str(je)}", status=400)

                # Process JSON file
                if module == "lwc":
                    result = run_lwc_json_file(
                        json_data=json_data,
                        delivery_type=delivery_type,
                        emails=emails,
                        validate=validate,
                        **config
                    )
                else:  # apex
                    result = run_apex_json_file(
                        json_data=json_data,
                        delivery_type=delivery_type,
                        emails=emails,
                        validate=validate,
                        **config
                    )

                return Response(f"✅ Delivery completed: {result}", status=200)

            except Exception as e:
                return Response(f"❌ Error processing JSON file: {str(e)}", status=400)

        # Handle other process types (drive, sheet)
        elif process_type == "drive":
            if not input_data or not validate_notebook_link(input_data):
                return Response("❌ Invalid Drive folder link. Must be a valid Google Drive folder link.", status=400)
            if module == "lwc":
                result = run_lwc_google_drive(
                    folder_link=input_data,
                    delivery_type=delivery_type,
                    emails=emails,
                    validate=validate,
                    **config
                )
            else:  # apex
                result = run_apex_google_drive(
                    folder_link=input_data,
                    delivery_type=delivery_type,
                    emails=emails,
                    validate=validate,
                    **config
                )
            return Response(f"✅ Delivery completed: {result}", status=200)

        elif process_type == "sheet":
            if not input_data or not validate_sheet_link(input_data):
                return Response("❌ Invalid Sheets link. Must be a valid Google Sheets link.", status=400)
            if module == "lwc":
                result = run_lwc_sheet(
                    sheet_link=input_data,
                    delivery_type=delivery_type,
                    emails=emails,
                    validate=validate,
                    **config
                )
            else:  # apex
                result = run_apex_sheet(
                    sheet_link=input_data,
                    delivery_type=delivery_type,
                    emails=emails,
                    validate=validate,
                    **config
                )
            return Response(f"✅ Delivery completed: {result}", status=200)

        else:
            return Response("❌ Unsupported process type.", status=400)

    except ValueError as ve:
        return Response(f"❌ Validation error: {str(ve)}", status=400)
    except json.JSONDecodeError as je:
        return Response(f"❌ JSON parsing error: {str(je)}", status=400)
    except Exception as e:
        print(f"Unexpected error in deliver_notebook: {str(e)}")
        return Response(f"❌ Delivery failed: An internal server error occurred. Contact support with details.", status=500)