import json
import os
import re
import logging
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]

# Configure logging
logging.basicConfig(filename="moved_files.log", level=logging.INFO, format="%(asctime)s - %(message)s")

def get_drive_service():
    """Helper function to get Google Drive service using environment credentials."""
    if not GOOGLE_CREDENTIALS:
        raise ValueError("Google credentials not found in environment variables")
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDENTIALS),
        scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)

def get_sheets_service():
    """Helper function to get Google Sheets service using environment credentials."""
    if not GOOGLE_CREDENTIALS:
        raise ValueError("Google credentials not found in environment variables")
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDENTIALS),
        scopes=SCOPES
    )
    return build('sheets', 'v4', credentials=creds)

def create_google_drive_folder(batch_name: str, parent_folder_id: str):
    """
    Checks if a folder exists in Google Drive, and creates it only if it does not exist.
    
    :param batch_name: The name of the batch (used in folder name)
    :param parent_folder_id: Google Drive parent folder ID where the new folder will be created
    :return: Folder ID (existing or newly created)
    """
    try:
        service = get_drive_service()

        # Generate folder name with current date
        current_date = datetime.today().strftime("%Y-%m-%d")
        folder_name = f"{batch_name}-{current_date}"

        # Check if the folder already exists
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        existing_folders = results.get("files", [])

        if existing_folders:
            folder_id = existing_folders[0]["id"]
            print(f"✅ Skipping create step, folder '{folder_name}' already exists. (ID: {folder_id})")
            return folder_id

        # Metadata for the new folder
        folder_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id]
        }

        # Create the folder
        folder = service.files().create(body=folder_metadata, fields="id").execute()
        
        print(f"✅ Folder Created: {folder_name} (ID: {folder['id']})")
        return folder["id"]

    except Exception as e:
        print(f"❌ Error creating folder: {e}")
        return None

def get_sheet_id(sheet_url: str) -> str:
    """Extract the Google Sheet ID from the URL"""
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url)
    return match.group(1) if match else None

def get_google_sheet_data(sheet_id: str, tab_name: str) -> list:
    """Read the specified Google Sheet tab and return a list of file links"""
    sheets_service = get_sheets_service()
    try:
        result = sheets_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{tab_name}!A:Z").execute()
        values = result.get("values", [])
    except Exception as e:
        print(f"Error accessing Google Sheet tab '{tab_name}': {e}")
        return []
    if not values:
        print(f"No data found in the sheet tab '{tab_name}'.")
        return []
    # Find the index of 'colab_task_link' column
    headers = values[0]
    if "colab_task_link" not in headers:
        print("Error: 'colab_task_link' column not found in the sheet.")
        return []
    link_index = headers.index("colab_task_link")
    links = [row[link_index] for row in values[1:] if len(row) > link_index]
    return links

def extract_file_id(link: str) -> str:
    """
    Given a Colab link like:
      https://colab.research.google.com/drive/1AbCxyzEXAMPLE123?usp=sharing
    return the file ID: 1AbCxyzEXAMPLE123

    If it fails, return "".
    """
    pattern = re.compile(r"/drive/([a-zA-Z0-9_\-]+)")
    match = pattern.search(link)
    return match.group(1) if match else ""

def move_file(file_id: str, dest_folder_id: str):
    """Move a file to a different folder and log the result"""
    try:
        service = get_drive_service()
        # Get the file's current parents and name
        file = service.files().get(fileId=file_id, fields="parents, name").execute()
        previous_parents = ",".join(file.get("parents", []))
        file_name = file.get("name", "Unknown")
        # Move the file to the destination folder
        service.files().update(
            fileId=file_id,
            addParents=dest_folder_id,
            removeParents=previous_parents,
            fields="id, parents"
        ).execute()
        # Log success
        log_message = f"Moved file: {file_name} (ID: {file_id})"
        print(log_message)
        logging.info(log_message)
    except Exception as e:
        error_message = f"Error moving file {file_id}: {e}"
        print(error_message)
        logging.error(error_message)

def move_files_from_sheet(tab_name: str, sheet_url: str, dest_folder_id: str):
    """
    Main function to read links from Google Sheets and move the files.

    :param tab_name: Name of the sheet tab to read links from
    :param sheet_url: URL of the Google Sheet
    :param dest_folder_id: Destination Google Drive folder ID
    """
    sheet_id = get_sheet_id(sheet_url)
    if not sheet_id:
        print("Invalid Google Sheets URL.")
        return
    links = get_google_sheet_data(sheet_id, tab_name)
    if not links:
        print(f"No valid file links found in the sheet tab '{tab_name}'.")
        return
    for link in links:
        file_id = extract_file_id(link)
        if file_id:
            move_file(file_id, dest_folder_id)
        else:
            log_message = f"Invalid file link: {link}"
            print(log_message)
            logging.warning(log_message)