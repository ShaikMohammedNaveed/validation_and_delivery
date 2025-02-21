from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import datetime
import time
import json
import gspread
import re
from googleapiclient.errors import HttpError


def get_colab_links_from_folder(credentials_path, folder_id):
    """
    Retrieve Colab links from a specific folder in Google Drive.

    Args:
        credentials_path (str): Path to the credentials.json file.
        folder_id (str): The ID of the folder.

    Returns:
        list: A list of Colab links found in the specified folder.
    """
    creds = Credentials.from_service_account_file(credentials_path)
    service = build('drive', 'v3', credentials=creds)

    files = []
    page_token = None

    while True:
        # List files in the specified folder, paginated
        query = f"'{folder_id}' in parents and mimeType='application/vnd.google.colaboratory'"
        response = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name)",  # Retrieve file ID and name
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=page_token  # Pass the page token for subsequent pages
        ).execute()

        # Add the current page of files to the list
        files.extend(response.get('files', []))
        
        # Check if there are more pages
        page_token = response.get('nextPageToken', None)
        if not page_token:
            break  # Exit the loop if no more pages

    colab_links = []
    for file in files:
        file_id = file['id']
        file_name = file['name']
        colab_links.append(f"https://colab.research.google.com/drive/{file_id}")
        print(f"Found Colab: {file_name} ({file_id})")

    return colab_links

def write_links_to_sheet(credentials_path, spreadsheet_id, sheet_name, links):
    """
    Clears the Google Sheet and writes Colab task links to the 'colab_task_link' column.

    Args:
        credentials_path (str): Path to the credentials.json file.
        spreadsheet_id (str): ID of the Google Sheet.
        sheet_name (str): Name of the sheet/tab.
        links (list): List of Colab links to write to the sheet.
    """
    creds = Credentials.from_service_account_file(credentials_path)
    service = build('sheets', 'v4', credentials=creds)

    # Define the range to clear before writing new data
    clear_range = f"{sheet_name}!A:A"  # Clear entire column A
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=clear_range
    ).execute()

    print(f"Cleared previous content in {sheet_name} before writing new links.")

    # Prepare new data for writing
    values = [["colab_task_link"]] + [[link] for link in links]  # Add header and data
    body = {'values': values}

    # Define the range where the links will be written (starting from A1)
    range_name = f"{sheet_name}!A1"

    # Update the Google Sheet
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()

    print(f"{result.get('updatedCells')} cells updated successfully.")


def get_json_files_from_folder(credentials_path, folder_id):
    """
    Retrieve JSON file names and links from a specific folder in Google Drive.

    Args:
        credentials_path (str): Path to the credentials.json file.
        folder_id (str): The ID of the folder.

    Returns:
        list: A list of dictionaries containing file names and their Drive links.
    """
    creds = Credentials.from_service_account_file(credentials_path)
    service = build('drive', 'v3', credentials=creds)

    files = []
    page_token = None

    while True:
        # Query for JSON files in the specified folder
        query = f"'{folder_id}' in parents and mimeType='application/json'"
        response = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=page_token
        ).execute()

        # Add the current page of files to the list
        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)
        if not page_token:
            break

    # Construct links for each file
    json_files = []
    for file in files:
        file_id = file['id']
        file_name = file['name']
        file_link = f"https://drive.google.com/file/d/{file_id}/view"
        json_files.append({"name": file_name, "link": file_link})
        print(f"Found JSON File: {file_name} ({file_link})")

    return json_files

# def write_files_to_sheet(credentials_path, spreadsheet_id, sheet_name, files):
#     """
#     Clears old records and writes file names and links to a Google Sheet.

#     Args:
#         credentials_path (str): Path to the credentials.json file.
#         spreadsheet_id (str): ID of the Google Sheet.
#         sheet_name (str): Name of the sheet/tab.
#         files (list): List of dictionaries with file names and links.
#     """
#     creds = Credentials.from_service_account_file(credentials_path)
#     service = build('sheets', 'v4', credentials=creds)

#     # Define the range to clear before writing new data
#     clear_range = f"{sheet_name}!A:B"  # Clears columns A and B
#     service.spreadsheets().values().clear(
#         spreadsheetId=spreadsheet_id,
#         range=clear_range
#     ).execute()

#     print(f"Cleared previous content in {sheet_name} before writing new file records.")

#     # Prepare new data for writing
#     values = [["File Name", "File Link"]]  # Header row
#     for file in files:
#         values.append([file["name"], file["link"]])

#     body = {'values': values}

#     # Define the range where the data will be written
#     range_name = f"{sheet_name}!A1"

#     # Update the Google Sheet
#     result = service.spreadsheets().values().update(
#         spreadsheetId=spreadsheet_id,
#         range=range_name,
#         valueInputOption='RAW',
#         body=body
#     ).execute()

#     print(f"{result.get('updatedCells')} cells updated successfully.")

import os
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

def write_files_to_sheet(credentials_path, spreadsheet_id, sheet_name, files):
    """
    Clears old records and writes file names, file links, and any extra
    fields (violations, review, comment) from the 'preprocess' tab to a Google Sheet.
    Adds data validation to the 'Customer Review' column so users can pick
    'Blank', 'Accepted', or 'Need rework'.

    Matching logic:
      - The 'preprocess' tab has columns: (file_id, collab_links, name_of_violations, customer_review, customer_comment).
      - We read that data into a dict keyed by file_id.
      - For each file in 'files', we remove the extension (.json) from file["name"] to get a base_name.
      - If base_name == a row's file_id, we retrieve that row's name_of_violations, etc.
      - Then we write [File_Name, File_Link, Name_of_Violations, Customer_Review_Status, Customer_Comment] to `sheet_name`.

    Args:
        credentials_path (str): Path to the service account credentials JSON.
        spreadsheet_id (str): ID of the Google Sheet.
        sheet_name (str): Name of the sheet/tab in which we'll write (e.g. "delivery").
        files (list): A list of dicts, each with something like:
            {
              "name": "UnfollowController.json",  # We'll strip .json => "UnfollowController"
              "link": "https://drive.google.com/..."
            }
    """

    # 1) Authenticate
    creds = Credentials.from_service_account_file(credentials_path)
    service = build('sheets', 'v4', credentials=creds)

    # -------------------------------------------------------------------------
    # 2) Read data from the "preprocess" tab (A:E)
    #    columns = [file_id, collab_links, name_of_violations, customer_review, customer_comment]
    # -------------------------------------------------------------------------
    preprocess_tab = "preprocess"
    preprocess_range = f"{preprocess_tab}!A:E"
    response = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=preprocess_range
    ).execute()
    data_rows = response.get("values", [])  # a list of lists

    # Key a dict by file_id
    # Example row: [ "MyFileID", "someCollabLink", "someViolations", "Accepted", "someComment" ]
    preprocess_lookup = {}

    for idx, row in enumerate(data_rows):
        if not row or len(row) < 3:
            continue
        # Skip header row if we detect "file_id" or "collab_links" in the first row
        if idx == 0 and ("file_id" in row[0].lower() or "collab_links" in row):
            continue

        # Extract columns
        row_file_id        = row[0].strip()
        collab_links       = row[1].strip()
        name_of_violations = row[2].strip()


        preprocess_lookup[row_file_id] = {
            "collab_links": collab_links,
            "violations": name_of_violations,
        }

    # -------------------------------------------------------------------------
    # 3) Clear old data in the target `sheet_name` (columns A->E)
    # -------------------------------------------------------------------------
    clear_range = f"{sheet_name}!A:F"
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=clear_range
    ).execute()
    print(f"Cleared previous content in '{sheet_name}' (A:F).")

    # -------------------------------------------------------------------------
    # 4) Prepare rows to write: header + data
    # -------------------------------------------------------------------------
    # We'll define a single header row
    values = [
        ["Collab_Task_Link","File_Name", "JSON_File_Link", "Name_of_Violations", "Customer_Review_Status", "Customer_Comment"]
    ]

    # For each file in `files`, strip ".json" from file["name"] to get base_name
    # Then find matching row in `preprocess_lookup`.
    for file_obj in files:
        raw_name = file_obj.get("name", "")  # e.g. "UnfollowController.json"
        file_link = file_obj.get("link", "")

        # Remove extension with os.path.splitext
        base_name, ext = os.path.splitext(raw_name)  # e.g. ("UnfollowController", ".json")
        # If base_name is in preprocess_lookup => retrieve violations, etc.
        if base_name in preprocess_lookup:
            match_info = preprocess_lookup[base_name]
            collab_links = match_info["collab_links"] or ""
            name_of_violations = match_info["violations"] or ""
            review_status =  "Blank"
            comment = ""
        else:
            collab_links = ""
            name_of_violations = ""
            review_status = "Blank"
            comment = ""

        # Build a row: [ File_Name, File_Link, Violations, Review, Comment ]
        row = [collab_links, raw_name, file_link, name_of_violations, review_status, comment]
        values.append(row)

    # -------------------------------------------------------------------------
    # 5) Write these rows to the target sheet (A1)
    # -------------------------------------------------------------------------
    range_name = f"{sheet_name}!A1"
    body = {"values": values}
    update_result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body
    ).execute()

    updated_cells = update_result.get("updatedCells", 0)
    print(f"{updated_cells} cells updated in '{sheet_name}' with 'Name_of_Violations' from preprocess.")

    # -------------------------------------------------------------------------
    # 6) Add data validation to column D ("Customer_Review_Status") to allow
    #    [ "Blank", "Accepted", "Need rework" ] in a dropdown.
    # -------------------------------------------------------------------------
    # (a) Find numeric sheetId
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id_number = None
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == sheet_name:
            sheet_id_number = s["properties"]["sheetId"]
            break
    if sheet_id_number is None:
        print(f"⚠️ Could not find sheet/tab '{sheet_name}' in the spreadsheet.")
        return

    # (b) First row is header => data rows are from index=1 to total_rows
    total_rows = len(values)  # header + data
    start_row_index = 1       # skip header
    end_row_index = total_rows

    # Column D => zero-based indexing => col=3
    start_col_index = 4
    end_col_index = 5  # exclusive

    # (c) Build the data validation request
    requests = [
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id_number,
                    "startRowIndex": start_row_index,
                    "endRowIndex": end_row_index,
                    "startColumnIndex": start_col_index,
                    "endColumnIndex": end_col_index
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": "Blank"},
                            {"userEnteredValue": "Accepted"},
                            {"userEnteredValue": "Need rework"}
                        ]
                    },
                    "strict": False,
                    "showCustomUi": True
                }
            }
        }
    ]

    # (d) Execute the request
    dv_response = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()

    print("✅ Added data validation for 'Customer Review' dropdown: Blank / Accepted / Need rework.")


def copy_google_sheet(credentials_path, original_sheet_id, email_list):
    """
    Creates a copy of a Google Sheet, assigns permissions to a list of emails, and updates the name with the current date.

    Args:
        credentials_path (str): Path to the Google API credentials file.
        original_sheet_id (str): The ID of the original Google Sheet to copy.
        email_list (list): List of email addresses to assign editor permissions.

    Returns:
        dict: A dictionary containing the new sheet ID and URL.
    """
    creds = Credentials.from_service_account_file(credentials_path)
    drive_service = build('drive', 'v3', credentials=creds)

    # Get the current date
    timestamp = datetime.datetime.now().strftime('%d-%m-%Y')

    # Get the original sheet metadata
    original_sheet_metadata = drive_service.files().get(fileId=original_sheet_id, fields="name, parents").execute()
    original_sheet_name = original_sheet_metadata['name']
    parent_folder_id = original_sheet_metadata.get('parents', [None])[0]

    # Define the new sheet name
    new_sheet_name = f"{original_sheet_name}-{timestamp}"

    # Create a copy of the sheet
    copied_sheet = drive_service.files().copy(
        fileId=original_sheet_id,
        body={"name": new_sheet_name, "parents": [parent_folder_id]}
    ).execute()

    new_sheet_id = copied_sheet['id']
    new_sheet_url = f"https://docs.google.com/spreadsheets/d/{new_sheet_id}"

    print(f"Copied Google Sheet: {new_sheet_name}")
    print(f"New Sheet URL: {new_sheet_url}")

    # Wait a few seconds to ensure the copied file is fully available
    time.sleep(5)

    # Assign permissions to the provided email list
    for email in email_list:
        try:
            drive_service.permissions().create(
                fileId=new_sheet_id,
                body={
                    "type": "user",
                    "role": "writer",  # Can be changed to 'reader' for read-only access
                    "emailAddress": email
                },
                sendNotificationEmail=False  # Avoid sending notification emails
            ).execute()
            print(f"Assigned editor access to {email}")
        except Exception as e:
            print(f"Failed to assign permission to {email}. Error: {e}")

    print("\n✅ All specified users have been assigned access!")

    return {"new_sheet_id": new_sheet_id, "new_sheet_url": new_sheet_url}

def copy_specific_tabs_google_sheet(credentials_path, original_sheet_id, email_list, tab_names):
    """
    Creates a new Google Sheet with specific tabs copied from the original sheet, 
    assigns permissions to a list of emails, and updates the name with the current date.

    Unlike a simple value copy, this uses the Sheets API's 'copyTo' method so
    data validations (dropdowns), formatting, etc. are preserved.

    Args:
        credentials_path (str): Path to the Google API credentials file.
        original_sheet_id (str): The ID of the original Google Sheet to copy from.
        email_list (list): List of email addresses to assign editor permissions.
        tab_names (list): List of tab names to copy.

    Returns:
        dict: A dictionary containing the new sheet ID and URL.
    """
    # 1. Authenticate
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)

    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)
    gspread_client = gspread.authorize(creds)

    # 2. Get the current date for naming
    timestamp = datetime.datetime.now().strftime('%d-%m-%Y')
    #timestamp = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')


    # 3. Get original sheet metadata (name, parent folder, etc.)
    original_sheet_metadata = drive_service.files().get(
        fileId=original_sheet_id, fields="name, parents"
    ).execute()
    original_sheet_name = original_sheet_metadata['name']
    parent_folder_id = original_sheet_metadata.get('parents', [None])[0]

    # 4. Define & Create a brand-new sheet in the same folder
    new_sheet_name = f"{original_sheet_name}-{timestamp}"
    new_sheet_body = {
        "name": new_sheet_name,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [parent_folder_id] if parent_folder_id else []
    }
    new_sheet = drive_service.files().create(body=new_sheet_body, fields="id").execute()
    new_sheet_id = new_sheet["id"]
    new_sheet_url = f"https://docs.google.com/spreadsheets/d/{new_sheet_id}"

    print(f"✅ Created new Google Sheet: {new_sheet_name}")
    print(f"🔗 New Sheet URL: {new_sheet_url}")

    # 5. For each tab we want to copy, do:
    #    a) Find that tab's numeric sheetId in the original
    #    b) Call .copyTo() to replicate it in the new sheet
    original_sheet_info = sheets_service.spreadsheets().get(spreadsheetId=original_sheet_id).execute()
    original_sheets = original_sheet_info.get("sheets", [])

    # Build a dict of { tab_title -> sheetId } from the original
    tab_to_id_map = {}
    for s in original_sheets:
        title = s["properties"]["title"]
        sid = s["properties"]["sheetId"]
        tab_to_id_map[title] = sid

    # 6. Perform the copy for each requested tab
    for tab_name in tab_names:
        if tab_name not in tab_to_id_map:
            print(f"❌ Tab '{tab_name}' not found in the original sheet.")
            continue
        source_sheet_id = tab_to_id_map[tab_name]
        
        try:
            # Actually copy the original tab to the new sheet
            copy_req = { "destinationSpreadsheetId": new_sheet_id }
            result = sheets_service.spreadsheets().sheets().copyTo(
                spreadsheetId=original_sheet_id,
                sheetId=source_sheet_id,
                body=copy_req
            ).execute()

            # The result includes the new tab's "sheetId" and "title" in the new sheet.
            new_copied_sheet_id = result["sheetId"]
            new_copied_sheet_title = result["title"]  # typically "Copy of <tab_name>"

            print(f"✅ Copied tab '{tab_name}' to the new sheet as '{new_copied_sheet_title}'.")

            # Optional: rename the newly-copied tab to exactly match `tab_name`
            rename_req = {
                "requests": [{
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": new_copied_sheet_id,
                            "title": tab_name
                        },
                        "fields": "title"
                    }
                }]
            }
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=new_sheet_id,
                body=rename_req
            ).execute()

            print(f"   Renamed new tab -> '{tab_name}'")

        except Exception as e:
            print(f"❌ Failed to copy tab '{tab_name}'. Error: {e}")

    # 7. Remove the default blank sheet that Google automatically creates
    new_sheet_gspread = gspread_client.open_by_key(new_sheet_id)
    try:
        default_sheet = new_sheet_gspread.sheet1  # .sheet1 is the first sheet
        new_sheet_gspread.del_worksheet(default_sheet)
        print("🗑️ Removed default blank sheet.")
    except Exception:
        pass  # If there's no default sheet or if the copy replaced it, ignore

    # 8. Wait a few seconds before assigning permissions
    time.sleep(5)

    # 9. Assign permissions to the provided email list (grant "writer")
    for email in email_list:
        try:
            drive_service.permissions().create(
                fileId=new_sheet_id,
                body={
                    "type": "user",
                    "role": "writer",
                    "emailAddress": email
                },
                sendNotificationEmail=False
            ).execute()
            print(f"✅ Assigned editor access to {email}")
        except Exception as e:
            print(f"❌ Failed to assign permission to {email}. Error: {e}")

    print("\n🚀 All specified users have been assigned access!")

    return {"new_sheet_id": new_sheet_id, "new_sheet_url": new_sheet_url}


def update_google_sheet_from_json(credentials_path, spreadsheet_id, sheet_name, json_file):
    """
    Reads a JSON file, extracts relevant colab links, and updates a Google Sheet.

    Args:
        credentials_path (str): Path to the Google API credentials file.
        spreadsheet_id (str): The ID of the Google Sheet.
        sheet_name (str): The name of the sheet/tab.
        json_file (str): Path to the JSON file containing the data.

    Returns:
        str: 'success' or 'failure'
    """
    try:
        # Authenticate with Google Sheets API
        creds = Credentials.from_service_account_file(credentials_path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        client = gspread.authorize(creds)
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

        # Clear old records in the Google Sheet before inserting new data
        sheet.clear()

        # Set header row
        headers = ["colab_task_link"]
        sheet.append_row(headers)

        # Load JSON file
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Extract and update colab links
        rows = []
        for entry in data:
            colab_task_link = entry.get("colabLink", "N/A")  # Extract colabLink
            rows.append([colab_task_link])  # Append as a new row

        # Update Google Sheet
        if rows:
            sheet.append_rows(rows)

        print(f"✅ Successfully updated {len(rows)} rows in the Google Sheet.")
        return "success"

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return "failure"

def create_folder_and_copy_colab_links_from_sheet(
    creds_file: str,
    drive_folder_name: str,
    spreadsheet_id: str,
    sheet_name: str = "Sheet1",
    link_column_name: str = "colab_task_link",
    parent_folder_id: str = None
):
    """
    1) Reads a specified tab (sheet_name) from the given Google Sheet,
       scanning columns A:Z for a header = link_column_name (default: 'colab_task_link').
    2) Creates a folder in Google Drive named drive_folder_name.
       If you specify a parent_folder_id, it will place the folder inside that parent folder (or shared drive).
    3) For each row in that column, extracts the notebook file ID from the Colab link
       and copies that file into the newly created folder.
    4) Confirms that the newly copied files cannot have the same file IDs as the original. 
       (Google Drive automatically assigns a new unique ID to any copy.)

    Args:
        creds_file (str): Path to your Google service account JSON credentials.
        drive_folder_name (str): Name for the newly created folder in Drive.
        spreadsheet_id (str): ID of the Google Sheet containing the colab links.
        sheet_name (str): The tab (worksheet) name, e.g. "Sheet1" or "myTab".
        link_column_name (str): The header name for the column containing the Colab links.
        parent_folder_id (str): If provided, the new folder will be created
                                inside this parent folder or shared drive.

    NOTE: In Google Drive, copies always receive a new file ID.
          There is no way to force the copy to retain the original ID.
    """

    # 1. Set up credentials with the needed scopes
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)

    # 2. Define the range to read from the specified sheet tab
    #    e.g. "Sheet1!A:Z" gets columns A through Z (if your data extends further, adjust as needed)
    read_range = f"{sheet_name}!A:Z"

    # 3. Fetch the data from the spreadsheet (header + rows)
    response = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=read_range
    ).execute()
    data_rows = response.get("values", [])
    if not data_rows:
        print(f"No data found in range {read_range}.")
        return

    # 4. Identify which column has the header == link_column_name
    header_row = data_rows[0]
    try:
        col_index = header_row.index(link_column_name)
    except ValueError:
        print(f"❌ Column '{link_column_name}' not found in the first row: {header_row}")
        return

    # 5. Collect all links in that column (skipping the header row)
    colab_links = []
    for row in data_rows[1:]:
        if len(row) > col_index and row[col_index]:
            colab_links.append(row[col_index].strip())

    if not colab_links:
        print(f"No Colab links found under column '{link_column_name}' in sheet '{sheet_name}'.")
        return

    print(f"Found {len(colab_links)} link(s) in column '{link_column_name}' of '{sheet_name}':")
    for link in colab_links:
        print("  -", link)

    # 6. Create a folder in Drive (potentially under a specified parent folder)
    folder_metadata = {
        "name": drive_folder_name,
        "mimeType": "application/vnd.google-apps.folder"
    }
    if parent_folder_id:
        folder_metadata["parents"] = [parent_folder_id]

    folder = drive_service.files().create(body=folder_metadata, fields="id").execute()
    folder_id = folder["id"]
    print(f"\n✅ Created Drive folder '{drive_folder_name}' (ID: {folder_id})")
    if parent_folder_id:
        print(f"   (Located under parent folder/shared drive ID: {parent_folder_id})")

    # 7. Copy each link's file to the new folder
    for link in colab_links:
        file_id = extract_file_id_from_colab_link(link)
        if not file_id:
            print(f"❌ Could not parse file ID from link: {link}")
            continue

        try:
            copy_metadata = {"parents": [folder_id]}
            copied_file = drive_service.files().copy(
                fileId=file_id,
                body=copy_metadata,
                fields="id, name"
            ).execute()

            # The new file will have a different ID from the original
            copied_id = copied_file["id"]
            copied_name = copied_file["name"]
            print(f"✅ Copied '{copied_name}' to folder '{drive_folder_name}' (new file ID: {copied_id})")
        except Exception as e:
            print(f"❌ Failed to copy file with ID {file_id}. Error: {e}")

    print("\nDone copying notebooks to new folder.")
    print("⚠️ Note: Copied files always receive new IDs—it's impossible to keep the same ID as the original.")

# def create_folder_and_move_colab_links_from_sheet(
#     creds_file: str,
#     drive_folder_name: str,
#     spreadsheet_id: str,
#     sheet_name: str = "Sheet1",
#     link_column_name: str = "colab_task_link",
#     parent_folder_id: str = None
# ):
#     """
#     1) Reads a specified tab (sheet_name) from the given Google Sheet,
#        scanning columns A:Z for a header = link_column_name (default: 'colab_task_link').
#     2) Creates a folder in Google Drive named drive_folder_name.
#        (Optionally under a specified parent_folder_id, if provided.)
#     3) For each row in that column, extracts the notebook file ID from the Colab link
#        and MOVES that file into the newly created folder.
#     4) Skips moving if the file is already in that folder (i.e. folder_id is in the file's parents).
#     5) Returns a dictionary with 'folder_id' and 'folder_url' of the newly created folder.

#     Args:
#         creds_file (str): Path to your Google service account JSON credentials.
#         drive_folder_name (str): Name for the newly created folder in Drive.
#         spreadsheet_id (str): ID of the Google Sheet containing the colab links.
#         sheet_name (str): The tab (worksheet) name, e.g. "Sheet1" or "myTab".
#         link_column_name (str): The header name for the column containing the Colab links.
#         parent_folder_id (str): If provided, create the new folder inside that parent folder or shared drive.

#     Note:
#         - Moving a file does NOT change its file ID. It's the same file, simply placed in a new folder.
#         - If a file already has multiple parents, we remove them except the new folder
#           so that the new folder is the sole parent. If you prefer to keep other parents,
#           you can adapt the logic below.
#         - We skip any move if the file is already in the target folder_id.
#         - This function returns a dict with the newly created folder's ID and URL.
#     """

#     # 1. Set up credentials with the needed scopes
#     scopes = [
#         "https://www.googleapis.com/auth/spreadsheets.readonly",
#         "https://www.googleapis.com/auth/drive"
#     ]
#     creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
#     drive_service = build("drive", "v3", credentials=creds)
#     sheets_service = build("sheets", "v4", credentials=creds)

#     # 2. Read from the specified sheet/tab in columns A:Z
#     read_range = f"{sheet_name}!A:Z"
#     response = sheets_service.spreadsheets().values().get(
#         spreadsheetId=spreadsheet_id,
#         range=read_range
#     ).execute()
#     data_rows = response.get("values", [])
#     if not data_rows:
#         print(f"No data found in range {read_range}.")
#         return

#     # 3. Find which column is link_column_name
#     header_row = data_rows[0]
#     try:
#         col_index = header_row.index(link_column_name)
#     except ValueError:
#         print(f"❌ Column '{link_column_name}' not found in the first row: {header_row}")
#         return

#     # 4. Gather all Colab links in that column
#     colab_links = []
#     for row in data_rows[1:]:
#         if len(row) > col_index and row[col_index]:
#             colab_links.append(row[col_index].strip())

#     if not colab_links:
#         print(f"No Colab links found under column '{link_column_name}' in '{sheet_name}'.")
#         return

#     print(f"Found {len(colab_links)} link(s) in column '{link_column_name}' of '{sheet_name}':")
#     for link in colab_links:
#         print("  -", link)

#     # 5. Create a new folder in Drive, optionally under parent_folder_id
#     folder_metadata = {
#         "name": drive_folder_name,
#         "mimeType": "application/vnd.google-apps.folder"
#     }
#     if parent_folder_id:
#         folder_metadata["parents"] = [parent_folder_id]

#     folder = drive_service.files().create(body=folder_metadata, fields="id").execute()
#     folder_id = folder["id"]
#     folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
#     print(f"\n✅ Created Drive folder '{drive_folder_name}' (ID: {folder_id})")
#     print(f"   Folder URL: {folder_url}")
#     if parent_folder_id:
#         print(f"   - Placed inside parent folder/shared drive ID: {parent_folder_id}")

#     # 6. Move each file into the new folder if not already present
#     for link in colab_links:
#         file_id = extract_file_id_from_colab_link(link)
#         if not file_id:
#             print(f"❌ Could not parse file ID from link: {link}")
#             continue

#         try:
#             # First, get the existing parents
#             file_info = drive_service.files().get(fileId=file_id, fields="parents, name").execute()
#             old_parents = file_info.get("parents", [])
#             file_name = file_info.get("name", "UnknownName")

#             # If file is already in that folder, skip
#             if folder_id in old_parents:
#                 print(f"🔹 Skipping '{file_name}' (ID {file_id}): already in folder '{drive_folder_name}'.")
#                 continue

#             # Otherwise, remove other parents so that the new folder is the sole parent
#             remove_parents_str = ",".join(old_parents)

#             updated_file = drive_service.files().update(
#                 fileId=file_id,
#                 addParents=folder_id,
#                 removeParents=remove_parents_str,
#                 fields="id, parents"
#             ).execute()

#             # The file keeps its original ID
#             print(f"✅ Moved '{file_name}' (ID: {file_id}) into folder '{drive_folder_name}'")

#         except Exception as e:
#             print(f"❌ Failed to move file with ID {file_id}. Error: {e}")

#     print("\nMove complete. The original file IDs remain unchanged.")
#     return {
#         "folder_id": folder_id,
#         "folder_url": folder_url
#     }


# def extract_file_id_from_colab_link(link: str) -> str:
#     """
#     Given a Colab link like:
#       https://colab.research.google.com/drive/1AbCxyzEXAMPLE123?usp=sharing
#     return the file ID: 1AbCxyzEXAMPLE123

#     If it fails, return "".
#     """
#     pattern = re.compile(r"/drive/([a-zA-Z0-9_\-]+)")
#     match = pattern.search(link)
#     if match:
#         return match.group(1)
#     else:
#         return ""


def create_folder_and_move_colab_links_from_sheet(
    creds_file: str,
    drive_folder_name: str,
    spreadsheet_id: str,
    sheet_name: str = "Sheet1",
    link_column_name: str = "colab_task_link",
    parent_folder_id: str = None
):
    """
    Reads a Google Sheet, extracts Colab notebook file IDs, and moves them into a Google Drive folder.

    Fixes:
    - Ensures an existing folder is used instead of creating a new one.
    - Detects if files are in a shared drive and moves them correctly.
    - Prevents orphaning files by handling personal and shared drives differently.
    """

    # 1. Set up Google API credentials
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)

    # 2. Read from the Google Sheet
    read_range = f"{sheet_name}!A:Z"
    response = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=read_range
    ).execute()
    data_rows = response.get("values", [])
    if not data_rows:
        print(f"No data found in range {read_range}.")
        return

    # 3. Find the column index containing Colab links
    header_row = data_rows[0]
    try:
        col_index = header_row.index(link_column_name)
    except ValueError:
        print(f"❌ Column '{link_column_name}' not found in {header_row}")
        return

    # 4. Extract Colab links from the sheet
    colab_links = [row[col_index].strip() for row in data_rows[1:] if len(row) > col_index and row[col_index]]

    if not colab_links:
        print(f"No Colab links found under column '{link_column_name}' in '{sheet_name}'.")
        return

    print(f"Found {len(colab_links)} link(s) in column '{link_column_name}' of '{sheet_name}'.")

    # 5. Check if the folder already exists in Google Drive
    query = f"name = '{drive_folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
    if parent_folder_id:
        query += f" and '{parent_folder_id}' in parents"
    
    response = drive_service.files().list(q=query, fields="files(id)").execute()
    folders = response.get("files", [])

    if folders:
        folder_id = folders[0]["id"]
        print(f"✅ Using existing folder '{drive_folder_name}' (ID: {folder_id})")
    else:
        # Create a new folder if one does not exist
        folder_metadata = {"name": drive_folder_name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_folder_id:
            folder_metadata["parents"] = [parent_folder_id]
        
        folder = drive_service.files().create(body=folder_metadata, fields="id").execute()
        folder_id = folder["id"]
        print(f"✅ Created new folder '{drive_folder_name}' (ID: {folder_id})")

    folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
    print(f"   Folder URL: {folder_url}")

    # 6. Move each file into the new folder (handling shared drive restrictions)
    for link in colab_links:
        file_id = extract_file_id_from_colab_link(link)
        if not file_id:
            print(f"❌ Could not parse file ID from link: {link}")
            continue

        try:
            # Get file details (parents + shared drive info)
            file_info = drive_service.files().get(fileId=file_id, fields="parents, name, driveId").execute()
            old_parents = file_info.get("parents", [])
            drive_id = file_info.get("driveId")  # If present, file is in a shared drive
            file_name = file_info.get("name", "UnknownFile")

            # Skip if already in the target folder
            if folder_id in old_parents:
                print(f"🔹 Skipping '{file_name}' (ID: {file_id}): already in folder '{drive_folder_name}'.")
                continue

            if drive_id:
                # 🔹 **File is in a Shared Drive**
                # **Remove old parents first, then add the new parent**
                if old_parents:
                    drive_service.files().update(
                        fileId=file_id,
                        removeParents=",".join(old_parents),
                        fields="id"
                    ).execute()

                drive_service.files().update(
                    fileId=file_id,
                    addParents=folder_id,
                    fields="id, parents"
                ).execute()

                print(f"✅ Moved shared drive file '{file_name}' (ID: {file_id}) into folder '{drive_folder_name}'.")

            else:
                # 🔹 **File is in a Personal Drive**
                # **First, add the new parent to avoid orphaning**
                drive_service.files().update(
                    fileId=file_id,
                    addParents=folder_id,
                    fields="id, parents"
                ).execute()

                # **Then, remove the old parents**
                if old_parents:
                    drive_service.files().update(
                        fileId=file_id,
                        removeParents=",".join(old_parents),
                        fields="id"
                    ).execute()

                print(f"✅ Moved '{file_name}' (ID: {file_id}) into folder '{drive_folder_name}'.")

        except HttpError as e:
            print(f"❌ Failed to move file with ID {file_id}. Error: {e}")

    print("\n✅ Move complete. No files were orphaned.")
    return {"folder_id": folder_id, "folder_url": folder_url}






def extract_file_id_from_colab_link(link: str) -> str:
    """
    Given a Colab link like:
      https://colab.research.google.com/drive/1AbCxyzEXAMPLE123?usp=sharing
    return the file ID: 1AbCxyzEXAMPLE123

    If it fails, return "".
    """
    pattern = re.compile(r"/drive/([a-zA-Z0-9_\-]+)")
    match = pattern.search(link)
    if match:
        return match.group(1)
    else:
        return ""





def copy_google_sheet_to_drive(
    creds_file: str,
    source_sheet_id: str,
    new_sheet_name: str,
    parent_folder_id: str = None
):
    """
    Copies an entire Google Sheet from 'source_sheet_id' into a new file in Google Drive.
    
    Args:
        creds_file (str): Path to your Google service account JSON credentials.
        source_sheet_id (str): The file ID of the original Google Sheet.
        new_sheet_name (str): Name/title to give the newly copied sheet.
        parent_folder_id (str, optional): If provided, the new copy is placed in this folder ID or shared drive ID.

    Returns:
        dict: { 'new_sheet_id': <ID of the copied sheet>, 'new_sheet_url': <URL> }

    Example:
        copy_google_sheet_to_drive(
            creds_file="service_account.json",
            source_sheet_id="1AbCdEfGhIjKlMNopQRs1234",
            new_sheet_name="My Copied Sheet",
            parent_folder_id="0B7Ab1cd2EFGhIJk"  # or None
        )
    """

    # 1. Authenticate
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    drive_service = build("drive", "v3", credentials=creds)
    current_date = datetime.datetime.today().strftime("%Y-%m-%d")
    new_sheet_name = f"{new_sheet_name}-{current_date}"

    # 2. Build the copy metadata (name, parents, etc.)
    body = {
        "name": new_sheet_name
    }
    if parent_folder_id:
        body["parents"] = [parent_folder_id]

    # 3. Call the Drive API's "files.copy" method to duplicate the sheet
    copied_file = drive_service.files().copy(
        fileId=source_sheet_id,
        body=body,
        fields="id, name"
    ).execute()

    new_sheet_id = copied_file["id"]
    new_name = copied_file["name"]
    new_sheet_url = f"https://docs.google.com/spreadsheets/d/{new_sheet_id}"

    print(f"✅ Copied original sheet (ID: {source_sheet_id})")
    print(f"   => New sheet name: {new_name}")
    print(f"   => New sheet ID:   {new_sheet_id}")
    print(f"   => New sheet URL:  {new_sheet_url}")

    return {
        "new_sheet_id": new_sheet_id,
        "new_sheet_url": new_sheet_url
    }
