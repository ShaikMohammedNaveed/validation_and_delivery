import os
import json
import datetime
import shutil
import zipfile
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Function to split JSONL into individual JSON files
def split_jsonl_to_json(input_file, output_directory):
    """
    Reads a JSONL file and saves each JSON object as an individual .json file.
    
    Args:
    input_file (str): Path to the .jsonl file.
    output_directory (str): Directory where individual .json files will be saved.
    """
    try:
        # Ensure the output directory exists
        os.makedirs(output_directory, exist_ok=True)

        with open(input_file, 'r', encoding='utf-8') as jsonl_file:
            for index, line in enumerate(jsonl_file):
                try:
                    # Parse the JSON object
                    json_object = json.loads(line.strip())

                    # Safely retrieve file_id from JSON object
                    file_id = json_object.get('metadata', {}).get('data', {}).get('file_id', f"unknown_{index}")
                    
                    # Define the output file name
                    output_file = os.path.join(output_directory, f"{file_id}.json")

                    # Write the JSON object to a file
                    with open(output_file, 'w', encoding='utf-8') as json_file:
                        json.dump(json_object, json_file, ensure_ascii=False, indent=4)
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON on line {index + 1}: {e}")
        print(f"Saved {index + 1} JSON files to {output_directory}.")
        return 'success'
    except Exception as e:
        print(f"An error occurred: {e}")
        return 'failure'


import json
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

def split_jsonl_to_sheet(input_file, spreadsheet_id, sheet_name, creds_file):
    """
    Reads a JSONL file and writes specific fields to a Google Sheet.
    
    Columns: file_id, collab_links, name_of_violations, customer_review, customer_comment

    Steps:
      1) Clear the target sheet (A-E).
      2) Write a header row.
      3) Parse each JSON line, gather data, append rows.
      4) Write them to the sheet.
    """
    # 1. Authenticate with Google Sheets
    creds = Credentials.from_service_account_file(creds_file)
    service = build('sheets', 'v4', credentials=creds)

    # 2. Clear existing data in columns A-E
    clear_range = f"'{sheet_name}'!A:E"
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=clear_range
    ).execute()
    print(f"Cleared '{sheet_name}' (columns A–E).")

    # We'll store rows here (including header) before writing to the sheet
    rows_to_write = []

    # 3. Add header row first
    header_row = [
        "file_id",
        "collab_links",
        "name_of_violations",
        "customer_review",
        "customer_comment"
    ]
    rows_to_write.append(header_row)

    try:
        with open(input_file, 'r', encoding='utf-8') as jsonl_file:
            for index, line in enumerate(jsonl_file, start=1):
                line = line.strip()
                if not line:
                    continue  # Skip empty lines

                try:
                    # Parse the JSON object from each line
                    json_object = json.loads(line)

                    # Safely retrieve file_id
                    file_id = (
                        json_object
                        .get('metadata', {})
                        .get('data', {})
                        .get('file_id', f"unknown_{index}")
                    )

                    # Extract original_uri for collab_links
                    collab_links = (
                        json_object
                        .get('metadata', {})
                        .get('data', {})
                        .get('original_uri', "")
                    )

                    # Deduplicated list of issues
                    # "issues" might be a list or a JSON string
                    issues_raw = (
                        json_object
                        .get('data', {})
                        .get('issues', [])
                    )
                    distinct_violations = set()

                    # Convert issues_raw into a Python list (if it's a JSON string)
                    if isinstance(issues_raw, str):
                        cleaned = issues_raw.strip("```json").strip('```').strip()
                        try:
                            issues_list = json.loads(cleaned)
                        except (json.JSONDecodeError, TypeError):
                            issues_list = []
                    elif isinstance(issues_raw, list):
                        issues_list = issues_raw
                    else:
                        issues_list = []

                    # Gather distinct violation names
                    for issue in issues_list:
                        if isinstance(issue, dict):
                            violation_name = issue.get("value")
                            if violation_name:
                                distinct_violations.add(violation_name)

                    name_of_violations = ",".join(sorted(distinct_violations))

                    # We'll leave "customer_review" and "customer_comment" blank
                    row = [
                        file_id,              # A
                        collab_links,         # B
                        name_of_violations,   # C
                        "",                   # D (customer_review)
                        ""                    # E (customer_comment)
                    ]

                    rows_to_write.append(row)

                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON on line {index}: {e}")
        
        # 4. Update all rows (header + data) to the Google Sheet in one shot
        if len(rows_to_write) > 1:  # 1 is just the header
            range_name = f"'{sheet_name}'!A1"
            body = {"values": rows_to_write}

            response = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",  # or "USER_ENTERED"
                body=body
            ).execute()

            updated_cells = response.get('updatedCells', 0)
            print(f"✅ Wrote {len(rows_to_write)-1} data row(s) + 1 header row = {updated_cells} cells updated in '{sheet_name}'.")
        else:
            print("No valid JSON rows found to write.")

        return "success"
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return "failure"



def zip_folder_with_timestamp(folder_path: str) -> str:
    """
    Zips the contents of a folder with a timestamp appended to the name and deletes the original folder.

    Args:
        folder_path: Path to the folder to be zipped.

    Returns:
        The path to the created zip file.
    """
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Folder '{folder_path}' does not exist.")

    timestamp = datetime.datetime.now().strftime('%d-%m-%Y')
    zip_file_name = f"{os.path.basename(folder_path)}-{timestamp}.zip"
    zip_file_path = os.path.join(os.path.dirname(folder_path), zip_file_name)

    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)

    # Delete the original folder
    shutil.rmtree(folder_path)
    print(f"Folder '{folder_path}' has been zipped and deleted.")

    return zip_file_path



# def update_google_sheet(credentials_path, spreadsheet_id, sheet_name, input_file):
#     """
#     Reads a JSONL file, extracts relevant notebook metadata, and updates a Google Sheet.

#     Args:
#         credentials_path (str): Path to the credentials.json file.
#         spreadsheet_id (str): ID of the Google Sheet.
#         sheet_name (str): Name of the sheet/tab.
#         input_file (str): Path to the JSONL file.

#     Returns:
#         str: 'success' or 'failure'
#     """

#     try:
#         # Authenticate with Google Sheets API
#         creds = Credentials.from_service_account_file(credentials_path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
#         client = gspread.authorize(creds)
#         sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

#         # Clear old records in the Google Sheet before inserting new data
#         sheet.clear()

#         # Set the header row
#         headers = ["Colab Task Link", "Metadata Type", "Tags", "User Query", "Number of Turns", "Drive Folder Link"]
#         sheet.append_row(headers)

#         # Process JSONL file
#         rows = []
#         with open(input_file, 'r', encoding='utf-8') as jsonl_file:
#             for index, line in enumerate(jsonl_file):
#                 try:
#                     # Parse JSON object
#                     json_object = json.loads(line.strip())

#                     # Extract required fields safely
#                     metadata = json_object.get("metadata", {}).get("data", {})
#                     content_metadata = json_object.get("data", {}).get("content_metadata", {})

#                     colab_task_link = metadata.get("original_uri", "N/A")
#                     metadata_type = f"{content_metadata.get('category', 'N/A')} < {content_metadata.get('subcategory', 'N/A')}"
#                     tags = ", ".join(content_metadata.get("tags", []))
#                     number_of_turns = json_object.get("data", {}).get("number_of_turns", "N/A")
#                     drive_folder_link = content_metadata.get("screenshot", "N/A")

#                     # Extract all user queries and format them as bullet points (-)
#                     user_queries = [
#                         f"- {message['content']}" for message in json_object.get("data", {}).get("messages", [])
#                         if message.get("role") == "User"
#                     ]
#                     user_query_text = "\n".join(user_queries) if user_queries else "N/A"

#                     # Append extracted data to rows
#                     rows.append([colab_task_link, metadata_type, tags, user_query_text, number_of_turns, drive_folder_link])

#                 except json.JSONDecodeError as e:
#                     print(f"Skipping invalid JSON on line {index + 1}: {e}")

#         # Update Google Sheet with extracted data
#         if rows:
#             sheet.append_rows(rows)

#         print(f"✅ Successfully updated {len(rows)} rows in the Google Sheet.")
#         return "success"

#     except Exception as e:
#         print(f"❌ An error occurred: {e}")
#         return "failure"


import json
import gspread
from google.oauth2.service_account import Credentials
from gspread_formatting import *

def update_google_sheet(credentials_path, spreadsheet_id, sheet_name, input_file):
    """
    Reads a JSONL file, extracts relevant notebook metadata, and updates a Google Sheet.

    Args:
        credentials_path (str): Path to the credentials.json file.
        spreadsheet_id (str): ID of the Google Sheet.
        sheet_name (str): Name of the sheet/tab.
        input_file (str): Path to the JSONL file.

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

        # Set the header row with new columns
        headers = [
            "Colab Task Link", "Metadata Type", "Tags", "User Query",
            "Number of Turns", "Drive Folder Link", "Completion Status",
            "Customer Review Status", "Customer Review Comments", "Categories"
        ]
        sheet.append_row(headers)

        # Process JSONL file
        rows = []
        with open(input_file, 'r', encoding='utf-8') as jsonl_file:
            for index, line in enumerate(jsonl_file):
                try:
                    # Parse JSON object
                    json_object = json.loads(line.strip())

                    # Extract required fields safely
                    metadata = json_object.get("metadata", {}).get("data", {})
                    content_metadata = json_object.get("data", {}).get("content_metadata", {})

                    colab_task_link = metadata.get("original_uri", "N/A")
                    metadata_type = f"{content_metadata.get('category', 'N/A')} < {content_metadata.get('subcategory', 'N/A')}"
                    tags = ", ".join(content_metadata.get("tags", []))
                    number_of_turns = json_object.get("data", {}).get("number_of_turns", "N/A")
                    drive_folder_link = content_metadata.get("screenshot", "N/A")

                    # Extract all user queries and format them as bullet points (-)
                    user_queries = [
                        f"- {message['content']}" for message in json_object.get("data", {}).get("messages", [])
                        if message.get("role") == "User"
                    ]
                    user_query_text = "\n".join(user_queries) if user_queries else "N/A"

                    # Default values for new columns
                    completion_status = "Delivered"  # Default dropdown value
                    customer_review_status = ""  # Dropdown (Accepted, Need Rework)
                    customer_review_comments = ""
                    categories = ""  # Dropdown (Various categories)

                    # Append extracted data to rows
                    rows.append([
                        colab_task_link, metadata_type, tags, user_query_text,
                        number_of_turns, drive_folder_link, completion_status,
                        customer_review_status, customer_review_comments, categories
                    ])

                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON on line {index + 1}: {e}")

        # Update Google Sheet with extracted data
        if rows:
            sheet.append_rows(rows)

        # Apply dropdowns and formatting
        apply_dropdown_and_formatting(client, spreadsheet_id, sheet_name, len(rows))

        print(f"✅ Successfully updated {len(rows)} rows in the Google Sheet.")
        return "success"

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return "failure"

def apply_dropdown_and_formatting(client, spreadsheet_id, sheet_name, num_rows):
    """
    Applies dropdowns and conditional formatting to the Google Sheet.

    Args:
        client: Authenticated Google Sheets client.
        spreadsheet_id (str): ID of the Google Sheet.
        sheet_name (str): Name of the sheet/tab.
        num_rows (int): Number of rows with data.

    """
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    # Define the dropdown values
    completion_status_values = ["Delivered", "Unclaimed", "Done", "In progress"]
    customer_review_status_values = ["Accepted", "Need Rework"]
    categories_values = [
        "Too Much Technical", "Natural", "Verbose", "Repeated Rephrasing",
        "Unqualified", "Concise", "Good"
    ]

    # Get the range for dropdowns
    last_row = num_rows + 1  # Including header row

    # Apply dropdowns
    set_data_validation(sheet, f"G2:G{last_row}", completion_status_values)  # Completion Status
    set_data_validation(sheet, f"H2:H{last_row}", customer_review_status_values)  # Customer Review Status
    set_data_validation(sheet, f"J2:J{last_row}", categories_values)  # Categories

    # Apply conditional formatting for Customer Review Status
    rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f"H2:H{last_row}", sheet)],
        booleanRule=BooleanRule(
            condition=BooleanCondition('TEXT_EQ', ['Accepted']),
            format=CellFormat(backgroundColor=Color(0.56, 0.93, 0.56))  # Green for Accepted
        )
    )
    rule2 = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f"H2:H{last_row}", sheet)],
        booleanRule=BooleanRule(
            condition=BooleanCondition('TEXT_EQ', ['Need Rework']),
            format=CellFormat(backgroundColor=Color(0.96, 0.26, 0.21))  # Red for Need Rework
        )
    )

    # Apply formatting rules
    rules = get_conditional_format_rules(sheet)
    rules.clear()  # Clear existing rules
    rules.append(rule)
    rules.append(rule2)
    rules.save()

def set_data_validation(sheet, cell_range, values):
    """
    Sets dropdown values for a given range in a Google Sheet.

    Args:
        sheet: Google Sheet object.
        cell_range (str): The A1 notation range where dropdown should be applied.
        values (list): List of values for the dropdown.
    """
    rule = DataValidationRule(
        condition=BooleanCondition('ONE_OF_LIST', values),
        showCustomUi=True,
        strict=True
    )
    set_data_validation_for_cell_range(sheet, cell_range, rule)
