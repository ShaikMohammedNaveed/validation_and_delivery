import re
import io
import os
import json
from io import StringIO
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
import nbformat
from dotenv import load_dotenv
load_dotenv()

from apex_validator.apex_validator import (  # Adjust the import based on your file name
    detect_notebook_type,
    validate_apex_metadata_formatting,
    validate_apex_code_block,
    validate_dynamic_issues,
    validate_issue_count,
    validate_notebook_structure,
    validate_content_formatting,
    validate_static_bold_formatting,
    validate_issue_block_headers,
    load_notebook,
)

def validate_apex_notebook(notebook_link: str) -> str:
    """
    Downloads a Jupyter Notebook from a Google Drive link, validates it as an Apex notebook,
    and returns the validation output as a string.

    Parameters:
        notebook_link (str): The shareable Google Drive or Colab link to the notebook.
    """

    output_buffer = StringIO()

    try:
        # Step 1: Extract the file ID from the notebook link
        file_id_match = re.search(r'/drive/([a-zA-Z0-9_-]+)', notebook_link) or \
                        re.search(r'/file/d/([a-zA-Z0-9_-]+)', notebook_link) or \
                        re.search(r'fileId=([a-zA-Z0-9_-]+)', notebook_link)
        if not file_id_match:
            raise ValueError("Invalid notebook link format. Could not extract file ID.")
        file_id = file_id_match.group(1)

        # Step 2: Set up credentials from environment variable
        credentials_json = os.getenv("GOOGLE_CREDENTIALS")
        if not credentials_json:
            raise ValueError("Google credentials not found in environment variables")
        try:
            creds = service_account.Credentials.from_service_account_info(
                json.loads(credentials_json),
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
            drive_service = build('drive', 'v3', credentials=creds)
        except Exception as e:
            raise Exception(f"Failed to set up Google Drive API credentials: {e}")

        # Step 3: Verify file accessibility
        try:
            drive_service.files().get(fileId=file_id).execute()
        except Exception as e:
            if "403" in str(e):
                raise PermissionError("Permission denied: Ensure the notebook is shared with the service account.")
            elif "404" in str(e):
                raise FileNotFoundError("Notebook not found. Check the link.")
            else:
                raise Exception(f"Failed to verify notebook accessibility: {e}")

        # Step 4: Download the file content into memory
        try:
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                output_buffer.write(f"Download progress: {int(status.progress() * 100)}%\n")
            fh.seek(0)
            # Attempt to decode with UTF-8 first, fallback to binary if needed
            try:
                content = fh.read().decode("utf-8")
            except UnicodeDecodeError:
                # If UTF-8 fails, treat as binary and assume JSON structure
                content = fh.read().decode("utf-8", errors="replace")
                # Attempt to clean up potential Colab-specific formatting
                content = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F-\x9F]', '', content)  # Remove control characters
        except Exception as e:
            raise Exception(f"Failed to download notebook content: {e}")

        # Step 5: Parse the notebook content
        try:
            # Ensure content is valid JSON before parsing
            try:
                json_data = json.loads(content)
                # Handle Colab-specific or malformed JSON by extracting the 'cells' if nested
                if isinstance(json_data, dict) and "cells" in json_data:
                    cells = json_data["cells"]
                elif isinstance(json_data, list) and all(isinstance(item, dict) for item in json_data):
                    cells = json_data  # Assume direct list of cells
                else:
                    raise ValueError("Unexpected JSON structure in notebook.")
            except json.JSONDecodeError as je:
                raise Exception(f"Invalid JSON in notebook: {str(je)}")

            # Convert cells to nbformat structure if not already
            if not isinstance(cells, list) or not all(isinstance(cell, dict) for cell in cells):
                raise ValueError("Invalid cell structure in notebook.")
            
            notebook_data = {"cells": cells, "nbformat": 4, "nbformat_minor": 5}  # Minimal nbformat structure
            cells = notebook_data["cells"]

            # Preserve raw Markdown without normalization
            for cell in cells:
                if cell.get("cell_type") == "markdown":
                    cell_source = cell.get("source", [])
                    # Join lines as-is, preserving exact Markdown (including **)
                    cell["source"] = [line.strip() for line in cell_source if line]  # Keep original lines, remove empty

        except Exception as e:
            raise Exception(f"Failed to parse the notebook content: {e}")
        
        # Step 6: Detect notebook type and validate
        notebook_type = detect_notebook_type(cells)
        if notebook_type != "apex":
            raise ValueError("Notebook is not detected as an Apex notebook.")

        validation_errors = []
        metadata_errors = validate_apex_metadata_formatting(cells)
        apex_code_errors = validate_apex_code_block(cells)
        dynamic_issue_errors = validate_dynamic_issues(cells)
        issue_count_errors = validate_issue_count(cells, notebook_type)
        structure_errors = validate_notebook_structure(cells, notebook_type)
        content_errors = validate_content_formatting(cells, notebook_type)
        static_bold_errors = validate_static_bold_formatting(cells, notebook_type)
        issue_block_errors = validate_issue_block_headers(cells)

        # Collect all errors
        validation_errors.extend(metadata_errors)
        validation_errors.extend(apex_code_errors)
        validation_errors.extend(dynamic_issue_errors)
        validation_errors.extend(issue_count_errors)
        validation_errors.extend(structure_errors)
        validation_errors.extend(content_errors)
        # validation_errors.extend(static_bold_errors)
        validation_errors.extend(issue_block_errors)

        # Step 7: Format and return the output
        if not validation_errors:
            output_buffer.write(f"✅ {file_id}.ipynb is a valid Apex notebook.\n")
            output_buffer.write('-' * 40 + "\n")
        else:
            output_buffer.write(f"❌ Errors in Apex notebook {file_id}.ipynb:\n")
            for error in validation_errors:
                output_buffer.write(f"- {error}\n" + "-" * 40 + "\n")

    except ValueError as ve:
        output_buffer.write(f"❌ {str(ve)}\n")
    except PermissionError as pe:
        output_buffer.write(f"❌ {str(pe)}\n")
    except FileNotFoundError as fnfe:
        output_buffer.write(f"❌ {str(fnfe)}\n")
    except Exception as e:
        output_buffer.write(f"❌ Unexpected error: {str(e)}\n")

    return output_buffer.getvalue()

# notebook_link = "https://colab.research.google.com/drive/18jiTZITB3dg6XO6d_XCt9xDzMU-jD8qR?usp=sharing"

# print(validate_apex_notebook(notebook_link))