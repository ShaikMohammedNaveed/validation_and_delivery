import re
import io
import os
import json
from io import StringIO
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
import nbformat
from lwc_validator import NotebookValidator

def download_and_validate_notebook(notebook_link: str) -> str:
    output_buffer = StringIO()

    try:
        # Extract file ID
        file_id_match = re.search(r'/drive/([a-zA-Z0-9_-]+)', notebook_link) or \
                        re.search(r'/file/d/([a-zA-Z0-9_-]+)', notebook_link) or \
                        re.search(r'fileId=([a-zA-Z0-9_-]+)', notebook_link)
        if not file_id_match:
            raise ValueError("Invalid notebook link format. Could not extract file ID.")
        file_id = file_id_match.group(1)

        # Load credentials from environment variable
        credentials_json = os.getenv("GOOGLE_CREDENTIALS")
        print(credentials_json)
        if not credentials_json:
            raise ValueError("Google credentials not found in environment variables")
        creds = service_account.Credentials.from_service_account_info(
            json.loads(credentials_json),
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        drive_service = build('drive', 'v3', credentials=creds)

        # Verify file access
        try:
            drive_service.files().get(fileId=file_id).execute()
        except Exception as e:
            if "403" in str(e):
                raise PermissionError("Permission denied: Share the notebook with the service account.")
            elif "404" in str(e):
                raise FileNotFoundError("Notebook not found.")
            else:
                raise Exception(f"Failed to verify notebook: {e}")

        # Download file
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            output_buffer.write(f"Download progress: {int(status.progress() * 100)}%\n")
        fh.seek(0)
        content = fh.read().decode("utf-8")

        # Parse notebook
        nb = nbformat.reads(content, as_version=4)
        markdown_cells = [cell['source'] for cell in nb.cells if cell['cell_type'] == 'markdown']
        markdown_content = "\n\n".join(markdown_cells)

        # Validate
        validator = NotebookValidator(markdown_content, f"{file_id}.ipynb")
        validator.validate_structure()

        if not validator.errors:
            output_buffer.write(f"✅ {file_id}.ipynb is valid.\n")
            output_buffer.write('-' * 40 + "\n")
        else:
            output_buffer.write(f"❌ Errors in {file_id}.ipynb:\n")
            for error in validator.errors:
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