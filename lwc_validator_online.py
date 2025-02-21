import re
import io
from io import StringIO, BytesIO
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
import nbformat
from lwc_validator import NotebookValidator  # Assuming this is a custom validator class
from constants import GOOGLE_API_CREDENTIALS_PATH

def download_and_validate_notebook(notebook_link: str) -> str:
    """
    Downloads a Jupyter Notebook from a Google Drive link using credentials,
    extracts its markdown content in-memory, and validates it using NotebookValidator.
    Returns the validation output as a string.

    Parameters:
        notebook_link (str): The shareable Google Drive link to the notebook.
    """
    output_buffer = StringIO()

    try:
        # Step 1: Extract the file ID from the notebook link (works for Colab and Drive links).
        file_id_match = re.search(r'/drive/([a-zA-Z0-9_-]+)', notebook_link) or \
                        re.search(r'/file/d/([a-zA-Z0-9_-]+)', notebook_link) or \
                        re.search(r'fileId=([a-zA-Z0-9_-]+)', notebook_link)

        if not file_id_match:
            raise ValueError("Invalid notebook link format. Could not extract file ID.")
        file_id = file_id_match.group(1)

        # Step 2: Set up credentials and create the Drive API service.
        SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
        try:
            creds = service_account.Credentials.from_service_account_file(
                GOOGLE_API_CREDENTIALS_PATH, scopes=SCOPES
            )
            drive_service = build('drive', 'v3', credentials=creds)
        except Exception as e:
            raise Exception(f"Failed to set up Google Drive API credentials: {e}")

        # Step 3: Verify file accessibility before downloading.
        try:
            drive_service.files().get(fileId=file_id).execute()
        except Exception as e:
            if "HttpError" in str(e) and "403" in str(e):
                raise PermissionError(
                    "Permission denied: Ensure the notebook is shared publicly or accessible with the provided credentials."
                )
            elif "HttpError" in str(e) and "404" in str(e):
                raise FileNotFoundError("Notebook not found. Please check the link or necessary permissions and try again")
            else:
                raise Exception(f"Failed to verify notebook accessibility: {e}")

        # Step 4: Download the file content into memory.
        try:
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                output_buffer.write(f"Download progress: {int(status.progress() * 100)}%\n")
            fh.seek(0)
            content = fh.read().decode("utf-8")
        except Exception as e:
            raise Exception(f"Failed to download notebook content: {e}")

        # Step 5: Parse the notebook content and extract Markdown cells.
        try:
            nb = nbformat.reads(content, as_version=4)
        except Exception as e:
            raise Exception(f"Failed to parse the notebook content: {e}")

        markdown_cells = [cell['source'] for cell in nb.cells if cell['cell_type'] == 'markdown']
        markdown_content = "\n\n".join(markdown_cells)

        # Step 6: Validate the notebook content.
        validator = NotebookValidator(markdown_content, f"{file_id}.ipynb")
        validator.validate_structure()

        # Capture validation output.
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
        output_buffer.write(f"❌ An unexpected error occurred: {str(e)}\n")

    return output_buffer.getvalue()