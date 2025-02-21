from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv
import re
import logging
import pathlib

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    # Helper functions for validation
    @staticmethod
    def is_valid_google_id(id_str: str) -> bool:
        if not id_str or not isinstance(id_str, str):
            return False
        return bool(re.match(r'^[a-zA-Z0-9-_]+$', id_str))

    @staticmethod
    def is_valid_dir_path(path: str) -> bool:
        if not path or not isinstance(path, str):
            return False
        return bool(re.match(r'^[a-zA-Z0-9_/\\-]+$', path))

    # Apex Configuration Constants (with fallback defaults and validation)
    APEX_INPUT_SHEET_ID: str = os.getenv(
        "APEX_INPUT_SHEET_ID",
        "1mFUh8Yhhqd3mtT3fJr8qu8gPuGVX8kGJ-UbM_PY3AJQ"  # Default Google Sheet ID
    )
    APEX_INPUT_SHEET_NAME: str = os.getenv("APEX_INPUT_SHEET_NAME", "apex_batch")
    APEX_TASK_LINK_COLUMN: str = os.getenv("APEX_TASK_LINK_COLUMN", "colab_task_link")
    APEX_OUTPUT_DIR: str = os.getenv("APEX_OUTPUT_DIR", "output/apex")
    APEX_JSON_OUTPUT_DIR: str = os.getenv("APEX_JSON_OUTPUT_DIR", "output/apex/json_files")
    APEX_GOOGLE_DRIVE_DIR: str = os.getenv(
        "APEX_GOOGLE_DRIVE_DIR",
        "https://drive.google.com/drive/folders/1yYbKxZYAY7URNYwmUnuM2BhiJEnY2qjf"
    )
    APEX_GDRIVE_DIR_FOLDER_ID_COLLABS: str = os.getenv(
        "APEX_GDRIVE_DIR_FOLDER_ID_COLLABS",
        "1yYbKxZYAY7URNYwmUnuM2BhiJEnY2qjf"
    )
    APEX_GOOGLE_DRIVE_JSON_FOLDER_ID: str = os.getenv(
        "APEX_GOOGLE_DRIVE_JSON_FOLDER_ID",
        "1yYbKxZYAY7URNYwmUnuM2BhiJEnY2qjf"
    )
    apex_email_list: list[str] = []  # Default empty, overridden by form or environment

    # LWC Configuration Constants (with fallback defaults and validation)
    LWC_INPUT_SHEET_ID: str = os.getenv(
        "LWC_INPUT_SHEET_ID",
        "1XEs_8KeOAkMp5Nk83v1RzncoXdA7NYLWE8op35vJ41A"  # Default Google Sheet ID
    )
    LWC_INPUT_SHEET_NAME: str = os.getenv("LWC_INPUT_SHEET_NAME", "lwc_batch")
    LWC_TASK_LINK_COLUMN: str = os.getenv("LWC_TASK_LINK_COLUMN", "colab_task_link")
    LWC_OUTPUT_DIR: str = os.getenv("LWC_OUTPUT_DIR", "output/lwc")
    LWC_ERROR_OUTPUT_DIR: str = os.getenv("LWC_ERROR_OUTPUT_DIR", "output/lwc/error")
    LWC_JSON_OUTPUT_DIR: str = os.getenv("LWC_JSON_OUTPUT_DIR", "output/lwc/json_files")
    LWC_GOOGLE_DRIVE_DIR: str = os.getenv(
        "LWC_GOOGLE_DRIVE_DIR",
        "https://drive.google.com/drive/folders/1vKgRppT7Lq9J5QK_Z7VOFWxKLQygTVfn"
    )
    LWC_GDRIVE_DIR_FOLDER_ID_COLLABS: str = os.getenv(
        "LWC_GDRIVE_DIR_FOLDER_ID_COLLABS",
        "1vKgRppT7Lq9J5QK_Z7VOFWxKLQygTVfn"
    )
    LWC_GOOGLE_DRIVE_JSON_FOLDER_ID: str = os.getenv(
        "LWC_GOOGLE_DRIVE_JSON_FOLDER_ID",
        "1vKgRppT7Lq9J5QK_Z7VOFWxKLQygTVfn"
    )
    lwc_email_list: list[str] = []  # Default empty, overridden by form or environment

    # Validate and create directories
    def validate_and_create_dirs(self):
        for dir_path in [
            self.APEX_OUTPUT_DIR,
            self.APEX_JSON_OUTPUT_DIR,
            self.LWC_OUTPUT_DIR,
            self.LWC_ERROR_OUTPUT_DIR,
            self.LWC_JSON_OUTPUT_DIR
        ]:
            if dir_path and self.is_valid_dir_path(dir_path):
                try:
                    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
                    logger.debug(f"Created directory or confirmed existence: {dir_path}")
                except Exception as e:
                    logger.error(f"Failed to create directory {dir_path}: {str(e)}")
                    raise ValueError(f"Cannot create or access directory {dir_path}: {str(e)}")
            else:
                logger.warning(f"Invalid directory path skipped: {dir_path}")

    # Validate Google IDs
    def validate_google_ids(self):
        google_ids = [
            self.APEX_INPUT_SHEET_ID,
            self.APEX_GDRIVE_DIR_FOLDER_ID_COLLABS,
            self.APEX_GOOGLE_DRIVE_JSON_FOLDER_ID,
            self.LWC_INPUT_SHEET_ID,
            self.LWC_GDRIVE_DIR_FOLDER_ID_COLLABS,
            self.LWC_GOOGLE_DRIVE_JSON_FOLDER_ID
        ]
        for id_str in google_ids:
            if id_str and not self.is_valid_google_id(id_str):
                logger.error(f"Invalid Google ID: {id_str}")
                raise ValueError(f"Invalid Google ID format: {id_str}. Must match [a-zA-Z0-9-_]+")

    # Initialize and validate settings
    def __init__(self):
        super().__init__()  # Call parent constructor
        self.validate_google_ids()
        self.validate_and_create_dirs()

        # Allow dynamic overriding of email lists from environment variables (comma-separated)
        self.apex_email_list = [
            email.strip() for email in os.getenv("APEX_EMAIL_LIST", "").split(",")
            if email.strip()
        ]
        self.lwc_email_list = [
            email.strip() for email in os.getenv("LWC_EMAIL_LIST", "").split(",")
            if email.strip()
        ]
        logger.debug(f"Initialized Apex email list: {self.apex_email_list}")
        logger.debug(f"Initialized LWC email list: {self.lwc_email_list}")

    # Method to update settings dynamically (e.g., from form)
    def update_from_form(self, module: str, form_data: dict):
        if module == "apex":
            config = {
                "input_sheet_id": form_data.get("apex_input_sheet_id", self.APEX_INPUT_SHEET_ID),
                "input_sheet_name": form_data.get("apex_input_sheet_name", self.APEX_INPUT_SHEET_NAME),
                "task_link_column": form_data.get("apex_task_link_column", self.APEX_TASK_LINK_COLUMN),
                "output_dir": form_data.get("apex_output_dir", self.APEX_OUTPUT_DIR),
                "json_output_dir": form_data.get("apex_json_output_dir", self.APEX_JSON_OUTPUT_DIR),
                "gdrive_dir_folder_id_collabs": form_data.get("apex_gdrive_dir_folder_id_collabs", self.APEX_GDRIVE_DIR_FOLDER_ID_COLLABS),
                "google_drive_json_folder_id": form_data.get("apex_google_drive_json_folder_id", self.APEX_GOOGLE_DRIVE_JSON_FOLDER_ID),
                "email_list": [
                    email.strip() for email in form_data.get("emails", "").split(",")
                    if email.strip()
                ] if "emails" in form_data else self.apex_email_list
            }
        elif module == "lwc":
            config = {
                "input_sheet_id": form_data.get("lwc_input_sheet_id", self.LWC_INPUT_SHEET_ID),
                "input_sheet_name": form_data.get("lwc_input_sheet_name", self.LWC_INPUT_SHEET_NAME),
                "task_link_column": form_data.get("lwc_task_link_column", self.LWC_TASK_LINK_COLUMN),
                "output_dir": form_data.get("lwc_output_dir", self.LWC_OUTPUT_DIR),
                "json_output_dir": form_data.get("lwc_json_output_dir", self.LWC_JSON_OUTPUT_DIR),
                "gdrive_dir_folder_id_collabs": form_data.get("lwc_gdrive_dir_folder_id_collabs", self.LWC_GDRIVE_DIR_FOLDER_ID_COLLABS),
                "google_drive_json_folder_id": form_data.get("lwc_google_drive_json_folder_id", self.LWC_GOOGLE_DRIVE_JSON_FOLDER_ID),
                "email_list": [
                    email.strip() for email in form_data.get("emails", "").split(",")
                    if email.strip()
                ] if "emails" in form_data else self.lwc_email_list
            }
        else:
            raise ValueError("Invalid module specified")

        # Validate updated config
        if not self.is_valid_google_id(config["input_sheet_id"]):
            raise ValueError(f"Invalid {module.upper()} Input Sheet ID: {config['input_sheet_id']}")
        if not config["input_sheet_name"] or not isinstance(config["input_sheet_name"], str):
            raise ValueError(f"Invalid {module.upper()} Sheet Name: {config['input_sheet_name']}")
        if not config["task_link_column"] or not isinstance(config["task_link_column"], str):
            raise ValueError(f"Invalid {module.upper()} Task Link Column: {config['task_link_column']}")
        if not self.is_valid_dir_path(config["output_dir"]):
            raise ValueError(f"Invalid {module.upper()} Output Dir: {config['output_dir']}")
        if not self.is_valid_dir_path(config["json_output_dir"]):
            raise ValueError(f"Invalid {module.upper()} JSON Output Dir: {config['json_output_dir']}")
        if not self.is_valid_google_id(config["gdrive_dir_folder_id_collabs"]):
            raise ValueError(f"Invalid {module.upper()} Collab Folder ID: {config['gdrive_dir_folder_id_collabs']}")
        if not self.is_valid_google_id(config["google_drive_json_folder_id"]):
            raise ValueError(f"Invalid {module.upper()} JSON Folder ID: {config['google_drive_json_folder_id']}")

        return config

settings = Settings()