from data_ingest.src.input_connectors.retrievers.gdrive_retriever import GDriveFile, GDriveRetriever, DownloadStatus
from data_ingest.src.gdrive_utils.auth import build_services
import os

def save_files_with_original_names(gdrive_files, destination_folder):
    drive_service = build_services(services=["drive"])["drive"]
    for gdrive_file in gdrive_files:
        if gdrive_file.status == DownloadStatus.OK and gdrive_file.content is not None:
            try:
                # Retrieve the file's metadata to get the original filename
                file_metadata = drive_service.files().get(fileId=gdrive_file.file_id, fields='name').execute()
                original_filename = file_metadata.get('name')

                # Ensure the destination folder exists
                os.makedirs(destination_folder, exist_ok=True)

                # Construct the full path where the file will be saved
                file_path = os.path.join(destination_folder, original_filename)

                # Save the file content to the specified destination folder with the original filename
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(gdrive_file.content)
                print(f"File saved as {file_path}")
            except Exception as e:
                print(f"Failed to save file {gdrive_file.file_id} to {destination_folder}: {e}")

if __name__ == "__main__":
    # Example usage
    file_links = ['https://colab.research.google.com/drive/1rFsLeQJ_jciPM72qmn9dzTYVE7t9-zfW']  # Replace with your actual file links or IDs
    destination_folder = 'temp'  # Specify your destination folder here
    retriever = GDriveRetriever(gdrive_files_uri=file_links)
    retrieved_files = retriever.retrieve()
    save_files_with_original_names(retrieved_files, destination_folder)