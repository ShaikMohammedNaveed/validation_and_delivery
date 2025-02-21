from flask import Flask, render_template, request
import os

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
from lwc_validator.lwc_validator_endpoint import validate_lwc_notebook
from apex_validator.apex_validator_endpoint import validate_apex_notebook
from delivery_workflow.delivery_workflow import deliver_notebook

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/lwc-validation', methods=['GET', 'POST'])
def lwc_validation():
    output = ""
    if request.method == 'POST':
        notebook_link = request.form.get('notebook_link')
        if notebook_link:
            output = validate_lwc_notebook(notebook_link)
    return render_template('lwc_validation.html', output=output)

@app.route('/apex-validation', methods=['GET', 'POST'])
def apex_validation():
    output = ""
    if request.method == 'POST':
        notebook_link = request.form.get('notebook_link')
        if notebook_link:
            output = validate_apex_notebook(notebook_link)
    return render_template('apex_validation.html', output=output)

@app.route('/delivery', methods=['GET', 'POST'])
def delivery():
    module = request.form.get('module') if request.method == 'POST' else None
    output = ""
    if request.method == 'POST' and module in ['lwc', 'apex']:
        input_data = request.form.get('input_data')  # Drive folder or sheet link
        delivery_type = request.form.get('delivery_type', 'normal')
        emails = [email.strip() for email in request.form.get('emails', '').split(',')]  # Comma-separated emails
        process_type = request.form.get('process_type', 'drive')
        batch_name = request.form.get('batch_name', None)
        validate = request.form.get('validate', 'true').lower() == 'true'

        # Add configuration constants from form
        from delivery_workflow.config import settings  # Ensure this import works

        apex_config = {
            'apex_input_sheet_id': request.form.get('apex_input_sheet_id', settings.APEX_INPUT_SHEET_ID),
            'apex_input_sheet_name': request.form.get('apex_input_sheet_name', settings.APEX_INPUT_SHEET_NAME),
            'apex_task_link_column': request.form.get('apex_task_link_column', settings.APEX_TASK_LINK_COLUMN),
            'apex_output_dir': request.form.get('apex_output_dir', settings.APEX_OUTPUT_DIR),
            'apex_json_output_dir': request.form.get('apex_json_output_dir', settings.APEX_JSON_OUTPUT_DIR),
            'apex_gdrive_dir_folder_id_collabs': request.form.get('apex_gdrive_dir_folder_id_collabs', settings.APEX_GDRIVE_DIR_FOLDER_ID_COLLABS),
            'apex_google_drive_json_folder_id': request.form.get('apex_google_drive_json_folder_id', settings.APEX_GOOGLE_DRIVE_JSON_FOLDER_ID)
        }

        lwc_config = {
            'lwc_input_sheet_id': request.form.get('lwc_input_sheet_id', settings.LWC_INPUT_SHEET_ID),
            'lwc_input_sheet_name': request.form.get('lwc_input_sheet_name', settings.LWC_INPUT_SHEET_NAME),
            'lwc_task_link_column': request.form.get('lwc_task_link_column', settings.LWC_TASK_LINK_COLUMN),
            'lwc_output_dir': request.form.get('lwc_output_dir', settings.LWC_OUTPUT_DIR),
            'lwc_json_output_dir': request.form.get('lwc_json_output_dir', settings.LWC_JSON_OUTPUT_DIR),
            'lwc_gdrive_dir_folder_id_collabs': request.form.get('lwc_gdrive_dir_folder_id_collabs', settings.LWC_GDRIVE_DIR_FOLDER_ID_COLLABS),
            'lwc_google_drive_json_folder_id': request.form.get('lwc_google_drive_json_folder_id', settings.LWC_GOOGLE_DRIVE_JSON_FOLDER_ID)
        }

        json_file = request.files.get('json_file') if process_type == 'json' else None

        if input_data or json_file:
            output = deliver_notebook(module, input_data, delivery_type, emails, process_type, batch_name, validate, json_file)
    return render_template('delivery.html', module=module, output=output)

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(debug=True, host='0.0.0.0', port=port)  # Keeping debug=True for development