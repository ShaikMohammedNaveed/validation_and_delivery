from flask import Flask, render_template, request
import os

app = Flask(__name__)

from lwc_validator_online import download_and_validate_notebook

@app.route('/', methods=['GET', 'POST'])
def index():
    output = ""
    if request.method == 'POST':
        notebook_link = request.form.get('notebook_link')
        if notebook_link:
            output = download_and_validate_notebook(notebook_link)
    return render_template('index.html', output=output)

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))  # Render assigns PORT
    app.run(debug=False, host='0.0.0.0', port=port)