from flask import Flask, request, jsonify
from config import s3_client, table, BUCKET_NAME
from metadata_extractor import CSVMetadataProcessor
from logger import setup_logger
logger = setup_logger(name=__name__)
app = Flask(__name__)


processor = CSVMetadataProcessor(table)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    try:
        file_content = file.read()
        if len(file_content) > processor.MAX_FILE_SIZE:
            return jsonify({"error": "File size exceeds 10MB limit"}), 400

        # Upload the file to the S3 bucket.
        s3_client.put_object(Bucket=BUCKET_NAME, Key=file.filename, Body=file_content)

        # Process the CSV to extract metadata and store it in the database.
        metadata = processor.process_file(file_content, file.filename)

        # Return the metadata as a JSON response.
        return jsonify(metadata), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
