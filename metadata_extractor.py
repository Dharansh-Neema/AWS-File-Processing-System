import io
import pandas as pd
from datetime import datetime

class CSVMetadataProcessor:
    """
    Class responsible for processing CSV files:
      - Extract metadata (row/column counts, column names, file size, timestamp)
      - Store metadata in a DynamoDB table.
    """

    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB limit

    def __init__(self, table):
        """
        Initialize with a DynamoDB table instance.
        """
        self.table = table

    def process_file(self, file_content, filename):
        """
        Extract metadata from the CSV file, store it in the database, and return the metadata.

        Parameters:
            file_content (bytes): The CSV file content.
            filename (str): The name of the file.

        Returns:
            dict: Extracted metadata.
        """
        file_size = len(file_content)
        if file_size > self.MAX_FILE_SIZE:
            raise ValueError("File size exceeds 10MB limit.")

        # Parse CSV using pandas
        df = pd.read_csv(io.BytesIO(file_content))
        metadata = {
            "filename": filename,
            "upload_timestamp": datetime.utcnow().isoformat(),
            "file_size_bytes": file_size,
            "row_count": df.shape[0],
            "column_count": df.shape[1],
            "column_names": df.columns.tolist()
        }

        # Store metadata in the DynamoDB table
        self.table.put_item(Item=metadata)

        return metadata
