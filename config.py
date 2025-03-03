import os
import boto3
from logger import setup_logger

logger = setup_logger(name="config.py")

S3_ENDPOINT = os.environ.get("S3_ENDPOINT_URL", "http://localhost:4566")
DYNAMODB_ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT_URL", "http://localhost:4566")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "CSVMetadata")
BUCKET_NAME = os.environ.get("S3_BUCKET", "csv-storage")

# Initialize boto3 clients/resources with Localstack endpoints
s3_client = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name="us-east-1")
dynamodb_resource = boto3.resource("dynamodb", endpoint_url=DYNAMODB_ENDPOINT)
table = dynamodb_resource.Table(TABLE_NAME)

logger.debug("Configuration of s3_client,dynamodb and table is done")
def create_s3_bucket(bucket_name):
    """
    Create a S3 bucket.
    """
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        logger.debug("S3 Bucket created")
    except Exception as e:
            logger.error("Error occurred while creating bucket",e)
            raise e
            
if __name__ == "__main__":
    create_s3_bucket(BUCKET_NAME)
    # print(dynamodb_resource)