import boto3
import os
from botocore.config import Config
from botocore.exceptions import ClientError

# ✅ Optimized config (connection pooling)
config = Config(
    max_pool_connections=50
)

# ✅ Create ONE global S3 client (reused across requests)
s3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION", "ap-south-1"),
    config=config
)

def get_csv_from_s3(bucket: str, key: str) -> bytes:
    """
    Fetch CSV file from S3 and return as BYTES (faster than string)
    """
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()  # ✅ return bytes

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "NoSuchKey":
            raise Exception(f"Symbol not found in S3: {key}")
        elif error_code == "AccessDenied":
            raise Exception("Access denied to S3 bucket")
        else:
            raise Exception(f"S3 error: {str(e)}")

    except Exception as e:
        raise Exception(f"Unexpected error: {str(e)}")