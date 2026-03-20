# app/services/s3_service.py

import aioboto3
import os
from botocore.exceptions import ClientError

# One global session reused across requests
_session = aioboto3.Session()

async def get_csv_from_s3(bucket: str, key: str) -> bytes:
    """
    Fetch CSV file from S3 and return as BYTES
    """
    try:
        async with _session.client(
            "s3",
            region_name=os.getenv("AWS_REGION", "ap-south-1")
        ) as s3:
            response = await s3.get_object(Bucket=bucket, Key=key)
            return await response["Body"].read()

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