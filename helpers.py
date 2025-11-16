# helpers.py
import re
from datetime import datetime
from botocore.exceptions import ClientError

def find_latest_file_in_s3(s3_client, bucket, prefix, file_pattern):
    """Finn siste fil i en S3-mappe basert på dato i filnavnet."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            return None

        latest_file, latest_date = None, None
        for obj in response['Contents']:
            key = obj['Key']
            match = re.search(file_pattern, key)
            if match:
                file_date = datetime.strptime(match.group(1), '%d-%m-%Y')
                if latest_date is None or file_date > latest_date:
                    latest_date, latest_file = file_date, key
        return latest_file

    except ClientError as e:
        print(f"Kunne ikke liste objekter i S3: {e}")
        return None
