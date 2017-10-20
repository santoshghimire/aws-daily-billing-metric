import os
import boto3
from datetime import datetime, timezone, timedelta

# read environment variables
cw_region = os.environ.get('REGION', 'us-east-1')
bucket_name = os.environ.get('S3_BUCKET', 'fb-lambda-storage')
s3_folder = os.environ.get('S3_FOLDER', 'daily-billing')


def lambda_handler(event, context):
    """
    This function is first run when the lambda is triggered.
    """
    client = boto3.client('cloudwatch', region_name=cw_region)
    print(client)
    # Get local tz, start and end time
    local_timezone = datetime.now(timezone.utc).astimezone().tzinfo
    end_time = datetime.now().replace(tzinfo=local_timezone)

    start_time = end_time - timedelta(1)
    start_time = datetime(
        year=start_time.year, month=start_time.month, day=start_time.day,
        hour=0, minute=0, second=0
    )
    print("start_time = {}".format(start_time))
    print("end_time = {}".format(end_time))

    # get s3 client
    s3_client = boto3.client('s3')
    print(s3_client)
    filename = "{}.json".format(context.function_name)
    s3_full_path = os.path.join(s3_folder, filename)
    print(s3_full_path)
    return True
