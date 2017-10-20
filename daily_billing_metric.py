import os
import json
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

    filename = "{}.json".format(context.function_name)
    s3_full_path = os.path.join(s3_folder, filename)

    last_processed_datapoint = get_last_processed_dp_from_s3(
        s3_full_path=s3_full_path, s3_client=s3_client)
    print("Last processed timestamp = {}".format(
        last_processed_datapoint))

    # get EstimatedCharges metrics
    sorted_datapoints = get_metric_stats(
        client=client, start_time=start_time,
        end_time=end_time, period=60 * 60,
        metric_name='EstimatedCharges'
    )

    # filter todays and yesterdays data points
    todays_datapoints = get_todays_datapoints(sorted_datapoints)
    yesterdays_last_datapoint = get_yesterdays_latest_datapoint(
        sorted_datapoints)
    print("yesterdays_last_datapoint = {}".format(yesterdays_last_datapoint))
    print("todays_datapoints = {}".format(todays_datapoints))
    yesterdays_update = None

    timestamp = end_time
    if not todays_datapoints:
        # seems like a new day
        # reset to 0
        print("No significant data from EstimatedCharges")
        status = put_daily_billing_metric(
            client=client, value=0, timestamp=timestamp)
        print("Metric reset to 0 status: {}".format(status))
        return

    new_latest_timestamp = todays_datapoints[0]['Timestamp'].strftime(
        "%Y-%m-%dT%H:%M:%S")
    if new_latest_timestamp == last_processed_datapoint.get('Timestamp'):
        # processed datapoint returned by EstimatedCharges
        # ignore current difference
        current_difference = 0
        print("DP already processed, setting difference to 0")
    elif len(todays_datapoints) == 1:
        # calculate difference from yesterday's dp and todays dp
        current_difference, yesterdays_update = \
            calculate_difference_from_yesterday(
                yesterdays_last_datapoint=yesterdays_last_datapoint,
                todays_first_datapoint=todays_datapoints[0]
            )
        print("0: {}".format(todays_datapoints[0]))
    else:
        # calculate difference
        current_difference = todays_datapoints[0]['Sum'] - \
            todays_datapoints[1]['Sum']
        current_difference = round(current_difference, 2)

        print("0: {}".format(todays_datapoints[0]))
        print("1: {}".format(todays_datapoints[1]))

    print("current_difference = {}".format(current_difference))

    # save custom metric for today
    print("***** Saving Daily Charge for today ******")
    status = save_custom_metric(
        client=client, start_time=start_time, end_time=end_time,
        current_difference=current_difference, timestamp=timestamp
    )
    print("Daily charge save status = {}".format(status))

    if yesterdays_update is not None:
        print("yesterdays_update = {}".format(yesterdays_update))
        yesterdays_last_timestamp = (end_time - timedelta(1)).replace(
            hour=23, minute=59, second=59
        )
        print("***** Saving Daily Charge for yesterday ******")
        # update custom metric for yesterday
        status = save_custom_metric(
            client=client, start_time=start_time,
            end_time=yesterdays_last_timestamp,
            current_difference=yesterdays_update,
            timestamp=yesterdays_last_timestamp
        )
        print("Daily charge save status for yesterday = {}".format(status))

    new_processed_dp = todays_datapoints[0]
    new_processed_dp['Timestamp'] = new_latest_timestamp
    s3_json = os.path.join("/tmp", s3_full_path)
    with open(s3_json, 'w') as jsonfile:
        jsonfile.write(json.dumps(new_processed_dp))
    upload_to_s3(file_name=s3_json, conn=s3_client)
    return True


def put_daily_billing_metric(client, value, timestamp):
    # Put custom metrics
    response = client.put_metric_data(
        MetricData=[
            {
                'MetricName': 'Daily Charge',
                'Dimensions': [
                    {
                        'Name': 'Currency',
                        'Value': 'USD'
                    },
                ],
                'Unit': 'None',
                'StatisticValues': {
                    'SampleCount': 1,
                    'Sum': value,
                    'Minimum': value,
                    'Maximum': value
                },
                'Timestamp': timestamp
            },
        ],
        Namespace='AWS/Billing'
    )
    try:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False
    except:
        return False


def get_metric_stats(
    client, start_time, end_time, period=1800,
    metric_name='Daily Charge'
):
    response = client.get_metric_statistics(
        Namespace='AWS/Billing', MetricName=metric_name,
        StartTime=start_time, EndTime=end_time, Period=period,
        Dimensions=[{'Name': 'Currency', 'Value': 'USD'}],
        Statistics=['Sum']
    )
    sorted_datapoints = sorted(
        response['Datapoints'], key=lambda k: k['Timestamp'],
        reverse=True
    )
    return sorted_datapoints


def upload_to_s3(file_name, path=None, conn=None):
    if not conn:
        s3_client = boto3.client('s3')
    else:
        s3_client = conn
    if path:
        full_path = os.path.join(path, file_name)
    else:
        full_path = file_name
    s3_client.upload_file(
        file_name, bucket_name, full_path.split('tmp/')[-1])
    print(
        'Uploaded file {0} to s3 !'.format(file_name.split('/')[-1]))


def download_file(file_name, conn=None):
    if not conn:
        s3_client = boto3.client('s3')
    else:
        s3_client = conn
    file_name = file_name.split('tmp/')[-1]

    file_name_only = file_name.split('/')[-1]
    file_name_only_len = len(file_name_only)
    file_name_len = len(file_name)
    file_dir = '/tmp/' + file_name[0:file_name_len - file_name_only_len]
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    try:
        s3_client.download_file(
            bucket_name, file_name, '/tmp/' + file_name)
        return '/tmp/' + file_name
    except:
        print('Cannot download file', file_name)
        return


def get_todays_datapoints(sorted_datapoints):
    todays_datapoints = [
        i for i in sorted_datapoints if i['Timestamp'].day ==
        datetime.today().day
    ]
    return todays_datapoints


def get_yesterdays_latest_datapoint(sorted_datapoints):
    yesterday_datapoints = [
        i for i in sorted_datapoints if i['Timestamp'].day ==
        (datetime.today() - timedelta(1)).day
    ]
    return yesterday_datapoints[0]


def calculate_difference_from_yesterday(
    yesterdays_last_datapoint, todays_first_datapoint
):
    # calculate price upto and from midnight
    current_difference = 0
    yesterdays_update = 0
    total_difference = todays_first_datapoint['Sum'] -\
        yesterdays_last_datapoint['Sum']

    # logic for month transition
    if total_difference < 0:
        yesterdays_update = 0
        current_difference = round(todays_first_datapoint['Sum'], 2)
        return current_difference, yesterdays_update

    print("*******")
    print("Yesterday and today: total diff = {}".format(total_difference))

    if not total_difference:
        return current_difference, yesterdays_update

    total_seconds = (
        todays_first_datapoint['Timestamp'] -
        yesterdays_last_datapoint['Timestamp']
    ).total_seconds()

    yesterday_midnight = yesterdays_last_datapoint['Timestamp'].replace(
        hour=23, minute=59, second=59
    )
    yesterdays_seconds_to_midnight = (
        yesterday_midnight - yesterdays_last_datapoint['Timestamp']
    ).total_seconds()

    todays_seconds_from_midnight = (
        todays_first_datapoint['Timestamp'] -
        (yesterday_midnight + timedelta(seconds=1))
    ).total_seconds()

    current_difference = float(total_difference) *\
        todays_seconds_from_midnight / total_seconds

    yesterdays_update = float(total_difference) *\
        yesterdays_seconds_to_midnight / total_seconds

    current_difference = round(current_difference, 2)
    yesterdays_update = round(yesterdays_update, 2)

    print("total_seconds = {}".format(total_seconds))
    print("yesterday_midnight = {}".format(yesterday_midnight))
    print("yesterdays_seconds_to_midnight = {}".format(
        yesterdays_seconds_to_midnight))
    print("todays_seconds_from_midnight = {}".format(
        todays_seconds_from_midnight))
    print("current_difference = {}".format(current_difference))
    print("yesterdays_update = {}".format(yesterdays_update))
    print("*******")

    return current_difference, yesterdays_update


def save_custom_metric(
    client, start_time, end_time, current_difference, timestamp
):
    # get existing metric data
    dailycharge_datapoints = get_metric_stats(
        client=client, start_time=start_time,
        end_time=end_time, period=1800,
        metric_name='Daily Charge'
    )
    if dailycharge_datapoints:
        prev_saved_value = dailycharge_datapoints[0]['Sum']
        print("Dailycharge_datapoints = {}".format(dailycharge_datapoints))
        print("Saved daily charge value {}".format(prev_saved_value))
        new_value = current_difference + prev_saved_value
    else:
        print("No daily charge data")
        new_value = current_difference

    print("Saving new daily charge value {}".format(new_value))
    status = put_daily_billing_metric(client, new_value, timestamp)
    return status


def get_last_processed_dp_from_s3(s3_full_path, s3_client):
    # get last processed datapoint from s3 file
    s3_json = download_file(file_name=s3_full_path, conn=s3_client)
    if not s3_json:
        last_processed_datapoint = {}
    else:
        with open(s3_json) as jsonfile:
            last_processed_datapoint = json.load(jsonfile)
    return last_processed_datapoint
