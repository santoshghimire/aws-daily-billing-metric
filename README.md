# aws-daily-billing-metric
A metric to calculate the daily billing amount in AWS CloudWatch

This is a script to be run with AWS Lambda. The lambda should have three environment variables:
REGION: AWS Region for CloudWatch.
S3_BUCKET: S3 bucket name where temp data file is stored.
S3_FOLDER: S3 folder name where temp data file is stored.

This script needs to be scheduled to run once every hour with CloudWatch Schedule. So, it will create one data point in the metric in an hour.

## Description
1. It pulls stats from the `AWS/Billing` metric provided by AWS.
2. The daily billing value reported should reset everyday.
3. It creates a new metric named `Daily Charge`.
4. The currency is in USD>