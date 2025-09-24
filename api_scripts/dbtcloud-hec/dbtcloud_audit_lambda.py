# -----------------------------------------------------------------------------
# dbtcloud_audit_lambda.py
#
# This script is designed to fetch audit logs from dbt Cloud and send them to
# Splunk via the HTTP Event Collector (HEC). It is intended to be run as an AWS
# Lambda function, but can be adapted for other environments.
#
# NOTE: This Lambda function must be given permission to read secrets from AWS Secrets Manager.
#   See https://docs.aws.amazon.com/lambda/latest/dg/with-secrets-manager.html for more information.
#
# The Lambda function must be configured to run on a schedule.
#
# Before using, you must set the following configuration variables below:
#   - DBT_CLOUD_BASE_URL: The base URL for your dbt Cloud instance (see docs).
#   - ACCOUNT_ID: Your dbt Cloud account ID.
#   - SPLUNK_HEC_URL: The full URL for your Splunk HEC endpoint.
#   - SECRET_NAME: The name of the AWS Secrets Manager secret containing your
#                  dbt Cloud API token and Splunk HEC token.
#                  The secret name is looks for by default is "dbt-Cloud-Splunk-HEC",
#                  with key-value pairs for dbt-cloud-api-token and splunk-hec-token.
#   - REGION_NAME: The AWS region where your secret is stored.
#   - MINUTES_LOOKBACK: How far back to look for audit logs (in minutes).
#
# 
#  See the dbt Cloud and Splunk documentation for more details on obtaining
# these values.
# -----------------------------------------------------------------------------

import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime, timedelta
import urllib.request
import urllib.parse

# base_url: Enter your base url here, you can read how to determine this at https://docs.getdbt.com/docs/cloud/about-cloud/access-regions-ip-addresses
DBT_CLOUD_BASE_URL = ""
# get your account id from dbt Cloud url after signing in
# https://<dbt_cloud_base_url>/dashboard/<account_id>/...
ACCOUNT_ID = 
# minutes to lookback in dbt Cloud audit logs
# align this to how regularly you want to run this lambda
MINUTES_LOOKBACK = 1440

# your splunk hec url
SPLUNK_HEC_URL = ""

# your secret name in aws secrets manager
SECRET_NAME = "dbt-Cloud-Splunk-HEC"
# your region name
REGION_NAME = "us-east-1"


def fetch_audit_logs(account_id, api_token, start_date=None, end_date=None):
    """
    Fetches audit logs from the dbt Cloud API.

    This function handles pagination and yields pages of logs.
    """
    base_url = f"{DBT_CLOUD_BASE_URL}/api/v3/accounts/{account_id}/audit-logs/"
    headers = {
        'Authorization': f'Token {api_token}',
        'Content-Type': 'application/json'
    }
    
    params = {'limit': 100, 'offset': 0}
    if start_date:
        params['logged_at_start'] = start_date.isoformat()
    if end_date:
        params['logged_at_end'] = end_date.isoformat()

    while True:
        try:
            query_string = urllib.parse.urlencode(params)
            url = f"{base_url}?{query_string}"
            req = urllib.request.Request(url, headers=headers, method='GET')
            with urllib.request.urlopen(req) as response:
                response_data = response.read().decode('utf-8')
                data = json.loads(response_data)
            
            logs = data.get('data', [])
            
            if not logs:
                break
            
            yield logs
            
            pagination = data.get('extra', {}).get('pagination', {})
            total_count = pagination.get('total_count', 0)
            print(f"Fetching next page... {params['offset'] + params['limit']} of {total_count}")
            
            if params['offset'] + params['limit'] >= total_count:
                break
            
            params['offset'] += params['limit']
            
        except urllib.error.HTTPError as e:
            print(f"Error fetching logs: {e.code} - {e.reason}")
            break
        except urllib.error.URLError as e:
            print(f"Error fetching logs: {e.reason}")
            break
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")
            break


def log_to_splunk_hec(hec_url, hec_token, logs):
    """
    Sends logs to Splunk HEC.
    """
    headers = {
        'Authorization': f'Splunk {hec_token}',
        'Content-Type': 'application/json'
    }
    
    for log_entry in logs:
        created_at_str = log_entry.get('created_at')
        if created_at_str:
            # Try parsing with the new format first (with microseconds and timezone offset)
            try:
                dt_object = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S.%f%z')
                created_at_str = int(dt_object.timestamp())
            except ValueError:
                # If new format fails, try the old ISO format (with 'Z' for UTC)
                try:
                    if created_at_str.endswith('Z'):
                        dt_object = datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%SZ')
                    else:
                        # This case might cover other ISO formats without 'Z' but with an offset
                        dt_object = datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%S%z')
                    created_at_str = int(dt_object.timestamp())
                except ValueError as e:
                    print(f"Warning: Could not parse timestamp {created_at_str} with either format. Error: {e}")
        
        payload = {
            "sourcetype": "_json",
            "host": DBT_CLOUD_BASE_URL,
            "source": "dbtcloud_audit",
            "event": log_entry,
            "time": created_at_str
        }
        
        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(hec_url, data=data, headers=headers, method='POST')
            with urllib.request.urlopen(req) as response:
                response.read()
            print(f"Successfully sent log to Splunk HEC: {log_entry.get('id')}")
        except urllib.error.HTTPError as e:
            print(f"Error sending log to Splunk HEC: {e.code} - {e.reason}")
            break
        except urllib.error.URLError as e:
            print(f"Error sending log to Splunk HEC: {e.reason}")
            break


def get_secrets():

    secret_name = SECRET_NAME
    region_name = REGION_NAME

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])

    return(secret)

def lambda_handler(event, context):
    """
    Main function to retrieve dbt Cloud audit logs and send to Splunk HEC.
    """
    # Use global variables
    account_id = ACCOUNT_ID
    splunk_hec_url = SPLUNK_HEC_URL
    minutes_lookback = MINUTES_LOOKBACK

    secrets = get_secrets()
    api_token = secrets['dbt-cloud-api-token']
    splunk_hec_token = secrets['splunk-hec-token']

    if not api_token:
        print("Error: API token must be provided via DBT_CLOUD_API_TOKEN environment variable or set as a global variable.")
        return {'statusCode': 500, 'body': json.dumps('API token not configured.')}

    end_date = datetime.now()
    start_date = end_date - timedelta(minutes=minutes_lookback)

    print(f"Fetching audit logs for account {account_id} from {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')} (last {minutes_lookback} minutes)...")
    
    logs_iterator = fetch_audit_logs(account_id, api_token, start_date, end_date)
    
    all_logs = [log for page in logs_iterator for log in page]

    if not all_logs:
        print("No audit logs found for the specified period.")
    else:
        log_to_splunk_hec(splunk_hec_url, splunk_hec_token, all_logs)

    return {
        'statusCode': 200,
        'body': json.dumps('dbt Cloud audit logs processed and sent to Splunk HEC.')
    }
