import json
import os
import requests
import boto3
import time

def lambda_handler(event, context):
    # Environment variables
    s3_bucket = os.environ['S3_BUCKET']
    api_key = os.environ['PARTNER_API_KEY']
    cf_distribution_id = os.environ['CF_DISTRIBUTION_ID']

    # Files to invalidate in CloudFront
    files_to_invalidate = []

    # URLs to fetch data from
    urls = [
        "https://seats.aero/partnerapi/search?origin_airport=ABX&destination_airport=SYD,BNE,MEL&cabin=economy&take=1000",
        "https://seats.aero/partnerapi/search?origin_airport=SYD&destination_airport=ABX&cabin=economy&take=1000",
        "https://seats.aero/partnerapi/search?origin_airport=MEL&destination_airport=ABX&cabin=economy&take=1000",
        "https://seats.aero/partnerapi/search?origin_airport=BNE&destination_airport=ABX&cabin=economy&take=1000"
    ]

    for url in urls:
        data = fetch_data(url, api_key)
        if data:
            file_name = get_file_name(url)
            save_to_s3(s3_bucket, file_name, data)
            # Add file path to the invalidation list
            files_to_invalidate.append('/' + file_name)
        else:
            print(f"Failed to fetch data from {url}")

    if files_to_invalidate:
        invalidate_cf_files(cf_distribution_id, files_to_invalidate)

    return {
        'statusCode': 200,
        'body': json.dumps('Data fetched and saved to S3, and CloudFront invalidation initiated successfully')
    }

def fetch_data(url, api_key):
    headers = {
        "accept": "application/json",
        "Partner-Authorization": api_key
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data from {url}: {response.status_code}")
        return None

def save_to_s3(bucket, file_name, data):
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=bucket, Key=file_name, Body=json.dumps(data))
    except Exception as e:
        print(f"Error saving data to S3: {str(e)}")
        raise

def get_file_name(url):
    # Extract parameters from URL to create a meaningful file name
    parts = url.split('?')[1].split('&')
    params = {p.split('=')[0]: p.split('=')[1] for p in parts}
    origin = params.get('origin_airport')
    dest = params.get('destination_airport').replace(',', '_')
    return f"flight_data_{origin}_to_{dest}.json"

def invalidate_cf_files(cf_distribution_id, files):
    client = boto3.client('cloudfront')
    try:
        # Create CloudFront invalidation
        response = client.create_invalidation(
            DistributionId=cf_distribution_id,
            InvalidationBatch={
                'Paths': {
                    'Quantity': len(files),
                    'Items': files
                },
                'CallerReference': str(time.time())  # Unique value for each invalidation
            }
        )
        print("CloudFront Invalidation Response:", response)
    except Exception as e:
        print(f"Error creating CloudFront invalidation: {str(e)}")
        raise
