import boto3
import json
import datetime
import sys
from awsglue.utils import getResolvedOptions

# Get Glue job parameters
args = getResolvedOptions(sys.argv, ['bucket', 'prefix', 'tasktoken'])

bucketname = args['bucket']
prefix = args['prefix']
sf_tasktoken = args['tasktoken']

s3 = boto3.client('s3')

# List parquet objects
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucketname, Prefix=prefix)

entries = []

for page in pages:
    for obj in page.get('Contents', []):
        key = obj['Key']
        if key.endswith('.parquet'):
            head = s3.head_object(Bucket=bucketname, Key=key)

            entries.append({
                "url": f"s3://{bucketname}/{key}",
                "mandatory": True,
                "meta": {
                    "content_length": head['ContentLength'],
                    "content_md5": head['ETag'].strip('"'),
                    "last_modified": head['LastModified'].strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            })

if not entries:
    raise Exception("No parquet files found under prefix!")

# Create Redshift-compatible manifest JSON
manifest = {"entries": entries}

print(json.dumps(manifest, indent=2))

# Generate manifest file key
current_datetime = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
manifest_key = f"manifest/files_list_{current_datetime}.manifest"

# Upload manifest JSON to S3
s3.put_object(
    Body=json.dumps(manifest),
    Bucket=bucketname,
    Key=manifest_key,
    ContentType='application/json'
)

print(f"Manifest written to s3://{bucketname}/{manifest_key}")

# Notify Step Functions
sf_client = boto3.client('stepfunctions')
sf_client.send_task_success(
    taskToken=sf_tasktoken,
    output=json.dumps({
        "manifestfilepath": manifest_key,
        "bucketname": bucketname
    })
)

print("Step Functions task success sent with manifestfilepath and bucketname:", manifest_key, bucketname)