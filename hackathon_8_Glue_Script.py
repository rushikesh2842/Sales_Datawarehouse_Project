import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum, coalesce, lit
import boto3
from botocore.exceptions import ClientError

## @params: [JOB_NAME, BASE_OUTPUT_PATH]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'curated_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from Glue catalog table
table_name = "maze"
database_name = "intl_sales_dev"
data_frame = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name).toDF()


# Group data by region and perform aggregations
grouped_df = data_frame.groupBy("region") \
        .agg(sum("unitssold"),
          sum("totalrevenue"),
          sum("totalcost"),
          sum("totalprofit"))

# Get today's date for partitioning
today = spark.sql("SELECT current_date()").first()[0]

# Define base output path from arguments
curated_path = args['curated_path']

# Define year, month, and day partitions
year_partition = f"year={today.year}"
month_partition = f"month={today.month}"
day_partition = f"day={today.day}"

# Concatenate base output path and partitions
output_path = f"{curated_path}/{year_partition}/{month_partition}/{day_partition}/"
print(output_path)
# Extract bucket name from output path
bucket_name = output_path.split('/')[2]

# Remove existing files within the day's partition (if any)
s3 = boto3.client('s3')
try:
  objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=output_path.split(bucket_name + '/')[1])
  if 'Contents' in objects:
    for obj in objects['Contents']:
      s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
except ClientError as e:
  if e.response['Error']['Code'] == '404':
    pass # Ignore if folder doesn't exist
  else:
    raise e # Re-raise other exceptions

# Save as Parquet with compression, partitioned by year, month, and day
grouped_df.write.parquet(output_path, compression="snappy")

job.commit()
