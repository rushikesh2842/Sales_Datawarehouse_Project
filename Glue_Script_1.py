import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Assuming your data has headers
rdd = spark.sparkContext.textFile("s3://dehlive-sales-030798167757-us-east-1/raw/sales/input/sales_rds_excercise.csv")

# Skip the header row if it exists
header = rdd.first()
if header.startswith("uuid,Country"):
  rdd = rdd.filter(lambda line: line != header)

# Split the data into rows
rdd = rdd.map(lambda line: line.split(","))

# Create a tuple of (Region, (UnitsSold, TotalRevenue, TotalCost, TotalProfit))
grouped_data = rdd.map(lambda row: (row[6], (int(row[8]), float(row[11]), float(row[12]), float(row[13]))))

# Group data by region and sum the aggregations
region_aggregates = grouped_data.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3]))

# Optionally, collect the results as a list for immediate display
results = region_aggregates.collect()

region_aggregatesDF = region_aggregates.toDF()


# Flatten the struct column
flattened_df = region_aggregatesDF.select(col("_1").alias("Region"),
                                          col("_2._1").alias("UnitsSold"),
                                          col("_2._2").alias("TotalRevenue"),
                                          col("_2._3").alias("TotalCost"),
                                          col("_2._4").alias("TotalProfit"))



flattened_df.write.parquet('s3://dehlive-sales-030798167757-us-east-1/raw/sales/parquetresult/')
## stop the current session 

job.commit()
