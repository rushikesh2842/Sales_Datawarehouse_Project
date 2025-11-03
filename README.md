## Sales Datawarehouse Project
This project was created in one of the hackathons from https://www.linkedin.com/company/dataengineeringhub/ hosted by https://www.linkedin.com/in/mentorsachin/.  In this project, I

•	Configured EventBridge to trigger a state machine on each S3 object creation, initiating a fully serverless data pipeline.

•	Orchestrated AWS Glue PySpark jobs for data transformation and manifest generation, converting raw data to Parquet format and preparing it for loading.

•	Integrated Amazon Redshift stored procedures for automated upsert operations from staging to core tables using manifest paths.

•	Enabled seamless access to processed data for data scientists via S3 Data Lake, and for data analysts via Redshift tables for large-scale SQL and analytics workloads
