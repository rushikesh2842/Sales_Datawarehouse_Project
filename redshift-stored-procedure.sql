CREATE OR REPLACE PROCEDURE etl_pipeline_cluster_proc.elt_sales_summary(bucketname character varying(200), manifestfilepath character varying(500))
LANGUAGE plpgsql
AS $$
begin
  -- Purge staging table
  truncate etl_pipeline_cluster_staging.sales_summary;

  -- Copy data from S3 bucket using full manifest path directly
  execute 'copy etl_pipeline_cluster_staging.sales_summary 
  from ' || quote_literal('s3://' || bucketname || '/' || manifestfilepath) ||
  ' iam_role default format as parquet manifest;';

  -- Insert transformed data into target table
  insert into etl_pipeline_cluster_core.sales_summary
  select * from etl_pipeline_cluster_staging.sales_summary;

end;
$$