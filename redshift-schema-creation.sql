CREATE SCHEMA etl_pipeline_cluster_staging;
CREATE SCHEMA etl_pipeline_cluster_core;
CREATE SCHEMA etl_pipeline_cluster_proc;

CREATE TABLE etl_pipeline_cluster_staging.sales_summary (
    region VARCHAR(255),
    unitssold BIGINT,
    totalrevenue FLOAT8,
    totalcost FLOAT8,
    totalprofit FLOAT8
);

CREATE TABLE etl_pipeline_cluster_core.sales_summary (
    region VARCHAR(255),
    unitssold BIGINT,
    totalrevenue FLOAT8,
    totalcost FLOAT8,
    totalprofit FLOAT8
);