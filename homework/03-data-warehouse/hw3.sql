CREATE OR REPLACE EXTERNAL TABLE `project-64505.nytaxi.external_yellow_tripdata` 
OPTIONS (
  format = 'parquet',
  uris = ['gs://dezoomcamp_hw3_2026_64505/yellow_tripdata_2024-0*.parquet'] 
);

CREATE OR REPLACE TABLE project-64505.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM project-64505.nytaxi.external_yellow_tripdata;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids
FROM project-64505.nytaxi.external_yellow_tripdata;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids
FROM project-64505.nytaxi.yellow_tripdata_non_partitioned;

SELECT count(1) FROM project-64505.nytaxi.yellow_tripdata_non_partitioned where fare_amount=0;

CREATE OR REPLACE TABLE project-64505.nytaxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM project-64505.nytaxi.external_yellow_tripdata;
