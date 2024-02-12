## create external table
CREATE OR REPLACE EXTERNAL TABLE dezc2024ds.green_taxi_data_2022_non_partitioned_external
OPTIONS(
  format = 'PARQUET',
  uris = ['gs://dezc2024_bucket/green_taxi_2022_parquet/*']
);

## create native table
CREATE OR REPLACE TABLE dezc2024ds.green_taxi_data_2022_non_partitioned_native
AS
SELECT *
FROM dezc2024ds.green_taxi_data_2022_non_partitioned_external;

## Total count of record
SELECT COUNT(*) AS total_records
FROM dezc2024ds.green_taxi_data_2022_non_partitioned_external;

## Total distinct number pulocation_id from external table
SELECT COUNT(DISTINCT pulocation_id) AS total_pulocation
FROM `dezc2024ds.green_taxi_data_2022_non_partitioned_external`;

## Total distinct number pulocation_id from native table
SELECT COUNT(DISTINCT pulocation_id) AS total_pulocation
FROM `dezc2024ds.green_taxi_data_2022_non_partitioned_native`;

## Total records fare_amount = 0
SELECT COUNT(*) AS total_records
FROM `dezc2024ds.green_taxi_data_2022_non_partitioned_external`
WHERE fare_amount = 0;

## Create table with clustering and partition
CREATE OR REPLACE TABLE dezc2024ds.green_taxi_data_2022_clustered_partitioned
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY pulocation_id 
AS
SELECT *
FROM dezc2024ds.green_taxi_data_2022_non_partitioned_native;

## Query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive) from native table
SELECT COUNT(DISTINCT (pulocation_id)) AS total
FROM dezc2024ds.green_taxi_data_2022_non_partitioned_native
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30';

## Query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive) from clustered partition table
SELECT COUNT(DISTINCT (pulocation_id)) AS total
FROM dezc2024ds.green_taxi_data_2022_clustered_partitioned
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30';
