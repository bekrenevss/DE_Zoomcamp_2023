-- Create external table
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamphomework.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dezoomcamphomework_bucket/fhv_tripdata_2019-*.csv.gz']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dezoomcamphomework.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM dezoomcamphomework.nytaxi.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE dezoomcamphomework.nytaxi.yellow_tripdata_partitioned
PARTITION BY
  DATE(pickup_datetime) as 
SELECT * FROM dezoomcamphomework.nytaxi.external_yellow_tripdata;

-- Question 1
SELECT COUNT(*)
from dezoomcamphomework.nytaxi.external_yellow_tripdata;

-- Question 2. External table - 0 B
SELECT count(distinct Affiliated_base_number)
from dezoomcamphomework.nytaxi.external_yellow_tripdata;

-- Question 2. Materialized table - 317.94 MB
SELECT count(distinct Affiliated_base_number)
from dezoomcamphomework.nytaxi.yellow_tripdata_non_partitioned;

-- Question 3 717748
SELECT COUNT(*)
FROM dezoomcamphomework.nytaxi.yellow_tripdata_non_partitioned
WHERE
  PUlocationID IS NULL
  AND DOlocationID IS NULL;

-- Question 4
-- Partition by pickup_datetime Cluster on affiliated_base_number

-- Question 5 Non partitioned 647.87 MB
SELECT count(distinct Affiliated_base_number)
FROM dezoomcamphomework.nytaxi.yellow_tripdata_non_partitioned
WHERE
  pickup_datetime >= CAST('03/01/2019' AS TIMESTAMP FORMAT 'MM/DD/YYYY')
  and pickup_datetime < CAST('04/01/2019' AS TIMESTAMP FORMAT 'MM/DD/YYYY');


-- Question 5 Partitioned 23.5 MB
SELECT count(distinct Affiliated_base_number)
FROM dezoomcamphomework.nytaxi.yellow_tripdata_partitioned
WHERE
  pickup_datetime >= CAST('03/01/2019' AS TIMESTAMP FORMAT 'MM/DD/YYYY')
  and pickup_datetime < CAST('04/01/2019' AS TIMESTAMP FORMAT 'MM/DD/YYYY');