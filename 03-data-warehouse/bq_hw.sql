-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `quantum-age-411318.ny_taxi.external_green_taxi_2022_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-kbc/nyc_2022_green_taxi_data/*.parquet']
);

-- Check external green trip data
SELECT * FROM quantum-age-411318.ny_taxi.external_green_taxi_2022_data limit 10;

-- Count records in external green trip data
SELECT COUNT(*) FROM quantum-age-411318.ny_taxi.external_green_taxi_2022_data;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE quantum-age-411318.ny_taxi.green_taxi_2022_data_non_paritioned AS
SELECT * FROM quantum-age-411318.ny_taxi.external_green_taxi_2022_data;

--Count the distinct number of PULocationIDs for the entire dataset on both the tables
SELECT COUNT(DISTINCT(PULocationID)) FROM `quantum-age-411318.ny_taxi.external_green_taxi_2022_data`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `quantum-age-411318.ny_taxi.green_taxi_2022_data_non_paritioned`;

--Count records with fare_amount of 0
SELECT COUNT(*) FROM `quantum-age-411318.ny_taxi.green_taxi_2022_data_non_paritioned` WHERE fare_amount = 0;

--Create partitioned and clustered table
CREATE OR REPLACE TABLE quantum-age-411318.ny_taxi.green_taxi_2022_data_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM quantum-age-411318.ny_taxi.external_green_taxi_2022_data;

--Retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `quantum-age-411318.ny_taxi.green_taxi_2022_data_non_paritioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT COUNT(DISTINCT(PULocationID)) 
FROM `quantum-age-411318.ny_taxi.green_taxi_2022_data_partitoned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Count records in external green trip data
SELECT COUNT(*) FROM quantum-age-411318.ny_taxi.green_taxi_2022_data_partitoned_clustered;
