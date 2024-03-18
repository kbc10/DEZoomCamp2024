# Q1
CREATE MATERIALIZED VIEW between_taxi_zones AS
    SELECT  
        taxi_zone_pu.Zone as pickup_zone, 
        taxi_zone_do.Zone as dropoff_zone, 
        AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
        MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time,
        MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time
    FROM 
        trip_data
    JOIN 
        taxi_zone as taxi_zone_pu ON trip_data.PULocationID = taxi_zone_pu.location_id
    JOIN 
        taxi_zone as taxi_zone_do ON trip_data.DOLocationID = taxi_zone_do.location_id
    GROUP BY
        taxi_zone_pu.Zone, taxi_zone_do.Zone
    ORDER BY avg_trip_time DESC
    LIMIT 10;

#Q2
CREATE MATERIALIZED VIEW between_taxi_zones_with_count AS
    SELECT  
        taxi_zone.Zone as pickup_zone, 
        taxi_zone_1.Zone as dropoff_zone, 
        AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
        MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time,
        MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
        count(*) AS trip_count
    FROM 
        trip_data
    JOIN 
        taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
    JOIN 
        taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id
    GROUP BY
        taxi_zone.Zone, taxi_zone_1.Zone
    ORDER BY avg_trip_time DESC
    LIMIT 10;

#Q3
WITH t AS (
        SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time
        FROM trip_data
    )
    SELECT 
        taxi_zone.Zone as pickup_zone, 
        count (*) as trip_count
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.PULocationID = taxi_zone.location_id
    WHERE 
        trip_data.tpep_pickup_datetime > t.latest_pickup_time - interval '17 hours'
    GROUP BY
        taxi_zone.Zone
    ORDER BY
        trip_count DESC
    LIMIT 3;

