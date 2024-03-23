{{ config(materialized='view') }}

with tripdata as 
(
    select * from {{ source('staging', 'fhv_tripdata') }}
    where EXTRACT(year FROM pickup_datetime) = 2019
)
select

    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    dispatching_base_num,
    sr_flag,
    affiliated_base_number

from tripdata


