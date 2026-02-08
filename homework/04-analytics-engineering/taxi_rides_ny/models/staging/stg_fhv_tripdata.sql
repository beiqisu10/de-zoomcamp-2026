with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num as dispatching_base_num,
        pickup_datetime::timestamp as pickup_datetime,
        dropOff_datetime::timestamp as dropoff_datetime,
        PUlocationID::integer as pickup_location_id,
        DOlocationID::integer as dropoff_location_id,
        SR_Flag as store_and_fwd_flag,
        Affiliated_base_number as affiliated_base_number
    from source
    where dispatching_base_num is not null
)

select * from renamed
