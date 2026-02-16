with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        *
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num is null
)

select * from renamed
