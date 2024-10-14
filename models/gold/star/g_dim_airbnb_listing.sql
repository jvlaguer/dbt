{{
    config(
        unique_key='listing_id',
        alias='dim_airbnb_listing'
    )
}}

with

source  as (

    SELECT * FROM {{ ref('lga_airbnb_listing') }}

),

cleaned as (
    SELECT
    listing_id,
    host_id,
    scraped_date::timestamp,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    has_availability,
    case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from::timestamp end as valid_from,
    dbt_valid_to::timestamp as valid_to
    FROM source
),

unknown as (
    select
        0 as listing_id,
        0 as host_id,
        '1900-01-01'::timestamp as scraped_date,
        'unknown' as listing_neighbourhood,
        'unknown' as property_type,
        'unknown' as room_type,
        0 as accommodates,
        'unknown' as has_availability,
        '1900-01-01'::timestamp  as valid_from,
        null::timestamp as valid_to

)

SELECT * FROM unknown
UNION ALL
SELECT * FROM cleaned